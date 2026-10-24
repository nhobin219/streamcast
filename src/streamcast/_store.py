"""`Store` and `Transaction` — the CALLER's database, for an outbox and an inbox.

Exactly-once across a process boundary needs the process's own state, the rows
it publishes and the place it has read to, to commit together. They can only
do that in the database the state already lives in, so streamcast keeps its
outbox and inbox there: four tables beside the caller's own, written in the
caller's transaction (`_outbox`, `_inbox`). Nothing here talks to the broker.

    store = await streamcast.SqliteStore.open("state.db")
    store = await streamcast.PostgresStore.connect(dsn)     # streamcast[postgres]

    async with store.transaction() as tx:     # BEGIN ... COMMIT; ROLLBACK on a raise
        await tx.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (10, 7))

**The protocols are public**, so another database is an adapter, not a change
here. The caller writes SQL in its driver's own placeholder style (`?` for
SQLite, `%s` for psycopg); the library writes its own with `?` and `sql`
translates it for the dialect.

**The transaction is an explicit object, never a reentrant depth.** A depth
counted per connection lets another task's block join one that suspended, and
commit or roll back work it never saw. A `tx` passed by value makes "which
transaction" a fact the code states.

**One transaction at a time per store.** A store holds one connection and a
lock, so a second `transaction()` waits for the first to end. A process that
wants transactions in parallel opens more stores. A task that asks for a
second while it holds the first is refused with `RuntimeError` rather than
left waiting on itself for ever: the case is a row's processing transaction
still open (`Inbox.commit` ends it) when something else, an outbox's
`flush` say, opens one on the same store.

**A lost connection inside a block is the caller's to see**, never a silent
re-run: a context manager cannot run its block again, and a block that did
work, lost its connection and ran again would do the work twice. Everything
the block wrote rolls back with it.

**A COMMIT whose answer was lost may have committed**, so every write the
library makes is safe to find already done: a received position only moves
forward, an inbox insert of an offset already there inserts nothing, a delete
removes rows that may be gone, and an enqueue takes its seq inside the
transaction it commits with.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, TypeVar

from streamcast._tasks import join

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
    from contextlib import AbstractAsyncContextManager
    from os import PathLike

Dialect = Literal["sqlite", "postgres"]
T = TypeVar("T")

SCHEMA_VERSION: Final = 1
"""The version of the library's tables this build writes. A store whose
tables are newer is refused at open rather than written in an older shape."""


class Transaction(Protocol):
    """One open transaction of a `Store`: the library's statements and the
    caller's own, committed together."""

    @property
    def store(self) -> Store: ...

    async def execute(self, sql: str, params: Sequence[object] = ()) -> int:
        """Run a statement; the number of rows it touched."""
        ...

    async def fetch(
        self, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[Any, ...]]:
        """Run a query; its rows."""
        ...

    def on_commit(self, callback: Callable[[], None]) -> None:
        """Call `callback` after COMMIT returns, never if it rolls back."""
        ...


class Store(Protocol):
    """A database the caller owns, with the library's four tables in it."""

    @property
    def dialect(self) -> Dialect: ...

    def transaction(self) -> AbstractAsyncContextManager[Transaction]: ...

    async def close(self) -> None: ...


def sql(dialect: Dialect, statement: str) -> str:
    """The library's `?`-placeholder statement, in `dialect`'s style.

    Only the library's own statements pass through here, and none has a `?`
    anywhere but as a placeholder.
    """
    return statement if dialect == "sqlite" else statement.replace("?", "%s")


def store_ddl(dialect: Dialect) -> list[str]:
    """The statements that create the library's tables, for a role that may
    not run DDL itself: an administrator runs these once, and the store is
    opened with `create=False`.

    The stream leads every key and every column is prefixed, so the tables sit
    beside the caller's without a name to collide. Rows are JSON text on both
    databases, rather than JSONB on Postgres, so the bytes the inbox's receive
    window counts are the bytes stored; times are integer UTC microseconds,
    the unit `streamcast_ts` already has.
    """
    integer = "INTEGER" if dialect == "sqlite" else "BIGINT"
    return [
        # Pending rows. A row the broker acknowledged is deleted; nothing is
        # marked, and no offset is kept: `landed` answers from the reply.
        f"""CREATE TABLE IF NOT EXISTS streamcast_outbox (
            streamcast_stream TEXT NOT NULL,
            streamcast_publisher_id TEXT NOT NULL,
            streamcast_seq {integer} NOT NULL,
            streamcast_row TEXT NOT NULL,
            streamcast_enqueued_at {integer} NOT NULL,
            PRIMARY KEY (streamcast_stream, streamcast_publisher_id, streamcast_seq))""",
        # The gapless counter, and the stream's id bound at first connect.
        f"""CREATE TABLE IF NOT EXISTS streamcast_publisher_state (
            streamcast_stream TEXT NOT NULL,
            streamcast_publisher_id TEXT NOT NULL,
            streamcast_next_seq {integer} NOT NULL,
            streamcast_stream_id TEXT NOT NULL,
            PRIMARY KEY (streamcast_stream, streamcast_publisher_id))""",
        # Received rows not yet processed. A processed row is deleted.
        f"""CREATE TABLE IF NOT EXISTS streamcast_inbox (
            streamcast_stream TEXT NOT NULL,
            streamcast_subscriber_id TEXT NOT NULL,
            litelink_offset {integer} NOT NULL,
            streamcast_ts {integer},
            streamcast_row TEXT NOT NULL,
            streamcast_received_at {integer} NOT NULL,
            PRIMARY KEY (streamcast_stream, streamcast_subscriber_id, litelink_offset))""",
        # How far each subscriber has received, and the stream's id.
        f"""CREATE TABLE IF NOT EXISTS streamcast_subscriber_state (
            streamcast_stream TEXT NOT NULL,
            streamcast_subscriber_id TEXT NOT NULL,
            streamcast_last_received_offset {integer} NOT NULL,
            streamcast_stream_id TEXT NOT NULL,
            PRIMARY KEY (streamcast_stream, streamcast_subscriber_id))""",
        "CREATE TABLE IF NOT EXISTS streamcast_schema (streamcast_version INTEGER NOT NULL)",
    ]


async def _prepare(store: Store, *, create: bool) -> None:
    """Create the tables if asked, then refuse a schema newer than this build."""
    async with store.transaction() as tx:
        if create:
            for statement in store_ddl(store.dialect):
                await tx.execute(statement)

        found = await tx.fetch("SELECT streamcast_version FROM streamcast_schema")
        if not found:
            await tx.execute(
                sql(store.dialect, "INSERT INTO streamcast_schema VALUES (?)"),
                (SCHEMA_VERSION,),
            )
        elif max(int(v) for (v,) in found) > SCHEMA_VERSION:
            msg = (
                f"this database's streamcast tables are version {found[0][0]}, "
                f"newer than this build's {SCHEMA_VERSION}; upgrade streamcast"
            )
            raise RuntimeError(msg)


class _Lock:
    """One transaction at a time, and a task never waiting on itself."""

    __slots__ = ("_lock", "_owner")

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[Any] | None = None

    @contextlib.asynccontextmanager
    async def held(self) -> AsyncIterator[None]:
        task = asyncio.current_task()
        if task is not None and task is self._owner:
            msg = (
                "this task already holds a transaction on this store, and would "
                "wait on itself; end it first (an inbox row's: Inbox.commit())"
            )
            raise RuntimeError(msg)

        async with self._lock:
            self._owner = task
            try:
                yield
            finally:
                self._owner = None


class _Open:
    """What a store's transaction object shares: its store, its state, and
    the callbacks waiting for COMMIT."""

    __slots__ = ("_callbacks", "_live", "_store")

    def __init__(self, store: Store) -> None:
        self._store = store
        self._live = True
        self._callbacks: list[Callable[[], None]] = []

    @property
    def store(self) -> Store:
        return self._store

    def on_commit(self, callback: Callable[[], None]) -> None:
        self._check()
        self._callbacks.append(callback)

    def _check(self) -> None:
        if not self._live:
            msg = "this transaction has ended; open another with store.transaction()"
            raise RuntimeError(msg)

    def _committed(self) -> None:
        self._live = False
        for callback in self._callbacks:
            callback()

    def _ended(self) -> None:
        self._live = False


# -- SQLite ------------------------------------------------------------------


class _SqliteTransaction(_Open):
    __slots__ = ()

    async def execute(self, sql: str, params: Sequence[object] = ()) -> int:
        self._check()
        store: SqliteStore = self._store  # ty: ignore[invalid-assignment]
        return await store._run(lambda c: c.execute(sql, tuple(params)).rowcount)  # noqa: SLF001

    async def fetch(
        self, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[Any, ...]]:
        self._check()
        store: SqliteStore = self._store  # ty: ignore[invalid-assignment]
        return await store._run(lambda c: c.execute(sql, tuple(params)).fetchall())  # noqa: SLF001


class SqliteStore:
    """A SQLite database file, through the stdlib's `sqlite3`.

    **Its one connection runs on a thread of its own**, so a commit's fsync
    never blocks the event loop -- the argument `_writer` makes for the
    broker's log. `BEGIN IMMEDIATE` takes the write lock at the start of a
    block, so two processes on one file serialise there rather than failing
    at COMMIT, and WAL lets readers in other processes keep reading.
    """

    __slots__ = ("_connection", "_executor", "_lock")

    dialect: Final[Dialect] = "sqlite"

    def __init__(
        self, connection: sqlite3.Connection, executor: ThreadPoolExecutor
    ) -> None:
        self._connection = connection
        self._executor = executor
        self._lock = _Lock()

    @classmethod
    async def open(
        cls,
        path: str | PathLike[str],
        *,
        create: bool = True,
        busy_timeout: float = 30.0,
    ) -> SqliteStore:
        """Open `path`, creating the library's tables unless `create=False`."""
        executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="streamcast-store"
        )

        def connecting() -> sqlite3.Connection:
            connection = sqlite3.connect(
                path,
                timeout=busy_timeout,
                isolation_level=None,
                check_same_thread=False,
            )
            connection.execute("PRAGMA journal_mode=WAL")
            # FULL: a COMMIT that returned is on disk, which is what lets the
            # sender delete a row once the broker holds it, and the receiver
            # advance past one.
            connection.execute("PRAGMA synchronous=FULL")
            return connection

        loop = asyncio.get_running_loop()
        connection = await loop.run_in_executor(executor, connecting)
        store = cls(connection, executor)
        try:
            await _prepare(store, create=create)
        except BaseException:
            await store.close()
            raise

        return store

    async def _run(self, work: Callable[[sqlite3.Connection], Any]) -> Any:
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(
            loop.run_in_executor(self._executor, work, self._connection)
        )
        # The statement runs on the shared connection whether or not its
        # waiter is cancelled, so the waiter outlives it: a cancelled BEGIN
        # that went on to run would otherwise leave a transaction open that
        # nobody rolls back, and every later BEGIN on the connection fails.
        cancelled = await join(future)
        if cancelled:
            if not future.cancelled():
                future.exception()

            raise asyncio.CancelledError

        return future.result()

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        async with self._lock.held():
            tx = _SqliteTransaction(self)
            try:
                # Inside the try: a cancellation that lands once BEGIN has run
                # is rolled back below rather than leaving it open.
                await self._run(lambda c: c.execute("BEGIN IMMEDIATE"))
                yield tx
            except BaseException:
                tx._ended()  # noqa: SLF001
                with contextlib.suppress(sqlite3.Error):
                    await self._run(lambda c: c.execute("ROLLBACK"))

                raise

            try:
                await self._run(lambda c: c.execute("COMMIT"))
            except BaseException:
                tx._ended()  # noqa: SLF001
                with contextlib.suppress(sqlite3.Error):
                    await self._run(lambda c: c.execute("ROLLBACK"))

                raise

        tx._committed()  # noqa: SLF001

    async def close(self) -> None:
        async with self._lock.held():
            with contextlib.suppress(sqlite3.Error):
                await self._run(lambda c: c.close())

        self._executor.shutdown(wait=True)


# -- Postgres ----------------------------------------------------------------


class _PostgresTransaction(_Open):
    __slots__ = ()

    async def execute(self, sql: str, params: Sequence[object] = ()) -> int:
        self._check()
        store: PostgresStore = self._store  # ty: ignore[invalid-assignment]

        async def run() -> int:
            cursor = await store._connection.execute(sql, tuple(params) or None)  # noqa: SLF001
            return cursor.rowcount

        return await store._run(run)  # noqa: SLF001

    async def fetch(
        self, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[Any, ...]]:
        self._check()
        store: PostgresStore = self._store  # ty: ignore[invalid-assignment]

        async def run() -> list[tuple[Any, ...]]:
            cursor = await store._connection.execute(sql, tuple(params) or None)  # noqa: SLF001
            return list(await cursor.fetchall())

        return await store._run(run)  # noqa: SLF001


class PostgresStore:
    """A Postgres database, through one psycopg 3 async connection.

    **Explicit BEGIN and COMMIT on an autocommit connection**, and no session
    state -- no `SET`, `LISTEN` or advisory lock -- so it works behind a
    transaction pooler. Pass libpq's keepalive and `options=-c
    statement_timeout=...` settings in the DSN so a database that vanished
    fails a transaction in bounded time rather than hanging it.
    """

    __slots__ = ("_connection", "_lock")

    dialect: Final[Dialect] = "postgres"

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._lock = _Lock()

    @classmethod
    async def connect(
        cls, dsn: str, *, create: bool = True, **kwargs: Any
    ) -> PostgresStore:
        """Connect to `dsn`, creating the library's tables unless
        `create=False`. Other keywords go to `psycopg.AsyncConnection.connect`."""
        try:
            import psycopg  # noqa: PLC0415 — an optional extra
        except ImportError:  # pragma: no cover — depends on the install
            msg = "PostgresStore needs psycopg 3: install streamcast[postgres]"
            raise ImportError(msg) from None

        connection = await psycopg.AsyncConnection.connect(
            dsn, autocommit=True, **kwargs
        )
        store = cls(connection)
        try:
            await _prepare(store, create=create)
        except BaseException:
            await store.close()
            raise

        return store

    async def _run(self, command: Callable[[], Awaitable[T]]) -> T:
        """One command on the connection, which its waiter outlives.

        psycopg's connection runs one command at a time. A waiter cancelled
        while one is in flight — twice, as `Inbox.close` cancels its receiver,
        the second landing on the ROLLBACK the first began — would release
        the store's lock with the command still running, and the next
        transaction's BEGIN fails "another command is already in progress".
        """
        task = asyncio.ensure_future(command())
        cancelled = await join(task)
        if cancelled:
            if not task.cancelled():
                task.exception()

            raise asyncio.CancelledError

        return task.result()

    async def _statement(self, sql: str) -> None:
        async def run() -> None:
            await self._connection.execute(sql)

        await self._run(run)

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        async with self._lock.held():
            tx = _PostgresTransaction(self)
            try:
                # Inside the try, as for SQLite: a cancellation that lands
                # once BEGIN has run is rolled back below.
                await self._statement("BEGIN")
                yield tx
            except BaseException:
                tx._ended()  # noqa: SLF001
                with contextlib.suppress(Exception):
                    await self._statement("ROLLBACK")

                raise

            try:
                await self._statement("COMMIT")
            except BaseException:
                tx._ended()  # noqa: SLF001
                with contextlib.suppress(Exception):
                    await self._statement("ROLLBACK")

                raise

        tx._committed()  # noqa: SLF001

    async def close(self) -> None:
        async with self._lock.held():
            with contextlib.suppress(Exception):
                await self._connection.close()


__all__ = [
    "SCHEMA_VERSION",
    "Dialect",
    "PostgresStore",
    "SqliteStore",
    "Store",
    "Transaction",
    "sql",
    "store_ddl",
]
