"""`inbox` — exactly-once consuming into the caller's own transaction.

    store = await streamcast.SqliteStore.open("state.db")
    async with streamcast.inbox(uri, subscriber="billing", store=store) as sub:
        async for offset, ts, row, tx in sub:
            await tx.execute("INSERT INTO invoices ...", (...))
        # each tx deletes its inbox row and commits when the loop asks for the
        # next row; a raise rolls it back, the row stays, and the loop ends

**Two halves, each one transaction of the caller's database.**

1. **The receiver**, a task of this object's, reads the subscription. Each
   batch it receives goes into `streamcast_inbox` (an offset already there
   inserts nothing), and `streamcast_last_received_offset` moves forward to
   the last row inserted, in the same transaction.
2. **Processing.** `recv` and iteration hand out the oldest inbox row with a
   transaction that already holds its delete. The caller's writes and the
   row's removal commit together or not at all; a row whose transaction
   rolls back stays, and is handed out again.

**Rows are processed in any order.** Each is deleted when processed, so a
slow row holds none behind it; `recv` hands them out lowest offset first,
and on Postgres several workers, each with its own store, take different
rows (`FOR UPDATE SKIP LOCKED`).

**Resume is from `last_received_offset + 1`.** Rows received and not
processed are still in the inbox and are handed out first. A subscriber with
no state starts at the stream's latest offset, or at `offset=`; one with
state refuses `offset=`, since moving a saved position is its own act. **An
empty inbox does NOT mean caught up**: it means everything received has been
processed. Whether everything the broker holds has been received is the
subscription's question, `caught_up()`.

**The stream's id is checked at every connect** against the one the state
recorded: a stream recreated under the same name, whose offsets started
again, is refused with `StreamMismatch` before a row is received, never
resumed above an end it will reach only later.

**The inbox is a bounded receive window, not a backlog store.** The backlog
stays in the log, durable and published. At `inbox_max_bytes` (and
`inbox_max_rows`, if set) of unprocessed rows the receiver inserts nothing
more and CLOSES its subscription -- a subscriber that merely stopped reading
would hold broker memory until `max_backlog` dropped it -- and reconnects from
`last_received_offset + 1` once the inbox is below `inbox_resume_at` of each
bound. Frames already read past the bound are discarded unreceived and come
again; an empty inbox always takes one row, so a row larger than the bound is
delivered rather than blocking the subscriber for ever.

**The receiver's count** of rows and bytes is read from the inbox at start,
kept as it inserts and as this object's transactions delete, and read again
every `POLL_EVERY` while paused, since other processes' workers may be the
ones deleting.

A clean context exit commits the open processing transaction; an exit by
exception rolls it back. Exactly-once processing covers writes in that
transaction, not external side effects.

The file `Cursor` (`connect(cursor=)`) stays the at-least-once option for a
consumer without a database.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, Self

import msgspec
from websockets.exceptions import ConnectionClosed

from streamcast._client import _arrived, connect
from streamcast._errors import StreamMismatch, TooSlow
from streamcast._limits import BACKOFF, INBOX_MAX_BYTES, INBOX_RESUME_AT, POLL_EVERY
from streamcast._protocol import stream_name
from streamcast._store import sql
from streamcast._tasks import join

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from streamcast._client import Subscription
    from streamcast._store import Store, Transaction

_ENCODE = msgspec.json.Encoder()
_DECODE = msgspec.json.Decoder()

RECEIVE_BATCH = 500
"""Rows the receiver takes from the subscription per inbox transaction."""

Received = tuple[int, int | None, dict[str, Any]]
"""`(offset, ts, row)`."""


def _now_us() -> int:
    return time.time_ns() // 1_000


def _check_subscriber_id(subscriber: str) -> str:
    if not subscriber:
        msg = "a subscriber id names one consumer's place in a stream; it is empty"
        raise ValueError(msg)

    return subscriber


class Inbox:
    """A subscription whose rows arrive in the caller's database first.

    Not constructed directly: `streamcast.inbox(...)` builds it, checks the
    stream, and starts the receiver.
    """

    __slots__ = (
        "_arrived",
        "_backoff",
        "_bytes",
        "_catch_up",
        "_caught_up",
        "_closed",
        "_failure",
        "_last_received",
        "_max_bytes",
        "_max_rows",
        "_open",
        "_on_pause_change",
        "_paused",
        "_poll_every",
        "_resume_at",
        "_rows",
        "_store",
        "_stream",
        "_stream_id",
        "_subscriber",
        "_subscription",
        "_task",
        "_uri",
        "_ws",
        "_freed",
    )

    def __init__(
        self,
        uri: str,
        subscriber: str,
        store: Store,
        *,
        inbox_max_bytes: int,
        inbox_max_rows: int | None,
        inbox_resume_at: float,
        catch_up: bool,
        on_pause_change: Callable[[bool], None] | None,
        poll_every: float,
        backoff: Sequence[float],
        websockets_kwargs: dict[str, Any],
    ) -> None:
        if inbox_max_bytes < 1 or (inbox_max_rows is not None and inbox_max_rows < 1):
            msg = (
                f"inbox_max_bytes={inbox_max_bytes}, inbox_max_rows={inbox_max_rows}: "
                f"a bound holds at least one row"
            )
            raise ValueError(msg)

        if not 0 < inbox_resume_at <= 1:
            msg = f"inbox_resume_at={inbox_resume_at}: a fraction of each bound, in (0, 1]"
            raise ValueError(msg)

        self._uri = uri
        self._stream = stream_name(uri)
        self._subscriber = subscriber
        self._store = store
        self._max_bytes = inbox_max_bytes
        self._max_rows = inbox_max_rows
        self._resume_at = inbox_resume_at
        self._catch_up = catch_up
        self._poll_every = poll_every
        self._backoff = tuple(backoff)
        self._ws = websockets_kwargs
        self._rows = 0
        self._bytes = 0
        self._last_received: int | None = None
        self._stream_id: str | None = None
        self._arrived = asyncio.Event()  # the receiver inserted rows
        self._freed = asyncio.Event()  # processing deleted rows
        self._caught_up = False
        self._paused = False
        self._on_pause_change = on_pause_change
        self._subscription: Subscription | None = None
        self._closed = False
        self._failure: BaseException | None = None
        self._task: asyncio.Task[None] | None = None
        # The processing transaction handed out with the last row, until the
        # loop asks for the next one: `(context, transaction)`.
        self._open: (
            tuple[AbstractAsyncContextManager[Transaction], Transaction] | None
        ) = None

    def __repr__(self) -> str:
        return f"<streamcast.Inbox {self._stream!r} subscriber={self._subscriber!r}>"

    @property
    def store(self) -> Store:
        return self._store

    @property
    def last_received(self) -> int | None:
        """The last offset received into the inbox, or None before any."""
        return self._last_received

    @property
    def paused(self) -> bool:
        """Whether the receiver is waiting for the inbox's low-water bounds."""
        return self._paused

    def caught_up(self) -> bool:
        """Whether the receiver has received everything the broker held when
        it greeted the current connection, and nothing more is waiting on the
        socket. False while paused or reconnecting. An empty inbox is not this:
        it says only that what was received has been processed."""
        return self._caught_up and not self._paused

    # -- processing ---------------------------------------------------------

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[int, int | None, dict[str, Any], Transaction]:
        try:
            return await self.recv()
        except _Closed:
            raise StopAsyncIteration from None

    async def recv(self) -> tuple[int, int | None, dict[str, Any], Transaction]:
        """The oldest inbox row, `(offset, ts, row, tx)`, with `tx` open and
        holding the row's delete. Commits the previous row's `tx` first."""
        rows, tx = await self._take(1)
        offset, ts, row = rows[0]
        return offset, ts, row, tx

    async def recv_many(
        self, limit: int = RECEIVE_BATCH
    ) -> tuple[list[Received], Transaction]:
        """Up to `limit` of the oldest inbox rows in one `tx`: one commit for
        all of them, which is the throughput lever. At least one row."""
        if limit < 1:
            msg = f"limit={limit}: a batch holds at least one row"
            raise ValueError(msg)

        return await self._take(limit)

    async def commit(self) -> None:
        """Commit the open processing transaction now, rather than when the
        next row is asked for."""
        await self._end(None)

    async def _take(self, limit: int) -> tuple[list[Received], Transaction]:
        await self._end(None)
        while True:
            if self._closed:
                raise _Closed

            self._arrived.clear()
            context = self._store.transaction()
            tx = await context.__aenter__()
            try:
                taken = await self._select(tx, limit)
            except BaseException as exc:
                await context.__aexit__(type(exc), exc, exc.__traceback__)
                raise

            if taken:
                self._open = (context, tx)
                return taken, tx

            await context.__aexit__(None, None, None)
            if self._failure is not None:
                raise self._failure

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._arrived.wait(), self._poll_every)

    async def _select(self, tx: Transaction, limit: int) -> list[Received]:
        dialect = self._store.dialect
        lock = " FOR UPDATE SKIP LOCKED" if dialect == "postgres" else ""
        found = await tx.fetch(
            sql(
                dialect,
                "SELECT litelink_offset, streamcast_ts, streamcast_row FROM "
                "streamcast_inbox WHERE streamcast_stream = ? AND "
                "streamcast_subscriber_id = ? ORDER BY litelink_offset LIMIT ?" + lock,
            ),
            (self._stream, self._subscriber, limit),
        )
        if not found:
            return []

        size = 0
        for offset, _ts, text in found:
            size += len(str(text).encode())
            await tx.execute(
                sql(
                    dialect,
                    "DELETE FROM streamcast_inbox WHERE streamcast_stream = ? AND "
                    "streamcast_subscriber_id = ? AND litelink_offset = ?",
                ),
                (self._stream, self._subscriber, int(offset)),
            )

        rows, freed = len(found), size

        def deleted() -> None:
            self._rows = max(0, self._rows - rows)
            self._bytes = max(0, self._bytes - freed)
            self._freed.set()

        tx.on_commit(deleted)
        return [
            (int(offset), None if ts is None else int(ts), _DECODE.decode(text))
            for offset, ts, text in found
        ]

    async def _end(self, exc: BaseException | None) -> None:
        if self._open is None:
            return

        context, _tx = self._open
        self._open = None
        if exc is None:
            await context.__aexit__(None, None, None)
        else:
            await context.__aexit__(type(exc), exc, exc.__traceback__)

    # -- opening and closing ------------------------------------------------

    async def _start(self, offset: int | None) -> None:
        """Read the state, check the stream with the first connection, and
        start the receiver on it. Raises here what the stream refuses."""
        async with self._store.transaction() as tx:
            state = await self._state(tx)
            self._rows, self._bytes = await self._count(tx)

        if state is not None and offset is not None:
            msg = (
                f"subscriber {self._subscriber!r} has a saved position in "
                f"{self._stream!r} ({state[0]}); offset= would move it. Delete "
                f"its streamcast_subscriber_state row to start again"
            )
            raise ValueError(msg)

        start = state[0] + 1 if state is not None else offset
        sub = await self._connect(start)
        try:
            info = sub.info
            if info.stream_id is None or info.end_offset is None:
                msg = f"{self._stream!r} has no log: an inbox needs offsets to resume from"
                raise ValueError(msg)

            async with self._store.transaction() as tx:
                if state is None:
                    first = info.end_offset if start is None else start
                    await self._record(tx, first - 1, info.stream_id)
                    self._last_received = first - 1
                elif state[1] != info.stream_id:
                    raise StreamMismatch(self._stream, state[1], info.stream_id)
                else:
                    self._last_received = state[0]

            self._stream_id = info.stream_id
            # Identity validation awaited the network. Processing in another
            # worker may have changed occupancy meanwhile; decide from the
            # durable window now, not the count read before connecting.
            async with self._store.transaction() as tx:
                self._rows, self._bytes = await self._count(tx)

            self._paused = self._over(self._rows, self._bytes)
            # The starting state, once, before `inbox()` returns: inside the
            # try, so a callback that raises still closes the subscription.
            if self._on_pause_change is not None:
                self._on_pause_change(self._paused)

            if self._paused:
                # Validate the saved identity/offset before waiting, but leave
                # no unread socket holding broker memory during the pause.
                await sub.close()

        except BaseException:
            await sub.close()
            raise

        # Ownership precedes scheduling: cancellation before the receiver
        # first runs cannot execute its finally block (#191).
        self._subscription = None if self._paused else sub
        self._task = asyncio.create_task(
            self._receive(None if self._paused else sub),
            name=f"streamcast-inbox-{self._stream}",
        )

    async def close(self, exc: BaseException | None = None) -> None:
        """Stop the receiver, after committing (or, with `exc`, rolling back)
        the open processing transaction."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()

        try:
            await self._end(exc)
        finally:
            stopping = asyncio.create_task(self._stop_receiver())
            cancelled = await join(stopping)
            stopping.result()
            if cancelled:
                raise asyncio.CancelledError

    async def _stop_receiver(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

        sub, self._subscription = self._subscription, None
        if sub is not None:
            await sub.close()

    # -- the receiver -------------------------------------------------------

    async def _connect(self, start: int | None) -> Subscription:
        kwargs: dict[str, Any] = dict(self._ws)
        if self._catch_up:
            kwargs["catch_up"] = True

        return await connect(self._uri, offset=start, **kwargs)

    async def _receive(self, sub: Subscription | None) -> None:
        attempt = 0
        try:
            while not self._closed:
                if sub is None:
                    await self._wait_below_low_water()
                    try:
                        sub = await self._reconnect()
                        self._subscription = sub
                    except (OSError, ConnectionClosed, TimeoutError, TooSlow):
                        await asyncio.sleep(
                            self._backoff[min(attempt, len(self._backoff) - 1)]
                        )
                        attempt += 1
                        continue

                attempt = 0
                try:
                    full = await self._fill(sub)
                except (OSError, ConnectionClosed, TimeoutError, TooSlow):
                    full = False

                # Full, or the connection ended: either way it is closed, and
                # the next one starts from the received position.
                self._caught_up = False
                with contextlib.suppress(Exception):
                    await sub.close()

                sub = self._subscription = None
                if full:
                    self._pause(paused=True)

        except Exception as exc:  # noqa: BLE001 — surfaced by `recv`
            self._failure = exc
            self._arrived.set()
        finally:
            # Every way out, a failure included: a subscription left open
            # and unread holds broker memory until `max_backlog` drops it.
            if sub is not None:
                with contextlib.suppress(Exception):
                    await sub.close()

    async def _reconnect(self) -> Subscription:
        # From the table, not memory: another worker's receiver on this
        # subscriber id may have received further.
        async with self._store.transaction() as tx:
            state = await self._state(tx)

        assert state is not None  # recorded at `_start`
        self._last_received = max(state[0], self._last_received or state[0])
        sub = await self._connect(self._last_received + 1)
        if sub.info.stream_id != self._stream_id:
            await sub.close()
            raise StreamMismatch(
                self._stream, self._stream_id or "", sub.info.stream_id or ""
            )

        return sub

    async def _fill(self, sub: Subscription) -> bool:
        """Receive into the inbox until it is full (True) or the connection
        ends (raises). Closes nothing; the caller does."""
        end = sub.info.end_offset
        while not self._closed:
            # Everything the broker held when it greeted this connection is
            # in, and nothing more is waiting on the socket.
            connection = sub._connection  # noqa: SLF001 — None during a catch-up
            self._caught_up = (
                end is not None
                and self._last_received is not None
                and self._last_received >= end - 1
                and connection is not None
                and not _arrived(connection)
            )
            batch = await sub.recv_many(RECEIVE_BATCH)
            self._caught_up = False
            take: list[tuple[int, int | None, str]] = []
            rows, size = self._rows, self._bytes
            full = False
            for offset, ts, row in batch:
                assert offset is not None  # a durable stream's frames carry one
                if self._last_received is not None and offset <= self._last_received:
                    continue  # already received; a resume never repeats, but cheap

                if rows and self._over(rows, size):
                    full = True
                    break

                text = _ENCODE.encode(row).decode()
                take.append((offset, ts, text))
                rows += 1
                size += len(text.encode())
                if self._over(rows, size):
                    full = True
                    break

            if take:
                # Added, not assigned: processing may have deleted rows while
                # the insert waited for the store.
                added, grew = await self._insert(take)
                self._rows += added
                self._bytes += grew
                self._arrived.set()

            if full:
                return True

        return False

    def _over(self, rows: int, size: int) -> bool:
        return size >= self._max_bytes or (
            self._max_rows is not None and rows >= self._max_rows
        )

    def _below_low_water(self) -> bool:
        if self._bytes >= self._max_bytes * self._resume_at:
            return False

        return self._max_rows is None or self._rows < self._max_rows * self._resume_at

    async def _wait_below_low_water(self) -> None:
        while not self._closed:
            self._freed.clear()
            # Recount before the first decision too: another process may
            # have ACKed while startup validated the connection's identity.
            async with self._store.transaction() as tx:
                self._rows, self._bytes = await self._count(tx)

            if self._below_low_water():
                # Only here: a close ends the wait still paused, and is not
                # a resume to report.
                self._pause(paused=False)
                return

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._freed.wait(), self._poll_every)

    def _pause(self, *, paused: bool) -> None:
        """Move the receiver between paused and receiving, and say so.

        Called only on a change, synchronously, in the receiver's own task,
        so the callback sees each transition once and in order. Whatever it
        raises fails the receiver, as any receiver failure does: `recv`
        raises it.
        """
        if paused == self._paused:
            return

        self._paused = paused
        if self._on_pause_change is not None:
            self._on_pause_change(paused)

    async def _insert(self, take: list[tuple[int, int | None, str]]) -> tuple[int, int]:
        """Insert what is above the TABLE's received position, and move it;
        `(rows, bytes)` actually inserted.

        Judged against the state row, read in this transaction and locked
        (Postgres) or serialised (SQLite), never against this receiver's
        memory: several workers on one subscriber id each run a receiver, and
        one that lags would otherwise insert again rows another worker has
        already received, processed and deleted -- `ON CONFLICT` cannot see a
        row that is gone.
        """
        dialect = self._store.dialect
        now = _now_us()
        lock = " FOR UPDATE" if dialect == "postgres" else ""
        async with self._store.transaction() as tx:
            [(position,)] = await tx.fetch(
                sql(
                    dialect,
                    "SELECT streamcast_last_received_offset FROM "
                    "streamcast_subscriber_state WHERE streamcast_stream = ? AND "
                    "streamcast_subscriber_id = ?" + lock,
                ),
                (self._stream, self._subscriber),
            )
            fresh = [row for row in take if row[0] > int(position)]
            if not fresh:
                self._last_received = max(int(position), self._last_received or 0)
                return 0, 0

            last = fresh[-1][0]
            for offset, ts, text in fresh:
                await tx.execute(
                    sql(
                        dialect,
                        "INSERT INTO streamcast_inbox (streamcast_stream, "
                        "streamcast_subscriber_id, litelink_offset, streamcast_ts, "
                        "streamcast_row, streamcast_received_at) VALUES "
                        "(?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    ),
                    (self._stream, self._subscriber, offset, ts, text, now),
                )

            # Forward only: a COMMIT whose answer was lost and a re-run must
            # never move the position back.
            await tx.execute(
                sql(
                    dialect,
                    "UPDATE streamcast_subscriber_state SET "
                    "streamcast_last_received_offset = ? WHERE streamcast_stream = ? "
                    "AND streamcast_subscriber_id = ? AND "
                    "streamcast_last_received_offset < ?",
                ),
                (last, self._stream, self._subscriber, last),
            )

        self._last_received = last
        return len(fresh), sum(len(text.encode()) for _, _, text in fresh)

    # -- the state ----------------------------------------------------------

    async def _state(self, tx: Transaction) -> tuple[int, str] | None:
        found = await tx.fetch(
            sql(
                self._store.dialect,
                "SELECT streamcast_last_received_offset, streamcast_stream_id FROM "
                "streamcast_subscriber_state WHERE streamcast_stream = ? AND "
                "streamcast_subscriber_id = ?",
            ),
            (self._stream, self._subscriber),
        )
        return (int(found[0][0]), str(found[0][1])) if found else None

    async def _record(self, tx: Transaction, last: int, stream_id: str) -> None:
        await tx.execute(
            sql(
                self._store.dialect,
                "INSERT INTO streamcast_subscriber_state (streamcast_stream, "
                "streamcast_subscriber_id, streamcast_last_received_offset, "
                "streamcast_stream_id) VALUES (?, ?, ?, ?)",
            ),
            (self._stream, self._subscriber, last, stream_id),
        )

    async def _count(self, tx: Transaction) -> tuple[int, int]:
        dialect = self._store.dialect
        length = (
            "length(CAST(streamcast_row AS BLOB))"
            if dialect == "sqlite"
            else "octet_length(streamcast_row)"
        )
        [(rows, size)] = await tx.fetch(
            sql(
                dialect,
                f"SELECT count(*), coalesce(sum({length}), 0) FROM streamcast_inbox "
                "WHERE streamcast_stream = ? AND streamcast_subscriber_id = ?",
            ),
            (self._stream, self._subscriber),
        )
        return int(rows), int(size)


class _Closed(Exception):  # noqa: N818 — a signal, not an error
    """The inbox was closed: iteration ends."""


class inbox:  # noqa: N801 — a sibling of `connect` and `outbox`
    """Open a subscription that receives into the caller's database.

        async with streamcast.inbox(uri, subscriber="billing", store=store) as sub:
            async for offset, ts, row, tx in sub:
                ...

    `subscriber` names one consumer's place in the stream. `offset=` places a
    subscriber with no saved state (the default is the latest offset), and is
    refused for one that has some. `catch_up=True` is `connect`'s, for a
    receiver that falls past what the server replays. The receive window is
    `inbox_max_bytes` (`INBOX_MAX_BYTES`), `inbox_max_rows` (None) and
    `inbox_resume_at` (`INBOX_RESUME_AT`); see the module.
    Startup validates identity and recounts durable occupancy: at or above
    either high bound it closes the validation subscription and starts paused;
    below high it starts active, including between the watermarks. This is a
    fresh decision, not restoration of the previous process's hysteresis.
    A paused receiver resumes strictly below both low bounds.

    `on_pause_change(paused)` is called once with the starting state before
    this returns, then on every transition, synchronously in the receiver's
    task: keep it quick and non-blocking (set a flag, put on a queue), since
    the receiver waits for it. What it raises fails the inbox: here at start,
    afterwards from `recv` once the rows already received are processed. `Inbox.paused` is the same state, read-only;
    callers supply no pause flag and recompute nothing from counts.

    A clean exit commits the open processing transaction, and an exit by
    exception rolls it back, so the row it held is handed out again. Unknown
    keywords go to `websockets.connect`.
    """

    __slots__ = ("_inbox", "_offset")

    def __init__(
        self,
        uri: str,
        *,
        subscriber: str,
        store: Store,
        offset: int | None = None,
        inbox_max_bytes: int = INBOX_MAX_BYTES,
        inbox_max_rows: int | None = None,
        inbox_resume_at: float = INBOX_RESUME_AT,
        catch_up: bool = False,
        on_pause_change: Callable[[bool], None] | None = None,
        poll_every: float = POLL_EVERY,
        backoff: Sequence[float] = BACKOFF,
        **kwargs: Any,
    ) -> None:
        self._offset = offset
        self._inbox = Inbox(
            uri,
            _check_subscriber_id(subscriber),
            store,
            inbox_max_bytes=inbox_max_bytes,
            inbox_max_rows=inbox_max_rows,
            inbox_resume_at=inbox_resume_at,
            catch_up=catch_up,
            on_pause_change=on_pause_change,
            poll_every=poll_every,
            backoff=backoff,
            websockets_kwargs=kwargs,
        )

    async def __aenter__(self) -> Inbox:
        await self._inbox._start(self._offset)  # noqa: SLF001
        return self._inbox

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._inbox.close(exc)


__all__ = ["RECEIVE_BATCH", "Inbox", "inbox"]
