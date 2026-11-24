"""The adapter contract: `SqliteStore` and `PostgresStore` behave alike.

Every test runs on both (`opener`): what the outbox and the inbox rely on is
the contract, and an adapter that passes it is one they work over.
"""

from __future__ import annotations

import asyncio

import psycopg
import pytest

import streamcast
from streamcast._store import sql


def p(store, statement: str) -> str:
    """A test's own SQL, `?`-style, in the store's placeholder style."""
    return sql(store.dialect, statement)


async def scratch(store) -> None:
    async with store.transaction() as tx:
        await tx.execute("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY, v TEXT)")


class TestATransaction:
    async def test_it_commits_and_rolls_back(self, opener):
        store = await opener()
        await scratch(store)
        async with store.transaction() as tx:
            assert (
                await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (1, "a")) == 1
            )

        with pytest.raises(RuntimeError, match="boom"):
            async with store.transaction() as tx:
                await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (2, "b"))
                raise RuntimeError("boom")

        async with store.transaction() as tx:
            assert await tx.fetch("SELECT k, v FROM t ORDER BY k") == [(1, "a")]

        await store.close()

    async def test_on_commit_runs_after_commit_and_never_on_rollback(self, opener):
        store = await opener()
        await scratch(store)
        calls: list[str] = []
        async with store.transaction() as tx:
            tx.on_commit(lambda: calls.append("committed"))
            assert calls == []

        assert calls == ["committed"]
        with pytest.raises(RuntimeError):
            async with store.transaction() as tx:
                tx.on_commit(lambda: calls.append("rolled back"))
                raise RuntimeError

        assert calls == ["committed"]
        await store.close()

    async def test_a_transaction_is_dead_after_its_block(self, opener):
        store = await opener()
        async with store.transaction() as tx:
            pass

        with pytest.raises(RuntimeError, match="has ended"):
            await tx.fetch("SELECT 1")

        await store.close()

    async def test_one_transaction_at_a_time_per_store(self, opener):
        """The second waits for the first to end; it never joins it."""
        store = await opener()
        await scratch(store)
        order: list[str] = []
        first_open = asyncio.Event()

        async def first() -> None:
            async with store.transaction() as tx:
                first_open.set()
                await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (1, "a"))
                await asyncio.sleep(0)
                order.append("first ends")

        async def second() -> None:
            await first_open.wait()
            async with store.transaction() as tx:
                order.append("second begins")
                assert await tx.fetch("SELECT k FROM t") == [(1,)]

        await asyncio.gather(first(), second())
        assert order == ["first ends", "second begins"]
        await store.close()


class TestTheTables:
    async def test_they_are_created_and_a_role_without_ddl_uses_store_ddl(self, opener):
        with pytest.raises(Exception):  # noqa: B017, PT011 — no table: the driver's error
            await opener(create=False)

        store = await opener()  # creates them
        await store.close()
        again = await opener(create=False)
        async with again.transaction() as tx:
            assert await tx.fetch("SELECT count(*) FROM streamcast_inbox") == [(0,)]

        await again.close()
        statements = streamcast.store_ddl(again.dialect)
        assert any("streamcast_subscriber_state" in s for s in statements)

    async def test_a_newer_schema_is_refused(self, opener):
        store = await opener()
        async with store.transaction() as tx:
            await tx.execute("UPDATE streamcast_schema SET streamcast_version = 99")

        await store.close()
        with pytest.raises(RuntimeError, match="newer than this build"):
            await opener()


class TestALostConnection:
    async def test_the_block_raises_and_its_writes_roll_back(self, opener):
        store = await opener()
        await scratch(store)
        if store.dialect != "postgres":
            await store.close()
            pytest.skip("a SQLite file has no connection to lose")

        [(pid,)] = await (
            await store._connection.execute("SELECT pg_backend_pid()")
        ).fetchall()  # noqa: SLF001
        other = await opener()
        with pytest.raises(psycopg.OperationalError):
            async with store.transaction() as tx:
                await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (1, "a"))
                async with other.transaction() as killer:
                    await killer.execute(
                        p(store, "SELECT pg_terminate_backend(?)"), (pid,)
                    )

                await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (2, "b"))

        async with other.transaction() as tx:
            assert await tx.fetch("SELECT k FROM t") == []

        async with store.transaction() as tx:
            assert await tx.fetch("SELECT k FROM t") == [], "the same store reconnects"
            await tx.execute(p(store, "INSERT INTO t VALUES (?, ?)"), (3, "c"))

        async with other.transaction() as tx:
            assert await tx.fetch("SELECT k, v FROM t") == [(3, "c")]

        await other.close()
        await store.close()

    async def test_explicit_close_does_not_reconnect(self, postgres_dsn):
        store = await streamcast.PostgresStore.connect(postgres_dsn)
        await store.close()
        with pytest.raises(RuntimeError, match="closed"):
            async with store.transaction():
                pytest.fail("closed store reopened")

    async def test_reconnect_preserves_the_original_dsn_and_keywords(
        self, postgres_dsn, monkeypatch
    ):
        connect = psycopg.AsyncConnection.connect
        calls = []

        async def recording(cls, dsn, **kwargs):
            calls.append((dsn, kwargs))
            return await connect(dsn, **kwargs)

        monkeypatch.setattr(psycopg.AsyncConnection, "connect", classmethod(recording))
        dsn = postgres_dsn + " connect_timeout=2"
        keywords = {"application_name": "outbox-recovery", "prepare_threshold": None}
        store = await streamcast.PostgresStore.connect(
            dsn, application_name="outbox-recovery", prepare_threshold=None
        )
        await store._connection.close()
        async with store.transaction() as tx:
            assert await tx.fetch("SELECT 1") == [(1,)]

        assert calls == [(dsn, {"autocommit": True, **keywords})] * 2
        await store.close()

    async def test_cancelled_reconnect_keeps_the_opened_connection(self, postgres_dsn):
        store = await streamcast.PostgresStore.connect(postgres_dsn)
        await store._connection.close()
        connect = store._connect
        opened = asyncio.Event()
        release = asyncio.Event()
        connections = []

        async def delayed_connect():
            connection = await connect()
            connections.append(connection)
            opened.set()
            await release.wait()
            return connection

        store._connect = delayed_connect

        async def read():
            async with store.transaction() as tx:
                await tx.fetch("SELECT 1")

        task = asyncio.create_task(read())
        await opened.wait()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(connections) == 1
        assert store._connection is connections[0], (
            "the cancelled waiter cannot orphan it"
        )
        async with store.transaction() as tx:
            assert await tx.fetch("SELECT 1") == [(1,)]

        await store.close()
        assert connections[0].closed


class TestATaskWaitingOnItself:
    async def test_a_second_transaction_from_the_holder_is_refused(self, opener):
        """Not left to wait for ever: the holder could never let it in.

        Falsify by dropping the owner check in `_Lock.held`: this test then
        hangs until its timeout.
        """
        store = await opener()
        async with store.transaction():
            with pytest.raises(RuntimeError, match="wait on itself"):
                async with store.transaction():
                    pass

        async with store.transaction() as tx:
            assert await tx.fetch("SELECT 1") == [(1,)], "the store still works"

        await store.close()


class TestACancelledStatement:
    async def test_the_next_transaction_finds_the_connection_free(self, opener):
        """A transaction cancelled while its statement runs, and again while
        it rolls back — an inbox's receiver cancelled at `close` — leaves the one connection
        free for the next. Its statement runs on whether or not its waiter is
        cancelled, so the waiter outlives it: let go at once, Postgres answers
        the next BEGIN with "another command is already in progress", and
        SQLite's next BEGIN finds a transaction it never opened.
        """
        store = await opener()
        slow = (
            "SELECT pg_sleep(0.3)"
            if store.dialect == "postgres"
            else "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n "
            "WHERE i < 3000000) SELECT count(*) FROM n"
        )
        running = asyncio.Event()

        async def statement() -> None:
            async with store.transaction() as tx:
                running.set()
                await tx.fetch(slow)

        task = asyncio.create_task(statement())
        await running.wait()
        await asyncio.sleep(0.05)
        # Twice, as `Inbox.close` does: the second lands on the ROLLBACK the
        # first one started.
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
        async with store.transaction() as tx:
            assert await tx.fetch("SELECT 1") == [(1,)], "the store still works"

        await store.close()
