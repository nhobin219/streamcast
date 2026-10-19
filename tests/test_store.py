"""The adapter contract: `SqliteStore` and `PostgresStore` behave alike.

Every test runs on both (`opener`): what the outbox and the inbox rely on is
the contract, and an adapter that passes it is one they work over.
"""

from __future__ import annotations

import asyncio

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

        import psycopg

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

        await other.close()
        await store.close()


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
