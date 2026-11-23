"""Database outages must not permanently poison a running sender."""

from __future__ import annotations

import asyncio
import contextlib

import psycopg
import pytest

import streamcast
from streamcast import _outbox, _store
from tests.conftest import trade
from tests.test_outbox import landed


@pytest.mark.parametrize("point", ["poll", "reconcile", "read", "ack", "ack-commit"])
@pytest.mark.parametrize(
    "failure",
    [
        psycopg.OperationalError,
        psycopg.errors.ConnectionFailure,
        psycopg.errors.AdminShutdown,
        psycopg.errors.CrashShutdown,
        psycopg.errors.CannotConnectNow,
    ],
)
async def test_outage_then_recovery_keeps_the_sender_and_sequence(
    point, failure, serve, log, tmp_path, monkeypatch
):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")
    async with store.transaction() as tx:
        await _outbox.enqueue(tx, "trades", "p", [trade(0)])

    down = False
    reads = 0
    fetch = _store._SqliteTransaction.fetch
    execute = _store._SqliteTransaction.execute
    transaction = _store.SqliteStore.transaction
    deleting = False
    sleep = _outbox.Outbox._sleep
    delays = asyncio.Queue()
    retry = asyncio.Queue()

    def check(statement):
        nonlocal down, reads
        if statement.startswith("SELECT streamcast_seq"):
            reads += 1

        trigger = (
            (point == "poll" and reads == 1)
            or (
                point == "reconcile"
                and statement.startswith("SELECT streamcast_next_seq")
            )
            or (point == "read" and reads == 2)
            or (
                point == "ack" and statement.startswith("DELETE FROM streamcast_outbox")
            )
        )
        if down or trigger:
            down = True
            raise failure("database temporarily unavailable; retry")

    async def failing_fetch(self, statement, params=()):
        check(statement)
        return await fetch(self, statement, params)

    async def failing_execute(self, statement, params=()):
        nonlocal deleting
        check(statement)
        if statement.startswith("DELETE FROM streamcast_outbox"):
            deleting = True

        return await execute(self, statement, params)

    @contextlib.asynccontextmanager
    async def uncertain_commit(self):
        nonlocal down
        async with transaction(self) as tx:
            yield tx

        if point == "ack-commit" and deleting:
            down = True
            raise failure("connection lost after commit; retry")

    async def controlled_sleep(self, seconds):
        if self._outage:
            await delays.put(seconds)
            await retry.get()
        else:
            await sleep(self, seconds)

    monkeypatch.setattr(_store._SqliteTransaction, "fetch", failing_fetch)
    monkeypatch.setattr(_store._SqliteTransaction, "execute", failing_execute)
    monkeypatch.setattr(_store.SqliteStore, "transaction", uncertain_commit)
    monkeypatch.setattr(_outbox.Outbox, "_sleep", controlled_sleep)
    stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.outbox(
            uri, publisher="p", store=store, backoff=(0.1, 0.2), poll_every=0.01
        ) as out:
            try:
                async with asyncio.timeout(3):
                    # More attempts than backoff entries: the final delay caps,
                    # it is not a retry budget that permanently stops delivery.
                    for delay in (0.1, 0.2, 0.2, 0.2):
                        assert await delays.get() == delay
                        errors = []
                        for _ in range(2):
                            with pytest.raises(psycopg.OperationalError) as caught:
                                async with store.transaction() as tx:
                                    await out.send(trade(1), tx=tx)

                            errors.append(caught.value)

                        assert errors[0] is not errors[1], (
                            "a current error, never stored"
                        )
                        assert out._failure is None
                        assert out._task is not None
                        assert not out._task.done()
                        await retry.put(None)

                # Remove the fault while the sender is held at its next retry.
                async with asyncio.timeout(3):
                    await delays.get()

                down = False
                monkeypatch.setattr(_store._SqliteTransaction, "fetch", fetch)
                monkeypatch.setattr(_store._SqliteTransaction, "execute", execute)
                monkeypatch.setattr(_store.SqliteStore, "transaction", transaction)
                async with store.transaction() as tx:
                    assert await out.send(trade(1), tx=tx) == 2

                await retry.put(None)
                await out.flush(timeout=3)
                assert await out.pending_rows() == 0
            finally:
                await retry.put(None)

    assert landed(log) == [0.0, 1.0], (
        "ack loss must reconcile without duplicate delivery"
    )
    await store.close()


@pytest.mark.parametrize(
    "error",
    [
        psycopg.errors.UndefinedTable("missing table"),
        psycopg.errors.InsufficientPrivilege("permission denied"),
        psycopg.errors.DiskFull("disk full"),
        psycopg.errors.InvalidPassword("authentication refused"),
        ValueError("adapter bug"),
    ],
)
@pytest.mark.parametrize("point", ["poll", "session"])
async def test_non_transient_failures_surface(error, point, tmp_path, monkeypatch):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")

    async def has_rows(self):
        if point == "poll":
            raise error

        return True

    async def session(self):
        raise error

    monkeypatch.setattr(_outbox.Outbox, "_has_rows", has_rows)
    monkeypatch.setattr(_outbox.Outbox, "_session", session)
    out = _outbox.Outbox("ws://unused/trades", "p", store)
    await asyncio.wait_for(out._run(), timeout=1)
    with pytest.raises(type(error)) as caught:
        async with store.transaction() as tx:
            await out.send(trade(0), tx=tx)

    assert caught.value is error
    with pytest.raises(type(error)):
        await out.close()

    with contextlib.suppress(Exception):
        await store.close()
