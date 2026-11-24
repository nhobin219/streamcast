"""Database outages must not permanently poison a running sender."""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace

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

    def check(statement, params: tuple[object, ...] = ()):
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
                point == "ack"
                and statement.startswith("DELETE FROM streamcast_outbox")
                and params[-1] == 1
            )
        )
        if down or trigger:
            down = True
            raise failure("database temporarily unavailable; retry")

    async def failing_fetch(self, statement, params: tuple[object, ...] = ()):
        check(statement, params)
        return await fetch(self, statement, params)

    async def failing_execute(self, statement, params: tuple[object, ...] = ()):
        nonlocal deleting
        check(statement, params)
        if statement.startswith("DELETE FROM streamcast_outbox") and params[-1] == 1:
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


async def test_final_ack_commit_reply_loss_reconciles_without_another_enqueue(
    serve, log, tmp_path, monkeypatch
):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")
    async with store.transaction() as tx:
        await _outbox.enqueue(tx, "trades", "p", [trade(0)])

    execute = _store._SqliteTransaction.execute
    transaction = _store.SqliteStore.transaction
    deleting = set()
    injected = False
    empty_polls = 0
    observed = asyncio.Event()
    release = asyncio.Event()

    async def mark_final_ack(self, statement, params: tuple[object, ...] = ()):
        if statement.startswith("DELETE FROM streamcast_outbox") and params[-1] == 1:
            deleting.add(self)

        return await execute(self, statement, params)

    @contextlib.asynccontextmanager
    async def lose_final_commit_reply(self):
        nonlocal injected
        async with transaction(self) as tx:
            yield tx

        if tx in deleting and not injected:
            injected = True
            raise psycopg.OperationalError("lost reply after final ACK COMMIT")

    async def observe_empty_polls(self, seconds):
        nonlocal empty_polls
        if injected and seconds == self._poll_every:
            empty_polls += 1
            if empty_polls == 3:
                observed.set()
                await release.wait()

    stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
    async with serve(stream, maintain=False) as uri:
        out = _outbox.Outbox(uri, "p", store, poll_every=0.01, backoff=(0.1,))
        waiter = asyncio.create_task(out.landed(1))
        try:
            await asyncio.wait_for(out._wake.wait(), timeout=3)
            assert 1 in out._waiting, "register before delivery starts"
            monkeypatch.setattr(_store._SqliteTransaction, "execute", mark_final_ack)
            monkeypatch.setattr(
                _store.SqliteStore, "transaction", lose_final_commit_reply
            )
            monkeypatch.setattr(_outbox.Outbox, "_sleep", observe_empty_polls)
            out.start()
            await asyncio.wait_for(observed.wait(), timeout=3)
            assert injected and empty_polls == 3
            assert await out.pending_rows() == 0
            assert landed(log) == [0.0]
            assert waiter.done(), "reconcile the final ACK without another enqueue"
            assert await waiter is None, "greeting reconciliation has no saved offset"
            assert out._waiting == {}
            assert out._failure is None
        finally:
            release.set()
            await out.close()
            await asyncio.gather(waiter, return_exceptions=True)
            await store.close()


async def test_idle_recovery_resets_notice_backoff_and_unknown_error_budget(
    tmp_path, monkeypatch
):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")
    outcomes = iter(
        [
            psycopg.OperationalError("outage one"),
            psycopg.OperationalError("still down after two minutes"),
            False,
            psycopg.OperationalError("another outage much later"),
            False,
        ]
    )
    clock = iter([0.0, 120.0, 600.0])
    monkeypatch.setattr(_outbox, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    said = []
    delays = []
    idle = 0

    async def has_rows(self):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome

        return outcome

    async def sleep(self, seconds):
        nonlocal idle
        delays.append(seconds)
        if seconds == self._poll_every:
            idle += 1
            if idle == 2:
                self._closed = True

    monkeypatch.setattr(_outbox.Outbox, "_has_rows", has_rows)
    monkeypatch.setattr(_outbox.Outbox, "_sleep", sleep)
    out = _outbox.Outbox(
        "ws://unused/trades",
        "p",
        store,
        poll_every=0.7,
        backoff=(0.1, 0.2),
        notice=said.append,
    )
    await asyncio.wait_for(out._run(), timeout=1)
    assert out._failure is None
    assert not out._outage
    assert delays == [0.1, 0.2, 0.7, 0.1, 0.7]
    assert len(said) == 4
    assert "cannot publish" in said[0] and "outage one" in said[0]
    assert "publishing again" in said[1]
    assert "cannot publish" in said[2] and "another outage" in said[2]
    assert "publishing again" in said[3]
    await out.close()
    await store.close()


@pytest.mark.parametrize("point", ["poll", "session"])
async def test_real_sqlstateless_connect_failure_surfaces_after_grace(
    postgres_dsn, monkeypatch, point
):
    store = await streamcast.PostgresStore.connect(postgres_dsn)
    await store._connection.close()
    connect = store._connect
    errors = []

    async def connect_to_missing_database():
        try:
            return await psycopg.AsyncConnection.connect(
                postgres_dsn + " dbname=missing_database"
            )
        except psycopg.OperationalError as exc:
            assert exc.sqlstate is None, "capture the real connect-time exception"
            errors.append(exc)
            raise

    store._connect = connect_to_missing_database
    now = [0.0]
    monkeypatch.setattr(_outbox, "time", SimpleNamespace(monotonic=lambda: now[0]))
    elapsed = iter([120.0, 240.0, 300.0, 360.0])
    delays = []

    async def sleep(self, seconds):
        delays.append(seconds)
        now[0] = next(elapsed)
        if len(delays) == 4:
            # Bound the unfixed control without a timing-dependent timeout.
            self._closed = True

    async def has_rows(self):
        return True

    async def session(self):
        async with store.transaction():
            pytest.fail("the missing database accepted a connection")

    if point == "session":
        monkeypatch.setattr(_outbox.Outbox, "_has_rows", has_rows)
        monkeypatch.setattr(_outbox.Outbox, "_session", session)

    monkeypatch.setattr(_outbox.Outbox, "_sleep", sleep)
    out = _outbox.Outbox("ws://unused/trades", "p", store, backoff=(0.1, 0.2))
    await asyncio.wait_for(out._run(), timeout=3)
    try:
        assert len(errors) == 4
        assert isinstance(out._failure, RuntimeError)
        assert "300 seconds" in str(out._failure)
        assert out._failure.__cause__ is errors[-1]
        assert errors[-1] is not errors[0]
        assert delays == [0.1, 0.2, 0.2]
        store._connect = connect
        with pytest.raises(RuntimeError, match="check credentials"):
            async with store.transaction() as tx:
                await out.send(trade(0), tx=tx)

        with pytest.raises(RuntimeError, match="reopen the outbox"):
            await out.close()
    finally:
        await store.close()
