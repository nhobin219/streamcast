"""The durable window determines startup; a live receiver owns its socket."""

from __future__ import annotations

import asyncio
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _inbox

from .conftest import SCHEMA, trade
from .hosting import serve_streams
from .test_inbox import FAST, held, received, until


@pytest.fixture
def log(tmp_path):
    with litelink.new(tmp_path / "data", "trades", schema=SCHEMA) as handle:
        yield handle


async def paused(inbox):
    return inbox.paused


async def no_subscribers(stream):
    return stream.subscribers == 0


async def received_at(store, offset):
    return await received(store) == offset


@pytest.mark.parametrize("bound", ["rows", "bytes", "both"])
async def test_full_startup_pauses_and_low_water_equality_waits(
    serve, log, opener, monkeypatch, bound
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    size = len(_inbox._ENCODE.encode(trade(0)))
    limits: dict[str, Any] = {
        "inbox_max_rows": 2 if bound in ("rows", "both") else 100,
        "inbox_max_bytes": 2 * size if bound in ("bytes", "both") else 1_000_000,
    }
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, **limits, **FAST
        ) as first:
            await stream.send_many([trade(0)] * 3)
            await until(lambda: paused(first))

        checked = asyncio.Event()
        original = _inbox.Inbox._below_low_water

        def boundary(inbox):
            below = original(inbox)
            if inbox._rows == 1:
                assert not below, "low-water equality must hold the pause"
                checked.set()

            return below

        monkeypatch.setattr(_inbox.Inbox, "_below_low_water", boundary)
        async with streamcast.inbox(
            uri, subscriber="s", store=store, **limits, **FAST
        ) as restored:
            assert restored.paused and not restored.caught_up()
            await until(lambda: no_subscribers(stream))
            assert await received(store) == 2
            assert (await restored.recv())[0] == 1
            await restored.commit()
            await asyncio.wait_for(checked.wait(), 5)
            assert restored.paused
            assert await held(store) == [2]
            assert await received(store) == 2
            assert (await restored.recv())[0] == 2
            await restored.commit()
            assert (await asyncio.wait_for(restored.recv(), 5))[0] == 3
            await restored.commit()

    assert await held(store) == []
    assert await received(store) == 3
    await store.close()


@pytest.mark.parametrize("remaining", [1, 2, 3])
async def test_startup_below_high_is_active_including_low_water_equality(
    serve, log, opener, remaining
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=4, **FAST
        ) as first:
            await stream.send_many([trade(0)] * 5)
            await until(lambda: paused(first))
            for _ in range(4 - remaining):
                await first.recv()
                await first.commit()

        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=4, **FAST
        ) as restored:
            assert not restored.paused
            await until(lambda: received_at(store, 5))
            assert len(await held(store)) == remaining + 1

    await store.close()


async def test_startup_recounts_processing_during_identity_connection(
    serve, log, opener, monkeypatch
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=2, **FAST
        ) as first:
            await stream.send_many([trade(0)] * 2)
            await until(lambda: paused(first))

        original_connect = _inbox.Inbox._connect

        async def process_at_connect(inbox, start):
            sub = await original_connect(inbox, start)
            async with store.transaction() as tx:
                await tx.execute("DELETE FROM streamcast_inbox")

            return sub

        monkeypatch.setattr(_inbox.Inbox, "_connect", process_at_connect)
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=2, **FAST
        ) as restored:
            assert not restored.paused
            assert restored._rows == 0 and restored._bytes == 0
            await stream.send(trade(0))
            assert (await asyncio.wait_for(restored.recv(), 5))[0] == 3

    assert await received(store) == 3
    await store.close()


async def test_full_startup_refuses_changed_identity(serve, log, opener):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=1, **FAST
        ) as first:
            await stream.send(trade(0))
            await until(lambda: paused(first))

        async with store.transaction() as tx:
            await tx.execute(
                "UPDATE streamcast_subscriber_state SET streamcast_stream_id = 'old'"
            )

        with pytest.raises(streamcast.StreamMismatch, match="old"):
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_rows=1, **FAST
            ):
                pytest.fail("identity must be checked before startup returns")

        assert await held(store) == [1]
        assert await received(store) == 1
        await until(lambda: no_subscribers(stream))

    await store.close()


@pytest.mark.parametrize("cancel", [False, True])
async def test_immediate_exit_before_receiver_runs_closes_owned_subscription(
    serve, log, opener, monkeypatch, cancel
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    original = _inbox.Inbox._receive
    entered = False

    async def receive(*args):
        nonlocal entered
        entered = True
        await original(*args)

    monkeypatch.setattr(_inbox.Inbox, "_receive", receive)
    async with serve(stream, maintain=False) as uri:
        manager = streamcast.inbox(uri, subscriber="s", store=store, **FAST)
        receiver = await manager.__aenter__()
        assert receiver._task is not None
        if cancel:
            receiver._task.cancel()

        await manager.__aexit__(None, None, None)
        assert not entered, "this exercises cancellation before the first turn"
        await until(lambda: no_subscribers(stream))
        assert await held(store) == []
        assert await received(store) == 0
        assert receiver._task.done()
        async with streamcast.inbox(
            uri, subscriber="s", store=store, **FAST
        ) as restarted:
            await stream.send(trade(0))
            assert (await asyncio.wait_for(restarted.recv(), 5))[0] == 1

    await store.close()


class Recorder:
    """An `on_pause_change` that records each call, and checks that
    `Inbox.paused` already says the same when it is called."""

    def __init__(self) -> None:
        self.calls: list[bool] = []
        self.inbox: Any = None

    def __call__(self, paused: bool) -> None:
        if self.inbox is not None:
            assert self.inbox.paused is paused

        self.calls.append(paused)


async def calls_are(recorder, expected):
    return recorder.calls == expected


async def fill(serve_uri, stream, store, rows, **limits):
    """Run an inbox until `rows` are received and it has paused, then leave."""
    async with streamcast.inbox(
        serve_uri, subscriber="s", store=store, **limits, **FAST
    ) as first:
        await stream.send_many([trade(0)] * rows)
        await until(lambda: paused(first))


@pytest.mark.parametrize(
    ("held_rows", "max_rows", "starts_paused"),
    [(4, 2, True), (2, 2, True), (1, 2, False)],
    ids=["over", "at", "under"],
)
async def test_the_starting_state_is_reported_once_before_inbox_returns(
    serve, log, opener, held_rows, max_rows, starts_paused
):
    """Over and at the high bound start paused, under it active; either way
    the callback has been called exactly once, with that state, by the time
    `inbox()` hands the object back. "Over" is a restart with a smaller
    bound than the run that filled it."""
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        # A run bounded at `held_rows` leaves that many unprocessed.
        await fill(uri, stream, store, held_rows + 1, inbox_max_rows=held_rows)

        recorder = Recorder()
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=max_rows,
            on_pause_change=recorder,
            **FAST,
        ) as sub:
            assert recorder.calls == [starts_paused]
            assert sub.paused is starts_paused

    await store.close()


async def test_each_transition_is_reported_once_and_in_order(serve, log, opener):
    """Active at start, paused at the high bound, resumed strictly below
    `inbox_resume_at` of it, paused again: four calls, no repeats, and
    `Inbox.paused` agrees at each."""
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    recorder = Recorder()
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=2,
            on_pause_change=recorder,
            **FAST,
        ) as sub:
            recorder.inbox = sub
            assert recorder.calls == [False]
            await stream.send_many([trade(0)] * 4)
            await until(lambda: calls_are(recorder, [False, True]))
            # Low water is 1 row (half of 2): one processed leaves 1, which is
            # not strictly below it; the second leaves 0 and resumes.
            await sub.recv()
            await sub.commit()
            await asyncio.sleep(0.3)
            assert recorder.calls == [False, True]
            await sub.recv()
            await sub.commit()
            await until(lambda: calls_are(recorder, [False, True, False, True]))

        # Closing while paused is not a resume.
        assert recorder.calls == [False, True, False, True]

    await store.close()


async def test_inbox_resume_at_sets_where_a_startup_pause_ends(serve, log, opener):
    """Restarted at the high bound of 4 with `inbox_resume_at=0.75`: 3 held
    rows is exactly low water and stays paused; 2 is below it and resumes."""
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    recorder = Recorder()
    async with serve(stream, maintain=False) as uri:
        await fill(uri, stream, store, 6, inbox_max_rows=4)
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=4,
            inbox_resume_at=0.75,
            on_pause_change=recorder,
            **FAST,
        ) as sub:
            recorder.inbox = sub
            assert recorder.calls == [True]
            await sub.recv()
            await sub.commit()
            await asyncio.sleep(0.3)
            assert recorder.calls == [True], "3 of 4 is not below 0.75 of 4"
            await sub.recv()
            await sub.commit()
            await until(lambda: received_at(store, 6))
            # Resumed, then rows 5 and 6 bring it back to the high bound.
            assert recorder.calls == [True, False, True]

    await store.close()


async def test_no_callback_is_the_same_receiver(serve, log, opener):
    """`on_pause_change=None` — the default — through a startup pause and a
    resume: nothing is called, and the rows still arrive."""
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        await fill(uri, stream, store, 3, inbox_max_rows=2)
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=2,
            on_pause_change=None,
            **FAST,
        ) as sub:
            assert sub.paused
            for expected in (1, 2, 3):
                assert (await asyncio.wait_for(sub.recv(), 5))[0] == expected
                await sub.commit()

            assert not sub.paused

    await store.close()


async def test_a_callback_that_raises_at_start_fails_inbox_and_closes_the_socket(
    serve, log, opener
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)

    def refuse(paused: bool) -> None:
        raise RuntimeError("callback failed")

    async with serve(stream, maintain=False) as uri:
        with pytest.raises(RuntimeError, match="callback failed"):
            async with streamcast.inbox(
                uri, subscriber="s", store=store, on_pause_change=refuse, **FAST
            ):
                pytest.fail("inbox() must not return")

        await until(lambda: no_subscribers(stream))

    await store.close()


async def test_a_dropped_connection_in_the_band_reconnects_at_once(log, opener):
    """Receiving with 3 of 4 rows held, the broker drops the connection: the
    inbox is not paused, so it reconnects and receives at once rather than
    waiting for low water while saying it is receiving.

    Falsify by waiting for low water after every drop: row 4 never arrives.
    """
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    recorder = Recorder()
    async with serve_streams([stream], "127.0.0.1", 0, maintain=False) as server:
        uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/trades"
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=4,
            on_pause_change=recorder,
            **FAST,
        ) as sub:
            recorder.inbox = sub
            await stream.send_many([trade(0)] * 3)
            await until(lambda: received_at(store, 3))
            dropped = set(server.connections)
            for peer in dropped:
                await peer.close(1001, "going away")

            async def reconnected() -> bool:
                return bool(set(server.connections) - dropped)

            await until(reconnected)
            await stream.send(trade(0))
            await until(lambda: received_at(store, 4))
            assert recorder.calls == [False, True]

    await store.close()


async def test_a_callback_that_raises_in_the_receiver_fails_recv(serve, log, opener):
    """The receiver's own call raising: rows already received are still
    handed out, then `recv` raises it rather than waiting for ever."""
    store = await opener()
    stream = streamcast.Stream("trades", log=log)

    def refuse_pause(paused: bool) -> None:
        if paused:
            raise RuntimeError("callback failed")

    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=1,
            on_pause_change=refuse_pause,
            **FAST,
        ) as sub:
            await stream.send_many([trade(0)] * 2)
            assert (await asyncio.wait_for(sub.recv(), 5))[0] == 1
            await sub.commit()
            with pytest.raises(RuntimeError, match="callback failed"):
                await asyncio.wait_for(sub.recv(), 5)

    await store.close()
