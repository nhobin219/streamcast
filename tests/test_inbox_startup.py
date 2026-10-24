"""The durable window determines startup; a live receiver owns its socket."""

from __future__ import annotations

import asyncio
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _inbox

from .conftest import SCHEMA, trade
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
