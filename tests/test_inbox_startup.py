"""Startup pause: identity validation, durable recount and the existing receiver.

Hooks force ACK/startup interleavings without timing assumptions.
"""

from __future__ import annotations

import asyncio

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


@pytest.mark.parametrize("bound", ["rows", "bytes", "both"])
async def test_restored_pause_waits_at_equality_and_recovers(
    serve, log, opener, monkeypatch, bound
):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    size = len(_inbox._ENCODE.encode(trade(0)))
    max_rows = 2 if bound in ("rows", "both") else 100
    max_bytes = 2 * size if bound in ("bytes", "both") else 1_000_000
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=max_rows,
            inbox_max_bytes=max_bytes,
            **FAST,
        ) as first:
            await stream.send_many([trade(0)] * 3)
            await until(lambda: paused(first))
            assert await held(store) == [1, 2]
            assert (await first.recv())[0] == 1
            await first.commit()

        checked = asyncio.Event()
        original = _inbox.Inbox._below_low_water

        def boundary(inbox):
            result = original(inbox)
            if inbox.last_received == 2:
                assert not result, "equality must hold the restored pause"
                checked.set()

            return result

        monkeypatch.setattr(_inbox.Inbox, "_below_low_water", boundary)
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            initially_paused=True,
            inbox_max_rows=max_rows,
            inbox_max_bytes=max_bytes,
            **FAST,
        ) as restored:
            await asyncio.wait_for(checked.wait(), 5)
            assert restored.paused and not restored.caught_up()
            await until(lambda: no_subscribers(stream))
            assert await held(store) == [2]
            assert await received(store) == 2
            await asyncio.wait_for(restored.close(), 5)
            assert not any(
                task.get_name() == "streamcast-inbox-trades" and not task.done()
                for task in asyncio.all_tasks()
            )
            assert await held(store) == [2]

        checked.clear()
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            initially_paused=True,
            inbox_max_rows=max_rows,
            inbox_max_bytes=max_bytes,
            **FAST,
        ) as restored:
            await asyncio.wait_for(checked.wait(), 5)
            assert (
                sum(
                    task.get_name() == "streamcast-inbox-trades" and not task.done()
                    for task in asyncio.all_tasks()
                )
                == 1
            )
            monkeypatch.setattr(_inbox.Inbox, "_below_low_water", original)
            assert (await restored.recv())[0] == 2
            await restored.commit()
            assert (await asyncio.wait_for(restored.recv(), 5))[0] == 3
            await restored.commit()
            await stream.send(trade(0))
            assert (await asyncio.wait_for(restored.recv(), 5))[0] == 4

    assert await held(store) == []
    assert await received(store) == 4
    await store.close()


async def test_startup_recounts_an_ack_during_identity_connection(
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
        original_below = _inbox.Inbox._below_low_water
        connected = 0
        checked = asyncio.Event()

        async def ack_at_connect(inbox, start):
            nonlocal connected
            connected += 1
            sub = await original_connect(inbox, start)
            if connected == 1:
                async with store.transaction() as tx:
                    await tx.execute("DELETE FROM streamcast_inbox")

            return sub

        def below(inbox):
            assert inbox._rows == 0 and inbox._bytes == 0
            checked.set()
            return original_below(inbox)

        monkeypatch.setattr(_inbox.Inbox, "_connect", ack_at_connect)
        monkeypatch.setattr(_inbox.Inbox, "_below_low_water", below)
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            inbox_max_rows=2,
            initially_paused=True,
            **FAST,
        ) as restored:
            await asyncio.wait_for(checked.wait(), 5)
            await stream.send(trade(0))
            assert (await asyncio.wait_for(restored.recv(), 5))[0] == 3

    assert await received(store) == 3
    await store.close()


async def test_restored_pause_refuses_changed_identity(serve, log, opener):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, initially_paused=True, **FAST
        ):
            pass

        async with store.transaction() as tx:
            await tx.execute(
                "UPDATE streamcast_subscriber_state SET streamcast_stream_id = 'old'"
            )

        with pytest.raises(streamcast.StreamMismatch, match="old"):
            async with streamcast.inbox(
                uri, subscriber="s", store=store, initially_paused=True, **FAST
            ):
                pytest.fail("identity must be checked before startup returns")

        assert await held(store) == []
        assert await received(store) == 0
        await until(lambda: no_subscribers(stream))

    await store.close()


async def paused(inbox):
    return inbox.paused


async def no_subscribers(stream):
    return stream.subscribers == 0


@pytest.mark.parametrize("seed", [None, False])
async def test_default_does_not_restore_a_pause(serve, log, opener, seed):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=2, **FAST
        ) as first:
            await stream.send_many([trade(0)] * 3)
            await until(lambda: paused(first))
            await first.recv()
            await first.commit()

        options = {} if seed is None else {"initially_paused": seed}
        async with streamcast.inbox(
            uri, subscriber="s", store=store, inbox_max_rows=2, **options, **FAST
        ) as restored:
            await until(lambda: received_at(store, 3))
            assert await held(store) == [2, 3]
            await until(lambda: paused(restored))

        assert not any(
            task.get_name() == "streamcast-inbox-trades" and not task.done()
            for task in asyncio.all_tasks()
        )

    await store.close()


async def test_startup_recount_failure_is_surfaced(serve, log, opener, monkeypatch):
    store = await opener()
    stream = streamcast.Stream("trades", log=log)
    original = _inbox.Inbox._count
    counts = 0

    async def fail_recount(inbox, tx):
        nonlocal counts
        counts += 1
        if counts == 2:
            raise RuntimeError("recount failed")

        return await original(inbox, tx)

    monkeypatch.setattr(_inbox.Inbox, "_count", fail_recount)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, initially_paused=True, **FAST
        ) as restored:
            with pytest.raises(RuntimeError, match="recount failed"):
                await asyncio.wait_for(restored.recv(), 5)

        assert await held(store) == []
        assert await received(store) == 0
        monkeypatch.setattr(_inbox.Inbox, "_count", original)
        async with streamcast.inbox(
            uri, subscriber="s", store=store, initially_paused=True, **FAST
        ) as recovered:
            await stream.send(trade(0))
            assert (await asyncio.wait_for(recovered.recv(), 5))[0] == 1

    await store.close()


async def received_at(store, offset):
    return await received(store) == offset
