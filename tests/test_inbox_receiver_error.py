"""Terminal receiver failures notify observers; reconnects and shutdown do not."""

from __future__ import annotations

import asyncio
import logging

import pytest
from websockets.exceptions import ConnectionClosed

import streamcast
from streamcast import _inbox

from .conftest import trade


async def test_terminal_failure_notifies_once_after_storing_the_error(
    serve, log, opener, monkeypatch
):
    failure = ValueError("receiver failed")
    observed = []

    async def fail(inbox, subscription):
        raise failure

    def on_error(error):
        assert sub._failure is error
        assert sub._arrived.is_set()
        observed.append(error)

    monkeypatch.setattr(_inbox.Inbox, "_fill", fail)
    store = await opener()
    stream = streamcast.StreamHandle("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri, subscriber="s", store=store, on_receiver_error=on_error
        ) as sub:
            assert observed == []
            assert sub._task is not None
            await sub._task
            assert observed == [failure]
            for _ in range(2):
                with pytest.raises(ValueError) as raised:
                    await sub.recv()

                assert raised.value is failure

    assert observed == [failure], "recv and close never repeat the notification"
    await store.close()


@pytest.mark.parametrize("during_reconnect", [False, True])
@pytest.mark.parametrize(
    "error_type", [OSError, ConnectionClosed, TimeoutError, streamcast.TooSlow]
)
async def test_recoverable_disconnects_do_not_notify(
    serve, log, opener, monkeypatch, during_reconnect, error_type
):
    failure = (
        ConnectionClosed(None, None) if error_type is ConnectionClosed else error_type()
    )
    observed = []
    connected = asyncio.Event()
    fill = _inbox.Inbox._fill
    reconnect = _inbox.Inbox._reconnect
    fills = reconnects = 0

    async def disconnect(inbox, subscription):
        nonlocal fills
        fills += 1
        if fills == 1:
            raise failure

        return await fill(inbox, subscription)

    async def reconnect_once(inbox):
        nonlocal reconnects
        reconnects += 1
        if during_reconnect and reconnects == 1:
            raise failure

        subscription = await reconnect(inbox)
        connected.set()
        return subscription

    monkeypatch.setattr(_inbox.Inbox, "_fill", disconnect)
    monkeypatch.setattr(_inbox.Inbox, "_reconnect", reconnect_once)
    store = await opener()
    stream = streamcast.StreamHandle("trades", log=log)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.inbox(
            uri,
            subscriber="s",
            store=store,
            on_receiver_error=observed.append,
            backoff=(0,),
        ) as sub:
            await asyncio.wait_for(connected.wait(), 5)
            assert reconnects == (2 if during_reconnect else 1)
            await stream.send(trade(0))
            assert (await sub.recv())[0] == 1
            assert sub._failure is None
            assert observed == []

    assert observed == [], "normal close and cancellation never notify"
    await store.close()


async def test_raising_error_callback_is_logged_without_failing_the_receiver_task(
    serve, log, opener, monkeypatch, caplog
):
    failure = ValueError("receiver failed")
    callback_failure = RuntimeError("observer failed")
    observed = []

    async def fail(inbox, subscription):
        raise failure

    def on_error(error):
        observed.append(error)
        raise callback_failure

    monkeypatch.setattr(_inbox.Inbox, "_fill", fail)
    store = await opener()
    stream = streamcast.StreamHandle("trades", log=log)
    with caplog.at_level(logging.ERROR, logger="streamcast"):
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, on_receiver_error=on_error
            ) as sub:
                assert sub._task is not None
                await sub._task  # the observer's exception must not escape the task
                with pytest.raises(ValueError) as raised:
                    await sub.recv()

                assert raised.value is failure
                assert sub._failure is failure

    assert observed == [failure]
    records = [record for record in caplog.records if record.name == "streamcast"]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert "on_receiver_error" in records[0].getMessage()
    assert records[0].exc_info[1] is callback_failure
    await store.close()
