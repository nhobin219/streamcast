"""The raw WebSocket upgrade must not wait for replay storage."""

from __future__ import annotations

import asyncio
import json
import threading

from websockets.asyncio.client import connect

import streamcast
from tests.conftest import trade
from tests.hosting import application
from tests.test_asgi import running


async def test_upgrade_completes_while_replay_scan_is_blocked(log, monkeypatch):
    stream = streamcast.StreamHandle("trades", log=log)
    await stream.send_many([trade(i) for i in range(3)])
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    scan = log.scan

    def held_scan(**kwargs):
        loop.call_soon_threadsafe(entered.set)
        if not release.wait(5):
            raise RuntimeError("upgrade waited for replay scan")

        return scan(**kwargs)

    monkeypatch.setattr(log, "scan", held_scan)
    async with running(application(stream, maintain=False)) as base:
        try:
            # The timeout bounds a broken handshake; the event, not a sleep,
            # holds the storage read until the client has actually upgraded.
            async with connect(f"{base}/trades?offset=1", open_timeout=2) as raw:
                await asyncio.wait_for(entered.wait(), timeout=2)
                assert not release.is_set()
                await stream.send(trade(3))
                release.set()
                greeting = json.loads(await raw.recv())
                assert greeting["end_offset"] == 4
                assert greeting["replay"] == [1, 4]
                frames = [json.loads(await raw.recv()) for _ in range(4)]
                assert [frame[0] for frame in frames] == [1, 2, 3, 4]
                assert [frame[2]["event_ts"] for frame in frames] == [
                    trade(i)["event_ts"] for i in range(4)
                ]

        finally:
            release.set()
