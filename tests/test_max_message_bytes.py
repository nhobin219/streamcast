"""A per-stream maximum message size, beside the other per-stream bounds.

The frame limit used to be websockets' `max_size`, one for the whole server.
A server hosting a stream of large rows beside one of small ones had to raise
it for every stream, so a publisher on the small stream could send frames as
large as the large one allows; or it kept 1 MiB, and a 1.1 MB batch to the
large stream was closed with 1009. `max_message_bytes` sets it per stream.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode

import streamcast
from tests.hosting import application as asgi
from tests.test_asgi import running

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"text": {"type": "string"}},
    "required": ["text"],
}


@pytest.fixture
async def streams(tmp_path, request):
    limits = getattr(request, "param", (1_000, 100_000))
    made = [
        streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(
                    name=name, schema=SCHEMA, max_message_bytes=limit
                ),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        for name, limit in zip(("small", "large"), limits, strict=True)
    ]
    try:
        yield made
    finally:
        for stream in made:
            await stream.aclose()


def row(size: int) -> dict[str, str]:
    return {"text": "x" * size}


def too_big(closed: ConnectionClosed, *, limit: int) -> None:
    """Closed as websockets closes a frame over its limit, naming the sizes."""
    assert closed.rcvd is not None, closed
    assert closed.rcvd.code == CloseCode.MESSAGE_TOO_BIG
    reason = json.loads(closed.rcvd.reason)
    assert reason["error"] == "too_big"
    assert reason["limit"] == limit
    assert reason["size"] > limit


class TestServe:
    async def test_each_stream_refuses_a_frame_over_its_own_limit(self, streams, serve):
        async with serve(*streams, maintain=False) as uri:
            base = uri.rsplit("/", 1)[0]
            small = await streamcast.publish(f"{base}/small")
            assert await small.send(row(10)) == 1
            with pytest.raises(ConnectionClosed) as raised:
                await small.send(row(5_000))

            too_big(raised.value, limit=1_000)
            async with streamcast.publish(f"{base}/large") as large:
                assert await large.send(row(5_000)) == 1

    async def test_the_frames_before_it_are_answered(self, streams, serve):
        # Below the transport's limit, so this stream's own check refuses it.
        async with serve(*streams, maintain=False) as uri:
            producer = await streamcast.publish(uri.rsplit("/", 1)[0] + "/small")
            before = [await producer.submit(row(10)) for _ in range(5)]
            with pytest.raises(ConnectionClosed):
                await producer.send(row(5_000))

            assert [await done for done in before] == [1, 2, 3, 4, 5]

    @pytest.mark.parametrize("streams", [(1_000, 2 * 2**20)], indirect=True)
    async def test_a_large_stream_takes_frames_over_the_old_server_limit(
        self, streams, serve
    ):
        # Past websockets' 1 MiB default, which used to close the connection.
        async with serve(
            *streams, maintain=False, transport_max_bytes=2 * 2**20
        ) as uri:
            base = uri.rsplit("/", 1)[0]
            async with streamcast.publish(f"{base}/large") as large:
                assert await large.send(row(3 * 2**19)) == 1

    @pytest.mark.parametrize("streams", [(1_000, 1_000)], indirect=True)
    async def test_equal_handle_limits_apply_to_every_stream(self, streams, serve):
        async with serve(*streams, maintain=False) as uri:
            base = uri.rsplit("/", 1)[0]
            for name in ("small", "large"):
                producer = await streamcast.publish(f"{base}/{name}")
                with pytest.raises(ConnectionClosed) as raised:
                    await producer.send(row(5_000))

                assert raised.value.rcvd is not None
                assert raised.value.rcvd.code == CloseCode.MESSAGE_TOO_BIG

    @pytest.mark.parametrize("streams", [(None, None)], indirect=True)
    async def test_transport_ceiling_applies_to_every_stream(self, streams, serve):
        # A server configured before this keyword behaves as it did: the
        # transport's limit, and websockets' close for a frame over it.
        async with serve(*streams, maintain=False, max_size=4_096) as uri:
            base = uri.rsplit("/", 1)[0]
            async with streamcast.publish(f"{base}/large") as producer:
                assert await producer.send(row(1_000)) == 1
                with pytest.raises(ConnectionClosed):
                    await producer.send(row(5_000))

    async def test_handle_limit_cannot_exceed_transport_ceiling(self, streams):
        with pytest.raises(ValueError, match="transport_max_bytes"):
            asgi(streams, transport_max_bytes=4_096)

        # App construction does not take ownership on a rejected definition.
        assert all(handle.log.end_offset() == 1 for handle in streams)


class TestAsgi:
    async def test_a_mounted_stream_refuses_a_frame_over_its_limit(self, streams):
        app = asgi(
            streams,
            maintain=False,
        )
        async with running(app) as base:
            small = await streamcast.publish(f"{base}/small")
            with pytest.raises(ConnectionClosed) as raised:
                await small.send(row(5_000))

            too_big(raised.value, limit=1_000)

            async with streamcast.publish(f"{base}/large") as large:
                assert await large.send(row(5_000)) == 1
