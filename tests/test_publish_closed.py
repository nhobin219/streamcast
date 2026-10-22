"""A send on a connection that is closing or closed raises `ConnectionClosed`.

That is the documented contract a publisher reconnects on, so nothing else
may surface in its place. Two ways something else did:

- **websockets' own abort.** On a connection no longer open, `send_context`
  aborts the transport before raising `ConnectionClosed`. On an asyncio
  transport whose `connection_lost` has already run, `abort()` raises
  `AttributeError` ('NoneType' object has no attribute 'call_soon'), and that
  is what the send raised.
- **The reader draining the pending replies first.** A send waiting inside
  `connection.send` on a closing connection raises only once the connection
  is lost. By then the reader has failed and removed every pending future,
  the send's own included. Removing "the last entry" then raised
  `IndexError('pop from an empty deque')`, and the future's exception was
  never retrieved.
"""

from __future__ import annotations

import asyncio
import gc
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed

import streamcast

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}


@pytest.fixture
async def stream(tmp_path):
    made = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
    try:
        yield made
    finally:
        await made.aclose()


@pytest.fixture
async def unretrieved():
    """What asyncio reports as never retrieved while the test runs, collected
    at its end."""
    loop = asyncio.get_running_loop()
    reported: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))

    def collected() -> list[dict[str, Any]]:
        gc.collect()
        return [c for c in reported if "never retrieved" in str(c.get("message"))]

    try:
        yield collected
    finally:
        loop.set_exception_handler(previous)


class TestTheAbortItself:
    @pytest.mark.parametrize("call", ["send", "submit", "send_many", "submit_many"])
    async def test_a_failing_abort_raises_connection_closed(
        self, stream, serve, monkeypatch, call
    ):
        async with serve(stream, maintain=False) as uri:
            producer = await streamcast.publish(uri)
            assert await producer.send({"i": 1}) == 1
            await producer.connection.close()

            # asyncio's selector transport after a close with data still
            # buffered: `connection_lost` ran, its loop is None, and its abort
            # raises — what websockets' `send_context` meets (#106).
            def abort() -> None:
                raise AttributeError("'NoneType' object has no attribute 'call_soon'")

            monkeypatch.setattr(producer.connection.transport, "abort", abort)

            row = {"i": 2}
            with pytest.raises(ConnectionClosed) as raised:
                if call in {"send", "submit"}:
                    await getattr(producer, call)(row)
                else:
                    await getattr(producer, call)([row])

            assert isinstance(raised.value.__cause__, AttributeError)


class TestTheReaderGotThereFirst:
    async def test_the_send_raises_connection_closed(self, stream, serve, unretrieved):
        async with serve(stream, maintain=False) as uri:
            producer = await streamcast.publish(uri)
            assert await producer.send({"i": 1}) == 1  # the reader is running
            reader = producer._reader  # noqa: SLF001
            assert reader is not None

            connection = producer.connection
            original = connection.send

            async def send(frame: str, *, text: bool) -> None:
                # The interleaving #154 saw: the connection is lost while this
                # send waits, and the reader fails every pending reply —
                # this send's own included — before the send raises.
                await connection.close()
                await reader
                await original(frame, text=text)

            connection.send = send
            with pytest.raises(ConnectionClosed):
                await producer.send({"i": 2})

            connection.send = original

        assert unretrieved() == [], "the dropped reply's failure is retrieved"
