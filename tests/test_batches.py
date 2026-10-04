"""`recv_many` and `batches`: what has arrived, in one batch, never waiting for more.

Most of these run against a fake connection, because "already arrived" is
exactly what a real socket makes a matter of timing. The fake behaves as
`websockets` does where it matters: a buffered frame comes back without
suspending, an empty queue waits, and a cancelled `recv` loses nothing. The
tests against a real server assert only what timing cannot change: every row,
once, in order, within the limit.
"""

from __future__ import annotations

import asyncio
import collections

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close as CloseFrame

import streamcast
from streamcast._client import Subscription
from streamcast._errors import Close
from streamcast._protocol import encode_projected, greeting, parse_greeting, refusal


class FakeConnection:
    """The two things `Subscription` asks of a connection: `recv` and `close`."""

    def __init__(self) -> None:
        self._frames: collections.deque[bytes] = collections.deque()
        self._closed: ConnectionClosed | None = None
        self._arrived = asyncio.Event()
        self.reads = 0

    def arrive(self, *offsets: int) -> None:
        for offset in offsets:
            self._frames.append(encode_projected(offset, 0, {"i": offset}))

        self._arrived.set()

    def end(self, code: int = 1000, reason: str = "") -> None:
        self._closed = ConnectionClosed(CloseFrame(code, reason), None)
        self._arrived.set()

    async def recv(self, decode: bool | None = None) -> bytes:  # noqa: ARG002
        while not self._frames:
            if self._closed is not None:
                raise self._closed

            self._arrived.clear()
            await self._arrived.wait()  # cancelling here takes nothing

        self.reads += 1
        return self._frames.popleft()

    async def close(self, code: int = 1000, reason: str = "") -> None:  # noqa: ARG002
        self.end(code)


def subscription(
    connection: FakeConnection, cursor: streamcast.Cursor | None = None
) -> Subscription:
    info = parse_greeting(greeting(stream="t", end_offset=1, replay=None, durable=True))
    return Subscription(connection, info, "t", cursor)  # ty: ignore[invalid-argument-type]


def offsets(batch: list) -> list[int]:
    return [offset for offset, _ts, _row in batch]


class TestWhatABatchHolds:
    async def test_everything_already_arrived_comes_as_one_batch(self):
        connection = FakeConnection()
        connection.arrive(1, 2, 3, 4, 5)
        sub = subscription(connection)

        assert offsets(await sub.recv_many(limit=10)) == [1, 2, 3, 4, 5]

    async def test_a_batch_stops_at_the_limit(self):
        connection = FakeConnection()
        connection.arrive(1, 2, 3, 4, 5)
        sub = subscription(connection)

        got = [offsets(await sub.recv_many(limit=2)) for _ in range(3)]
        assert got == [[1, 2], [3, 4], [5]]

    async def test_it_waits_for_the_first_row_and_never_for_more(self):
        connection = FakeConnection()
        sub = subscription(connection)

        waiting = asyncio.ensure_future(sub.recv_many(limit=10))
        await asyncio.sleep(0.05)
        assert not waiting.done()  # nothing has arrived: it waits

        connection.arrive(1)
        # One row arrived and nothing behind it: a batch of one, now.
        assert offsets(await asyncio.wait_for(waiting, 1)) == [1]

    async def test_the_read_left_waiting_is_the_next_batchs_first_row(self):
        """Kept, not cancelled: nothing is lost between batches."""
        connection = FakeConnection()
        connection.arrive(1)
        sub = subscription(connection)

        assert offsets(await sub.recv_many()) == [1]
        connection.arrive(2, 3)
        assert offsets(await sub.recv_many()) == [2, 3]
        connection.arrive(4)
        assert (await sub.recv())[0] == 4  # `recv` takes it too

    def test_a_batch_holds_at_least_one_row(self):
        sub = subscription(FakeConnection())
        with pytest.raises(ValueError, match="limit=0"):
            asyncio.run(sub.recv_many(limit=0))


class TestWhatTheCallerHasSeen:
    """A row read ahead is not a row delivered: `offset`, the cursor and a
    refusal's resume point count only what the caller was handed."""

    async def test_a_batch_is_saved_when_the_next_is_asked_for(self, tmp_path):
        cursor = streamcast.Cursor(tmp_path / "cursor", every=0)
        connection = FakeConnection()
        connection.arrive(1, 2, 3)
        sub = subscription(connection, cursor)

        assert offsets(await sub.recv_many()) == [1, 2, 3]
        # Handed over, not yet known to be handled: nothing saved.
        assert cursor.load() is None

        connection.arrive(4)
        await sub.recv_many()
        assert cursor.load() == 3  # the whole first batch, and no further

    async def test_a_row_read_ahead_is_not_received(self, tmp_path):
        cursor = streamcast.Cursor(tmp_path / "cursor", every=0)
        connection = FakeConnection()
        connection.arrive(1)
        sub = subscription(connection, cursor)

        assert offsets(await sub.recv_many()) == [1]
        connection.arrive(2)
        await asyncio.sleep(0.01)  # the waiting read takes row 2 off the socket
        assert connection.reads == 2

        # But the caller never got it: it is not the offset, and a clean
        # exit's commit does not save it.
        assert sub.offset == 1
        sub.commit()
        assert cursor.load() == 1
        await sub.close()  # and closing with it still held is quiet

    async def test_a_failure_mid_batch_hands_over_the_rows_before_it(self):
        connection = FakeConnection()
        connection.arrive(1, 2)
        connection.end(Close.TOO_SLOW, refusal("too_slow", backlog=8))
        sub = subscription(connection)

        assert offsets(await sub.recv_many()) == [1, 2]
        with pytest.raises(streamcast.TooSlow) as raised:
            await sub.recv_many()

        # Built when raised, so it names the last row the caller was given:
        # resuming above it misses nothing and repeats nothing.
        assert raised.value.offset == 2

    async def test_offsets_must_still_increase_within_a_batch(self):
        connection = FakeConnection()
        connection.arrive(3, 2)
        sub = subscription(connection)

        assert offsets(await sub.recv_many()) == [3]
        with pytest.raises(streamcast.ProtocolError, match="must increase"):
            await sub.recv_many()


class TestTheLoop:
    async def test_batches_end_on_an_ordinary_close(self):
        connection = FakeConnection()
        connection.arrive(1, 2, 3)
        connection.end(1000)
        sub = subscription(connection)

        assert [offsets(b) async for b in sub.batches(limit=2)] == [[1, 2], [3]]

    async def test_a_handler_that_raises_leaves_its_batch_unsaved(self, tmp_path):
        cursor = streamcast.Cursor(tmp_path / "cursor", every=0)
        connection = FakeConnection()
        connection.arrive(1, 2)
        sub = subscription(connection, cursor)

        handled = []
        with pytest.raises(RuntimeError):
            async for batch in sub.batches():
                handled.append(offsets(batch))
                connection.arrive(3, 4)
                if offsets(batch) == [3, 4]:
                    msg = "the handler failed on this batch"
                    raise RuntimeError(msg)

        assert handled == [[1, 2], [3, 4]]
        assert cursor.load() == 2  # the batch it failed on is read again


class TestAgainstAServer:
    async def test_every_row_once_in_order_within_the_limit(self, tmp_path, serve):
        schema = {
            "type": "object",
            "properties": {"i": {"type": "integer"}},
            "required": ["i"],
        }
        stream = streamcast.Stream.new("t", root=tmp_path, schema=schema)
        await stream.send_many([{"i": i} for i in range(300)])
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                    got: list[list[int]] = []
                    async for batch in sub.batches(limit=64):
                        got.append(offsets(batch))
                        if got[-1][-1] == 300:
                            break

            assert [o for batch in got for o in batch] == list(range(1, 301))
            assert all(1 <= len(batch) <= 64 for batch in got)
        finally:
            await stream.aclose()
