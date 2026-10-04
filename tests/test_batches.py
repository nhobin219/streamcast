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
from types import SimpleNamespace

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close as CloseFrame
from websockets.frames import Frame, Opcode

import streamcast
from streamcast._client import Subscription, _arrived
from streamcast._errors import Close
from streamcast._protocol import encode_projected, greeting, parse_greeting, refusal

SCHEMA = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}


class FakeConnection:
    """What `Subscription` asks of a connection: `recv`, `close`, and a look
    at the queue of frames, at the same private path `websockets` keeps it.

    `test_the_queue_it_looks_at_is_websockets` holds that path to the real one.
    """

    def __init__(self) -> None:
        self._frames: collections.deque[Frame] = collections.deque()
        self.recv_messages = SimpleNamespace(frames=SimpleNamespace(queue=self._frames))
        self._closed: ConnectionClosed | None = None
        self._arrived = asyncio.Event()
        self.reads = 0

    def arrive(self, *offsets: int) -> None:
        for offset in offsets:
            data = encode_projected(offset, 0, {"i": offset})
            self._frames.append(Frame(Opcode.TEXT, data))

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
        # A fragmented message, reassembled as `websockets` does.
        parts = [self._frames.popleft()]
        while not parts[-1].fin:
            parts.append(self._frames.popleft())

        return b"".join(bytes(part.data) for part in parts)

    def arrive_fragmented(self, offset: int) -> None:
        """One message in two frames: whole once the second is here."""
        data = encode_projected(offset, 0, {"i": offset})
        self._frames.append(Frame(Opcode.TEXT, data[:5], fin=False))
        self._frames.append(Frame(Opcode.CONT, data[5:]))
        self._arrived.set()

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

    async def test_a_fragmented_message_ends_the_batch_and_loses_nothing(self):
        """Only the head frame is looked at, so a message in fragments reads as
        not arrived: the batch ends before it, and the next one starts with it."""
        connection = FakeConnection()
        connection.arrive(1)
        connection.arrive_fragmented(2)
        connection.arrive(3)
        sub = subscription(connection)

        assert offsets(await sub.recv_many()) == [1]
        assert offsets(await sub.recv_many()) == [2, 3]

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

    async def test_from_the_socket_nothing_is_read_ahead(self, tmp_path):
        """A row that has not arrived is never started: between batches,
        everything not handed over is still in `websockets`' queue."""
        cursor = streamcast.Cursor(tmp_path / "cursor", every=0)
        connection = FakeConnection()
        connection.arrive(1)
        sub = subscription(connection, cursor)

        assert offsets(await sub.recv_many()) == [1]
        connection.arrive(2)
        await asyncio.sleep(0.01)
        assert connection.reads == 1, "row 2 is still the socket's"

        assert sub.offset == 1
        sub.commit()
        assert cursor.load() == 1
        await sub.close()

    async def test_a_batch_from_the_socket_starts_no_task(self, monkeypatch):
        """What it costs: rows already arrived are taken without a task or a
        turn of the loop, which had made a batch dearer per row than `recv`."""
        connection = FakeConnection()
        connection.arrive(*range(1, 101))
        sub = subscription(connection)

        def refused(*_: object, **__: object) -> None:
            msg = "a task per row"
            raise AssertionError(msg)

        monkeypatch.setattr(asyncio, "ensure_future", refused)
        monkeypatch.setattr(asyncio, "sleep", refused)
        assert offsets(await sub.recv_many(limit=500)) == list(range(1, 101))

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
    async def test_the_queue_it_looks_at_is_websockets(self, tmp_path, serve):
        """`_arrived` reads a private queue: this is what says the installed
        `websockets` still keeps it where it did, and pops it the same way."""
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    assert not _arrived(sub.connection)
                    await stream.send_many([{"i": i} for i in range(3)])
                    for _ in range(100):
                        if len(sub.connection.recv_messages.frames) == 3:
                            break

                        await asyncio.sleep(0.01)

                    assert _arrived(sub.connection)
                    assert offsets(await sub.recv_many()) == [1, 2, 3]
                    assert not _arrived(sub.connection)
        finally:
            await stream.aclose()

    async def test_every_row_once_in_order_within_the_limit(self, tmp_path, serve):
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
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
