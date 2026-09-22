"""A slow consumer is only its own problem.

The failure this library was built against: one consumer that stops reading
becomes the rate of the whole stream, because the broadcast awaits it. These
tests hold a subscriber still and check that (a) everyone else is unaffected,
(b) the broker's memory does not grow to meet it, and (c) what it did receive
is a contiguous prefix, so a durable stream loses nothing when it comes back.

Forcing a real overflow needs real backpressure, which is why the payloads
here are large and `max_queue=1` on the stalled client: the broker's pump only
blocks once `write_limit` of unacknowledged bytes have piled up in the socket,
and a client whose own library is happily draining into a 16-deep queue is not
yet a slow consumer.
"""

from __future__ import annotations

import asyncio

import pytest

import streamcast
from tests.conftest import trade

# Large enough that a handful fill the kernel buffer and `websockets`'
# 32 KB `write_limit`, so the broker's pump actually parks. A row is small,
# so the bulk goes in the one string column the schema has.
BULK = "x" * 65_536


def fat(i: int) -> dict:
    """A row big enough to apply real backpressure."""
    return {**trade(i), "tag": f"{i:06}{BULK}"}


class TestIsolation:
    async def test_a_stalled_consumer_does_not_delay_a_healthy_one(self, serve):
        stream = streamcast.Stream("trades", max_backlog=16)
        async with serve(stream) as uri:
            # Connected, greeted, and then never read from again.
            stalled = await streamcast.connect(uri, max_queue=1)
            async with streamcast.connect(uri) as healthy:
                try:
                    for i in range(40):
                        await stream.send(fat(i))
                        # `send` never yields — that is the atomicity the
                        # ordering guarantee rests on — so a publish loop
                        # with no await of its own starves every pump and
                        # drops even a healthy consumer. A real publisher
                        # awaits its upstream between messages; this stands
                        # in for that. See `Stream.send`.
                        await asyncio.sleep(0)

                    # The healthy one gets all forty, in order, while the
                    # other is parked. If the broadcast awaited consumers,
                    # this would not return.
                    received = [
                        await asyncio.wait_for(healthy.recv(), timeout=15)
                        for _ in range(40)
                    ]
                finally:
                    await stalled.close()

        assert [str(row["tag"])[:6] for _offset, row in received] == [
            f"{i:06}" for i in range(40)
        ]

    async def test_send_returns_without_waiting_for_any_consumer(self, serve):
        # The property stated as a measurement rather than as prose: with a
        # subscriber that never reads, publishing stays at memory speed.
        stream = streamcast.Stream("trades", max_backlog=4096)
        async with serve(stream) as uri:
            stalled = await streamcast.connect(uri, max_queue=1)
            try:
                await stream.send(fat(0))
                await asyncio.sleep(0.1)  # let the pump park on the socket

                started = asyncio.get_running_loop().time()
                for _ in range(500):
                    await stream.send(fat(0))

                elapsed = asyncio.get_running_loop().time() - started

            finally:
                await stalled.close()

        # 500 sends of 64 KB each. Awaiting a stalled TCP window even once
        # would blow past this by orders of magnitude.
        assert elapsed < 2.0


class TestDropping:
    @pytest.mark.slow
    async def test_a_consumer_that_stops_reading_is_dropped_not_buffered(self, serve):
        stream = streamcast.Stream("trades", max_backlog=8)
        async with serve(stream) as uri:
            stalled = await streamcast.connect(uri, max_queue=1)
            for i in range(200):
                await stream.send(fat(i))

            received = []
            with pytest.raises(streamcast.TooSlow) as raised:
                while True:
                    received.append(await asyncio.wait_for(stalled.recv(), timeout=15))

        assert raised.value.backlog == 8
        # What it received is a CONTIGUOUS PREFIX. This is the property that
        # makes dropping better than drop-oldest: a hole in the middle would
        # be invisible, and this is not one. Checked on the tag rather than
        # the offset, because this stream has no log and therefore no offsets
        # — see below.
        tags = [str(row["tag"])[:6] for _offset, row in received]
        assert tags == [f"{i:06}" for i in range(len(tags))]
        # Bounded: it did not get all 200, which is the whole point.
        assert len(tags) < 200

        # **On a live-only stream a drop IS data loss**, and the absence of a
        # resume point is how the library says so: nothing assigned offsets,
        # so there is nothing to reconnect at. That is the plainest argument
        # for attaching a log, and the next test is the other half of it.
        assert all(offset is None for offset, _row in received)
        assert raised.value.offset is None
        assert "resume at offset" not in str(raised.value)

    @pytest.mark.slow
    async def test_a_dropped_consumer_resumes_with_nothing_missed(self, serve, log):
        """The argument for attaching a log, end to end.

        Dropping a subscriber is data loss on a live-only stream and a
        reconnect on a durable one. This is the second case: fall behind, get
        dropped, come back one above what was received, and read the rest.
        """
        stream = streamcast.Stream("trades", log=log, max_backlog=8)
        total = 120
        async with serve(stream) as uri:
            stalled = await streamcast.connect(uri, max_queue=1)
            for i in range(total):
                await stream.send(fat(i))

            received = []
            with pytest.raises(streamcast.TooSlow):
                while True:
                    received.append(await asyncio.wait_for(stalled.recv(), timeout=15))

            resume = received[-1][0] + 1
            async with streamcast.connect(uri, offset=resume) as back:
                rest = [
                    await asyncio.wait_for(back.recv(), timeout=15)
                    for _ in range(total - len(received))
                ]

        offsets = [offset for offset, _ in received + rest]
        assert offsets == list(range(1, total + 1))
        assert [str(row["tag"])[:6] for _, row in received + rest] == [
            f"{i:06}" for i in range(total)
        ]

    async def test_dropping_one_subscriber_leaves_the_others_alone(self, serve):
        stream = streamcast.Stream("trades", max_backlog=64)
        async with serve(stream) as uri:
            stalled = await streamcast.connect(uri, max_queue=1)
            async with streamcast.connect(uri) as healthy:
                try:
                    for i in range(80):
                        await stream.send(fat(i))
                        await asyncio.sleep(0)  # as above: a publisher yields

                    received = [
                        await asyncio.wait_for(healthy.recv(), timeout=15)
                        for _ in range(80)
                    ]
                finally:
                    with pytest.raises(Exception):  # noqa: B017, PT011 — any end is fine
                        while True:
                            await asyncio.wait_for(stalled.recv(), timeout=15)

        assert [str(row["tag"])[:6] for _offset, row in received] == [
            f"{i:06}" for i in range(80)
        ]


async def test_the_broker_forgets_a_dropped_subscriber(serve):
    stream = streamcast.Stream("trades", max_backlog=4)
    async with serve(stream) as uri:
        stalled = await streamcast.connect(uri, max_queue=1)
        for i in range(60):
            await stream.send(fat(i))

        with pytest.raises(Exception):  # noqa: B017, PT011
            while True:
                await asyncio.wait_for(stalled.recv(), timeout=15)

        for _ in range(300):
            if stream.subscribers == 0:
                break

            await asyncio.sleep(0.01)

        assert stream.subscribers == 0
