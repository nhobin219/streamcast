"""Replay and resume: the partition that makes a reconnect exactly-once.

The claim, in one sentence: the broker records its frontier at the instant a
subscriber attaches, replays `[requested, frontier)` out of the log, and only
then switches it to the live queue — so everything below the frontier is
already durable and everything from it up is already in that subscriber's
queue, with nothing in both and nothing in neither.

These tests are the claim. The racing ones matter most: a partition that holds
only when nothing is being published is not a partition.
"""

from __future__ import annotations

import asyncio

import pytest

import streamcast
from streamcast._log import columns
from streamcast._protocol import encode
from tests.conftest import trade


async def collect(subscription, count):
    return [await subscription.recv() for _ in range(count)]


def prices(received):
    return [row["price"] for _offset, row in received]


def offsets(received):
    return [offset for offset, _row in received]


class TestReplay:
    async def test_a_subscriber_resumes_from_where_it_asks(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            async with streamcast.connect(uri, offset=4) as sub:
                assert sub.info.replay == (4, 11)
                assert sub.info.durable is True
                got = await collect(sub, 7)

        assert offsets(got) == list(range(4, 11))
        assert prices(got) == [85_565.0 + i for i in range(3, 10)]

    async def test_EARLIEST_replays_everything_the_log_still_holds(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert sub.info.replay == (1, 11)
                got = await collect(sub, 10)

        assert offsets(got) == list(range(1, 11))

    async def test_resuming_at_the_frontier_replays_nothing_and_is_not_an_error(
        self, serve, log
    ):
        # The common reconnect: a consumer that lost its connection rather
        # than falling behind has nothing outstanding.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(0), trade(1)])

            async with streamcast.connect(uri, offset=3) as sub:
                assert sub.info.replay == (3, 3)
                await stream.send(trade(2))
                assert (await sub.recv())[0] == 3

    async def test_a_replay_spans_the_buffer_and_the_sealed_table(self, serve, log):
        # A replay that never leaves SQLite has not exercised the tier it is
        # for. `target_seal_size` is 4 KB in the fixture, so this crosses it
        # and then reads back across the seam.
        stream = streamcast.Stream("trades", log=log)
        await stream.send_many([trade(i) for i in range(60)])
        while log.seal() is not None:
            pass

        await stream.send_many([trade(i) for i in range(60, 70)])
        assert log.table_files() > 0

        async with serve(stream) as uri:
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                got = await collect(sub, 70)

        assert offsets(got) == list(range(1, 71))
        assert got[0][1]["price"] == 85_565.0
        assert got[-1][1]["price"] == 85_565.0 + 69
        # A nullable column the caller omitted survives both tiers the same
        # way — NULL in Parquet, null on the wire, None in the subscriber.
        assert got[0][1]["tag"] == "t0"
        assert got[1][1]["tag"] is None

    async def test_a_replayed_row_is_byte_identical_to_the_live_one(self, serve, log):
        """I6, across the one boundary where it could break.

        A live row is encoded from the caller's dict; a replayed row is
        encoded from an Arrow batch. Both are projected through the log's
        declared columns, so the bytes must match — otherwise two subscribers
        holding the same offset hold different data, which is the exact thing
        this library exists to prevent.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            async with streamcast.connect(uri) as live:
                for i in range(6):
                    await stream.send(trade(i))

                live_rows = await collect(live, 6)

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as back:
                replayed = await collect(back, 6)

        assert replayed == live_rows

        # And the frames, not only what they decode to. Key order is the thing
        # that could differ, and dict equality would not catch it.
        declared = columns(log)
        for offset, message in live_rows:
            assert encode(offset, message, declared) == encode(
                offset, dict(reversed(list(message.items()))), declared
            )


class TestThePartition:
    async def test_a_subscriber_attaching_mid_flow_sees_every_offset_once(
        self, serve, log
    ):
        """The invariant, under publishing that does not stop for the attach.

        Whatever the interleaving, what arrives must be every offset from the
        one it asked for to the last one published, each exactly once and in
        order — no gap at the join, no row delivered from both the log and the
        queue.
        """
        stream = streamcast.Stream("trades", log=log)
        total = 400
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(50)])

            async def publish():
                for i in range(50, total):
                    await stream.send(trade(i))
                    # A yield per row, so the attach really does land in the
                    # middle rather than after the loop.
                    await asyncio.sleep(0)

            publisher = asyncio.create_task(publish())
            async with streamcast.connect(uri, offset=1) as sub:
                got = await collect(sub, total)

            await publisher

        assert offsets(got) == list(range(1, total + 1))
        assert prices(got) == [85_565.0 + i for i in range(total)]

    async def test_many_subscribers_attaching_at_once_all_see_the_same_stream(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(30)])

            async def subscriber():
                async with streamcast.connect(uri, offset=1) as sub:
                    return await collect(sub, 60)

            joining = [asyncio.create_task(subscriber()) for _ in range(5)]
            for i in range(30, 60):
                await stream.send(trade(i))
                await asyncio.sleep(0)

            received = await asyncio.gather(*joining)

        first = received[0]
        assert offsets(first) == list(range(1, 61))
        for one in received[1:]:
            assert one == first

    async def test_a_reconnect_at_last_seen_plus_one_misses_nothing(self, serve, log):
        # The recovery loop from the README, end to end: drop the connection
        # mid-stream, reconnect one above what was processed, and check the
        # two halves join with no gap and no repeat.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(20)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                first = await collect(sub, 8)

            # Published while nobody was listening. This is the window a
            # live-only multicaster loses and a log does not.
            await stream.send_many([trade(i) for i in range(20, 40)])

            async with streamcast.connect(uri, offset=first[-1][0] + 1) as sub:
                second = await collect(sub, 32)

        assert offsets(first + second) == list(range(1, 41))
        assert prices(first + second) == [85_565.0 + i for i in range(40)]


class TestDurabilityOrdering:
    async def test_every_offset_a_subscriber_saw_is_already_in_the_log(
        self, serve, log
    ):
        # The ordering that makes recovery a replay rather than a
        # reconciliation: a row a subscriber has seen is always a row the log
        # holds, never the other way round.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send_many([trade(i) for i in range(25)])
            got = await collect(sub, 25)

            highest = got[-1][0]
            stored = log.scan(end_offset=highest + 1).read_all().to_pylist()

        assert [row["litelink_offset"] for row in stored] == list(range(1, 26))


@pytest.mark.parametrize("offset", [1, 5, 10])
async def test_the_greeting_states_the_range_it_is_about_to_replay(serve, log, offset):
    # So that a subscriber knows what is coming before any of it arrives —
    # which on a stream that is quiet out of hours is the difference between
    # "connected" and "connected and the broker agreed with my cursor".
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream) as uri:
        await stream.send_many([trade(i) for i in range(10)])
        async with streamcast.connect(uri, offset=offset) as sub:
            assert sub.info.replay == (offset, 11)
            assert sub.info.end_offset == 11
