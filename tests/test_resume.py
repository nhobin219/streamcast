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


async def collect(subscription, count):
    return [await subscription.recv() for _ in range(count)]


class TestReplay:
    async def test_a_subscriber_resumes_from_where_it_asks(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([f"m{i}" for i in range(10)])

            async with streamcast.connect(uri, offset=4) as sub:
                assert sub.info.replay == (4, 11)
                assert sub.info.durable is True
                assert await collect(sub, 7) == [(i, f"m{i - 1}") for i in range(4, 11)]

    async def test_EARLIEST_replays_everything_the_log_still_holds(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([f"m{i}" for i in range(10)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert sub.info.replay == (1, 11)
                assert await collect(sub, 10) == [
                    (i, f"m{i - 1}") for i in range(1, 11)
                ]

    async def test_resuming_at_the_frontier_replays_nothing_and_is_not_an_error(
        self, serve, log
    ):
        # The common reconnect: a consumer that lost its connection rather
        # than falling behind has nothing outstanding.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many(["a", "b"])

            async with streamcast.connect(uri, offset=3) as sub:
                assert sub.info.replay == (3, 3)
                await stream.send("c")
                assert await sub.recv() == (3, "c")

    async def test_a_replay_spans_the_buffer_and_the_sealed_table(self, serve, log):
        # A replay that never leaves SQLite has not exercised the tier it is
        # for. `target_seal_size` is 4 KB in the fixture, so this crosses it
        # and then reads back across the seam.
        stream = streamcast.Stream("trades", log=log)
        payload = "x" * 200
        await stream.send_many([f"{i:04}{payload}" for i in range(60)])
        while log.seal() is not None:
            pass

        await stream.send_many([f"{i:04}{payload}" for i in range(60, 70)])
        assert log.table_files() > 0

        async with serve(stream) as uri:
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                received = await collect(sub, 70)

        assert [offset for offset, _ in received] == list(range(1, 71))
        assert received[0][1].startswith("0000")
        assert received[-1][1].startswith("0069")

    async def test_binary_survives_the_log_and_comes_back_as_bytes(self, serve, log):
        # litelink cannot store `binary` today, so this went to disk base64'd.
        # What the subscriber gets back must be indistinguishable from live.
        stream = streamcast.Stream("trades", log=log)
        payload = bytes(range(256))
        async with serve(stream) as uri:
            await stream.send(payload)
            await stream.send("and text beside it")

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert await sub.recv() == (1, payload)
                assert await sub.recv() == (2, "and text beside it")


class TestThePartition:
    async def test_a_subscriber_attaching_mid_flow_sees_every_offset_once(
        self, serve, log
    ):
        """The invariant, under publishing that does not stop for the attach.

        A subscriber joins while a publisher is running. Whatever the
        interleaving, what arrives must be every offset from the one it asked
        for to the last one published, each exactly once and in order — no gap
        at the join, no message delivered from both the log and the queue.
        """
        stream = streamcast.Stream("trades", log=log)
        total = 400
        async with serve(stream) as uri:
            await stream.send_many([f"m{i}" for i in range(50)])

            async def publish():
                for i in range(50, total):
                    await stream.send(f"m{i}")
                    # A yield per message, so the attach really does land in
                    # the middle rather than after the loop.
                    await asyncio.sleep(0)

            publisher = asyncio.create_task(publish())
            async with streamcast.connect(uri, offset=1) as sub:
                received = await collect(sub, total)

            await publisher

        offsets = [offset for offset, _ in received]
        assert offsets == list(range(1, total + 1))
        assert [message for _, message in received] == [f"m{i}" for i in range(total)]

    async def test_many_subscribers_attaching_at_once_all_see_the_same_stream(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([f"m{i}" for i in range(30)])

            async def subscriber():
                async with streamcast.connect(uri, offset=1) as sub:
                    return await collect(sub, 60)

            joining = [asyncio.create_task(subscriber()) for _ in range(5)]
            for i in range(30, 60):
                await stream.send(f"m{i}")
                await asyncio.sleep(0)

            received = await asyncio.gather(*joining)

        expected = [(i + 1, f"m{i}") for i in range(60)]
        for one in received:
            assert one == expected

    async def test_a_reconnect_at_last_seen_plus_one_misses_nothing(self, serve, log):
        # The recovery loop from the README, end to end: drop the connection
        # mid-stream, reconnect one above what was processed, and check the
        # two halves join with no gap and no repeat.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([f"m{i}" for i in range(20)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                first = await collect(sub, 8)

            # Published while nobody was listening. This is the window a
            # live-only multicaster loses and a log does not.
            await stream.send_many([f"m{i}" for i in range(20, 40)])

            last = first[-1][0]
            async with streamcast.connect(uri, offset=last + 1) as sub:
                second = await collect(sub, 32)

        offsets = [offset for offset, _ in first + second]
        assert offsets == list(range(1, 41))


class TestDurabilityOrdering:
    async def test_every_offset_a_subscriber_saw_is_already_in_the_log(
        self, serve, log
    ):
        # The ordering that makes recovery a replay rather than a
        # reconciliation: a message a subscriber has seen is always a message
        # the log holds, never the other way round.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send_many([f"m{i}" for i in range(25)])
            received = await collect(sub, 25)

            highest = received[-1][0]
            stored = log.scan(end_offset=highest + 1).read_all().to_pylist()

        assert [row["litelink_offset"] for row in stored] == list(range(1, 26))


@pytest.mark.parametrize("offset", [1, 5, 10])
async def test_the_greeting_states_the_range_it_is_about_to_replay(serve, log, offset):
    # So that a subscriber knows what is coming before any of it arrives —
    # which on a stream that is quiet out of hours is the difference between
    # "connected" and "connected and the broker agreed with my cursor".
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream) as uri:
        await stream.send_many([f"m{i}" for i in range(10)])
        async with streamcast.connect(uri, offset=offset) as sub:
            assert sub.info.replay == (offset, 11)
            assert sub.info.end_offset == 11
