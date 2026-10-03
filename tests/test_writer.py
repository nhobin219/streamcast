"""The durable append, on a writer thread per stream (`_writer`).

What moving the commit off the event loop must not cost: order within a
stream, `send_many`'s adjacent offsets, the subscribe partition, a bad row
failing alone. And what it buys, checked structurally rather than by timing:
the commit runs on another thread, and queued sends are grouped into one.
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

import litelink
import pytest

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


class TestOrder:
    async def test_concurrent_senders_get_dense_increasing_offsets(self, stream, serve):
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:  # live, from now

                async def publisher(k: int) -> list[int]:
                    return [await stream.send({"i": k * 1000 + n}) for n in range(50)]

                returned = await asyncio.gather(*(publisher(k) for k in range(16)))
                seen = [(await sub.recv())[0] for _ in range(16 * 50)]

        # Each publisher's offsets increase, and together they are dense.
        assert all(offsets == sorted(offsets) for offsets in returned)
        assert sorted(o for offsets in returned for o in offsets) == list(range(1, 801))
        # And a subscriber sees them in commit order, each once.
        assert seen == list(range(1, 801))

    async def test_send_many_keeps_its_rows_adjacent(self, stream):
        async def batch(k: int) -> list[int]:
            return await stream.send_many([{"i": k * 100 + n} for n in range(10)])

        groups = await asyncio.gather(*(batch(k) for k in range(20)))
        for offsets in groups:
            assert offsets == list(range(offsets[0], offsets[0] + 10))

    async def test_a_subscriber_joining_mid_flood_gets_every_row_once(
        self, stream, serve
    ):
        """The partition: committed-not-yet-delivered rows arrive live."""
        await stream.send({"i": -1})  # offset 1: the log is not empty
        async with serve(stream, maintain=False) as uri:
            flood = asyncio.gather(
                *(stream.send({"i": n}) for n in range(400))  # all queued at once
            )
            # Wherever the commits have got to when this joins, offsets below
            # the frontier are replayed and the rest arrive live.
            async with streamcast.connect(uri, offset=1) as sub:
                await flood
                seen = [(await sub.recv())[0] for _ in range(401)]

        assert seen == list(range(1, 402))

    async def test_the_frontier_covers_a_row_before_it_is_fanned_out(
        self, stream, monkeypatch
    ):
        """The partition, checked at the moment it could break.

        A row offered to subscribers while the frontier is still below it is
        in neither set for a subscriber that attaches in between: replay stops
        short of it and the live queue was filled before the join. Rare by
        timing — one loop turn — so asserted at every fan-out instead.
        """
        behind: list[tuple[int, int | None]] = []
        real = streamcast.Stream._fan_out  # noqa: SLF001

        def checking(self, row, frame):
            offset = json.loads(frame)[0]
            frontier = self.end_offset
            if frontier is None or frontier <= offset:
                behind.append((offset, frontier))

            real(self, row, frame)

        # On the class: `Stream` has `__slots__`, so not on the instance.
        monkeypatch.setattr(streamcast.Stream, "_fan_out", checking)
        await asyncio.gather(*(stream.send({"i": n}) for n in range(50)))
        await stream.send_many([{"i": n} for n in range(10)])

        assert behind == []


class TestFailure:
    async def test_a_bad_row_fails_alone(self, stream):
        good = [stream.send({"i": n}) for n in range(5)]
        with pytest.raises(Exception, match="i"):
            await stream.send({"i": "not an integer"})

        assert await asyncio.gather(*good) == [1, 2, 3, 4, 5]
        assert stream.log is not None
        assert stream.log.end_offset() == 6

    async def test_aclose_commits_what_is_queued(self, tmp_path):
        made = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        pending = [asyncio.ensure_future(made.send({"i": n})) for n in range(50)]
        await asyncio.sleep(0)  # queued, not all committed
        await made.aclose()

        assert [task.result() for task in pending] == list(range(1, 51))
        with litelink.open(tmp_path, "trades", read_only=True) as log:
            assert log.end_offset() == 51


class TestOffTheLoop:
    async def test_the_commit_runs_on_another_thread(self, stream, monkeypatch):
        assert stream.log is not None
        threads = []
        real = stream.log.extend

        def recording(rows):
            threads.append(threading.current_thread())
            return real(rows)

        monkeypatch.setattr(stream.log, "extend", recording)
        await stream.send({"i": 1})

        assert threads
        assert all(t is not threading.main_thread() for t in threads)

    @pytest.mark.parametrize(
        ("group_commit", "expected"),
        [(False, [1] * 21), (True, [1, 20]), (None, [1, 20])],  # None: the default
    )
    async def test_queued_sends_are_grouped_unless_the_stream_opts_out(
        self, tmp_path, monkeypatch, group_commit, expected
    ):
        """Deterministic: the first commit is held until the rest have queued.

        By default the twenty queued behind are one commit. Opted out, each
        send is promised a commit of its own, so they are twenty.
        """
        extra = {} if group_commit is None else {"group_commit": group_commit}
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA, **extra)
        assert stream.log is not None
        held = threading.Event()
        commits: list[int] = []
        real = stream.log.extend

        def gated(rows):
            if not commits:
                held.wait(5)

            commits.append(len(rows))
            return real(rows)

        monkeypatch.setattr(stream.log, "extend", gated)
        try:
            first = asyncio.ensure_future(stream.send({"i": 0}))
            await asyncio.sleep(0.05)  # the writer has taken it and is held
            rest = [asyncio.ensure_future(stream.send({"i": n})) for n in range(1, 21)]
            await asyncio.sleep(0)
            held.set()
            await asyncio.gather(first, *rest)
        finally:
            await stream.aclose()

        assert commits == expected


class TestTheGreeting:
    @pytest.mark.parametrize("group_commit", [False, True])
    async def test_it_says_which_commit_guarantee_the_stream_makes(
        self, tmp_path, serve, group_commit
    ):
        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=SCHEMA, group_commit=group_commit
        )
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    assert sub.info.group_commit is group_commit
        finally:
            await stream.aclose()

    async def test_it_is_on_by_default(self, stream, serve):
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                assert sub.info.group_commit is True

    def test_a_greeting_without_it_means_each_send_commits_alone(self):
        from streamcast._protocol import parse_greeting

        info = parse_greeting(
            '{"streamcast":4,"stream":"t","end_offset":1,"replay":null,"durable":true}'
        )
        assert info.group_commit is False

    async def test_a_stream_with_no_log_groups_nothing(self, serve):
        live = streamcast.Stream("t", group_commit=True)
        async with serve(live) as uri, streamcast.connect(uri) as sub:
            assert sub.info.group_commit is False
