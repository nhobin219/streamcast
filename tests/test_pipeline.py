"""A publisher with several sends in flight: `submit`, and the server's pipeline.

`send` waits for its acknowledgement, so a loop of it is one round trip per
row. `submit` keeps up to `max_in_flight` rows on the wire; the server queues
each frame for the writer as it reads it, so a single publisher's rows group
into one commit, and the replies come back in the order the rows were sent.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

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


def hold_commits(stream, monkeypatch) -> tuple[threading.Event, list[int]]:
    """Hold the writer's FIRST commit until released; record every commit's size."""
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
    return held, commits


class TestOrder:
    async def test_submitted_rows_are_acknowledged_in_the_order_sent(
        self, stream, serve
    ):
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                futures = [await producer.submit({"i": n}) for n in range(500)]
                offsets = await asyncio.gather(*futures)

        assert offsets == list(range(1, 501))

    async def test_one_publishers_rows_group_into_one_commit(
        self, stream, serve, monkeypatch
    ):
        """What pipelining is for: a single publisher keeps the writer fed."""
        held, commits = hold_commits(stream, monkeypatch)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                first = await producer.submit({"i": 0})
                await asyncio.sleep(0.05)  # the server has it; its commit is held
                rest = [await producer.submit({"i": n}) for n in range(1, 21)]
                await asyncio.sleep(0.05)  # all twenty read and queued behind it
                held.set()
                await asyncio.gather(first, *rest)

        assert commits == [1, 20]


class TestTheWindow:
    async def test_a_full_window_holds_the_next_submit(
        self, stream, serve, monkeypatch
    ):
        held, _commits = hold_commits(stream, monkeypatch)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri, max_in_flight=4) as producer:
                inflight = [await producer.submit({"i": n}) for n in range(4)]
                fifth = asyncio.ensure_future(producer.submit({"i": 4}))
                await asyncio.sleep(0.1)
                # Four unacknowledged, and the commit they wait on is held.
                assert not fifth.done()

                held.set()
                offsets = await asyncio.gather(*inflight, await fifth)

        assert offsets == [1, 2, 3, 4, 5]

    async def test_the_servers_bound_holds_a_client_allowed_more(
        self, stream, serve, monkeypatch
    ):
        """The server's `max_in_flight`: past it, the server stops reading."""
        held, commits = hold_commits(stream, monkeypatch)
        async with serve(stream, maintain=False, max_in_flight=2) as uri:
            async with streamcast.publish(uri, max_in_flight=10) as producer:
                futures = [await producer.submit({"i": n}) for n in range(10)]
                await asyncio.sleep(0.1)
                # The server read and queued at most its bound plus the frame
                # it is blocked trying to queue: the rest wait at the socket.
                assert stream.end_offset == 1  # nothing committed yet

                held.set()
                offsets = await asyncio.gather(*futures)

        assert offsets == list(range(1, 11))
        # Held to two replies owed, the server has at most four frames past
        # the socket at once — one being answered, two queued, one waiting to
        # queue — so no commit can take more. Unbounded, it reads all ten.
        assert sum(commits) == 10
        assert max(commits) <= 4

    def test_the_window_must_allow_a_send(self):
        with pytest.raises(ValueError, match="max_in_flight"):
            streamcast.Publication(None, None, "t", max_in_flight=0)  # ty: ignore[invalid-argument-type]


class TestFailures:
    async def test_a_refused_row_fails_in_its_place(self, stream, serve):
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                good = await producer.submit({"i": 1})
                bad = await producer.submit({"i": "not an integer"})
                after = await producer.submit({"i": 2})

                assert await good == 1
                with pytest.raises(streamcast.Rejected, match="i"):
                    await bad

                assert await after == 2

    async def test_close_waits_for_what_is_in_flight(self, stream, serve):
        async with serve(stream, maintain=False) as uri:
            producer = await streamcast.publish(uri)
            futures = [await producer.submit({"i": n}) for n in range(50)]
            await producer.close()

        assert [f.result() for f in futures] == list(range(1, 51))

    async def test_a_dropped_connection_fails_every_send_in_flight(
        self, stream, serve, monkeypatch
    ):
        held, _commits = hold_commits(stream, monkeypatch)
        server_closed = False
        async with serve(stream, maintain=False) as uri:
            producer = await streamcast.publish(uri)
            futures = [await producer.submit({"i": n}) for n in range(5)]
            await asyncio.sleep(0.05)
            await producer.connection.close()  # gone before any reply
            # The rows were read and queued, so they still commit; release
            # the hold so the server's handler can finish.
            held.set()
            server_closed = True

        assert server_closed
        results = await asyncio.wait_for(
            asyncio.gather(*futures, return_exceptions=True), timeout=5
        )
        assert all(isinstance(r, BaseException) for r in results), results
