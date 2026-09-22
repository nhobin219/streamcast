"""End to end: a broker on a port, subscribers on the other end of it.

Live fan-out, routing, the greeting, and the `websockets` compatibility the
API claims. Replay and resume are `test_resume.py`; refusals are
`test_refusals.py`.
"""

from __future__ import annotations

import asyncio

import pytest
import websockets

import streamcast


async def drain(subscription, count):
    """The next `count` messages, as `(offset, message)` pairs."""
    return [await subscription.recv() for _ in range(count)]


class TestLiveFanOut:
    async def test_every_subscriber_gets_the_same_bytes_in_the_same_order(self, serve):
        # The sentence the library exists for. Six consumers on one box, one
        # upstream connection, and no way for two of them to disagree.
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            subs = [await streamcast.connect(uri) for _ in range(6)]
            try:
                sent = [f"m{i}" for i in range(20)]
                for message in sent:
                    await stream.send(message)

                received = [await drain(sub, 20) for sub in subs]

            finally:
                await asyncio.gather(*(sub.close() for sub in subs))

        expected = list(enumerate(sent, start=1))
        for one in received:
            assert one == expected

    async def test_a_subscriber_joins_at_the_frontier_and_misses_what_came_before(
        self, serve
    ):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            await stream.send("before")
            async with streamcast.connect(uri) as sub:
                assert sub.info.end_offset == 2
                assert sub.info.replay is None
                await stream.send("after")
                assert await sub.recv() == (2, "after")

    async def test_bytes_arrive_as_bytes_and_text_as_text(self, serve):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send("text")
            await stream.send(b"\x00\xff bytes")
            assert await sub.recv() == (1, "text")
            assert await sub.recv() == (2, b"\x00\xff bytes")

    async def test_send_many_arrives_as_separate_messages(self, serve):
        # Batching is the broker's durability decision. Making it visible on
        # the wire would make every subscriber's parser depend on how the
        # publisher happened to poll.
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send_many(["a", "b", "c"])
            assert await drain(sub, 3) == [(1, "a"), (2, "b"), (3, "c")]

    async def test_the_subscriber_count_tracks_attach_and_detach(self, serve):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            assert stream.subscribers == 0
            async with streamcast.connect(uri) as sub:
                await stream.send("wait for the attach to complete")
                await sub.recv()
                assert stream.subscribers == 1

            # The detach happens in the broker's handler, which unwinds when
            # the peer closes — a beat after `close()` returns here.
            for _ in range(100):
                if stream.subscribers == 0:
                    break

                await asyncio.sleep(0.01)

            assert stream.subscribers == 0


class TestRouting:
    async def test_one_port_serves_many_streams(self, serve):
        # The multiplexing this library is named for: one process holding N
        # upstream subscriptions, every consumer reading whichever it wants.
        trades = streamcast.Stream("trades")
        quotes = streamcast.Stream("quotes")
        async with serve(trades, quotes) as uri:
            base = uri.rsplit("/", 1)[0]
            async with (
                streamcast.connect(f"{base}/trades") as t,
                streamcast.connect(f"{base}/quotes") as q,
            ):
                await trades.send("a trade")
                await quotes.send("a quote")
                assert await t.recv() == (1, "a trade")
                assert await q.recv() == (1, "a quote")

    async def test_an_unnamed_stream_is_served_at_the_root(self, serve):
        stream = streamcast.Stream()
        async with serve(stream) as uri:
            assert uri.endswith("/")
            async with streamcast.connect(uri) as sub:
                assert sub.info.stream == ""
                await stream.send("x")
                assert await sub.recv() == (1, "x")

    def test_two_streams_with_one_name_are_refused(self):
        # No correct resolution exists: whichever loses is unreachable, and
        # the subscriber that wanted it gets somebody else's messages — which
        # looks like working software.
        with pytest.raises(ValueError, match="both named 'trades'"):
            streamcast.serve([streamcast.Stream("trades"), streamcast.Stream("trades")])

    def test_serving_nothing_is_refused(self):
        with pytest.raises(ValueError, match="at least one Stream"):
            streamcast.serve([])


class TestWebsocketsCompatibility:
    async def test_connect_works_as_an_await_and_as_a_context_manager(self, serve):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            sub = await streamcast.connect(uri)
            try:
                await stream.send("x")
                assert await sub.recv() == (1, "x")
            finally:
                await sub.close()

            async with streamcast.connect(uri) as sub:
                await stream.send("y")
                assert await sub.recv() == (2, "y")

    async def test_keywords_reach_websockets(self, serve):
        # The claim the README makes: `serve` and `connect` pass everything
        # through. `compression` is the one with a different default, so it is
        # the one worth proving still reaches the other side.
        stream = streamcast.Stream("trades")
        async with serve(stream, compression="deflate") as uri:
            async with streamcast.connect(uri, compression="deflate") as sub:
                await stream.send("compressed")
                assert await sub.recv() == (1, "compressed")

    async def test_a_plain_websocket_client_can_subscribe(self, serve):
        # The affordance the URL-shaped subscribe exists for. Nothing from
        # streamcast on this side: a greeting it can read, then frames whose
        # layout is documented.
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri, websockets.connect(uri) as raw:
            import json

            info = json.loads(await raw.recv())
            assert info["streamcast"] == 1
            assert info["stream"] == "trades"

            await stream.send("hello")
            frame = await raw.recv()
            assert isinstance(frame, bytes)
            assert int.from_bytes(frame[:8], "big") == 1
            assert frame[9:] == b"hello"

    async def test_the_subscription_exposes_the_connection_it_does_not_wrap(
        self, serve
    ):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            pong = await sub.connection.ping()
            await asyncio.wait_for(pong, timeout=5)

    async def test_iteration_stops_cleanly_when_the_broker_goes_away(self, serve):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send("one")
                await stream.send("two")
                await asyncio.sleep(0.05)
                await stream.aclose()

                # Everything already delivered still arrives; the loop then
                # ends rather than raising, because 1001 is a broker finishing
                # with this connection on purpose.
                assert [pair async for pair in sub] == [(1, "one"), (2, "two")]


class TestLeaks:
    async def test_a_disconnect_from_a_quiet_stream_leaks_nothing(self, serve):
        """The regression this suite was written to catch.

        A subscriber that walks away is noticed by `send` raising — but only
        if there is something to send. On a quiet stream the broker's pump
        parks in `queue.get()` and nothing wakes it, so before `Subscriber.run`
        raced it against the connection's closed future, every disconnect left
        a task alive for ever and a `Subscriber` in the fan-out set. Nothing
        about it is visible until the box runs out of something.

        Deliberately NO message is sent, because sending one hides the bug.
        """
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            before = len(asyncio.all_tasks())
            for _ in range(25):
                async with streamcast.connect(uri):
                    pass

            for _ in range(200):
                if stream.subscribers == 0:
                    break

                await asyncio.sleep(0.01)

            assert stream.subscribers == 0
            # Handler tasks too, not just the bookkeeping. A count that
            # returns to zero while 25 coroutines stay parked is the half-fix.
            await asyncio.sleep(0.05)
            assert len(asyncio.all_tasks()) <= before + 2


async def test_a_subscription_has_no_send(serve):
    # Not a `send` that raises. The type simply does not carry one, which is
    # the same reason litelink's read handles have no `append`.
    stream = streamcast.Stream("trades")
    async with serve(stream) as uri, streamcast.connect(uri) as sub:
        assert not hasattr(sub, "send")
