"""End to end: a server on a port, subscribers on the other end of it.

Live fan-out, routing, the greeting, and the `websockets` compatibility the
API claims. Replay and resume are `test_resume.py`; refusals are
`test_refusals.py`.
"""

from __future__ import annotations

import asyncio

import pytest
import websockets

import streamcast
from tests.conftest import trade


async def drain(subscription, count):
    """The next `count` messages, as `(offset, message)` pairs."""
    return [await subscription.recv() for _ in range(count)]


class TestLiveFanOut:
    async def test_every_subscriber_gets_the_same_bytes_in_the_same_order(
        self, serve, log
    ):
        # The sentence the library exists for. Six consumers on one box, one
        # upstream connection, and no way for two of them to disagree.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            subs = [await streamcast.connect(uri) for _ in range(6)]
            try:
                for i in range(20):
                    await stream.send(trade(i))

                received = [await drain(sub, 20) for sub in subs]

            finally:
                await asyncio.gather(*(sub.close() for sub in subs))

        # Identical bytes to everyone (I6): same offsets, same rows, same
        # order, with no per-consumer view anywhere in the path.
        first = received[0]
        assert [offset for offset, _row in first] == list(range(1, 21))
        assert [row["price"] for _o, row in first] == [85_565.0 + i for i in range(20)]
        for one in received[1:]:
            assert one == first

    async def test_a_subscriber_joins_at_the_frontier_and_misses_what_came_before(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send(trade(0))
            async with streamcast.connect(uri) as sub:
                assert sub.info.end_offset == 2
                assert sub.info.replay is None
                await stream.send(trade(1))
                offset, row = await sub.recv()
                assert offset == 2
                assert row["price"] == 85_566.0

    async def test_a_row_arrives_as_the_row_that_was_sent(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send(trade(4))
            offset, row = await sub.recv()
            assert offset == 1
            # EXACTLY the row that was published — no offset key, nothing
            # injected — so a subscriber can forward or store it whole.
            assert row == trade(4)

    async def test_send_many_arrives_as_separate_messages(self, serve, log):
        # Batching is the server's durability decision. Making it visible on
        # the wire would make every subscriber's parser depend on how the
        # publisher happened to poll.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send_many([trade(i) for i in range(3)])
            got = await drain(sub, 3)
            assert [o for o, _ in got] == [1, 2, 3]
            assert [r["side"] for _, r in got] == [0, 1, 0]

    async def test_the_subscriber_count_tracks_attach_and_detach(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            assert stream.subscribers == 0
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                await sub.recv()
                assert stream.subscribers == 1

            # The detach happens in the server's handler, which unwinds when
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
                await trades.send(trade(1))
                await quotes.send(trade(2))
                assert (await t.recv())[1]["price"] == 85_566.0
                assert (await q.recv())[1]["price"] == 85_567.0

    async def test_an_unnamed_stream_is_served_at_the_root(self, serve):
        stream = streamcast.Stream()
        async with serve(stream) as uri:
            assert uri.endswith("/")
            async with streamcast.connect(uri) as sub:
                assert sub.info.stream == ""
                await stream.send(trade(0))
                # Live-only: nothing assigned an offset, so None arrives.
                assert (await sub.recv())[0] is None

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
    async def test_connect_works_as_an_await_and_as_a_context_manager(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            sub = await streamcast.connect(uri)
            try:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1
            finally:
                await sub.close()

            async with streamcast.connect(uri) as sub:
                await stream.send(trade(1))
                assert (await sub.recv())[0] == 2

    async def test_keywords_reach_websockets(self, serve, log):
        # The claim the README makes: `serve` and `connect` pass everything
        # through. `compression` is the one with a different default, so it is
        # the one worth proving still reaches the other side.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, compression="deflate") as uri:
            async with streamcast.connect(uri, compression="deflate") as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

    async def test_a_plain_websocket_client_can_subscribe(self, serve, log):
        # The affordance the URL-shaped subscribe exists for. Nothing from
        # streamcast on this side: a greeting it can read, then frames whose
        # layout is documented.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, websockets.connect(uri) as raw:
            import json

            info = json.loads(await raw.recv())
            assert info["streamcast"] == 1
            assert info["stream"] == "trades"

            # And every frame after it is a readable JSON row. No header to
            # slice, no payload kind, no library needed on this side.
            await stream.send(trade(0))
            offset, row = json.loads(await raw.recv())
            assert offset == 1
            assert row == trade(0)

    async def test_the_subscription_exposes_the_connection_it_does_not_wrap(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            pong = await sub.connection.ping()
            await asyncio.wait_for(pong, timeout=5)

    async def test_iteration_stops_cleanly_when_the_server_goes_away(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                await stream.send(trade(1))
                await asyncio.sleep(0.05)
                await stream.aclose()

                # Everything already delivered still arrives; the loop then
                # ends rather than raising, because 1001 is a server finishing
                # with this connection on purpose.
                assert [offset async for offset, _row in sub] == [1, 2]


class TestLeaks:
    async def test_a_disconnect_from_a_quiet_stream_leaks_nothing(self, serve, log):
        """The regression this suite was written to catch.

        A subscriber that walks away is noticed by `send` raising — but only
        if there is something to send. On a quiet stream the server's pump
        parks in `queue.get()` and nothing wakes it, so before `Subscriber.run`
        raced it against the connection's closed future, every disconnect left
        a task alive for ever and a `Subscriber` in the fan-out set. Nothing
        about it is visible until the box runs out of something.

        Deliberately NO message is sent, because sending one hides the bug.
        """
        stream = streamcast.Stream("trades", log=log)
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


async def test_a_subscription_has_no_send(serve, log):
    # Not a `send` that raises. The type simply does not carry one, which is
    # the same reason litelink's read handles have no `append`.
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream) as uri, streamcast.connect(uri) as sub:
        assert not hasattr(sub, "send")
