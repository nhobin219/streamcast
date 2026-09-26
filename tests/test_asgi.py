"""The ASGI transport: the same streams, mounted in someone else's app.

**Every claim here is a claim about what reaches the peer through a real ASGI
server**, which is why these run under uvicorn rather than Starlette's
`TestClient`. A `TestClient` calls the app in-process and would prove none of
the four things the mount actually risks: that `max_backlog` still drops a slow
consumer instead of buffering under us, that a refusal's close code arrives
with its reason intact, that a clean end is 1000 rather than 1006, and that a
replayed frame is still byte-identical to the live one.

The client is `streamcast.connect` throughout — the point is that a consumer
cannot tell which transport served it.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

import pytest

import streamcast
from streamcast._transport import Peer

from .conftest import trade

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


uvicorn = pytest.importorskip("uvicorn", reason="the asgi extra is not installed")
pytest.importorskip("starlette", reason="the asgi extra is not installed")

from starlette.applications import Starlette  # noqa: E402
from starlette.routing import Mount  # noqa: E402

from streamcast.asgi import _Peer, asgi  # noqa: E402

# Same reasoning as `tests/test_backpressure.py`: a real overflow needs real
# backpressure, so the rows are large and the stalled client's own queue is 1.
BULK = "x" * 65_536

# `Stream.new` declares in JSON Schema; conftest's `SCHEMA` is the Arrow one.
SCHEMA_JSON = {
    "type": "object",
    "properties": {name: {"type": "number"} for name in ("event_ts", "price")},
    "required": ["event_ts", "price"],
}


def fat(i: int) -> dict:
    return {**trade(i), "tag": f"{i:06}{BULK}"}


def _get_status(url: str) -> int:
    """A plain GET, on stdlib, so the 404 case costs no dev dependency."""
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url) as answer:  # noqa: S310 — a test-local URL
            return int(answer.status)

    except urllib.error.HTTPError as status:
        return int(status.code)


@contextlib.asynccontextmanager
async def running(app: Any) -> AsyncIterator[str]:
    """An ASGI app under a real uvicorn, yielding its `ws://host:port` base."""
    config = uvicorn.Config(
        app, host="127.0.0.1", port=0, log_level="critical", lifespan="on"
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve())
    try:
        # A readiness handshake, not a timing guess: uvicorn flips `started`
        # once the socket is listening. Bounded, so a bind failure fails the
        # test instead of hanging it.
        for _ in range(5_000):
            if server.started:
                break

            await asyncio.sleep(0.001)

        else:  # pragma: no cover — a bind failure
            pytest.fail("uvicorn never started")

        yield f"ws://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"

    finally:
        server.should_exit = True
        await serving


class TestItServes:
    async def test_a_mounted_stream_delivers_live_rows(self, log):
        """The headline: `connect` cannot tell it is not talking to `serve`."""
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base, streamcast.connect(f"{base}/trades") as sub:
            await stream.send(trade(0))
            offset, row = await sub.recv()

        assert (offset, row) == (1, trade(0))

    async def test_mounting_under_a_prefix_keeps_the_stream_name(self, log):
        """`Mount` moves the prefix into `root_path`, so the name is unchanged.

        Reading `scope["path"]` rather than the raw target is what makes this
        work — a stream mounted at `/streams` is still `trades`, not
        `streams/trades`, and its greeting says so.
        """
        stream = streamcast.Stream("trades", log=log)
        streams = asgi(stream, maintain=False, replicate=False)
        app = Starlette(routes=[Mount("/streams", streams)])
        async with running(app) as base:
            async with streamcast.connect(f"{base}/streams/trades") as sub:
                assert sub.info.stream == "trades"
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

    async def test_a_resume_replays_from_the_requested_offset(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            for i in range(5):
                await stream.send(trade(i))

            async with streamcast.connect(f"{base}/trades", offset=3) as sub:
                assert [(await sub.recv())[0] for _ in range(3)] == [3, 4, 5]

    async def test_the_unnamed_stream_is_served_at_the_root(self, log):
        stream = streamcast.Stream(log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base, streamcast.connect(f"{base}/") as sub:
            await stream.send(trade(0))
            assert (await sub.recv())[0] == 1


class TestTheWireIsUnchanged:
    """The two properties a transport swap is most likely to break silently."""

    async def test_the_greeting_is_text_and_data_frames_are_binary(self, log):
        """**The opcode split is the contract, not an encoder detail.**

        `encode` returns `bytes` and `greeting` returns `str`, so `websockets`
        sends one BINARY and the other TEXT. An adapter that called
        `send_text` for both — or `send_bytes` for both — would keep every
        test that decodes payloads passing while changing the wire for every
        existing client. So this asserts the types rather than the contents.

        Falsify by collapsing `_Peer.send`'s branch onto either method.
        """
        websockets = pytest.importorskip("websockets")
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            async with websockets.connect(f"{base}/trades") as raw:
                greeting = await raw.recv()
                await stream.send(trade(0))
                data = await raw.recv()

        assert isinstance(greeting, str), "the greeting must stay a TEXT frame"
        assert isinstance(data, bytes), "a data frame must stay BINARY"

    async def test_a_replayed_frame_is_byte_identical_to_the_live_one(self, log):
        """Invariant 10, across the transport rather than across the tier.

        Encoding is `_stream`'s and this transport does not touch it, so this
        should hold for free — which is exactly why it is worth asserting:
        a transport that re-serialised, re-ordered keys or transcoded would
        break a resume across the join and nothing else would notice.
        """
        websockets = pytest.importorskip("websockets")
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            async with websockets.connect(f"{base}/trades") as raw:
                await raw.recv()
                await stream.send(trade(0))
                live = await raw.recv()

            async with websockets.connect(f"{base}/trades?offset=1") as raw:
                await raw.recv()
                replayed = await raw.recv()

        assert replayed == live, "a replayed frame changed shape"


class TestWhatItRefuses:
    """Every refusal, with its code AND its reason — through uvicorn.

    The reason is the half that an abstraction layer is most likely to drop:
    Starlette takes `close(code, reason)`, but whether the reason survives to
    the peer is the ASGI server's business, and `_errors` builds sentences from
    it on the client side.
    """

    async def test_an_unknown_stream_is_refused_and_names_what_is_served(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            with pytest.raises(streamcast.StreamNotFound) as raised:
                await streamcast.connect(f"{base}/quotes")

            # The `serves=` field, which only exists in the close reason.
            assert "trades" in str(raised.value)

    async def test_a_malformed_path_is_refused_with_the_parse_detail(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            with pytest.raises(streamcast.ProtocolError) as raised:
                await streamcast.connect(f"{base}/trades?offset=abc")

            assert "offset" in str(raised.value)

    async def test_an_unreplayable_offset_is_refused(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            await stream.send(trade(0))
            with pytest.raises(streamcast.NotReplayable):
                await streamcast.connect(f"{base}/trades", offset=9_999)

    async def test_publishing_is_off_unless_the_mount_allows_it(self, log):
        """The same opt-in `serve` has, and for the same reason."""
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            with pytest.raises(streamcast.ProtocolError) as raised:
                await streamcast.publish(f"{base}/trades")

            assert "does not accept publishers" in str(raised.value)

    async def test_an_http_request_gets_a_404_rather_than_a_traceback(self, log):
        """A routing mistake in the host app, answered rather than raised."""
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            url = base.replace("ws://", "http://")
            status = await asyncio.to_thread(_get_status, f"{url}/trades")

        assert status == 404


class TestBackpressureSurvives:
    """`max_backlog` is a guarantee this library enforces, not the server's.

    The risk the mount introduces: under ASGI there is a second queue beneath
    ours, and a subscriber that stalls must still fill its OWN queue and be
    dropped rather than turn into unbounded buffering in uvicorn.
    """

    async def test_a_stalled_subscriber_is_dropped_with_4429_and_its_reason(self, log):
        stream = streamcast.Stream("trades", log=log, max_backlog=8)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            stalled = await streamcast.connect(f"{base}/trades", max_queue=1)
            try:
                for i in range(400):
                    await stream.send(fat(i))
                    await asyncio.sleep(0)

                with pytest.raises(streamcast.TooSlow) as raised:
                    while True:
                        await stalled.recv()

                # The backlog figure travels in the close reason.
                assert "8" in str(raised.value)

            finally:
                await stalled.close()

    async def test_a_stalled_subscriber_does_not_slow_a_healthy_one(self, log):
        """The property the whole design exists for, re-proven per transport."""
        stream = streamcast.Stream("trades", log=log, max_backlog=8)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            stalled = await streamcast.connect(f"{base}/trades", max_queue=1)
            try:
                async with streamcast.connect(f"{base}/trades") as healthy:
                    for i in range(40):
                        await stream.send(fat(i))
                        await asyncio.sleep(0)

                    seen: list[int] = []
                    for _ in range(40):
                        offset, _row = await healthy.recv()
                        assert offset is not None, "a durable stream must offset"
                        seen.append(offset)

                assert seen == sorted(seen), "the healthy consumer saw a reorder"
                assert len(set(seen)) == 40

            finally:
                await stalled.close()


class TestTheConnectionLifecycle:
    async def test_a_clean_close_is_1000_and_not_1006(self, log):
        """1006 means the socket died; 1000 means it was closed.

        A consumer distinguishes an ordinary end from a failure by exactly
        this, and the drain in `_client._close` exists because a paused reader
        never consumes the Close echo. Whatever the equivalent is under ASGI,
        the assertion is the same one.
        """
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            sub = await streamcast.connect(f"{base}/trades")
            await stream.send(trade(0))
            await sub.recv()
            await sub.close()

            assert sub.connection.close_code == 1000

    async def test_a_disconnect_detaches_the_subscriber(self, log):
        """The reader task's real job.

        A subscription never reads, so nothing would learn of a disconnect
        without `_Peer`'s reader — the pump would stay parked in `queue.get()`
        and the `Subscriber` would stay in the fan-out set for ever. On a quiet
        stream that leak is invisible until the box runs out of something.

        Falsify by not starting the reader in `_Peer.__aenter__`.
        """
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False)
        async with running(app) as base:
            async with streamcast.connect(f"{base}/trades") as sub:
                await stream.send(trade(0))
                await sub.recv()
                assert stream.subscribers == 1

            for _ in range(2_000):
                if stream.subscribers == 0:
                    break

                await asyncio.sleep(0.001)

            assert stream.subscribers == 0, "the subscriber was never detached"

    async def test_the_adapter_satisfies_the_transport_protocol(self):
        """Structural, so a renamed method is caught without a socket."""
        assert issubclass(_Peer, Peer)


class TestRemotePublishing:
    async def test_a_publisher_appends_through_the_mount(self, log):
        """`serve_publisher` iterates the connection, so this exercises
        `__aiter__` — the one method a subscription never touches."""
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False, publish=True)
        async with running(app) as base:
            async with streamcast.connect(f"{base}/trades") as sub:
                async with streamcast.publish(f"{base}/trades") as producer:
                    offset = await producer.send(trade(0))

                delivered, row = await sub.recv()

        assert offset == 1
        assert (delivered, row) == (1, trade(0))

    async def test_send_many_stays_one_transaction(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False, publish=True)
        async with running(app) as base:
            async with streamcast.publish(f"{base}/trades") as producer:
                offsets = await producer.send_many([trade(i) for i in range(20)])

        assert offsets == list(range(1, 21))

    async def test_a_rejected_row_answers_and_keeps_the_connection(self, log):
        stream = streamcast.Stream("trades", log=log)
        app = asgi(stream, maintain=False, replicate=False, publish=True)
        async with running(app) as base:
            async with streamcast.publish(f"{base}/trades") as producer:
                with pytest.raises(streamcast.Rejected) as raised:
                    await producer.send({"event_ts": "not a number", "price": 1.0})

                assert "event_ts" in str(raised.value)
                assert await producer.send(trade(0)) == 1


class TestTheChildren:
    async def test_async_with_owns_the_maintainers(self, tmp_path):
        """**Starlette does not run a mounted sub-app's lifespan.**

        Which is why the children are owned by `async with` rather than by the
        app's own lifespan events: an app that relied on `lifespan` alone would
        start no maintainer once mounted, and a log with nothing sealing it is
        the one failure this library says out loud it must never allow.
        """
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        try:
            streams = asgi(stream, replicate=False)
            assert streams._children, "a stream with a log needs a maintainer"
            async with streams:
                assert streams._started is True

            assert streams._started is False

        finally:
            await stream.aclose()

    async def test_leaving_the_block_closes_a_log_it_opened(self, tmp_path):
        """Symmetry with `serve`, which closes its streams on shutdown.

        A stream built by `Stream.new` owns its log, so something has to close
        it; `aclose` is a no-op for a handle the caller passed in. Without this
        a mounted app would leak the log it opened, and the leak is invisible
        until a second process tries to write.
        """
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        async with asgi(stream, maintain=False, replicate=False):
            await stream.send({"event_ts": 1.0, "price": 2.0})

        # The consequence, not the private attribute: the handle is shut, so
        # a write against it fails rather than silently reopening anything.
        with pytest.raises(Exception, match="closed"):
            await stream.send({"event_ts": 3.0, "price": 4.0})

    async def test_a_live_only_stream_needs_no_children(self):
        streams = asgi(streamcast.Stream("live"))
        assert streams._children == []

    async def test_the_documented_pattern_works_as_written(self, tmp_path):
        """`README.md` and `docs/API.md` both print this. So it is run.

        Lifespan and mount together, which is the shape a reader copies — and
        the one place the two halves could disagree: children owned by the
        host app's lifespan, routing owned by the mount.
        """
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        try:
            streams = asgi(stream, replicate=False)

            @contextlib.asynccontextmanager
            async def lifespan(_app: Any) -> AsyncIterator[None]:
                async with streams:
                    yield

            app = Starlette(lifespan=lifespan, routes=[Mount("/streams", streams)])
            async with running(app) as base:
                assert streams._started is True, "the lifespan did not start them"
                async with streamcast.connect(f"{base}/streams/trades") as sub:
                    await stream.send({"event_ts": 1.0, "price": 2.0})
                    assert (await sub.recv())[0] == 1

            assert streams._started is False, "the lifespan did not stop them"

        finally:
            await stream.aclose()
