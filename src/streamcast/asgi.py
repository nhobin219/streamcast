"""`asgi` — the same streams, mounted in someone else's ASGI app.

    from starlette.applications import Starlette
    from streamcast.asgi import asgi

    streams = asgi([trades, quotes])

    @asynccontextmanager
    async def lifespan(app):
        async with streams:          # owns the maintainers and the sidecars
            yield

    app = Starlette(lifespan=lifespan, routes=[Mount("/streams", streams)])

**Additive. `serve` is unchanged and neither is `_stream`.** A service that is
already an ASGI app otherwise has to run `serve()` on a second port: another
listener, another TLS terminator, another entry in the ingress, and a URL
shaped unlike the rest of the service. Nothing about a `Stream` requires that —
`serve` simply owns its socket — so this hands the same object to a transport
that does not.

**What it is not** is `serve` rebuilt on a framework. That was considered and
rejected: the routing it would replace is one function, the health endpoint
`serve` is asked for answers on the port it already listens on through
`process_request`, and moving the fan-out under ASGI puts `max_backlog` — a
guarantee this library enforces — behind a queue the deployer configures. The
reasoning is recorded on the issue this implements.

**The four methods are the whole boundary.** `_server` calls `_stream` in two
places, and everything below them uses `send`, `close`, `wait_closed` and
`async for`. `_transport.Peer` names those; `_Peer` here is Starlette behind
them. `_stream` and `_subscriber` are untouched, which is what `_stream.py`
holding the correctness and `_server.py` being transport was for.

**Starlette, not FastAPI and not uvicorn.** `streamcast[asgi]` depends on
Starlette alone: mounting works in any ASGI app, and nothing here reads above
the `WebSocket` class.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Final

try:
    from starlette.websockets import WebSocket, WebSocketDisconnect

except ModuleNotFoundError as missing:  # pragma: no cover — an install shape
    # Named here rather than surfacing `No module named 'starlette'` from a
    # submodule, because the fix is a marker nobody guesses from that.
    _msg = (
        "streamcast.asgi needs Starlette, which is not installed. "
        "Install the extra: pip install 'streamcast[asgi]'"
    )
    raise ModuleNotFoundError(_msg) from missing

import asyncio

from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from websockets.frames import Close as _CloseFrame

from streamcast._errors import Close, NotReplayable
from streamcast._protocol import Publish, parse_subscribe, refusal
from streamcast._server import _DETAIL_CHARS, _routes, _sidecars, _supervisors

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable
    from types import TracebackType

    from starlette.types import Receive, Scope, Send

    from streamcast._maintain import Maintain
    from streamcast._server import _Child
    from streamcast._stream import Stream

MAX_QUEUE: Final = 16
"""Frames from one peer that may sit unread before the reader stops reading.

`websockets` bounds its own receive queue at this and streamcast never raised
it, so matching keeps a publisher's flow control the same on both transports.
The bound is what makes a peer that floods the socket fill its own window
rather than this process's memory — the receive-side twin of `max_backlog`.

One extra slot is reserved on top of it for the end-of-stream sentinel, the
same arithmetic and the same reason as `_subscriber`'s overflow slot.
"""

_EOF: Final = None
"""Queued when the peer is gone, so `async for` ends rather than parking.

In the queue rather than beside it, because `serve_publisher` is suspended in
`get()` and a flag is only read between iterations — the identical argument
`_subscriber` makes for putting its overflow marker in the queue. `None` for
the same typing reason too: `str | bytes | None` narrows on `is None`.
"""


class _Peer:
    """Starlette's `WebSocket` behind `_transport.Peer`.

    **The reader task is the part that is not a rename.** `websockets` runs its
    own reader, so `wait_closed()` resolves and pongs are answered whether or
    not the application ever calls `recv`. ASGI has no such thing: a disconnect
    is a message on the receive channel and nothing learns of it until someone
    calls `receive()`. Since a subscription never reads, this reads for it —
    one task per connection, feeding `__aiter__` for the publish path and
    resolving `wait_closed()` for both.

    Without it a subscriber that walked away from a quiet stream would never be
    noticed: the pump parks in `queue.get()`, the `Subscriber` stays in the
    fan-out set, and every disconnect leaks one of each. That is the same
    defect `_subscriber.run` was written to close, arriving by a different
    route.
    """

    __slots__ = ("_closed", "_incoming", "_rcvd", "_reader", "_sent_close", "_ws")

    def __init__(self, websocket: WebSocket, *, max_queue: int = MAX_QUEUE) -> None:
        self._ws = websocket
        self._incoming: asyncio.Queue[str | bytes | None] = asyncio.Queue(
            maxsize=max_queue + 1
        )
        self._closed = asyncio.Event()
        self._rcvd: _CloseFrame | None = None
        self._sent_close = False
        self._reader: asyncio.Task[None] | None = None

    async def __aenter__(self) -> _Peer:
        self._reader = asyncio.create_task(self._read())

        return self

    async def __aexit__(self, *_exc: object) -> None:
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _read(self) -> None:
        """Drain the receive channel until the peer goes away.

        The close CODE is kept, not just the fact: `_server` distinguishes an
        ordinary disconnect from a failure by it, and a client that closed
        cleanly should not read as one that vanished.
        """
        try:
            while True:
                message = await self._ws.receive()
                if message["type"] == "websocket.disconnect":
                    self._rcvd = _CloseFrame(
                        int(message.get("code", 1005)), message.get("reason") or ""
                    )
                    return

                frame = message.get("text")
                if frame is None:
                    frame = message.get("bytes")

                if frame is not None:
                    # `put`, not `put_nowait`: a full queue must stop this task
                    # reading, which is what pushes back on the peer. Dropping
                    # a publisher's frame silently would acknowledge nothing
                    # and lose a row.
                    await self._incoming.put(frame)

        except (WebSocketDisconnect, RuntimeError, OSError):
            # The server tore the connection down under us. Same end as a
            # disconnect message, with no code to report.
            if self._rcvd is None:
                self._rcvd = _CloseFrame(1006, "")

        finally:
            self._closed.set()
            with contextlib.suppress(asyncio.QueueFull):
                self._incoming.put_nowait(_EOF)

    def _gone(self) -> ConnectionClosed:
        """The exception `websockets` would have raised for this disconnect.

        Reused rather than invented so `_stream`, `_subscriber` and `_server`
        keep catching one type. A new exception here would mean a transport
        the stream layer has to know the name of, which is the coupling this
        module exists to avoid.
        """
        return ConnectionClosedError(self._rcvd, None)

    async def send(self, message: str | bytes, /) -> None:
        """One frame, preserving TEXT for `str` and BINARY for `bytes`.

        The greeting is a `str` and every data frame is `bytes`. Collapsing
        them onto one opcode would change the wire for every existing client,
        so the branch is the contract rather than a convenience.
        """
        if self._closed.is_set():
            raise self._gone()

        try:
            if isinstance(message, str):
                await self._ws.send_text(message)

            else:
                await self._ws.send_bytes(message)

        except (WebSocketDisconnect, RuntimeError, OSError) as exc:
            raise self._gone() from exc

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Send the close. Idempotent, because two paths reach it.

        `Subscriber.pump` closes on overflow and `Stream.aclose` closes every
        subscriber; a shutdown that races a drop arrives twice, and Starlette
        raises on the second.
        """
        if self._sent_close:
            return

        self._sent_close = True
        with contextlib.suppress(WebSocketDisconnect, RuntimeError, OSError):
            await self._ws.close(code, reason)

    async def wait_closed(self) -> None:
        await self._closed.wait()

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self._frames()

    async def _frames(self) -> AsyncIterator[str | bytes]:
        while True:
            frame = await self._incoming.get()
            if frame is _EOF:
                return

            yield frame


def _relative(scope: Scope) -> str:
    """The request path relative to wherever this app was mounted.

    **`scope["path"]` is the WHOLE path, not the remainder.** Measured under
    Starlette 1.7 with `Mount("/streams", ...)`: `path` is `/streams/trades`
    and `root_path` is `/streams`. Routing on `path` alone would look up a
    stream called `streams/trades` and refuse every mounted subscribe with a
    4404 — which is what the first run of `tests/test_asgi.py` did.

    ASGI defines the app-relative path as `path` with `root_path` removed, so
    that is what this computes. A path equal to the prefix becomes `/`, which
    is where an unnamed stream is served.
    """
    root = scope.get("root_path", "")
    path = scope["path"]
    if root and path.startswith(root):
        return path[len(root) :] or "/"

    return path


class _Mounted:
    """An ASGI app for a set of streams, and the children that die with it.

    **Both an app and an async context manager, and the second one is not
    decoration.** Starlette does not run a mounted sub-app's lifespan — a
    documented gotcha, not a bug — so an app that only handled `lifespan`
    would start no maintainer once it was mounted, which is the one failure
    this library says out loud it must never allow: a log that grows with
    nothing sealing it. Measured before `_maintain` existed at 120,000 rows,
    every one still in the SQLite buffer.

    So the children are owned by `async with`, which the host app puts in its
    own lifespan, and `lifespan` is handled too for the case where this app is
    run directly. Both routes are idempotent, so doing both is harmless.
    """

    __slots__ = ("_children", "_publish", "_started", "_streams")

    def __init__(
        self,
        streams: Stream | Iterable[Stream],
        *,
        maintain: bool | Maintain = True,
        replicate: bool = True,
        publish: bool = False,
    ) -> None:
        # Resolved here, synchronously, exactly as `serve` does: a stream-set
        # collision or a missing litestream should fail at the call rather
        # than inside a lifespan event whose traceback names the framework.
        self._streams = _routes(streams)
        self._children: list[_Child] = [
            *_supervisors(self._streams, maintain),
            *_sidecars(self._streams, replicate),
        ]
        self._publish = publish
        self._started = False

    def __repr__(self) -> str:
        return (
            f"<streamcast.asgi serving={sorted(self._streams)} publish={self._publish}>"
        )

    def _start(self) -> None:
        if self._started:
            return

        self._started = True
        for child in self._children:
            child.start()

    async def _stop(self) -> None:
        if not self._started:
            return

        self._started = False
        for child in self._children:
            child.terminate()

        for child in self._children:
            await child.wait_closed()

        # LAST, and the order is the same one `_Served.wait_closed` keeps: a
        # replay in flight is reading the log in a worker thread, so closing
        # it first would pull the file out from under a scan. `aclose` is a
        # no-op for a stream whose log was handed in — that one is the
        # caller's — so this only closes what `Stream.new` opened.
        for stream in self._streams.values():
            await stream.aclose()

    async def __aenter__(self) -> _Mounted:
        self._start()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._stop()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        kind = scope["type"]
        if kind == "websocket":
            await self._websocket(scope, receive, send)
            return

        if kind == "lifespan":
            await self._lifespan(receive, send)
            return

        # An HTTP request reaching here is a routing mistake in the host app,
        # answered rather than raised: a 500 would read as this app failing.
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-length", b"0")],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                self._start()
                await send({"type": "lifespan.startup.complete"})

            elif message["type"] == "lifespan.shutdown":
                await self._stop()
                await send({"type": "lifespan.shutdown.complete"})

                return

    async def _websocket(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Route one connection, then hand it to the stream.

        The same order as `_server`'s handler, and the same refusals with the
        same codes — with one difference forced by ASGI: the handshake is
        accepted BEFORE a refusal can be sent, because a close code only
        exists on an accepted connection. `websockets` refuses after its
        handshake too, so what the client sees is identical; rejecting the
        upgrade instead would turn every 4400/4404/4416 into an HTTP 403 with
        the reason thrown away.
        """
        websocket = WebSocket(scope, receive, send)
        await websocket.accept()

        async with _Peer(websocket) as peer:
            query = scope.get("query_string", b"").decode("latin-1")
            target = _relative(scope)
            if query:
                target = f"{target}?{query}"

            try:
                await self._dispatch(peer, target)

            except ConnectionClosed:
                # The ordinary end of a subscription, as in `_server`: the peer
                # went away, or the pump dropped it and the next send raised.
                return

    async def _dispatch(self, peer: _Peer, target: str) -> None:
        try:
            name, requested = parse_subscribe(target)

        except ValueError as exc:
            await peer.close(
                Close.BAD_REQUEST,
                refusal("bad_request", detail=str(exc)[:_DETAIL_CHARS]),
            )
            return

        stream = self._streams.get(name)
        if stream is None:
            await peer.close(
                Close.NO_SUCH_STREAM,
                refusal("no_such_stream", serves=sorted(self._streams)),
            )
            return

        if isinstance(requested, Publish):
            if not self._publish:
                await peer.close(Close.BAD_REQUEST, refusal("publish_disabled"))
                return

            await stream.serve_publisher(peer)
            return

        try:
            await stream.serve_subscriber(peer, requested)

        except NotReplayable as exc:
            await peer.close(
                Close.NOT_REPLAYABLE,
                refusal("not_replayable", why=exc.why, **exc.fields),
            )


def asgi(
    streams: Stream | Iterable[Stream],
    *,
    maintain: bool | Maintain = True,
    replicate: bool = True,
    publish: bool = False,
) -> _Mounted:
    """An ASGI app serving `streams`, for mounting in an existing service.

    Takes the keywords of `serve` that are about the streams rather than the
    socket. There is no `host`, `port`, `ssl`, `compression` or
    `ping_interval`: those belong to the server the host app is running, and
    restating them here would be two places to set one thing.

    **Two consequences of the ASGI server owning the socket**, both worth
    knowing before mounting rather than after:

    * **Keepalive is the server's.** `serve` passes `ping_interval` to
      `websockets`, which defaults to pinging every 20s — and that is what
      makes a dead peer surface as a close within ~40s instead of sitting
      silent. Under ASGI it is the deployer's setting (uvicorn's
      `--ws-ping-interval`), so a deployment that disables it has no
      detection at that layer.
    * **`compression` is the server's too**, and its default is the opposite
      of this library's. `serve` turns permessage-deflate OFF because the
      encode is shared across subscribers while deflate is per connection —
      4 us of CPU per message at one subscriber against 691 us at 200. A host
      app that enables compression globally pays that, and the symptom is a
      CPU-bound server dropping subscribers for falling behind.

    `async with` the returned object from the host app's lifespan. That is
    what starts the maintainers, and Starlette does not run a mounted
    sub-app's lifespan for you.
    """
    return _Mounted(streams, maintain=maintain, replicate=replicate, publish=publish)


__all__ = ["asgi"]
