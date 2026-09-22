"""`serve` — a `Stream` behind a WebSocket port.

Thin on purpose. Everything that decides what a subscriber receives is in
`_stream`; this routes a path to a stream, turns a refusal into a close code,
and hands the rest straight to `websockets`. The whole handler is forty lines
because the interesting parts are testable without a socket.

**Routing is by `Stream.name`, and that is the name's only home.** An earlier
shape took a `{name: stream}` mapping, which meant a stream carried a name for
its greeting and the mapping carried one for routing, and the two could
disagree — a subscriber to `/trades` greeted as `quotes`. A stream with no
name is served at `/`, which is the same empty name it already has rather than
a second spelling of "no name".
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from websockets.asyncio.server import serve as _ws_serve
from websockets.exceptions import ConnectionClosed

from streamcast._errors import Close, NotReplayable
from streamcast._maintain import Maintain, Supervisor
from streamcast._protocol import parse_subscribe, refusal
from streamcast._stream import Stream

if TYPE_CHECKING:
    from collections.abc import Iterable

    from websockets.asyncio.server import Server, ServerConnection

_DETAIL_CHARS = 80
"""How much of a parse error travels in a 4400.

`refusal` trims by dropping whole fields, so an untruncated detail would push
itself off the close frame and the subscriber would get `{"error":
"bad_request"}` — the one case where saying less loses the entire diagnosis.
Eighty characters is every message `parse_subscribe` produces.
"""


def _routes(streams: Stream | Iterable[Stream]) -> dict[str, Stream]:
    """Name to stream, refusing a collision rather than resolving one.

    Two streams with one name is a configuration bug with no correct
    resolution: whichever loses is silently unreachable, and the subscriber
    that wanted it gets a stream of somebody else's messages — which looks
    like working software.
    """
    listed = [streams] if isinstance(streams, Stream) else list(streams)
    if not listed:
        msg = "serve needs at least one Stream"
        raise ValueError(msg)

    routes: dict[str, Stream] = {}
    for stream in listed:
        if stream.name in routes:
            msg = f"two streams are both named {stream.name!r}"
            raise ValueError(msg)

        routes[stream.name] = stream

    return routes


class _Served:
    """The websockets server, plus the maintainers that must die with it.

    A thin proxy rather than a new object model: everything
    `websockets.Server` exposes — `sockets`, `serve_forever`, `connections`,
    `is_serving` — is reached through `__getattr__` untouched, and only
    `close`/`wait_closed` are wrapped, because those are the two that must
    also stop a subprocess.

    Wrapping at all is a cost, and the alternative was worse: a separate
    `async with streamcast.maintaining(stream)` beside the `serve` is one more
    line to forget, and forgetting it is the whole defect `_maintain` exists
    to fix.
    """

    __slots__ = ("_maintainers", "_server", "_serving")

    def __init__(self, serving: Server, maintainers: list[Supervisor]) -> None:
        self._serving = serving
        self._maintainers = maintainers
        self._server: Server | None = None

    async def _start(self) -> _Served:
        if self._server is None:
            self._server = await self._serving
            # After the listener is up, so a bind failure does not leave a
            # subprocess sweeping a log nothing is writing to.
            for maintainer in self._maintainers:
                maintainer.start()

        return self

    def __await__(self):  # noqa: ANN204 — an awaitable's own protocol
        return self._start().__await__()

    async def __aenter__(self) -> _Served:
        return await self._start()

    async def __aexit__(self, *_exc: object) -> None:
        self.close()
        await self.wait_closed()

    def close(self, close_connections: bool = True) -> None:
        for maintainer in self._maintainers:
            maintainer.terminate()

        if self._server is not None:
            self._server.close(close_connections)

    async def wait_closed(self) -> None:
        if self._server is not None:
            await self._server.wait_closed()

        for maintainer in self._maintainers:
            await maintainer.wait_closed()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._server, name)


def _supervisors(
    routes: dict[str, Stream], maintain: bool | Maintain
) -> list[Supervisor]:
    """One maintainer per stream that has a log, or none.

    A live-only stream has nothing to sweep, so `maintain=True` on a server of
    them spawns nothing rather than a process with no work to do.
    """
    if maintain is False:
        return []

    plan = Maintain() if maintain is True else maintain

    return [
        Supervisor(Path(stream.log.root), stream.log.name, plan)
        for stream in routes.values()
        if stream.log is not None
    ]


def serve(
    streams: Stream | Iterable[Stream],
    host: str | None = None,
    port: int | None = None,
    *,
    maintain: bool | Maintain = True,
    compression: str | None = None,
    **kwargs: Any,
) -> _Served:
    """Serve one or more streams. Same shape as `websockets.serve`.

        async with streamcast.serve(stream, "localhost", 8765):
            async for message in upstream:
                await stream.send(message)

    Returns what `websockets.serve` returns, so `async with`, `await`, and
    `server.serve_forever()` all work exactly as they do there, and every
    keyword it takes is passed through — `ssl`, `process_request`,
    `ping_interval`, and the rest.

    **`compression` defaults to None here and to `"deflate"` there**, which is
    the one deviation. permessage-deflate keeps a 32 KB compressor per
    connection, so a frame this library deliberately encodes once is then
    compressed once *per subscriber*: fan-out becomes O(subscribers) CPU, on
    the loop, at the message rate. streamcast's case is many subscribers on
    one box or one LAN — that is what it was built for — where the bandwidth
    is free and the CPU is not. Pass `compression="deflate"` to turn it back
    on for subscribers across a WAN, where the trade reverses.

    **`maintain=True` starts one maintainer subprocess per stream that has a
    log**, and stops it when the server closes. That is a departure from
    litelink's "the library owns neither the thread nor the interval", and it
    is deliberate: a streamcast server already owns a socket, a task per
    subscriber and a queue per subscriber, so owning its own storage
    maintenance is consistent — and the alternative default reproduces the
    defect it exists to fix. Nothing in this library sealed before it existed;
    measured on 120,000 rows, every one of them still in the SQLite buffer.
    See `_maintain`, which also says why it is never a thread.

    `maintain=False` opts out, for a deployment that runs its own the way
    litelink's `examples/adsb/` does — four processes, one per storage role,
    which is the right shape once the costs justify it.

    A subscription is read-only and the server never calls `recv` on one. A
    client that sends anyway fills its own receive buffer, stops being able to
    send, and is closed by the keepalive when its pongs stop arriving.
    """
    routes = _routes(streams)
    maintainers = _supervisors(routes, maintain)

    async def handler(connection: ServerConnection) -> None:
        # `request` is optional on the connection because a `ServerConnection`
        # exists before its handshake completes. Inside a handler it has, so
        # this is narrowing rather than a check — but it closes rather than
        # defaulting the path, because a default would route a request whose
        # target is unknown to whichever stream is served at `/`.
        request = connection.request
        if request is None:  # pragma: no cover — unreachable from `serve`
            await connection.close(Close.BAD_REQUEST, refusal("bad_request"))
            return

        try:
            name, requested = parse_subscribe(request.path)
        except ValueError as exc:
            await connection.close(
                Close.BAD_REQUEST,
                refusal("bad_request", detail=str(exc)[:_DETAIL_CHARS]),
            )
            return

        stream = routes.get(name)
        if stream is None:
            await connection.close(
                Close.NO_SUCH_STREAM,
                refusal("no_such_stream", serves=sorted(routes)),
            )
            return

        try:
            await stream.serve_subscriber(connection, requested)
        except NotReplayable as exc:
            await connection.close(
                Close.NOT_REPLAYABLE,
                refusal("not_replayable", why=exc.why, **exc.fields),
            )
        except ConnectionClosed:
            # The ordinary end of a subscription: the peer went away, or the
            # pump closed it for falling behind and the next send raised. Not
            # an error, and letting it out would log a traceback per
            # disconnect — which on a dashboard box is a traceback per page
            # reload.
            return

    return _Served(
        _ws_serve(handler, host, port, compression=compression, **kwargs),
        maintainers,
    )


__all__ = ["serve"]
