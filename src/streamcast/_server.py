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
from typing import TYPE_CHECKING, Any, Protocol

from websockets.asyncio.server import serve as _ws_serve
from websockets.exceptions import ConnectionClosed

from streamcast._errors import Close, NotReplayable
from streamcast._maintain import Maintain, Supervisor
from streamcast._protocol import parse_subscribe, refusal
from streamcast._replicate import Sidecar
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


class _Child(Protocol):
    """What `_Served` needs of a supervised subprocess.

    Two kinds satisfy it — the maintainer and the litestream sidecar — and
    they are different enough that a shared base class would carry nothing.
    What they owe the server is a lifetime, which is these three calls.
    """

    def start(self) -> None: ...

    def terminate(self) -> None: ...

    async def wait_closed(self) -> None: ...


class _Served:
    """The websockets server, plus the subprocesses that must die with it.

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

    __slots__ = ("_children", "_server", "_serving", "_streams")

    def __init__(
        self, serving: Server, children: list[_Child], streams: list[Stream]
    ) -> None:
        self._serving = serving
        self._children = children
        self._streams = streams
        self._server: Server | None = None

    async def _start(self) -> _Served:
        if self._server is None:
            self._server = await self._serving
            # After the listener is up, so a bind failure does not leave a
            # subprocess sweeping a log nothing is writing to.
            for child in self._children:
                child.start()

        return self

    def __await__(self):  # noqa: ANN204 — an awaitable's own protocol
        return self._start().__await__()

    async def __aenter__(self) -> _Served:
        return await self._start()

    async def __aexit__(self, *_exc: object) -> None:
        self.close()
        await self.wait_closed()

    def close(self, close_connections: bool = True) -> None:
        for child in self._children:
            child.terminate()

        if self._server is not None:
            self._server.close(close_connections)

    async def wait_closed(self) -> None:
        if self._server is not None:
            await self._server.wait_closed()

        for child in self._children:
            await child.wait_closed()

        # LAST, and the order is the point: a replay in flight is reading the
        # log in a worker thread, so closing it before the connections are
        # done would pull the file out from under a scan. `aclose` is a no-op
        # for a stream whose log was handed in — that one is the caller's.
        for stream in self._streams:
            await stream.aclose()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._server, name)


def _supervisors(
    routes: dict[str, Stream], maintain: bool | Maintain
) -> list[Supervisor]:
    """One maintainer per stream that has a log, or none.

    A live-only stream has nothing to sweep, so `maintain=True` on a server of
    them spawns nothing rather than a process with no work to do.

    WAL replication is a separate concern with its own argument — see
    `_sidecars`, which runs litestream for a log that needs it.
    """
    if maintain is False:
        return []

    plan = Maintain() if maintain is True else maintain

    return [
        Supervisor(Path(stream.log.root), stream.log.name, plan)
        for stream in routes.values()
        if stream.log is not None
    ]


def _sidecars(routes: dict[str, Stream], replicate: bool) -> list[Sidecar]:
    """One litestream sidecar per stream whose log replicates its WAL.

    `wal_replication` is opt-in on the log, so this is empty for almost every
    deployment and starts nothing. When it is on, the log's whole point is
    surviving the loss of its machine — and a server that came up and
    replicated nothing would leave that belief in place with none of the
    protection, which is why a missing binary raises here rather than at the
    first missed push.
    """
    if not replicate:
        return []

    return [
        Sidecar.new(stream.log)
        for stream in routes.values()
        if stream.log is not None and stream.log.config.wal_replication
    ]


def serve(
    streams: Stream | Iterable[Stream],
    host: str | None = None,
    port: int | None = None,
    *,
    maintain: bool | Maintain = True,
    replicate: bool = True,
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

    **`compression` defaults to None here and to `"deflate"` there.** Not a
    judgement about LAN versus WAN — a WebSocket server is a network server
    and this one is used across boxes. It is that permessage-deflate is PER
    CONNECTION while the encode is shared. `send` encodes a frame once and
    hands the same bytes to every subscriber; deflate then compresses those
    identical bytes once per subscriber, so fan-out becomes O(subscribers)
    CPU at the message rate on a frame that was already serialised.

    *Measured* on a six-column trade row: encode 0.564 us once, deflate 3.454
    us each. CPU per message is 4 us at one subscriber and 691 us at 200. At
    50 subscribers and 30,000 msg/s that asks for 1.5M compressions/s where a
    core manages roughly 290k.

    **The two failure modes are not symmetric, which is what picks the
    default.** Wrong with it ON, the server goes CPU-bound, subscribers fall
    behind, and `max_backlog` drops them — an outage that reads like a bug.
    Wrong with it OFF, you send 26.9 Mbit/s per subscriber instead of 4.6 at
    30,000 msg/s: a bill, and one keyword to fix. So it is off, and
    `compression="deflate"` turns it on where bandwidth costs more than CPU —
    few subscribers, over a WAN. It is worth having there: 5.8x smaller on
    this row, 112 bytes to 19.

    There is no middle setting. Without context takeover — the variant that
    would in principle let one compressed frame be shared across connections
    — the same frames compress 1.1x, so "compress once, fan out" is not
    available.

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
    # Both resolved here, synchronously, so a missing litestream or a bad
    # stream set fails at the call rather than inside a task nobody awaits.
    children = [*_supervisors(routes, maintain), *_sidecars(routes, replicate)]

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
        children,
        list(routes.values()),
    )


__all__ = ["serve"]
