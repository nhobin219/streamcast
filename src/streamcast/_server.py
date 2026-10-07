"""`serve` — a `Stream` behind a WebSocket port.

**A log-backed pub/sub broker.** Publishers and subscribers are both clients of
this process, which owns the log, assigns every offset and is litelink's single
writer. Routing is exact-match on `Stream.name`: there is no topic tree and no
wildcard subscription, because a subscriber resumes by offset and an offset
belongs to one log — a subscription spanning streams would need a cursor per
stream, which is a different resume story than the one `_stream` enforces.

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

import asyncio
import inspect
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from websockets.asyncio.server import serve as _ws_serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Response

from streamcast._errors import Close, NotReplayable, ProtocolError
from streamcast._limits import MAX_BACKLOG, MAX_IN_FLIGHT, MAX_INBOUND, _bound
from streamcast._maintain import ROLES, Maintain, Supervisor, stop
from streamcast._protocol import Publish, parse_subscribe, refusal
from streamcast._replicate import Sidecar
from streamcast._stats import STATS_PATH, payload
from streamcast._stream import Stream

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

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

    __slots__ = ("_children", "_closing", "_server", "_serving", "_streams")

    def __init__(
        self, serving: Server, children: list[_Child], streams: list[Stream]
    ) -> None:
        self._serving = serving
        self._children = children
        self._streams = streams
        self._server: Server | None = None
        self._closing: asyncio.Future[None] | None = None

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
        """Begin closing: the maintainers' last work, then the listener.

        **The server keeps taking writes while the maintainers finish.** The
        seal and publish roles flush on the way out, one after the other
        (`_maintain.stop`), so that what this machine holds leaves it — and
        a broker that stopped accepting for that long would turn the flush
        into the longer window with no broker at all. Rows written meanwhile
        miss the flush; they are on disk, for the next server. `wait_closed`
        waits for all of it.
        """
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._close(close_connections))

    async def _close(self, close_connections: bool) -> None:
        try:
            await stop(self._children)
        finally:
            if self._server is not None:
                self._server.close(close_connections)

    async def wait_closed(self) -> None:
        if self._closing is not None:
            await self._closing

        if self._server is not None:
            await self._server.wait_closed()

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
    """FIVE maintainers — one per role of litelink's split — for every log
    this server serves, plus five more for each dedicated log.

    A live-only stream has nothing to sweep, so a server of them spawns
    nothing rather than processes with no work to do.

    **Five per server, not five per log.** A maintainer is a full interpreter
    with litelink, pyarrow, pyiceberg and duckdb loaded — ~150-200 MB RSS
    each — so each role's process covers every log, and a new stream costs a
    row in five loops rather than five more interpreters. The roles are split
    from each other because the work in them is, on CPU or the network; the
    logs need no such isolation. See `_maintain`.

    `Maintain.dedicated` names logs that still get their own five, for one
    large or hot enough that its compaction would hold up everyone else's.

    WAL replication is a separate concern with its own argument — see
    `_sidecars`, which runs litestream for the logs that need it.
    """
    if maintain is False:
        return []

    plan = Maintain() if maintain is True else maintain
    # A retired stream's log takes no more passes: it is finished, and
    # opened only for reading.
    logs = [
        stream.log
        for stream in routes.values()
        if stream.log is not None and stream.retirement is None
    ]

    # **A name that matches nothing is a raise, not a shrug.** Silently
    # ignoring it puts the log back in the shared loop — the one thing the
    # caller named it to avoid — and the symptom is a latency problem they
    # believe they already fixed. `_routes` refuses a name collision rather
    # than resolving one for the same reason.
    served = {log.name for log in logs}
    unknown = sorted(set(plan.dedicated) - served)
    if unknown:
        msg = (
            f"maintain=Maintain(dedicated=...) names {', '.join(map(repr, unknown))}, "
            f"which this server does not serve with a log. Served with a log: "
            f"{', '.join(map(repr, sorted(served))) or 'nothing'}. "
            f"Names are the LOG's, which need not be the route it is served at."
        )
        raise ValueError(msg)

    groups = [
        [(Path(log.root), log.name)] for log in logs if log.name in plan.dedicated
    ]
    shared = [
        (Path(log.root), log.name) for log in logs if log.name not in plan.dedicated
    ]
    # **A migrated stream's retired logs are not maintained.** `retire()`
    # publishes everything, evicts staging and sweeps both tables completely
    # before it marks the log retired, and litelink refuses a writer on one
    # from then on: there is nothing left to do, and nothing could open it
    # to try. One `publish` process is handed them anyway, to retire through
    # litelink any that a migration before streamcast 0.10 only sealed — see
    # `_maintain.finish_retiring`.
    if shared:
        groups.append(shared)

    retired = [target for stream in routes.values() for target in stream.retired]
    # The shared set's publish role if there is one, else the first.
    owner = len(groups) - 1 if shared else 0
    return [
        Supervisor(
            group,
            plan,
            role,
            retiring=retired if (index == owner and role == "publish") else (),
        )
        for index, group in enumerate(groups)
        for role in ROLES
    ]


def _sidecars(routes: dict[str, Stream], replicate: bool) -> list[Sidecar]:
    """ONE litestream for every log this server replicates, or none.

    **Opt-in twice:** `wal_replication` on the log, and `replicate=True` on
    the server. Off by default because most deployments replicate nothing, and
    one that does may run its own litestream. A log that asks for replication
    on a server that does not run it is warned about, by name, at every start
    — one line — rather than left believing it is protected.

    `wal_replication` is opt-in on the log, so this is empty for almost every
    deployment and starts nothing. When it is on, the log's whole point is
    surviving the loss of its machine — and a server that came up and
    replicated nothing would leave that belief in place with none of the
    protection, which is why a missing binary raises here rather than at the
    first missed push.

    **One process, not one per log.** litestream takes a `dbs` list, and each
    process was 40-170 MB — so four small streams spent most of a gigabyte
    replicating a producer of ~460 MB. The lock that keeps two litestreams off
    one database stays PER LOG, because it protects the database rather than
    the replicator; `Sidecar` holds one per log and replicates exactly the
    ones it holds.
    """
    shipping = [
        stream.log
        for stream in routes.values()
        if stream.log is not None
        and stream.retirement is None
        and stream.log.config.wal_replication
    ]
    if not replicate:
        if shipping:
            names = ", ".join(sorted(log.name for log in shipping))
            warnings.warn(
                f"log(s) {names} have wal_replication on, and this server was "
                f"started without replicate=True: nothing here replicates them. "
                f"Pass replicate=True, or run litestream for them yourself.",
                stacklevel=3,
            )

        return []

    return [Sidecar.new(shipping)] if shipping else []


def _info_hook(streams: list[Stream], path: str, chained: Any) -> Any:
    """A `process_request` that answers `path` with the stats, or defers.

    **Composed rather than assigned**, because `process_request` is a keyword
    a caller may already be using — for auth, for a health check of their own,
    for anything. Overwriting it would break that silently, so this answers
    its own path and hands every other request to whatever was passed in.

    Async, and the chained hook is awaited only if it returns an awaitable:
    `websockets` accepts either shape, so a caller's synchronous hook must
    keep working when it is wrapped by this one.
    """

    async def hook(connection: Any, request: Any) -> Any:
        if request.path == path:
            body = payload(streams)

            return Response(
                200,
                "OK",
                Headers(
                    {
                        "Content-Type": "application/json",
                        "Content-Length": str(len(body)),
                    }
                ),
                body,
            )

        if chained is None:
            return None

        answer = chained(connection, request)

        return await answer if inspect.isawaitable(answer) else answer

    return hook


def serve(
    streams: Stream | Iterable[Stream],
    host: str | None = None,
    port: int | None = None,
    *,
    maintain: bool | Maintain = True,
    replicate: bool = False,
    max_backlog: int | Mapping[str, int] = MAX_BACKLOG,
    max_inbound: int | Mapping[str, int] = MAX_INBOUND,
    max_in_flight: int | Mapping[str, int] = MAX_IN_FLIGHT,
    stats: bool | str = True,
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

    **`compression` defaults to None here and to `"deflate"` there**, because
    permessage-deflate is PER CONNECTION while the encode is shared. `send`
    encodes a frame once and hands the same bytes to every subscriber; deflate
    then compresses those identical bytes once per subscriber, so fan-out
    becomes O(subscribers) CPU at the message rate on a frame that was already
    serialised. Measured on a six-column trade row: encode 0.564 us once,
    deflate 3.454 us each, so CPU per message is 4 us at one subscriber and
    691 us at 200. At 50 subscribers and 30,000 msg/s that asks for 1.5M
    compressions/s where a core manages roughly 290k.

    **The failure modes decide the default, because they are not symmetric.**
    On and wrong, the server goes CPU-bound, subscribers fall behind, and
    `max_backlog` drops them — an outage that reads like a bug. Off and wrong,
    you send 26.9 Mbit/s per subscriber instead of 4.6: a bill, and one
    keyword to fix.

    `compression="deflate"` turns it on where bandwidth costs more than CPU —
    few subscribers, over a WAN — and it is worth having there, 5.8x smaller
    on this row, 112 bytes to 19. There is no middle setting: without context
    takeover, the variant that would let one compressed frame be shared across
    connections, the same frames compress 1.1x.

    **`maintain=True` starts ONE maintainer covering every stream that has a
    log** — a set of five subprocesses, one per role of litelink's split:
    seal, compact, publish, clean, clean-published — and stops them when the
    server closes. One set for the server rather than one per log: each is a
    full interpreter with litelink, pyarrow, pyiceberg and duckdb loaded —
    measured at 149 MB RSS on this box. The roles are split because their
    work is, on CPU or the network; the logs never needed the isolation.
    `Maintain(dedicated=("trades",))` gives a named log a set of its own, for
    one busy enough that its compaction would hold up the others'. That is a
    departure from
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

    **`GET /stats` answers on the same port** with every stream's `stats` —
    offsets, subscriber counts, and how long since each last took a row. It is
    for telling a quiet stream from a dead one without opening a subscription
    per stream to find out, and it carries no verdict: see `_stats` for why a
    freshness threshold cannot live in a library. `stats="/_internal/streams"`
    moves it; `stats=False` turns it off.

    **On by default.** It discloses strictly LESS than the socket beside it
    already does: a
    wrong-path connect is answered with `serves=` naming every stream, the
    greeting carries `end_offset`, and anyone who can reach the port can
    subscribe and read every row in full. Everything here except the
    subscriber count is derivable by subscribing, so gating it would protect
    nothing while leaving a stream that went quiet undiagnosable by default —
    which is the failure it exists to fix.

    A server that needs this private needs the port private, and one that
    needs the port public has already published the names.

    A `process_request` of your own still works with it: yours is called for
    every path but this one, whether it is sync or async.

    **Three keywords bound every queue the server keeps**, so a slow
    consumer, a slow disk or a fast publisher holds memory to a bound rather
    than running the process out of it: `max_backlog`, the frames a
    subscriber may fall behind before it is dropped; `max_inbound`, the rows
    a stream may have queued for commit before a send waits; `max_in_flight`,
    the replies owed one publisher connection before it stops being read.
    Each is one int for every stream, or a map from stream name to int that
    must name exactly the streams served. They are this process's settings,
    not the streams': a restart may change them. See `_limits`.

    A subscription is read-only and the server never calls `recv` on one. A
    client that sends anyway fills its own receive buffer, stops being able to
    send, and is closed by the keepalive when its pongs stop arriving.
    """
    routes = _routes(streams)
    # The queue bounds, before anything is written or started: a map that
    # misses a stream, or names one not served, fails the call.
    _bound(routes, max_backlog, max_inbound, max_in_flight)

    # **Every stream's metadata file, before anything else.** Written if it is
    # not there and synced to its S3 copy, and a failure is a failure to
    # start: a stream nothing else can read is found out here rather than at
    # the first remote read. First, too, because it is where a `Stream(log=…)`
    # learns its retired logs, which the maintainer below is handed.
    for stream in routes.values():
        stream.ensure_metadata()

    # Both resolved here, synchronously, so a missing litestream or a bad
    # stream set fails at the call rather than inside a task nobody awaits.
    children = [*_supervisors(routes, maintain), *_sidecars(routes, replicate)]

    if stats:
        kwargs["process_request"] = _info_hook(
            list(routes.values()),
            STATS_PATH if stats is True else stats,
            kwargs.get("process_request"),
        )

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
            name, requested, where = parse_subscribe(request.path)
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

        if isinstance(requested, Publish):
            # Every stream takes publishers; whether it can be written is the
            # stream's to say — a retired one refuses them itself (4410).
            try:
                await stream.serve_publisher(connection)
            except ConnectionClosed:
                return

            return

        try:
            await stream.serve_subscriber(connection, requested, where)
        except ProtocolError as exc:
            # A `where=` this stream cannot serve — a column it does not have,
            # a value that is not a scalar. Refused rather than served as a
            # subscription that silently never delivers, which looks exactly
            # like a quiet stream.
            await connection.close(
                Close.BAD_REQUEST,
                refusal("bad_request", detail=str(exc)[:_DETAIL_CHARS]),
            )
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
