"""One broadcast: the offsets, the subscribers, and the subscribe handshake.

A `Stream` is the whole of streamcast that is not transport. `serve` puts it
behind a WebSocket port and `connect` reads it from the other end, but the
ordering guarantee, the offset assignment and the replay partition are all
decided here — which means they can be tested without a socket, and are.

**The invariant the rest of the library rests on: `send` contains no `await`,
and neither does the pair of statements that attaches a subscriber.** Both are
therefore atomic against the event loop, and that atomicity is not a
performance note — it is the correctness argument:

* In `send`, the offset is assigned, the row is made durable, and the frame is
  offered to every subscriber with nothing able to interleave. Two concurrent
  senders cannot produce a subscriber that sees offset 8 before offset 7.
* In `_attach`, the subscriber joins the fan-out set and the frontier is read
  with nothing able to interleave. Every offset below the frontier is already
  durable in the log; every offset from it up is already in the new
  subscriber's queue. The two sets partition the stream exactly — no gap, no
  duplicate — and that is what makes a resume exactly-once.

If an `await` is ever added inside either, both properties are gone and
nothing will fail loudly. `tests/test_ordering.py` is the guard.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Final

import litelink
from websockets.frames import CloseCode

from streamcast import _log, _schema
from streamcast._errors import NotReplayable
from streamcast._protocol import EARLIEST, encode, greeting
from streamcast._subscriber import Subscriber

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable, Mapping, Sequence
    from os import PathLike

    from litelink import Row, WriteHandle
    from websockets.asyncio.server import ServerConnection

MAX_BACKLOG: Final = 8_192
"""Messages a subscriber may fall behind before it is dropped.

Counted in MESSAGES, not bytes, because that is what the queue holds — a
pointer to a frame every subscriber shares. A backlog is ~8 bytes per
subscriber per queued message plus one copy of each frame, so 8,192 across 200
subscribers is ~13 MB of pointers over whatever the frames themselves weigh.
Size it in bytes by multiplying by your own message size; there is no setting
that does it for you, because the library never sees a typical message until
it is running.
"""

MAX_REPLAY: Final = 100_000
"""How far back a subscribe may ask to resume from.

It bounds the server, not the log: a replay is a DuckDB scan in a worker
thread and a subscriber asking for ten million rows would hold one for
minutes. Past this, the answer is to read the log directly — which needs
nothing from streamcast and is what litelink is for.

**It is sized against `MAX_BACKLOG`, not independently.** A replay streams
while live messages queue behind it, so a subscriber that takes longer to
catch up than `MAX_BACKLOG` messages of live traffic is dropped the moment it
arrives, having done all the work. The defaults hold with room: a replay reads
at ~1M rows/s from local Parquet, so 100,000 messages is ~0.1 s, and a feed
would have to run above 80,000 messages/s to put 8,192 messages into the queue
in that time. Raise one and check the other.
"""


class Stream:
    """A broadcast, and the offsets that make it resumable.

    Constructed before there is a loop and serves any number of subscribers on
    whichever loop `serve` runs on. It owns no thread, starts nothing, and
    holds nothing open — including the log, which the caller opened and the
    caller closes.

        log = litelink.new("data", "trades", schema=schema)   # YOUR columns
        stream = streamcast.Stream("trades", log=log)

        async with streamcast.serve(stream, "localhost", 8765):
            async for frame in upstream:
                await stream.send(parse(frame))      # a row, not a blob

    **The schema is yours.** streamcast declares no columns; the log is an
    ordinary litelink table with whatever shape you gave it, so every column
    prunes, compresses and is queryable from any Iceberg engine. A row goes in
    and the same row comes back out, live or replayed.

    **`log` is what makes an offset a resume cursor**, and it is optional in
    both directions: a tickerplant's log is optional too — some kdb
    implementations omit it — so a stream without one is not a lesser thing,
    just one nothing can resume from. Without it
    nothing assigns offsets at all — `send` returns None and every frame
    carries `null` — so `?offset=` is refused outright rather than appearing to
    work until the day a subscriber needs it. With it, every row is durable
    *before* any subscriber sees it — so a server that dies
    between the two has published nothing it cannot replay, which is the
    ordering that makes recovery a replay rather than a reconciliation.
    """

    __slots__ = (
        "_columns",
        "_end_offset",
        "_log",
        "_max_backlog",
        "_max_replay",
        "_name",
        "_owned",
        "_shape",
        "_subscribers",
    )

    def __init__(
        self,
        name: str = "",
        *,
        log: WriteHandle | None = None,
        root: str | PathLike[str] | None = None,
        schema: Mapping[str, object] | None = None,
        sort_by: Sequence[str] | None = None,
        config: object | None = None,
        archive: str | None = None,
        s3: object | None = None,
        max_backlog: int = MAX_BACKLOG,
        max_replay: int = MAX_REPLAY,
    ) -> None:
        owned = None
        if root is not None or schema is not None:
            if log is not None:
                msg = "pass either an open `log=` or `root=`+`schema=`, not both"
                raise ValueError(msg)

            if root is None or schema is None:
                msg = "`root=` and `schema=` go together — one without the other"
                raise ValueError(msg)

            owned = _open_or_create(
                root,
                name,
                schema=schema,
                sort_by=sort_by,
                config=config,
                archive=archive,
                s3=s3,
            )
            log = owned

        # The declared column order, read once. It fixes the key order of
        # every frame, and a replayed row must encode to the same bytes as the
        # live one it repeats (I6) — so this cannot be re-derived per message
        # from whatever keys a caller's dict happened to carry.
        #
        # None for a stream with no log: there is no declared schema, so a
        # frame takes the row's own order. Such a stream is a multicaster and
        # nothing replays from it, so there is no second encoding to match.
        self._columns = None if log is None else _log.columns(log)
        # The shape a subscriber is told at subscribe, built once. None for a
        # stream with no log: there are no declared columns to publish.
        self._shape = None if log is None else _schema.from_arrow(log.schema)
        # Closed by `aclose` only when this object created it. A log the
        # caller opened stays the caller's — they may be sharing it, and a
        # library that closes a handle it was lent is a library you cannot
        # lend one to.
        self._owned = owned
        self._name = name
        self._log = log
        self._max_backlog = max_backlog
        self._max_replay = max_replay
        # Read ONCE, here, and maintained by `send` thereafter. litelink's
        # `end_offset()` is a SQLite read and `append` returns the offset it
        # assigned, so asking the log per message would be a round trip for a
        # number the previous call already returned. The one risk of a cached
        # counter — drifting from the log — cannot happen, because nothing
        # else writes this log: litelink allows exactly one writer.
        #
        # None with no log, and there is deliberately no counter to stand in.
        # An in-memory sequence would look exactly like a resume cursor to
        # every subscriber and to every operator reading a greeting, and would
        # be wrong the moment the process restarted. Nothing assigned an
        # offset, so nothing reports one.
        self._end_offset: int | None = log.end_offset() if log is not None else None
        self._subscribers: set[Subscriber] = set()

    def __repr__(self) -> str:
        durable = "durable" if self._log is not None else "live-only"
        return (
            f"<streamcast.Stream {self._name!r} {durable} "
            f"end_offset={self._end_offset} subscribers={len(self._subscribers)}>"
        )

    # -- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        """What this stream is served at. `""` is served at `/`."""
        return self._name

    @property
    def log(self) -> WriteHandle | None:
        """The litelink handle, or None.

        Closed by `aclose` when this `Stream` created it from `root=`+`schema=`,
        and never when it was passed in — that one is the caller's.
        """
        return self._log

    @property
    def schema(self) -> dict[str, object] | None:
        """The stream's shape as JSON Schema, or None without a log.

        What the greeting publishes, so a subscriber in another language can
        read the columns without this repo.
        """
        return None if self._shape is None else dict(self._shape)

    @property
    def durable(self) -> bool:
        """Whether a log is attached, and so whether offsets survive a restart."""
        return self._log is not None

    @property
    def end_offset(self) -> int | None:
        """The offset the next row will be assigned, or None without a log.

        The same quantity as litelink's `end_offset()`, and deliberately the
        same name — but an attribute here and a call there, because litelink
        reads it from SQLite and this is the counter `send` already maintains.

        None means nothing is assigning offsets, which is a different fact
        from "no rows yet" and should not be confused with 0 or 1.
        """
        return self._end_offset

    @property
    def subscribers(self) -> int:
        """How many are attached right now."""
        return len(self._subscribers)

    # -- publish -----------------------------------------------------------

    async def send(self, row: Row) -> int | None:
        """Make one row durable, then fan it out. Returns its offset.

        **None without a log**, because nothing assigned one. A live-only
        stream fans out and forgets; handing back a per-process counter would
        give the caller a number that behaves like a resume cursor until the
        day the server restarts.

        `row` is a mapping over the log's declared columns — litelink's `Row`,
        the same thing `litelink.append` takes. litelink validates it against
        the schema, so a wrong type or an unknown column raises here with a
        message naming the column, and nothing is broadcast.

        **Durable first.** With a log attached this returns only once the row
        is committed — one SQLite transaction at `synchronous=FULL`, which
        litelink measures at a ~400 us median — and a failure there raises
        with nothing broadcast. That ordering is the reason a crashed server
        is recoverable: a message a subscriber has seen is always a message
        the log holds, never the other way round.

        **It never awaits a consumer**, and today it does not await at all:
        the append is synchronous and the fan-out is a queue insert per
        subscriber. `async` is the signature `websockets` has, and it is what
        leaves room to move the append off the loop without breaking callers
        — see the module docstring for what that move would have to preserve.

        The frame is the row as JSON text, encoded once and shared by every
        subscriber (I6) — measured at 0.285 us for a six-column row. The key
        order comes from the log's schema rather than from this dict, which is
        what makes a replay of this row byte-identical to what goes out now.

        Throughput on the durable path is one fsync per call. `send_many` is
        the lever: it commits a whole group in one transaction.

        **A publish loop that never awaits starves every subscriber.** That
        is the other side of the atomicity above: nothing here yields, so a
        `for … : await stream.send(…)` over a list in memory runs to
        completion before any pump gets the loop back, and every subscriber
        sees the whole run arrive at once — which for a run longer than
        `max_backlog` means every one of them is dropped for falling behind.
        A real publisher awaits its upstream between messages and never meets
        this. A backfill from memory should `await asyncio.sleep(0)` in its
        loop, or hand the group to `send_many` and let the subscribers take
        it at their own pace.
        """
        offset = None
        if self._log is not None:
            offset = self._log.append(row)
            self._end_offset = offset + 1

        self._fan_out(encode(offset, row, self._columns))

        return offset

    async def send_many(self, rows: Iterable[Row]) -> list[int | None]:
        """Make a group of rows durable in ONE transaction, then fan each out.

        The write-throughput lever, and it is a call-site choice rather than a
        setting: one fsync for the group instead of one per message. litelink
        measures the same difference on `extend`.

        Each row still gets its own offset and its own frame, so a subscriber
        cannot tell a group from the same rows sent one at a time. That is
        deliberate — batching is the server's durability decision, and making
        it visible on the wire would make every subscriber's parser depend on
        how the publisher happened to poll.
        """
        batch = list(rows)
        if not batch:
            return []

        offsets: list[int | None] = [None] * len(batch)
        if self._log is not None:
            offsets = list(self._log.extend(batch))
            self._end_offset = offsets[-1] + 1  # ty: ignore[unsupported-operator]

        for offset, row in zip(offsets, batch, strict=True):
            self._fan_out(encode(offset, row, self._columns))

        return offsets

    def _fan_out(self, frame: bytes) -> None:
        """One encoded frame into every subscriber's queue.

        Encoded once by the caller and shared, so a queue entry is a pointer
        rather than a copy. `offer` cannot block, raise, or detach anything,
        which is what makes iterating the set here safe without a snapshot.
        """
        for subscriber in self._subscribers:
            subscriber.offer(frame)

    async def aclose(self, reason: str = "server shutting down") -> None:
        """Drop every subscriber with a 1001. Does not close the log.

        Concurrently, because `close` waits for each peer's close handshake
        and doing that in series is `close_timeout` per dead connection.
        """
        await asyncio.gather(
            *(
                subscriber.close(CloseCode.GOING_AWAY, reason)
                for subscriber in list(self._subscribers)
            ),
            return_exceptions=True,
        )

        # A log this object opened is a log this object closes. One handed in
        # is left alone: the caller may be sharing it, and closing a borrowed
        # handle is how a library becomes one you cannot lend to.
        if self._owned is not None:
            self._owned.close()
            self._owned = None

    # -- subscribe ---------------------------------------------------------

    async def serve_subscriber(
        self, connection: ServerConnection, requested: int | None
    ) -> None:
        """Attach one subscriber and serve it until the connection ends.

        Raises `NotReplayable` for an offset this stream cannot serve; `serve`
        turns that into a 4416. Everything else — the greeting, the replay,
        the live pump — happens here, and the order is load-bearing.
        """
        # `(log, start)` rather than `start`, so the handle a replay needs
        # travels with the decision that it is needed. The alternative is
        # re-narrowing `self._log` at the use site, which is an assertion
        # about a branch three statements away.
        resolved = await self._resolve(requested)

        subscriber = Subscriber(connection, max_backlog=self._max_backlog)
        # ── ATOMIC. Do not put an await between these two statements. ──
        # Joining the set first means nothing sent from here on is missed;
        # reading the frontier second means everything below it is already
        # durable. See the module docstring.
        self._subscribers.add(subscriber)
        frontier = self._end_offset
        # ──────────────────────────────────────────────────────────────

        replay: AsyncGenerator[tuple[int, bytes], None] | None = None
        replaying: tuple[int, int] | None = None
        try:
            if resolved is not None:
                if frontier is None:  # pragma: no cover — implied by `resolved`
                    # A resolved replay means `_resolve` found a log, and a
                    # log means the counter is an integer. Stated as a raise
                    # rather than an assert because the alternative — skipping
                    # the replay — would be a silent gap at the join.
                    msg = "a replay resolved against a stream with no offsets"
                    raise RuntimeError(msg)

                replaying = (resolved[1], frontier)
                replay = await self._replay_from(*resolved, frontier)

            await connection.send(
                greeting(
                    stream=self._name,
                    end_offset=frontier,
                    replay=replaying,
                    durable=self._log is not None,
                    schema=self._shape,
                )
            )
            await subscriber.run(replay)
        finally:
            self._subscribers.discard(subscriber)
            if replay is not None:
                # Releases the DuckDB result the scan is holding. A subscriber
                # that disconnects mid-replay leaves this generator suspended
                # otherwise, and under a reconnect storm that is an unbounded
                # number of live scans against one log.
                await replay.aclose()

    async def _resolve(self, requested: int | None) -> tuple[WriteHandle, int] | None:
        """The log and offset a replay should start at, or None for live-only.

        Every refusal here is cheap and answerable before a scan is opened.
        The one that is not — an offset below what the log still holds — is
        settled in `_replay_from`, because only the scan knows.
        """
        if requested is None:
            return None

        log = self._log
        if log is None:
            raise NotReplayable("not_durable")

        frontier = self._end_offset
        if frontier is None:  # pragma: no cover — a log always has a counter
            msg = "a stream with a log has no offset counter"
            raise RuntimeError(msg)

        if requested == EARLIEST:
            # The only call that asks the log where it starts, and it is in a
            # thread because `coverage()` resolves offset extents from table
            # statistics — local file reads, and a metadata GET when the log
            # has been evicted to its archive.
            first = await asyncio.to_thread(_log.earliest, log)
            if first is None:
                raise NotReplayable("empty")

            requested = first

        if requested > frontier:
            raise NotReplayable("ahead", offset=requested, end_offset=frontier)

        behind = frontier - requested
        if behind > self._max_replay:
            raise NotReplayable(
                "too_old",
                offset=requested,
                behind=behind,
                max_replay=self._max_replay,
            )

        return log, requested

    async def _replay_from(
        self, log: WriteHandle, start: int, frontier: int
    ) -> AsyncGenerator[tuple[int, bytes], None]:
        """The replay stream, with its first row checked against the request.

        **Pulled one row early on purpose.** A log whose retention has passed
        the requested offset would otherwise serve from wherever it does
        start, and the subscriber would receive a stream that silently begins
        above where it asked — a hole at the join, which is the one wrong
        answer a resume must never give. Reading the first row before the
        greeting turns that into a 4416 with the offset that would have
        worked.

        An empty range is not a hole: `start == frontier` is a subscriber
        resuming with nothing outstanding, and it is the common case for a
        reconnect that lost the connection rather than the race.
        """
        stream = _log.replay(log, start, frontier)
        first = await anext(stream, None)
        if first is None:
            return _empty()

        offset, _frame = first
        if offset > start:
            await stream.aclose()
            raise NotReplayable("evicted", offset=start, earliest=offset)

        return _prepend(first, stream)


def _open_or_create(
    root: str | PathLike[str],
    name: str,
    *,
    schema: Mapping[str, object],
    sort_by: Sequence[str] | None,
    config: object | None,
    archive: str | None,
    s3: object | None,
) -> WriteHandle:
    """The log for this stream, created if it is not there yet.

    The try/except every caller writes identically: a server has to `new` the
    first time and `open` every time after, and `new` raises rather than
    adopting an existing log. Doing it here is most of what makes `root=` +
    `schema=` worth having.

    **An existing log is checked against the declared schema**, because `open`
    takes none of the shape — it reads it from disk — so a declaration that
    disagreed would be silently ignored and every send would be validated
    against columns the caller did not write down. That is the failure this
    convenience would otherwise introduce.
    """
    declared = _schema.to_arrow(schema)
    try:
        log = litelink.open(root, name)
    except FileNotFoundError:
        return litelink.new(
            root,
            name,
            schema=declared,
            sort_by=sort_by,
            config=config,  # ty: ignore[invalid-argument-type]
            archive=archive,
            s3=s3,  # ty: ignore[invalid-argument-type]
        )

    if list(log.schema) != list(declared):
        # Read BEFORE closing. `log.schema` goes to the buffer's `meta` table,
        # so building this message after `close()` raises "Cannot operate on a
        # closed database" and buries the real complaint.
        found = log.schema.names
        log.close()
        msg = (
            f"the log at {root}/{name} has columns {found}, and this "
            f"stream declares {declared.names}. litelink fixes a log's shape at "
            f"creation, so an existing one cannot be re-declared — open it "
            f"yourself and pass `log=`, or point `root=` somewhere else."
        )
        raise ValueError(msg)

    return log


async def _empty() -> AsyncGenerator[tuple[int, bytes], None]:
    """Nothing to replay. A generator rather than None so the pump has one path."""
    return
    yield  # pragma: no cover — unreachable, and what makes this a generator


async def _prepend(
    first: tuple[int, bytes], rest: AsyncGenerator[tuple[int, bytes], None]
) -> AsyncGenerator[tuple[int, bytes], None]:
    """Put back the row `_replay_from` pulled to inspect it."""
    yield first
    async for item in rest:
        yield item


__all__ = ["MAX_BACKLOG", "MAX_REPLAY", "Stream"]
