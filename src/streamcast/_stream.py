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

from websockets.frames import CloseCode

from streamcast import _log
from streamcast._errors import NotReplayable
from streamcast._protocol import EARLIEST, encode, greeting, kind_of
from streamcast._subscriber import Subscriber

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from litelink import WriteHandle
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

It bounds the broker, not the log: a replay is a DuckDB scan in a worker
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

        stream = streamcast.Stream("trades", log=log)
        async with streamcast.serve(stream, "localhost", 8765):
            async for message in upstream:
                await stream.send(message)

    **`log` is what separates a multicaster from a tickerplant.** Without it
    the offsets are a counter in this process: they order the stream correctly
    and mean nothing after a restart, so `?offset=` is refused outright rather
    than appearing to work until the day a subscriber needs it. With it, every
    message is durable *before* any subscriber sees it — so a broker that dies
    between the two has published nothing it cannot replay, which is the
    ordering that makes recovery a replay rather than a reconciliation.
    """

    __slots__ = (
        "_end_offset",
        "_log",
        "_max_backlog",
        "_max_replay",
        "_name",
        "_subscribers",
    )

    def __init__(
        self,
        name: str = "",
        *,
        log: WriteHandle | None = None,
        max_backlog: int = MAX_BACKLOG,
        max_replay: int = MAX_REPLAY,
    ) -> None:
        if log is not None:
            # At construction, not at the first send. The failure this
            # prevents is a broker that starts fine, brings up its upstream
            # subscription, and raises inside `append` on the first message —
            # with the feed live and nowhere to put it.
            _log.validate(log)

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
        self._end_offset = log.end_offset() if log is not None else 1
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
        """The litelink handle, or None. Opened and closed by the caller."""
        return self._log

    @property
    def durable(self) -> bool:
        """Whether a log is attached, and so whether offsets survive a restart."""
        return self._log is not None

    @property
    def end_offset(self) -> int:
        """The offset the next message will be assigned.

        The same quantity as litelink's `end_offset()`, and deliberately the
        same name — but an attribute here and a call there, because litelink
        reads it from SQLite and this is the counter `send` already maintains.
        """
        return self._end_offset

    @property
    def subscribers(self) -> int:
        """How many are attached right now."""
        return len(self._subscribers)

    # -- publish -----------------------------------------------------------

    async def send(self, message: str | bytes) -> int:
        """Make one message durable, then fan it out. Returns its offset.

        **Durable first.** With a log attached this returns only once the row
        is committed — one SQLite transaction at `synchronous=FULL`, which
        litelink measures at a ~400 us median — and a failure there raises
        with nothing broadcast. That ordering is the reason a crashed broker
        is recoverable: a message a subscriber has seen is always a message
        the log holds, never the other way round.

        **It never awaits a consumer**, and today it does not await at all:
        the append is synchronous and the fan-out is a queue insert per
        subscriber. `async` is the signature `websockets` has, and it is what
        leaves room to move the append off the loop without breaking callers
        — see the module docstring for what that move would have to preserve.

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
        kind = kind_of(message)
        if self._log is not None:
            offset = self._log.append(_log.row(kind, message))
            self._end_offset = offset + 1
        else:
            offset = self._end_offset
            self._end_offset = offset + 1

        self._fan_out(encode(offset, kind, message))

        return offset

    async def send_many(self, messages: Iterable[str | bytes]) -> list[int]:
        """Make a group durable in ONE transaction, then fan each out.

        The write-throughput lever, and it is a call-site choice rather than a
        setting: one fsync for the group instead of one per message. litelink
        measures the same difference on `extend`.

        Each message still gets its own offset and its own frame, so a
        subscriber cannot tell a group from the same messages sent one at a
        time. That is deliberate — batching is the broker's durability
        decision, and making it visible on the wire would make every
        subscriber's parser depend on how the publisher happened to poll. A
        batch that should ARRIVE as one unit is one message: encode it
        yourself and call `send`.
        """
        batch = list(messages)
        if not batch:
            return []

        kinds = [kind_of(message) for message in batch]
        if self._log is not None:
            offsets = self._log.extend(
                [
                    _log.row(kind, message)
                    for kind, message in zip(kinds, batch, strict=True)
                ]
            )
            self._end_offset = offsets[-1] + 1
        else:
            first = self._end_offset
            offsets = list(range(first, first + len(batch)))
            self._end_offset = first + len(batch)

        for offset, kind, message in zip(offsets, kinds, batch, strict=True):
            self._fan_out(encode(offset, kind, message))

        return offsets

    def _fan_out(self, frame: bytes) -> None:
        """One encoded frame into every subscriber's queue.

        Encoded once by the caller and shared, so a queue entry is a pointer
        rather than a copy. `offer` cannot block, raise, or detach anything,
        which is what makes iterating the set here safe without a snapshot.
        """
        for subscriber in self._subscribers:
            subscriber.offer(frame)

    async def aclose(self, reason: str = "broker shutting down") -> None:
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
        start = None if resolved is None else resolved[1]

        subscriber = Subscriber(connection, max_backlog=self._max_backlog)
        # ── ATOMIC. Do not put an await between these two statements. ──
        # Joining the set first means nothing sent from here on is missed;
        # reading the frontier second means everything below it is already
        # durable. See the module docstring.
        self._subscribers.add(subscriber)
        frontier = self._end_offset
        # ──────────────────────────────────────────────────────────────

        replay: AsyncGenerator[tuple[int, bytes], None] | None = None
        try:
            if resolved is not None:
                replay = await self._replay_from(*resolved, frontier)

            await connection.send(
                greeting(
                    stream=self._name,
                    end_offset=frontier,
                    replay=None if start is None else (start, frontier),
                    durable=self._log is not None,
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
