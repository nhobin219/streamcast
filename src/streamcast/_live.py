"""A stream's history, kept current in memory: `Stream.live` (#59).

    async with await streamcast.Stream.live("ws://broker:8765/trades") as live:
        await live.sql("SELECT side, sum(amount) FROM log GROUP BY side")

A `Snapshot` is a fixed point. A `Live` is the same reader kept moving: the
published tables as a base, and the broker's rows appended in memory as they
arrive, so every `scan` and `sql` answers as of the last row received.

**The broker is the only address.** Its greeting names the stream's metadata
file and id, and a live view always wants the stream as the broker serves it
now — so the view reads them from the greeting, at open and again at every
reconnect, rather than taking a location the caller could get wrong or that
could go stale.

**Built from what exists.** Opening it is one connection
for the greeting, a published snapshot of the file it names, and then a
subscription at the snapshot's `end_offset` with `catch_up=True` — the join
catch-up already makes without a gap or a duplicate (I1, I4, I7). A query
freezes the tail at the last row received and runs the snapshot's own read
with the tail as one more piece, so `scan` and `sql` mean exactly what they
mean on a `Snapshot`.

**Memory is the publish lag, not the stream's age.** Every `rebase_every`
seconds, and after every reconnect, a fresh published snapshot replaces the
base and the tail rows it now covers are dropped. A row that arrives below the
base's end is dropped on arrival — the base already holds it — so nothing is
counted twice.

**Reading never waits on a query.** The receiving task only appends dicts;
they become Arrow when a query asks for them. A query runs in a thread, so a
long one stalls neither the loop nor the socket, and the server never drops
this view for falling behind because someone ran a slow aggregate (I2).

**A broken view raises; it does not serve stale answers.** A dropped
connection, `TooSlow` or a network error is reconnected from the last offset,
with catch-up, under a capped backoff. Anything else — a refusal catch-up
cannot fix, a protocol error — is kept and raised by the next query.
"""

from __future__ import annotations

import asyncio
import bisect
import contextlib
from typing import TYPE_CHECKING, Any, Final

import pyarrow as pa
from websockets.exceptions import ConnectionClosed

from streamcast import _log, _snapshot
from streamcast._errors import NotReplayable, TooSlow
from streamcast._limits import MAX_TAIL

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from litelink import S3Options

    from streamcast import _manifest
    from streamcast._protocol import Greeting

REBASE_EVERY: Final = 10.0
"""Seconds between rebases: how long a published row is also held in memory."""

_BACKOFF: Final = (0.1, 0.5, 1.0, 2.0, 5.0)
"""Seconds before each reconnect attempt; the last repeats."""

# What reconnecting can fix. Anything else is kept and raised by the next query.
_TRANSIENT: Final = (ConnectionClosed, OSError, TooSlow)


class Live:
    """A stream as of its latest row: the published tables plus the broker's tail.

    Made by `Stream.live`. `end_offset` is exclusive, as on `Snapshot`: every
    row a query can see is below it.
    """

    def __init__(
        self,
        broker: str,
        greeting: Greeting,
        s3_options: S3Options | None,
        rebase_every: float,
        base: _snapshot.Snapshot,
        *,
        where: dict[str, object] | None = None,
        start: int | None = None,
        max_tail: int = MAX_TAIL,
    ) -> None:
        self._broker = broker
        # What the view will hold from the broker, unpublished, before it
        # stops: a publisher that has stalled must not run this process out
        # of memory. See `_limits.MAX_TAIL`.
        self._max_tail = max_tail
        # The subscription's filter, and the same terms for the published
        # base — one narrowing, applied on both sides of the join.
        self._where = where
        self._filters: tuple[_manifest.Term, ...] = _terms(where)
        # The lowest offset any query sees; None for everything published.
        self._floor = start
        # Where the history is read, as the broker's latest greeting says.
        self._uri = greeting.metadata
        self._stream_id = greeting.stream_id
        self._s3 = s3_options
        self._rebase_every = rebase_every
        self._base = base
        # Rows received and not yet in a query: dicts, cheap to append.
        self._pending: list[dict[str, Any]] = []
        # Rows a query has already converted, kept as Arrow.
        self._tail: list[pa.Table] = []
        # Where the base's published rows end. Kept apart from the base's own
        # `end_offset`, which each query moves up to the newest row received.
        self._published_end = base.end_offset
        # Nothing below `start` is wanted, so the subscription begins there
        # when it is above what is published.
        self._end = max(base.end_offset, start or 0)
        self._lock = asyncio.Lock()
        self._advanced = asyncio.Condition()
        self._failure: BaseException | None = None
        # The newest `streamcast_ts` received, for `wait_for(ts=)`.
        self._newest_ts: int | None = None
        self._subscription: Any = None
        self._tasks: list[asyncio.Task[None]] = []

    # -- lifetime -------------------------------------------------------------

    async def _start(self) -> None:
        """Connect, and start receiving and rebasing. Raises what `connect` does."""
        self._subscription = await self._connect()
        self._tasks = [
            asyncio.create_task(self._receive()),
            asyncio.create_task(self._rebase_forever()),
        ]

    async def _connect(self) -> Any:
        from streamcast import _client  # noqa: PLC0415 — the client imports `_snapshot`

        connecting = _client.connect(
            self._broker,
            offset=self._end,
            catch_up=True,
            s3_options=self._s3,
            where=self._where,
        )
        try:
            subscription = await connecting
        except NotReplayable as refused:
            if refused.why != "empty":
                raise

            # Nothing in the log yet, so nothing to replay: from now is all.
            subscription = await _client.connect(self._broker, where=self._where)

        # The latest word on where the history is: a restarted broker may
        # serve a migrated stream, whose metadata the next rebase must read.
        self._uri = subscription.info.metadata
        self._stream_id = subscription.info.stream_id
        return subscription

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        if self._subscription is not None:
            await self._subscription.close()

        await self._base.close()

    async def __aenter__(self) -> Live:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # -- receiving --------------------------------------------------------------

    async def _receive(self) -> None:
        attempt = 0
        while True:
            try:
                async for offset, ts, row in self._subscription:
                    attempt = 0
                    await self._append(offset, ts, row)

                # An ordinary close ends the iteration: the server went away,
                # and reconnecting is the same as for a dropped connection.
            except _TRANSIENT:
                pass
            except Exception as exc:  # noqa: BLE001 — kept and raised by the next query
                self._failure = exc
                await self._wake()
                return

            await asyncio.sleep(_BACKOFF[min(attempt, len(_BACKOFF) - 1)])
            attempt += 1
            try:
                with contextlib.suppress(Exception):
                    await self._subscription.close()

                self._subscription = await self._connect()
                # A restart is when a migration happens: pick up the new log.
                await self.rebase()
            except _TRANSIENT:
                continue
            except Exception as exc:  # noqa: BLE001 — kept and raised by the next query
                self._failure = exc
                await self._wake()
                return

    async def _append(self, offset: int | None, ts: int | None, row: dict) -> None:
        if offset is None:
            msg = f"{self._broker} serves a stream with no log; it has no offsets"
            raise RuntimeError(msg)

        if offset < self._end:
            return  # the base already holds it, or it was received before

        held = len(self._pending) + sum(table.num_rows for table in self._tail)
        if held >= self._max_tail:
            msg = (
                f"the live view of {self._broker} holds max_tail={self._max_tail} "
                f"rows the published tables do not: publishing is behind or has "
                f"stalled. Raise max_tail, or publish more often."
            )
            raise RuntimeError(msg)

        self._pending.append({_log.COLUMN: offset, _log.STAMP: ts, **row})
        self._end = offset + 1
        if ts is not None and (self._newest_ts is None or ts > self._newest_ts):
            self._newest_ts = ts

        await self._wake()

    async def _wake(self) -> None:
        async with self._advanced:
            self._advanced.notify_all()

    # -- rebasing ---------------------------------------------------------------

    async def _rebase_forever(self) -> None:
        while True:
            await asyncio.sleep(self._rebase_every)
            with contextlib.suppress(Exception):
                # A failed rebase keeps the old base, which is still correct;
                # it only holds more in memory until the next one succeeds.
                await self.rebase()

    async def rebase(self) -> None:
        """Re-pin to what is published now, and drop the tail rows it covers."""
        fresh = await _snapshot.snapshot(
            _require(self._broker, self._uri),
            s3_options=self._s3,
            stream_id=self._stream_id,
        )
        async with self._lock:
            # Converted against the old base first, so a pending row is judged
            # by the base it arrived under; the cut below then trims it.
            self._flush()
            old, self._base = self._base, fresh
            cut = fresh.end_offset
            self._published_end = cut
            # Each table is in offset order — rows arrive that way — so the
            # rows the new base covers are a prefix.
            trimmed = []
            for table in self._tail:
                offsets = table.column(_log.COLUMN).to_pylist()
                kept = table.slice(bisect.bisect_left(offsets, cut))
                if kept.num_rows:
                    trimmed.append(kept)

            self._tail = trimmed
            self._end = max(self._end, cut)

        await old.close()

    # -- reading ----------------------------------------------------------------

    @property
    def end_offset(self) -> int:
        """One above the newest row a query can see."""
        return self._end

    async def wait_for(
        self, offset: int | None = None, *, ts: int | None = None
    ) -> None:
        """Return once a point is visible to a query, or raise why it never will be.

        Exactly one of them, as with `Snapshot`'s `as_of_offset` and `as_of_ts`:

        * `offset`: that row has arrived, so a query sees every row up to it.
        * `ts`: every row stamped at or before `ts` (UTC microseconds) has
          arrived. The view knows that only once it holds a row stamped AFTER
          `ts` — rows arrive in stamp order, so a later one proves nothing
          earlier is still on its way. **On a quiet stream that row may not
          come for a long time, and this waits until it does**, even though
          every earlier row is already here: bound it with
          `asyncio.timeout(...)` where the stream can go idle. Exact only
          while the server's clock is monotonic, as `as_of_ts` is. Refused on
          a log created before `streamcast_ts` existed, which has no stamps
          to wait on.
        """
        if (offset is None) == (ts is None):
            msg = "pass offset or ts, not both and not neither"
            raise ValueError(msg)

        if offset is not None:
            await self._until(lambda: self._end > offset)
            return

        assert ts is not None  # narrowed by the check above
        if not _snapshot._stamped(self._base.metadata.live_log):  # noqa: SLF001
            msg = (
                f"{self._broker} serves a log without streamcast_ts, so there is "
                f"no time to wait for; wait for an offset instead"
            )
            raise ValueError(msg)

        if self._past(ts):
            return

        # Nothing received since opening is past `ts`, but the published rows
        # may be: an idle stream asked about an hour ago is already complete.
        newest = (
            (await self.sql(f'SELECT max("{_log.STAMP}") AS newest FROM log'))
            .column("newest")[0]
            .as_py()
        )
        if newest is not None and newest > ts:
            return

        await self._until(lambda: self._past(ts))

    def _past(self, ts: int) -> bool:
        return self._newest_ts is not None and self._newest_ts > ts

    async def _until(self, reached: Callable[[], bool]) -> None:
        async with self._advanced:
            await self._advanced.wait_for(
                lambda: reached() or self._failure is not None
            )

        self._check()

    async def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> pa.Table:
        """`Snapshot.scan`, as of the newest row received."""
        async with self._lock:
            view = self._view()
            return await view.scan(
                columns=columns,
                where=where,
                filters=(*self._filters, *filters),
                start_offset=self._from(start_offset),
                end_offset=end_offset,
            )

    async def sql(
        self,
        query: str,
        *,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> pa.Table:
        """`Snapshot.sql` — over the table `log` — as of the newest row received."""
        async with self._lock:
            view = self._view()
            return await view.sql(
                query,
                filters=(*self._filters, *filters),
                start_offset=self._from(start_offset),
                end_offset=end_offset,
            )

    def _check(self) -> None:
        if self._failure is not None:
            msg = f"live view of {self._broker} stopped: {self._failure}"
            raise RuntimeError(msg) from self._failure

    def _flush(self) -> None:
        """Pending rows to Arrow, dropping any the base now holds."""
        if not self._pending:
            return

        rows = [r for r in self._pending if r[_log.COLUMN] >= self._published_end]
        self._pending = []
        if rows:
            self._tail.append(
                _snapshot._tail_table(self._base.metadata.live_log, rows)  # noqa: SLF001
            )

    def _from(self, start_offset: int | None) -> int | None:
        """A query's lower bound, never below the view's own start."""
        if self._floor is None:
            return start_offset

        return self._floor if start_offset is None else max(start_offset, self._floor)

    def _view(self) -> _snapshot.Snapshot:
        """The base, with the tail frozen at the newest row received. No await.

        No await between reading `_end` and the tail, so the two describe the
        same prefix: a row received during the query is in neither.
        """
        self._check()
        self._flush()
        base = self._base
        base._tail = pa.concat_tables(self._tail) if self._tail else None  # noqa: SLF001
        base.end_offset = max(self._end, self._published_end)
        return base


def _terms(where: dict[str, object] | None) -> tuple[_manifest.Term, ...]:
    """The subscription's filter as `filters=` terms, one per column."""
    if not where:
        return ()

    return tuple(
        (name, "in", list(value))
        if isinstance(value, (list, tuple))
        else (name, "==", value)
        for name, value in where.items()
    )


def _check_where(broker: str, where: dict[str, object], schema: dict | None) -> None:
    """Refuse what the two sides of the join would read differently.

    The subscription compares in Python, the published base in SQL, and a
    view must see one stream: so only terms both read the same way. `None`
    is "is null" to the first and matches nothing (`= NULL`) to the second;
    a binary column takes text in its encoding on the wire and holds bytes
    in the table. Nested columns the subscription refuses anyway.
    """
    properties = (schema or {}).get("properties") or {}
    for name, value in where.items():
        spec = properties.get(name)
        if spec is None:
            msg = f"where names {name!r}, which {broker} does not declare"
            raise ValueError(msg)

        if spec.get("contentEncoding") is not None:
            msg = (
                f"where names binary column {name!r}; a live view's where= takes "
                f"scalar columns only. Filter it in the query instead."
            )
            raise ValueError(msg)

        values = list(value) if isinstance(value, (list, tuple)) else [value]
        for item in values:
            if item is None or not isinstance(item, (str, int, float, bool)):
                msg = (
                    f"where={{{name!r}: {value!r}}}: a live view's where= takes "
                    f"non-null scalars, as equality or membership. Filter nulls "
                    f"in the query instead."
                )
                raise ValueError(msg)


def _require(broker: str, uri: str | None) -> str:
    if uri is None:
        msg = (
            f"{broker} serves a stream with no log: nothing is published, so "
            f"there is no history to keep current. Subscribe with "
            f"streamcast.connect instead."
        )
        raise ValueError(msg)

    return uri


async def live(
    broker: str,
    *,
    s3_options: S3Options | None = None,
    rebase_every: float = REBASE_EVERY,
    where: dict[str, object] | None = None,
    start_offset: int | None = None,
    max_tail: int = MAX_TAIL,
) -> Live:
    """See `Stream.live`."""
    from streamcast import _client  # noqa: PLC0415 — the client imports `_snapshot`

    # One connection for the greeting, closed before the tables are read:
    # nothing is held open while they are (I7). The subscription that stays
    # open is made at the snapshot's end, in `_start`.
    async with _client.connect(broker) as probe:
        greeting = probe.info

    uri = _require(broker, greeting.metadata)
    if where:
        _check_where(broker, where, greeting.schema)

    start = start_offset
    if start_offset == _snapshot.LATEST:
        # From now: the broker's frontier as of the greeting. Earlier rows
        # are never read, and later ones are, from the tables once published.
        start = greeting.end_offset

    elif start_offset is not None and start_offset < 0:
        msg = f"start_offset={start_offset} is negative; LATEST means from now"
        raise ValueError(msg)

    base = await _snapshot.snapshot(
        uri, s3_options=s3_options, stream_id=greeting.stream_id
    )
    view = Live(
        broker,
        greeting,
        s3_options,
        rebase_every,
        base,
        where=where,
        start=start,
        max_tail=max_tail,
    )
    try:
        await view._start()  # noqa: SLF001
    except BaseException:
        await base.close()
        raise

    return view


__all__ = ["REBASE_EVERY", "Live", "live"]
