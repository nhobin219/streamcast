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

if TYPE_CHECKING:
    from collections.abc import Sequence

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
        s3: S3Options | None,
        rebase_every: float,
        base: _snapshot.Snapshot,
    ) -> None:
        self._broker = broker
        # Where the history is read, as the broker's latest greeting says.
        self._uri = greeting.metadata
        self._stream_id = greeting.stream_id
        self._s3 = s3
        self._rebase_every = rebase_every
        self._base = base
        # Rows received and not yet in a query: dicts, cheap to append.
        self._pending: list[dict[str, Any]] = []
        # Rows a query has already converted, kept as Arrow.
        self._tail: list[pa.Table] = []
        # Where the base's published rows end. Kept apart from the base's own
        # `end_offset`, which each query moves up to the newest row received.
        self._published_end = base.end_offset
        self._end = base.end_offset
        self._lock = asyncio.Lock()
        self._advanced = asyncio.Condition()
        self._failure: BaseException | None = None
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
            self._broker, offset=self._end, catch_up=True, s3=self._s3
        )
        try:
            subscription = await connecting
        except NotReplayable as refused:
            if refused.why != "empty":
                raise

            # Nothing in the log yet, so nothing to replay: from now is all.
            subscription = await _client.connect(self._broker)

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

        self._pending.append({_log.COLUMN: offset, _log.STAMP: ts, **row})
        self._end = offset + 1
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
            _require(self._broker, self._uri), s3=self._s3, stream_id=self._stream_id
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

    async def wait_for(self, offset: int) -> None:
        """Return once `offset` is visible to a query, or raise why it never will be."""
        async with self._advanced:
            await self._advanced.wait_for(
                lambda: self._end > offset or self._failure is not None
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
                filters=filters,
                start_offset=start_offset,
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
                query, filters=filters, start_offset=start_offset, end_offset=end_offset
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
    broker: str, *, s3: S3Options | None = None, rebase_every: float = REBASE_EVERY
) -> Live:
    """See `Stream.live`."""
    from streamcast import _client  # noqa: PLC0415 — the client imports `_snapshot`

    # One connection for the greeting, closed before the tables are read:
    # nothing is held open while they are (I7). The subscription that stays
    # open is made at the snapshot's end, in `_start`.
    async with _client.connect(broker) as probe:
        greeting = probe.info

    base = await _snapshot.snapshot(
        _require(broker, greeting.metadata), s3=s3, stream_id=greeting.stream_id
    )
    view = Live(broker, greeting, s3, rebase_every, base)
    try:
        await view._start()  # noqa: SLF001
    except BaseException:
        await base.close()
        raise

    return view


__all__ = ["REBASE_EVERY", "Live", "live"]
