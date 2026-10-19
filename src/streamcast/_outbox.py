"""`outbox` — exactly-once publishing from the caller's own transaction.

    store = await streamcast.SqliteStore.open("state.db")
    async with streamcast.outbox(uri, publisher="enricher", store=store) as out:
        async with store.transaction() as tx:
            await tx.execute("UPDATE totals SET n = n + 1")
            seq = await out.send(row, tx=tx)        # enqueued in tx; shipped after COMMIT
        offset = await out.landed(seq)              # optional: the broker's offset

**A row to publish is a row in the caller's database first.** `send` writes
it to `streamcast_outbox` in the caller's transaction, beside whatever state
produced it, so the two commit together or not at all. A sender task ships
what is there, in seq order, over one exactly-once publication
(`publish(uri, publisher=...)`, `_dedupe`), and deletes each row once the
broker acknowledges it.

**The seq is assigned at enqueue, inside the caller's transaction, gapless.**
`streamcast_publisher_state.streamcast_next_seq` moves in the same
transaction as the rows that take it, so a rollback returns its seqs and a
crash leaves no hole. The update's row lock (Postgres), or SQLite's single
writer, holds a second enqueuer on the same publisher until the first commits,
so seqs commit in seq order. That is the price of one ordered sequence, and
why parallel producers take distinct publisher ids. The broker refuses a gap,
so a hole could only mean lost rows or a bug, and is never papered over.

**The greeting reconciles the two sides at every connect**, in one
transaction:

* rows at or below the broker's `last_seq` landed, and are deleted unsent;
* a `last_seq` below what the broker acknowledged -- the lowest pending seq
  minus one, or `next_seq - 1` with nothing pending -- means the broker lost
  acknowledged rows with its buffer: `AcknowledgedRowsLost`, and the sender
  stops (best effort; `docs/SPEC.md` §6b);
* a `last_seq` at or above `next_seq` means this database is the older one:
  `StoreBehindBroker`, and the sender stops;
* the stream's id is bound on the first connect and checked on every one
  after: a stream recreated under the same name is `StreamMismatch`.

No acknowledged seq is stored: rows are deleted only on acknowledgement, so
the lowest pending seq already says it, and a second copy could disagree.

**The sender**, carried over from a working consumer's:

* it connects only when it has rows, so a quiet process holds no idle
  connection for the broker's keepalive to close;
* it ships ordered batches of `SEND_BATCH` rows as one frame, pipelined up to
  `max_in_flight`;
* a batch the broker refuses (`Rejected`) is sent again one row at a time,
  so only the refused row is dropped: it is logged at ERROR and **renumbered
  away** -- deleted, every pending row above it moved down one, `next_seq`
  down one -- so the log's seqs stay 1..n;
* an outage is said once, when the connection first fails and when rows are
  being published again, never per attempt, since a process's logs may be a
  stream this sender ships to;
* it reconnects under a capped backoff (`BACKOFF`), and polls every
  `POLL_EVERY` for rows another process committed to the same database;
* a refusal of the connection itself -- `Superseded`, `ExactlyOnceUnavailable`,
  `StreamRetired`, `StreamMismatch`, `AcknowledgedRowsLost`,
  `StoreBehindBroker`, or a `SequenceGap` the sender did not cause -- is
  raised from the next `send` and from the block's exit, and never retried.

A row is JSON text in the outbox, as it is on the wire: a value JSON cannot
carry (bytes, a map with non-string keys) is refused at `send`. Send a binary
column in its declared text encoding, as a plain publisher does over JSON.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any

import msgspec
from websockets.exceptions import ConnectionClosed

from streamcast._dedupe import check_publisher_id
from streamcast._errors import (
    AcknowledgedRowsLost,
    Rejected,
    StoreBehindBroker,
    StreamMismatch,
)
from streamcast._limits import BACKOFF, MAX_IN_FLIGHT, POLL_EVERY, SEND_BATCH
from streamcast._protocol import stream_name
from streamcast._publish import Publication, publish
from streamcast._store import sql

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from types import TracebackType

    from streamcast._store import Store, Transaction

log = logging.getLogger("streamcast")

_ENCODE = msgspec.json.Encoder()
_DECODE = msgspec.json.Decoder()

LANDED_KEPT = 10_000
"""Offsets of recently acknowledged seqs kept for `landed`, oldest out first."""


def _now_us() -> int:
    return time.time_ns() // 1_000


# -- the library's statements on the outbox tables -----------------------------


async def enqueue(
    tx: Transaction, stream: str, publisher: str, rows: Sequence[Mapping[str, Any]]
) -> int:
    """Insert `rows` in `tx` under the next seqs; the first one's."""
    dialect = tx.store.dialect
    texts = [_text(row) for row in rows]
    await tx.execute(
        sql(
            dialect,
            "INSERT INTO streamcast_publisher_state (streamcast_stream, "
            "streamcast_publisher_id, streamcast_next_seq, streamcast_stream_id) "
            "VALUES (?, ?, 1, '') ON CONFLICT DO NOTHING",
        ),
        (stream, publisher),
    )
    # UPDATE first, then read: the update takes the row's lock (Postgres) or
    # the database's (SQLite) before anything is read, so two enqueuers can
    # never read the same `next_seq`. No RETURNING, which SQLite has only
    # from 3.35 and the stdlib's may predate.
    await tx.execute(
        sql(
            dialect,
            "UPDATE streamcast_publisher_state SET streamcast_next_seq = "
            "streamcast_next_seq + ? WHERE streamcast_stream = ? AND "
            "streamcast_publisher_id = ?",
        ),
        (len(texts), stream, publisher),
    )
    [(after,)] = await tx.fetch(
        sql(
            dialect,
            "SELECT streamcast_next_seq FROM streamcast_publisher_state "
            "WHERE streamcast_stream = ? AND streamcast_publisher_id = ?",
        ),
        (stream, publisher),
    )
    first = int(after) - len(texts)
    now = _now_us()
    for i, text in enumerate(texts):
        await tx.execute(
            sql(
                dialect,
                "INSERT INTO streamcast_outbox (streamcast_stream, "
                "streamcast_publisher_id, streamcast_seq, streamcast_row, "
                "streamcast_enqueued_at) VALUES (?, ?, ?, ?, ?)",
            ),
            (stream, publisher, first + i, text, now),
        )

    return first


async def pending(
    tx: Transaction, stream: str, publisher: str, *, above: int = 0, limit: int
) -> list[tuple[int, dict[str, Any]]]:
    """Up to `limit` pending rows with a seq above `above`, in seq order."""
    rows = await tx.fetch(
        sql(
            tx.store.dialect,
            "SELECT streamcast_seq, streamcast_row FROM streamcast_outbox "
            "WHERE streamcast_stream = ? AND streamcast_publisher_id = ? "
            "AND streamcast_seq > ? ORDER BY streamcast_seq LIMIT ?",
        ),
        (stream, publisher, above, limit),
    )
    return [(int(seq), _DECODE.decode(text)) for seq, text in rows]


async def count(tx: Transaction, stream: str, publisher: str) -> int:
    [(n,)] = await tx.fetch(
        sql(
            tx.store.dialect,
            "SELECT count(*) FROM streamcast_outbox WHERE streamcast_stream = ? "
            "AND streamcast_publisher_id = ?",
        ),
        (stream, publisher),
    )
    return int(n)


async def acked(tx: Transaction, stream: str, publisher: str, through: int) -> None:
    """Delete every pending row at or below `through`: the broker holds them."""
    await tx.execute(
        sql(
            tx.store.dialect,
            "DELETE FROM streamcast_outbox WHERE streamcast_stream = ? AND "
            "streamcast_publisher_id = ? AND streamcast_seq <= ?",
        ),
        (stream, publisher, through),
    )


async def reconcile(
    tx: Transaction, stream: str, publisher: str, stream_id: str, last_seq: int
) -> None:
    """Square this database with the broker's greeting; see the module."""
    dialect = tx.store.dialect
    found = await tx.fetch(
        sql(
            dialect,
            "SELECT streamcast_next_seq, streamcast_stream_id FROM "
            "streamcast_publisher_state WHERE streamcast_stream = ? AND "
            "streamcast_publisher_id = ?",
        ),
        (stream, publisher),
    )
    if not found:
        return

    next_seq, recorded = int(found[0][0]), str(found[0][1])
    if recorded and recorded != stream_id:
        raise StreamMismatch(stream, recorded, stream_id)

    if not recorded:
        await tx.execute(
            sql(
                dialect,
                "UPDATE streamcast_publisher_state SET streamcast_stream_id = ? "
                "WHERE streamcast_stream = ? AND streamcast_publisher_id = ?",
            ),
            (stream_id, stream, publisher),
        )

    [(lowest,)] = await tx.fetch(
        sql(
            dialect,
            "SELECT min(streamcast_seq) FROM streamcast_outbox WHERE "
            "streamcast_stream = ? AND streamcast_publisher_id = ?",
        ),
        (stream, publisher),
    )
    acknowledged = (int(lowest) if lowest is not None else next_seq) - 1
    if last_seq < acknowledged:
        raise AcknowledgedRowsLost(last_seq + 1, acknowledged)

    if last_seq >= next_seq:
        raise StoreBehindBroker(next_seq, last_seq)

    await acked(tx, stream, publisher, last_seq)


async def drop(tx: Transaction, stream: str, publisher: str, seq: int) -> None:
    """Renumber `seq` away: delete it, move every pending row above it down
    one in ascending order -- each target seq is free by then -- and take one
    off `next_seq`, so the seqs the broker sees stay gapless."""
    dialect = tx.store.dialect
    # The counter FIRST: it takes the publisher's row lock, so an enqueue in
    # another transaction that has taken seqs commits before this reads the
    # rows above `seq`, and its rows are renumbered with the rest. Taken
    # last, its row would be missed, and its seq left both pending and the
    # next to be handed out.
    await tx.execute(
        sql(
            dialect,
            "UPDATE streamcast_publisher_state SET streamcast_next_seq = "
            "streamcast_next_seq - 1 WHERE streamcast_stream = ? AND "
            "streamcast_publisher_id = ?",
        ),
        (stream, publisher),
    )
    await tx.execute(
        sql(
            dialect,
            "DELETE FROM streamcast_outbox WHERE streamcast_stream = ? AND "
            "streamcast_publisher_id = ? AND streamcast_seq = ?",
        ),
        (stream, publisher, seq),
    )
    above = await tx.fetch(
        sql(
            dialect,
            "SELECT streamcast_seq FROM streamcast_outbox WHERE streamcast_stream = ? "
            "AND streamcast_publisher_id = ? AND streamcast_seq > ? "
            "ORDER BY streamcast_seq",
        ),
        (stream, publisher, seq),
    )
    for (old,) in above:
        await tx.execute(
            sql(
                dialect,
                "UPDATE streamcast_outbox SET streamcast_seq = ? WHERE "
                "streamcast_stream = ? AND streamcast_publisher_id = ? AND "
                "streamcast_seq = ?",
            ),
            (int(old) - 1, stream, publisher, int(old)),
        )


def _text(row: Mapping[str, Any]) -> str:
    try:
        text = _ENCODE.encode(dict(row)).decode()
    except (TypeError, msgspec.EncodeError) as exc:
        msg = f"an outbox row must be JSON, as it is on the wire: {exc}"
        raise TypeError(msg) from None

    if _DECODE.decode(text) != dict(row):
        # bytes encode as base64 text and come back as a str: a different row.
        msg = "an outbox row must be JSON values only; send binary in its text form"
        raise TypeError(msg)

    return text


# -- the sender ------------------------------------------------------------------


class Outbox:
    """An exactly-once publication fed from the caller's database.

    `send` and `send_many` enqueue in the caller's transaction; a task of
    this object's ships what is pending (see the module). Not constructed
    directly: `streamcast.outbox(...)` builds it and starts its sender.
    """

    __slots__ = (
        "_backoff",
        "_closed",
        "_drained",
        "_failure",
        "_landed",
        "_max_in_flight",
        "_notice",
        "_outage",
        "_poll_every",
        "_publisher",
        "_send_batch",
        "_store",
        "_stream",
        "_task",
        "_uri",
        "_waiting",
        "_wake",
        "_ws",
    )

    def __init__(
        self,
        uri: str,
        publisher: str,
        store: Store,
        *,
        max_in_flight: int = MAX_IN_FLIGHT,
        send_batch: int = SEND_BATCH,
        poll_every: float = POLL_EVERY,
        backoff: Sequence[float] = BACKOFF,
        notice: Callable[[str], None] = log.warning,
        websockets_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        if send_batch < 1 or max_in_flight < 1:
            msg = f"send_batch={send_batch}, max_in_flight={max_in_flight}: at least 1"
            raise ValueError(msg)

        self._uri = uri
        self._stream = stream_name(uri)
        self._publisher = publisher
        self._store = store
        self._max_in_flight = max_in_flight
        self._send_batch = send_batch
        self._poll_every = poll_every
        self._backoff = tuple(backoff)
        self._notice = notice
        self._ws = dict(websockets_kwargs or {})
        self._wake = asyncio.Event()
        self._drained = asyncio.Event()
        self._closed = False
        self._failure: BaseException | None = None
        self._outage = False
        self._task: asyncio.Task[None] | None = None
        # seq -> offset for recent acknowledgements, and who is waiting on
        # which seq. `landed` reads the first and joins the second.
        self._landed: OrderedDict[int, int | None] = OrderedDict()
        self._waiting: dict[int, list[asyncio.Future[int | None]]] = {}

    def __repr__(self) -> str:
        return f"<streamcast.Outbox {self._stream!r} publisher={self._publisher!r}>"

    @property
    def publisher(self) -> str:
        return self._publisher

    @property
    def store(self) -> Store:
        return self._store

    async def send(self, row: Mapping[str, Any], *, tx: Transaction) -> int:
        """Enqueue `row` in `tx`; its seq. Shipped once `tx` commits."""
        return await self.send_many([row], tx=tx)

    async def send_many(
        self, rows: Iterable[Mapping[str, Any]], *, tx: Transaction
    ) -> int:
        """Enqueue `rows` in `tx` under consecutive seqs; the first one's."""
        self._raise_failure()
        if tx.store is not self._store:
            msg = "tx is another store's; an outbox enqueues in the store it was opened on"
            raise ValueError(msg)

        batch = list(rows)
        if not batch:
            msg = "send_many takes at least one row"
            raise ValueError(msg)

        first = await enqueue(tx, self._stream, self._publisher, batch)
        tx.on_commit(self._wake.set)
        return first

    async def pending(self) -> int:
        """Rows enqueued and not yet acknowledged by the broker."""
        async with self._store.transaction() as tx:
            return await count(tx, self._stream, self._publisher)

    async def landed(self, seq: int) -> int | None:
        """Wait until `seq` is acknowledged; the offset it landed at, or None
        where that is no longer known (acknowledged before this process, or
        long enough ago to have left `LANDED_KEPT`)."""
        self._raise_failure()
        if self._closed:
            msg = "the outbox is closed: nothing will acknowledge this seq"
            raise RuntimeError(msg)

        if seq in self._landed:
            return self._landed[seq]

        async with self._store.transaction() as tx:
            still = await pending(
                tx, self._stream, self._publisher, above=seq - 1, limit=1
            )

        if not still or still[0][0] != seq:
            return self._landed.get(seq)

        waiter: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._waiting.setdefault(seq, []).append(waiter)
        self._wake.set()
        return await waiter

    async def flush(self, timeout: float | None = None) -> None:
        """Wait until nothing is pending, or raise `TimeoutError`."""
        async with asyncio.timeout(timeout):
            while True:
                self._raise_failure()
                self._drained.clear()
                if await self.pending() == 0:
                    return

                self._wake.set()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._drained.wait(), self._poll_every)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(), name=f"streamcast-outbox-{self._stream}"
            )

    async def close(self) -> None:
        """Stop the sender. What is pending stays in the outbox, for the next
        process to ship; `flush` first to wait for it.

        Cancelled rather than drained: the sender may be waiting for the
        store while the caller's own transaction holds it, and a frame cut
        off mid-flight is safe -- if it landed, the next sender's greeting
        says so and the row is deleted unsent."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task

        closed = RuntimeError("the outbox closed before this seq was acknowledged")
        for waiters in self._waiting.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(closed)

        self._waiting.clear()
        self._raise_failure()

    def _raise_failure(self) -> None:
        if self._failure is not None:
            raise self._failure

    # -- the task -----------------------------------------------------------------

    async def _run(self) -> None:
        attempt = 0
        try:
            while not self._closed:
                if not await self._has_rows():
                    self._drained.set()
                    await self._sleep(self._poll_every)
                    continue

                try:
                    await self._session()
                except (OSError, ConnectionClosed, TimeoutError) as exc:
                    if not self._outage:
                        self._outage = True
                        self._notice(
                            f"streamcast outbox {self._stream!r}: cannot publish "
                            f"({type(exc).__name__}: {exc}); retrying"
                        )

                    await self._sleep(
                        self._backoff[min(attempt, len(self._backoff) - 1)]
                    )
                    attempt += 1
                    continue

                attempt = 0

        except Exception as exc:  # noqa: BLE001 — surfaced by the next call
            # Not BaseException: a cancel is `close`, not a failure to report.
            self._fail(exc)

    def _fail(self, exc: BaseException) -> None:
        self._failure = exc
        self._drained.set()
        for waiters in self._waiting.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(exc)

        self._waiting.clear()

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), seconds)

        self._wake.clear()

    async def _has_rows(self) -> bool:
        async with self._store.transaction() as tx:
            return bool(await pending(tx, self._stream, self._publisher, limit=1))

    async def _session(self) -> None:
        """One connection: reconcile with its greeting, then ship until drained."""
        out = await publish(
            self._uri,
            publisher=self._publisher,
            max_in_flight=self._max_in_flight,
            **self._ws,
        )
        try:
            mark = out.info.publisher
            assert mark is not None  # `publish(publisher=)` refuses a server without
            async with self._store.transaction() as tx:
                await reconcile(
                    tx,
                    self._stream,
                    self._publisher,
                    out.info.stream_id or "",
                    mark.last_seq,
                )

            # Rows the greeting says landed were deleted unsent: their offsets
            # are not known here, and their waiters are told so.
            for seq in [s for s in self._waiting if s <= mark.last_seq]:
                for waiter in self._waiting.pop(seq):
                    if not waiter.done():
                        waiter.set_result(None)

            if self._outage:
                self._outage = False
                self._notice(f"streamcast outbox {self._stream!r}: publishing again")

            await self._ship(out)
        finally:
            with contextlib.suppress(Exception):
                await out.close()

    async def _ship(self, out: Publication) -> None:
        """Send pending rows in ordered, pipelined batches until none is left."""
        sent = 0  # the highest seq on the wire this session
        flight: deque[
            tuple[int, list[tuple[int, dict[str, Any]]], asyncio.Future[Any]]
        ] = deque()
        while True:
            if len(flight) < self._max_in_flight and not self._closed:
                async with self._store.transaction() as tx:
                    batch = await pending(
                        tx,
                        self._stream,
                        self._publisher,
                        above=sent,
                        limit=self._send_batch,
                    )

                if batch:
                    first = batch[0][0]
                    future = await out.submit_many([row for _, row in batch], seq=first)
                    flight.append((first, batch, future))
                    sent = batch[-1][0]
                    continue

            if not flight:
                return

            first, batch, future = flight.popleft()
            try:
                offsets = await future
            except Rejected:
                # Every frame behind this one is now a gap and lands nothing;
                # let them come back before sending the batch row by row.
                for _, _, behind in flight:
                    with contextlib.suppress(Exception):
                        await behind

                flight.clear()
                await self._sift(out, batch)
                sent = 0  # re-read from the lowest pending seq
                continue

            await self._acknowledged(
                batch[-1][0], dict(zip((s for s, _ in batch), offsets, strict=True))
            )

    async def _sift(
        self, out: Publication, batch: list[tuple[int, dict[str, Any]]]
    ) -> None:
        """A refused batch, again one row at a time: only the refused row goes."""
        remaining = len(batch)
        while remaining:
            async with self._store.transaction() as tx:
                [(seq, row)] = await pending(tx, self._stream, self._publisher, limit=1)

            remaining -= 1
            try:
                offset = await out.send(row, seq=seq)
            except Rejected as exc:
                log.error(
                    "streamcast outbox %r: the broker refused seq %d and it is "
                    "dropped: %s; row: %r",
                    self._stream,
                    seq,
                    exc,
                    row,
                )
                async with self._store.transaction() as tx:
                    await drop(tx, self._stream, self._publisher, seq)

                continue

            await self._acknowledged(seq, {seq: offset})

    async def _acknowledged(self, through: int, offsets: dict[int, int | None]) -> None:
        async with self._store.transaction() as tx:
            await acked(tx, self._stream, self._publisher, through)

        for seq, offset in offsets.items():
            self._landed[seq] = offset
            for waiter in self._waiting.pop(seq, ()):
                if not waiter.done():
                    waiter.set_result(offset)

        while len(self._landed) > LANDED_KEPT:
            self._landed.popitem(last=False)


class outbox:  # noqa: N801 — a sibling of `publish` and `connect`
    """Open an exactly-once publication fed from the caller's database.

        async with streamcast.outbox(uri, publisher="enricher", store=store) as out:
            ...

    `publisher` names one ordered sequence (`publish`'s rule); the stream must
    be served with `exactly_once=True`. The sender starts on entry and stops on
    exit; whatever is still pending stays in the outbox for the next process.
    An exit after `flush()` leaves nothing behind. Other keywords configure
    the sender (`max_in_flight`, `send_batch`, `poll_every`, `backoff`,
    `notice`); any unknown keyword goes to `websockets.connect`.
    """

    __slots__ = ("_outbox",)

    def __init__(
        self,
        uri: str,
        *,
        publisher: str,
        store: Store,
        max_in_flight: int = MAX_IN_FLIGHT,
        send_batch: int = SEND_BATCH,
        poll_every: float = POLL_EVERY,
        backoff: Sequence[float] = BACKOFF,
        notice: Callable[[str], None] = log.warning,
        **kwargs: Any,
    ) -> None:
        self._outbox = Outbox(
            uri,
            check_publisher_id(publisher),
            store,
            max_in_flight=max_in_flight,
            send_batch=send_batch,
            poll_every=poll_every,
            backoff=backoff,
            notice=notice,
            websockets_kwargs=kwargs,
        )

    async def __aenter__(self) -> Outbox:
        self._outbox.start()
        return self._outbox

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self._outbox.close()
        else:
            with contextlib.suppress(BaseException):
                await self._outbox.close()


__all__ = ["Outbox", "outbox"]
