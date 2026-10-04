"""The litelink side: the stream's table, and how a replay comes back off it.

A server with a log attached is a tickerplant — kx's term for a process that
captures a feed, writes it to a log file, and publishes it to registered
subscribers (https://code.kx.com/q/architecture/). The log is what turns an
offset from a number that orders messages into a number a subscriber can
*resume from*, and everything here serves that one sentence.

**The schema is the caller's, per stream.** That is litelink's own model —
"the library owns exactly one column, `litelink_offset`; everything else is the
caller's schema" — and it is the whole reason to put litelink underneath this
rather than an append-only file.

streamcast adds exactly one column of its own, `streamcast_ts`, on the same
terms litelink owns its offset: stamped by the writer, never a key on the wire,
and absent from the shape a subscriber is told. It is one scalar beside the
application's columns, not a shape — see `STAMP`.

An earlier version of this module owned a fixed three-column schema and stored
each upstream frame whole, as text, in a `payload` column. It is worth saying
plainly why that was wrong, because it looked reasonable and it defended
itself in a docstring: litelink's own websocket example says *"Every field the
feed sends that is worth a column ... which is the reason to declare a schema
rather than store the frame whole."* Storing it whole threw away every
property the table was for —

* **Pruning.** §7 prunes on Iceberg statistics per column. Against a blob
  column there is nothing to prune on, so a query for one minute of trades
  reads every byte of every message in range.
* **Compression.** A `price` column of float64 compresses against its
  neighbours; the same numbers inside a JSON string do not.
* **The published table.** litelink's headline is that any Iceberg engine can
  read it with nothing installed. Pointed at a blob column it gets one string
  per row and has to parse JSON in SQL to ask anything.
* **The replay.** Rows had to be read out of Arrow as strings and re-encoded,
  when the columns were right there.

The argument that defended it was circular: that a caller's extra column
"would have to be filled by `send`, which has nothing to fill it with". True
only because `send` took bytes. `send` takes a row, the caller fills it, and
the premise disappears.
"""

from __future__ import annotations

import asyncio
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa

# litelink's own column name, imported rather than spelled again: a copy here
# would be a second home for the fact, and its failure mode is a scan for a
# column that is not there. It is NOT what goes on the wire — see
# `_protocol.OFFSET`, which this module aliases it to.
from litelink import OFFSET as COLUMN

from streamcast import _schema
from streamcast._protocol import encode_projected as _encode_projected

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence

    from litelink import LogHandle

    from streamcast._filter import Predicate


class Readable(Protocol):
    """What a replay reads from: a litelink handle, or a pinned published table.

    `rows` and `columns` need the table's columns and a batch reader over an
    offset range, and nothing else — which is what lets a catch-up read the
    published table through the same function a server replays with (I6).
    """

    @property
    def schema(self) -> pa.Schema: ...

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        published: bool = True,
    ) -> pa.RecordBatchReader: ...


STAMP: Final = "streamcast_ts"
"""The column streamcast owns: when the server took the row, in UTC microseconds.

A row's own timestamps are the publisher's — when the exchange matched, when
the sensor read. What no application column carries is when THIS server
received it, and the difference is what you want when something looks wrong:
`streamcast_ts - event_ts` is feed latency per row over the whole history.

**An int64 epoch, in microseconds**, which is how litelink stores every
timestamp: an integer is the same value in the table, in a JSON frame and in a
subtraction, with no type conversion and no silent truncation between them.
Microseconds because it is the unit the examples' own `event_ts` uses, so the
two subtract directly.

**Wall clock, so a clock step on the server shows up in it.** A stored time has
to mean something to a reader on another machine, and a monotonic clock does
not. It is monotonic in offset only while the server's clock is.

**One value per commit, not per row.** `send_many` is one transaction and its
rows become durable together; distinct values would claim an ordering in time
the commit does not have.

Stamped only on a log that has the column. `Stream.new` creates every log with
it, but a log from before it existed, or a handle a caller opened and passed in,
may not — and that log's shape is not this library's to change.
"""


SYSTEM: Final[Mapping[str, Mapping[str, object]]] = MappingProxyType(
    {STAMP: MappingProxyType({"type": "integer", "format": "int64"})}
)
"""Every column streamcast owns, as JSON Schema properties. The one definition.

Everything that needs to know which columns are the server's reads this: what
a new log is created with (`with_system`), which names a declaration may not
use, which columns the wire and the greeting leave out, whether a log is
current enough that `Stream.migrate` can leave it alone (`is_current`), and
what the stream's metadata file records per log.

**Adding a column here is an upgrade**: logs created afterwards have it, and
`Stream.migrate` with an unchanged schema moves an older stream onto a log
that does. Every entry is required — the server fills it on every row.

**A type here never changes.** A stream's logs are read together with `UNION
ALL BY NAME`, and a system column that changed type across a seam coerces
silently, exactly as a user column would. A column that needs a different
type is a NEW name — `streamcast_ts_v2` — beside the old one.
`tests/test_stamp.py` pins every existing entry for that reason.
"""

_SYSTEM_ARROW: Final = _schema.to_arrow(
    {
        "type": "object",
        "properties": {name: dict(spec) for name, spec in SYSTEM.items()},
        "required": list(SYSTEM),
    }
)


def columns(log: Readable) -> tuple[str, ...]:
    """The stream's declared columns, in the order the wire uses.

    Read once at `Stream` construction and held, because it fixes the key
    order of every frame — and a replayed row must serialise to the same bytes
    as the live one it repeats (I6). litelink's own column is NOT among them:
    the offset is element 0 of the triple `encode` writes, never a key in the
    message, so there is one statement of "the offset comes first" instead of
    two — and none of them is a column name a subscriber has to know.

    The `SYSTEM` columns are not among them either, for the same reason: they
    are the server's, not the application's. `streamcast_ts` is element 1 of
    the frame, beside the row rather than in it.
    """
    return tuple(name for name in log.schema.names if name not in SYSTEM)


def declared(schema: pa.Schema) -> pa.Schema:
    """The application's columns: `schema` without the ones streamcast owns.

    What a subscriber is told in the greeting, and what `Stream.new` compares a
    declaration against. Filtered here as well as in `columns` because the two
    are read from the same `log.schema` by different expressions, and filtering
    only one tells a subscriber about a column no frame will ever carry.
    """
    return pa.schema([field for field in schema if field.name not in SYSTEM])


def system(schema: pa.Schema) -> pa.Schema:
    """The `SYSTEM` columns `schema` has, in its order.

    A column by one of those names of any other type is refused rather than
    skipped: it is someone else's column wearing this library's name, and
    filling it — or silently not — would both be wrong about what it holds.
    """
    owned = [field for field in schema if field.name in SYSTEM]
    for field in owned:
        wanted = _SYSTEM_ARROW.field(field.name).type
        if field.type != wanted:
            msg = (
                f"the log's {field.name!r} column is {field.type}; streamcast "
                f"owns that name and stores {wanted} in it"
            )
            raise ValueError(msg)

    return pa.schema(owned)


def with_system(schema: pa.Schema, names: Iterable[str] | None = None) -> pa.Schema:
    """`schema` with every `SYSTEM` column appended, for creating a log.

    Last rather than first so a `SELECT *` reads the application's columns in
    the order they were declared. Not nullable: every row this library writes
    carries them, and a log that says so is one a reader can rely on.

    `names` appends only those, for rebuilding a log that has fewer — one
    created before a system column existed — exactly as it is.
    """
    wanted = None if names is None else set(names)
    for field in _SYSTEM_ARROW:
        if wanted is None or field.name in wanted:
            schema = schema.append(field)

    return schema


def is_current(log: LogHandle) -> bool:
    """Whether `log` has exactly today's `SYSTEM` columns.

    False for a log created before one of them existed, which is what makes
    `Stream.migrate` with an unchanged schema an upgrade rather than a no-op.
    """
    return list(system(log.schema)) == list(_SYSTEM_ARROW)


def stamped(log: LogHandle) -> bool:
    """Whether `log` has `streamcast_ts`, and so whether `send` fills it."""
    return STAMP in system(log.schema).names


def _next_batch(reader: pa.RecordBatchReader) -> pa.RecordBatch | None:
    """One batch, or None at the end.

    `StopIteration` is converted rather than propagated because this runs in a
    worker thread under `asyncio.to_thread`: a `StopIteration` crossing a
    coroutine boundary becomes a `RuntimeError` about a coroutine raising
    StopIteration, which names nothing about the actual end of a scan.
    """
    try:
        return reader.read_next_batch()
    except StopIteration:
        return None


async def rows(
    log: Readable, start: int, stop: int, *, published: bool = False
) -> AsyncGenerator[tuple[int, int | None, dict[str, object]], None]:
    """`(offset, ts, row)` for `[start, stop)`, oldest first.

    `ts` is the stored `streamcast_ts`, or None on a log created before the
    column existed — exactly what the live frame for that row carried (I10).

    **Every blocking call is in a thread**, which is not an optimisation. A
    replay is DuckDB reading Parquet — measured at 2.11 us per row warm and
    ~0.5 s cold for the first scan in a process, of which 91% is the read
    itself — and on the event loop that is the whole server stopped: no live
    message fanned out, no other subscriber served, no keepalive answered.
    litelink is built for this: its buffer and reader each hold their own lock
    and its SQLite connections are opened `check_same_thread=False`.

    Batches rather than rows, because that is the unit litelink hands back and
    the unit a thread hop should cost: one hop per batch amortises over
    thousands of rows, one per row would cost more than the read.

    Yields `(offset, frame)` rather than the frame alone so the caller can
    check the first offset against what was asked for — see `_stream`, where a
    replay that starts above the request is a refusal rather than a short
    serve.
    """
    declared = columns(log)
    stamped = STAMP in log.schema.names
    names = (COLUMN, STAMP, *declared) if stamped else (COLUMN, *declared)
    # **Local tiers only, unless the stream says otherwise** (`published`,
    # which is `Stream`'s `replay_published`). litelink's own default is to
    # read the published table too, so this is passed on every read rather
    # than left to it.
    #
    # Local on purpose. Serving a replay out of object storage means a long
    # network read held on a worker thread while the subscriber's socket sits
    # attached — the server queues for a consumer that is not reading, and
    # `max_backlog` drops it. That is the exact failure catch-up avoids by
    # doing the same read CLIENT-side with nothing connected, so a request
    # below the local floor is refused and the consumer is pointed there.
    reader = await asyncio.to_thread(
        log.scan,
        columns=names,
        start_offset=start,
        end_offset=stop,
        published=published,
    )
    try:
        while True:
            batch = await asyncio.to_thread(_next_batch, reader)
            if batch is None:
                return

            # **Arrow builds the dicts; popping the system columns leaves the
            # message.** `to_pylist()` is one C call for the whole batch, and
            # because the projection put them first, `pop` off the front leaves
            # exactly the key order the live path produces. Measured at
            # 1.00 us a row, against 1.50 for selecting the columns twice and
            # 2.58 for rebuilding each dict in Python.
            #
            # Checked rather than trusted: the property rests on DuckDB
            # returning columns in the order `scan` asked for, and a silent
            # reordering would put a subscriber's replayed bytes out of step
            # with the live ones (I6). One list compare per BATCH.
            if tuple(batch.schema.names) != names:
                msg = (
                    f"the scan returned columns {batch.schema.names} where "
                    f"{list(names)} was projected; a replayed message would "
                    f"not match the live one"
                )
                raise RuntimeError(msg)

            # **Maps as dicts**, because a live frame encodes the caller's dict
            # and Arrow's default is a list of pairs — `[["k","v"]]` on replay
            # against `{"k":"v"}` live, which breaks I6. "strict" raises on a
            # duplicate key rather than letting the last one win silently;
            # streamcast only ever stores a map it received as a dict, so a
            # duplicate means a log written by something else.
            for message in batch.to_pylist(maps_as_pydicts="strict"):
                offset = message.pop(COLUMN)
                ts = message.pop(STAMP) if stamped else None
                yield offset, ts, message

    finally:
        # Releases the DuckDB result the scan is holding. A subscriber that
        # disconnects mid-replay leaves this generator suspended otherwise,
        # and under a reconnect storm that is an unbounded number of live
        # scans against one log.
        reader.close()


def rows_from(log: LogHandle, offset: int, *, published: bool = False) -> int:
    """How many rows the log actually holds from `offset` onward, inclusive.

    **`max_replay` bounds the WORK a replay costs, and that work is rows.**
    Offset distance is a proxy for it, and an exact one only while the offset
    space is dense — which litelink's is not. A `restore` fences 2**20
    offsets that were never issued (2**40 without a WAL replica), so a consumer 150 rows behind measures as
    a million and a bounded server refuses a replay it could serve instantly.

    Asked only when the cheap proxy has already said "too far", so the common
    subscribe pays nothing for it. *Measured* at 30.8 ms over 1,000,000 rows
    in 59 files — it scales with FILE COUNT, around 0.4 ms each, because the
    manifests are read per file rather than pruned. That is affordable once
    per subscribe against a replay that costs 0.27 s for 100,000 rows, and it
    runs in a thread like every other read here.
    """
    # `>=`, because a replay serves the requested offset itself. `>` counts
    # one fewer than the replay will produce, which made a bounded server
    # report "19 messages behind" for a subscribe that would have read 20.
    quoted = f'SELECT count(*) AS n FROM log WHERE "{COLUMN}" >= {int(offset)}'

    return int(log.sql(quoted, published=published).read_all()["n"][0].as_py())


def earliest(log: LogHandle, *, published: bool = False) -> int | None:
    """The lowest offset a replay from `log` can serve, or None if it holds nothing.

    `published` is the stream's `replay_published`: whether a replay may read
    the published table, below the local tiers. litelink's `coverage()`
    partitions the log into the published-only range, the staging table and
    the buffer, so this is the lowest start among the tiers a replay reads —
    and with `published=False`, `coverage` leaves the published tier unasked
    rather than reading its manifests.

    Called once per subscribe that asks for EARLIEST and never otherwise, in a
    thread: a log with no stored published row yet reads the published
    table's manifests to answer.
    """
    coverage = log.coverage(published=published)
    tiers = [coverage.staging, coverage.buffer]
    if published:
        tiers.append(coverage.published)

    lows = [extent[0] for extent in tiers if extent is not None]
    return min(lows) if lows else None


def lowest(log: LogHandle) -> int | None:
    """The lowest offset `log` holds in ANY tier, or None if it holds nothing.

    Unlike `earliest`, which answers for what a replay reaches, this counts
    the published table always: it describes the log, for the stream's
    metadata file.
    """
    coverage = log.coverage()
    tiers = [coverage.published, coverage.staging, coverage.buffer]
    lows = [extent[0] for extent in tiers if extent is not None]

    return min(lows) if lows else None


async def replay(
    log: Readable,
    start: int,
    stop: int,
    where: Predicate | None = None,
    outbound: Callable[[Mapping[str, object]], dict[str, object]] | None = None,
    *,
    published: bool = False,
) -> AsyncGenerator[tuple[int, bytes], None]:
    """`rows`, encoded — what a subscriber's pump sends.

    Split from `rows` so the CLIENT can reuse the batch reader without an
    encode-then-decode round trip it would only undo: `_snapshot` reads the
    published tables the same way and hands the caller dicts directly, and 1.5 us a row
    through msgspec twice adds up over a catch-up of millions.

    **`where` is applied before the encode**, so a filtered replay does not
    pay msgspec for rows nobody asked for — 961 ns a row, against 2.11 us to
    read one, so it is about a third of a selective replay's cost.

    **The FIRST row is yielded whatever the filter says**, and that is not an
    oversight. `_stream._replay_from` checks it against the offset that was
    requested to catch a log whose retention has passed it — a hole at the
    join, the one wrong answer a resume must never give. Filtering it away
    would make the first MATCHING row stand in for the log's true floor, so a
    selective filter over an intact log would report itself as evicted. The
    caller drops it if it does not match; see `_prepend`.
    """
    first = True
    async for offset, ts, message in rows(log, start, stop, published=published):
        # `where` reads the stored values and `outbound` converts a copy for
        # the wire, in that order: a filter on a binary column compares bytes,
        # exactly as it does against a live row.
        if first:
            first = False
            wire = message if outbound is None else outbound(message)
            yield offset, _encode_projected(offset, ts, wire)
            continue

        if where is None or where(message):
            wire = message if outbound is None else outbound(message)
            yield offset, _encode_projected(offset, ts, wire)


__all__ = [
    "STAMP",
    "SYSTEM",
    "columns",
    "declared",
    "earliest",
    "is_current",
    "lowest",
    "replay",
    "rows",
    "rows_from",
    "stamped",
    "system",
    "with_system",
]
