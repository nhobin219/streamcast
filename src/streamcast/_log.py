"""The litelink side: what a message looks like as a row, and how it comes back.

A broker with a log attached is a tickerplant — kx's term for a process that
captures a feed, writes it to a log file, and publishes it to registered
subscribers (https://code.kx.com/q/architecture/). The log is what turns an offset
from a number that orders messages into a number a subscriber can *resume
from*, and everything in this module exists to serve that one sentence.

**streamcast owns the schema.** `SCHEMA` is not a suggestion and a log with
any other shape is refused at `Stream` construction rather than at the first
send — see `validate`. That is stricter than it could be, and the alternative
was considered and rejected: a log with an extra `venue` column would have to
be filled by `send`, which has nothing to fill it with, so the column would be
NULL on every row and the schema would be a lie told at creation. A stream
that needs more columns than this wants litelink directly.

**Binary payloads are base64 and that is litelink's constraint, not a choice.**
`litelink._types` refuses `binary` outright today — its buffer leg pushes the
read's boundary predicate into SQLite, which decodes blobs as UTF-8 and fails
— and says to encode as text for now. So a `bytes` message costs 4/3 its size
on disk and one encode per direction. A `str` message, which is what every
JSON feed sends, costs neither: it is stored as it arrived. When litelink's
§15 blob fields land, `kind` is already on the row to tell the two apart and
this becomes a storage change with no wire change.
"""

from __future__ import annotations

import asyncio
import base64
import time
from typing import TYPE_CHECKING, Final, cast

import pyarrow as pa

# Imported rather than spelled again. `litelink_offset` is the one column that
# library owns and it is named in its README, its API doc and every example —
# but a copy here would be a second home for the fact, and the failure of a
# drift is a scan for a column that is not there. An import fails at import
# time instead, which is the whole of the argument.
from litelink.log import OFFSET

from streamcast._protocol import BINARY, TEXT, encode

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from litelink import LogHandle, Row, WriteHandle

SCHEMA: Final = pa.schema(
    [
        # Microseconds, because that is what litelink's own examples store and
        # what every exchange feed publishes. The broker's clock at the moment
        # the message was accepted — NOT the publisher's event time, which is
        # inside the payload where only the application can read it.
        pa.field("recv_ts", pa.int64(), nullable=False),
        # `TEXT` or `BINARY`. int32 rather than bool because a third kind is
        # plausible (a §15 blob column would be one) and a bool that has to
        # become an enum is a schema migration; int32 rather than int8 because
        # litelink refuses int8, Iceberg widening it silently being the reason.
        pa.field("kind", pa.int32(), nullable=False),
        # The message. Text as it arrived; binary base64'd — see the module
        # docstring. Not nullable: a zero-length message is "", which is a
        # message, and NULL would be a message that was never sent.
        pa.field("payload", pa.string(), nullable=False),
    ]
)
"""The shape of a streamcast log. Pass it to `litelink.new` and pass nothing else.

    log = litelink.new(root, "trades", schema=streamcast.SCHEMA)

**No `sort_by`.** litelink's default is offset order, and every read this
module makes is an offset range — so the default is not a fallback here, it is
the correct answer, and naming a column would make bounded replay the slow
path (§7 measures a non-leading predicate at 119 ms against 13 ms).
"""

_REPLAY_COLUMNS: Final = (OFFSET, "kind", "payload")
"""What a replay reads. `recv_ts` is deliberately absent — it is stored so an
operator can ask when a message arrived, and it is not on the wire, so reading
it per replayed row would be bytes off disk that nothing consumes."""


def validate(log: LogHandle) -> None:
    """Refuse a log that is not a streamcast log, at construction.

    The failure this prevents is the expensive one: a `Stream` that opens
    fine, accepts a send, and raises inside `litelink.append` with a message
    about a missing column — after the caller's upstream subscription is live
    and messages are arriving with nowhere to go.

    **The comparison is exact**, including the Arrow types, and that was
    checked rather than assumed. litelink documents that it stores `string`
    and returns `large_string` — Iceberg has one string type and cannot tell
    them apart — which would make an exact check reject a perfectly good log.
    It does not apply here: `LogHandle.schema` answers from the schema the log
    recorded in its own `meta`, so `pa.string()` goes in and `pa.string()`
    comes back. Only a handle whose schema was recovered from Parquet footers
    sees `large_string`, and that is `litelink.snapshot`, which returns a read
    handle a `Stream` cannot take. A tolerant comparison here was written
    first and removed: it was six lines defending against a state this type
    cannot be in, and `test_stream.py` pins the fact instead.
    """
    declared = [(field.name, field.type) for field in log.schema]
    wanted = [(field.name, field.type) for field in SCHEMA]
    if declared == wanted:
        return

    msg = (
        f"{log.name!r} is not a streamcast log: its schema is "
        f"{[name for name, _ in declared]} and streamcast writes "
        f"{[name for name, _ in wanted]}. Create it with "
        f"`litelink.new(root, name, schema=streamcast.SCHEMA)`."
    )
    raise ValueError(msg)


def row(kind: int, message: str | bytes) -> Row:
    """One message, as the row that stores it.

    Takes the kind rather than deciding it, because `Stream.send` needs the
    same answer for the frame it puts on the wire and a second `isinstance`
    here would be a second place for text and binary to be told apart.
    """
    # The cast is the claim `kind` already makes. Branching on
    # `isinstance(message, str)` instead would narrow without one — and would
    # put a second decision about text-versus-binary in the library, which is
    # the thing `kind_of` exists to prevent.
    payload = (
        message
        if kind == TEXT
        else base64.b64encode(cast("bytes", message)).decode("ascii")
    )

    return {
        # Read once per message. `time.time_ns()` is a vDSO call — tens of
        # nanoseconds — against the ~400 us the append it rides along with
        # costs, so this is free at any rate the log can sustain.
        "recv_ts": time.time_ns() // 1_000,
        "kind": kind,
        "payload": payload,
    }


def earliest(log: LogHandle) -> int | None:
    """The lowest offset the log can still serve, or None if it holds nothing.

    **Three tiers, and `coverage()` reports two of them.** That is not a bug
    in litelink — `coverage()` answers "what can this reader serve" for a
    reader assembled from an archive and a replica, where the local Iceberg
    table is empty by construction. A broker reads its OWN log, where that
    table is the tier holding almost everything, and `coverage()` alone
    returns nothing the moment a seal empties the buffer. Measured: 60 rows
    sealed into 4 Parquet files, `coverage()` reporting `archive=None,
    buffered=None`, and every `offset=EARLIEST` subscribe refused as "this
    stream's log holds no rows yet".

    So `table_extent()` is asked as well, and the answer is the lowest of
    whichever tiers hold anything. Called once per subscribe that asks for
    EARLIEST and never otherwise: the extents resolve from table statistics,
    which is cheap against local files and a metadata GET against an archive,
    and neither is a thing to do per message.
    """
    coverage = log.coverage()
    tiers = (coverage.archive, log.table_extent(), coverage.buffered)
    lows = [extent[0] for extent in tiers if extent is not None]
    if not lows:
        return None

    return min(lows)


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


async def replay(
    log: WriteHandle, start: int, stop: int
) -> AsyncGenerator[tuple[int, bytes], None]:
    """Frames for `[start, stop)`, already encoded, oldest first.

    **Every blocking call is in a thread**, which is not an optimisation. A
    replay is DuckDB reading Parquet: milliseconds to seconds depending on how
    far behind the subscriber is, and on the event loop that is the whole
    broker stopped — no live message fanned out, no other subscriber served,
    no keepalive answered. litelink's handle is built for this: its buffer and
    reader each hold their own lock and its SQLite connections are opened
    `check_same_thread=False`, so the appending coroutine and this scan are
    the cross-thread case it already serialises.

    Batches, not rows, because that is the unit litelink hands back and the
    unit a thread hop should cost: one hop per batch amortises over thousands
    of rows, one hop per row would cost more than the read.

    Yields `(offset, frame)` rather than the frame alone so the caller can
    check the first offset against what was asked for — see `_stream`, where
    a replay that starts above the request is a refusal rather than a short
    serve.
    """
    # `include_archive` is deliberately not passed. litelink's default decides
    # from the tiers: local disk while the local table holds files, which is
    # every ordinary broker and keeps a replay off the network — and the
    # archive when the log has been fully evicted and it is the only place the
    # rows are, where refusing to look would be a silent short serve.
    reader = await asyncio.to_thread(
        log.scan, columns=_REPLAY_COLUMNS, start_offset=start, end_offset=stop
    )
    try:
        while True:
            batch = await asyncio.to_thread(_next_batch, reader)
            if batch is None:
                return

            # Column-at-a-time. `to_pylist()` on the batch would build a dict
            # per row with three string keys apiece; this builds three lists
            # and zips them, which is the same data without the dicts.
            offsets = batch.column(0).to_pylist()
            kinds = batch.column(1).to_pylist()
            payloads = batch.column(2).to_pylist()
            for offset, kind, payload in zip(offsets, kinds, payloads, strict=True):
                message = base64.b64decode(payload) if kind == BINARY else payload
                yield offset, encode(offset, kind, message)

    finally:
        # A subscriber that disconnects mid-replay leaves this generator
        # suspended; `aclose()` runs the finally and the reader releases its
        # DuckDB result. Without it the result set survives until the
        # generator is collected, which under a burst of reconnects is an
        # unbounded number of live scans.
        reader.close()


__all__ = ["SCHEMA", "earliest", "replay", "row", "validate"]
