"""The litelink side: the stream's table, and how a replay comes back off it.

A broker with a log attached is a tickerplant — kx's term for a process that
captures a feed, writes it to a log file, and publishes it to registered
subscribers (https://code.kx.com/q/architecture/). The log is what turns an
offset from a number that orders messages into a number a subscriber can
*resume from*, and everything here serves that one sentence.

**The schema is the caller's, per stream, and streamcast declares none of it.**
That is litelink's own model — "the library owns exactly one column,
`litelink_offset`; everything else is the caller's schema" — and it is the
whole reason to put litelink underneath this rather than an append-only file.

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
* **The archive.** litelink's headline is that any Iceberg engine can read the
  archive with nothing installed. Pointed at a blob column it gets one string
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
from typing import TYPE_CHECKING

# litelink's own column name, imported rather than spelled again: a copy here
# would be a second home for the fact, and its failure mode is a scan for a
# column that is not there. It is NOT what goes on the wire — see
# `_protocol.OFFSET`, which this module aliases it to.
from litelink.log import OFFSET as COLUMN

from streamcast._protocol import encode_projected as _encode_projected

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import pyarrow as pa
    from litelink import LogHandle, WriteHandle


def columns(log: LogHandle) -> tuple[str, ...]:
    """The stream's declared columns, in the order the wire uses.

    Read once at `Stream` construction and held, because it fixes the key
    order of every frame — and a replayed row must serialise to the same bytes
    as the live one it repeats (I6). `litelink_offset` is prepended by
    `encode` rather than listed here, so there is one statement of "the offset
    comes first" instead of two.
    """
    return tuple(log.schema.names)


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
    replay is DuckDB reading Parquet — measured at 2.11 us per row warm and
    ~0.5 s cold for the first scan in a process, of which 91% is the read
    itself — and on the event loop that is the whole broker stopped: no live
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
    names = (COLUMN, *declared)
    # `include_archive` is deliberately not passed. litelink's default decides
    # from the tiers: local disk while the local table holds files, which is
    # every ordinary broker and keeps a replay off the network — and the
    # archive when the log has been fully evicted and it is the only place the
    # rows are, where refusing to look would be a silent short serve.
    reader = await asyncio.to_thread(
        log.scan, columns=names, start_offset=start, end_offset=stop
    )
    try:
        while True:
            batch = await asyncio.to_thread(_next_batch, reader)
            if batch is None:
                return

            # **Arrow builds the dicts; popping the offset leaves the message.**
            # `to_pylist()` is one C call for the whole batch, and because the
            # projection put litelink's column first, `pop` off the front
            # leaves exactly the key order the live path produces. Measured at
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

            for message in batch.to_pylist():
                offset = message.pop(COLUMN)
                yield offset, _encode_projected(offset, message)

    finally:
        # Releases the DuckDB result the scan is holding. A subscriber that
        # disconnects mid-replay leaves this generator suspended otherwise,
        # and under a reconnect storm that is an unbounded number of live
        # scans against one log.
        reader.close()


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


__all__ = ["columns", "earliest", "replay"]
