"""The litelink side: the stream's table, and how a replay comes back off it.

A server with a log attached is a tickerplant — kx's term for a process that
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
    as the live one it repeats (I6). litelink's own column is NOT among them:
    the offset is element 0 of the pair `encode` writes, never a key in the
    message, so there is one statement of "the offset comes first" instead of
    two — and none of them is a column name a subscriber has to know.
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


async def rows(
    log: LogHandle, start: int, stop: int
) -> AsyncGenerator[tuple[int, dict[str, object]], None]:
    """`(offset, row)` for `[start, stop)`, oldest first.

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
    names = (COLUMN, *declared)
    # **Which tiers this reads was decided when the handle was built.** A
    # server opens its log without `include_archive`, so this is local files
    # and the buffer; `_catchup` passes a `snapshot`, which is the archive by
    # construction. One function serves both because neither decides anything
    # here.
    #
    # A server's log is opened local-only on purpose. Serving a replay out of
    # object storage means a long network read held on a worker thread while
    # the subscriber's socket sits attached — the server queues for a consumer
    # that is not reading, and `max_backlog` drops it. That is the exact
    # failure `_catchup` avoids by doing the same read CLIENT-side with
    # nothing connected, so a request below the local floor is refused and the
    # consumer is pointed there.
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
                yield offset, message

    finally:
        # Releases the DuckDB result the scan is holding. A subscriber that
        # disconnects mid-replay leaves this generator suspended otherwise,
        # and under a reconnect storm that is an unbounded number of live
        # scans against one log.
        reader.close()


def rows_from(log: LogHandle, offset: int) -> int:
    """How many rows the log actually holds from `offset` onward, inclusive.

    **`max_replay` bounds the WORK a replay costs, and that work is rows.**
    Offset distance is a proxy for it, and an exact one only while the offset
    space is dense — which litelink's is not. A `restore` fences 2**20
    offsets that were never issued, so a consumer 150 rows behind measures as
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

    return int(log.sql(quoted).read_all()["n"][0].as_py())


def earliest(log: LogHandle) -> int | None:
    """The lowest offset this handle can serve, or None if it holds nothing.

    "This handle", not "this log": which tiers it reads is fixed when it is
    built, so the archive may hold far older rows than this reports. Those are
    what `catch_up` is for — see `_catchup`.

    **Three tiers, and `coverage()` reports two of them.** That is not a bug
    in litelink — `coverage()` answers "what can this reader serve" for a
    reader assembled from an archive and a replica, where the local Iceberg
    table is empty by construction. A server reads its OWN log, where that
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
    tiers = [log.table_extent(), coverage.buffered]
    # **Only the tiers this handle actually reads.** litelink fixes that at
    # assembly — `include_archive` — and a server opens its log without it, so
    # the archive is not among them. Counting it would make `EARLIEST` resolve
    # below what the very next scan can return, and the subscribe would be
    # refused `evicted` for an offset the server had just called its earliest.
    if log.include_archive:
        tiers.append(coverage.archive)

    lows = [extent[0] for extent in tiers if extent is not None]
    if not lows:
        return None

    return min(lows)


async def replay(
    log: WriteHandle, start: int, stop: int
) -> AsyncGenerator[tuple[int, bytes], None]:
    """`rows`, encoded — what a subscriber's pump sends.

    Split from `rows` so the CLIENT can reuse the batch reader without an
    encode-then-decode round trip it would only undo: `_catchup` reads the
    archive the same way and hands the caller dicts directly, and 1.5 us a row
    through msgspec twice adds up over a catch-up of millions.
    """
    async for offset, message in rows(log, start, stop):
        yield offset, _encode_projected(offset, message)


__all__ = ["columns", "earliest", "replay", "rows", "rows_from"]
