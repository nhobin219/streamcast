"""A stream's history, read from any machine, as of a fixed point (#32).

    snapshot = await streamcast.Stream.snapshot(metadata_uri)
    rows = await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side")

A stream is a sequence of logs (`Stream.migrate`), each publishing an
ordinary Iceberg table, and `<stream>.metadata.json` says which, in order,
where each is published, and the offsets and `streamcast_ts` range each
holds. A snapshot reads that file and then the tables, on the reader's own
machine with its own credentials: the broker hands out the file's URI in its
greeting, and is otherwise consulted only for rows no table holds yet.

**A fixed point, three ways to name it** — at most one of them:

* nothing: everything published, which is everything the stream holds except
  what its live log has not published yet. No broker.
* `as_of_offset=N`: every row up to and including offset N. Past the live
  log's published end, the rest is read from `broker`, and refused without
  it. `LATEST` is the broker's frontier as of connect.
* `as_of_ts=T`: every row whose `streamcast_ts` is at most T. Published rows
  only — a row read from the broker carries no `streamcast_ts`, which is never
  on the wire — so a T past what the live log has published is refused rather
  than answered short. Exact except across a server clock step, which can put
  two rows out of order by timestamp while their offsets stay monotonic.

**Be correct or fail.** The reader never guesses: a missing or unreadable
metadata file raises, a file whose `stream_id` is not the one the greeting
named raises, and a range neither the published tables nor the broker holds
raises with both numbers. A short answer to an analytical question is a wrong
number, not an error, so it is never given.

**The heavy work is the reader's.** Pruning, the table reads and the query run
here; the broker's cost is one greeting and, when asked, a bounded tail.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import pyarrow as pa

from streamcast import _log, _manifest, _published, _remote, _schema
from streamcast._errors import NotReplayable, StreamcastError
from streamcast._metadata import Metadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    import duckdb
    from litelink import S3Options

    from streamcast._metadata import Entry

LATEST: Final = -1
"""`as_of_offset=LATEST`: up to the broker's frontier as of connect."""


class SnapshotUnavailable(StreamcastError):
    """A snapshot that cannot be served as asked: what is missing, and why."""


def _read(uri: str, s3: S3Options | None) -> bytes:
    if uri.startswith("file://"):
        with open(_published.path(uri), "rb") as file:  # noqa: PTH123
            return file.read()

    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    with filesystem.open_input_stream(key) as source:
        return source.read()


def metadata(uri: str, s3: S3Options | None, stream_id: str | None = None) -> Metadata:
    """The stream's metadata at `uri`, checked against `stream_id` if given.

    `stream_id` is the greeting's. A `file://` path that exists on two
    machines can name two streams, and reading the other one would return
    another stream's history without a word; the id is what tells them apart.
    """
    try:
        found = Metadata.from_json(_read(uri, s3).decode())
    except FileNotFoundError:
        if uri.startswith("file://"):
            msg = (
                f"there is no metadata at {uri}. This stream's history is local "
                f"to the server's machine and cannot be read from another one; "
                f"give the stream an s3:// published location to make it readable "
                f"elsewhere."
            )
        else:
            msg = f"there is no stream metadata at {uri}"

        raise SnapshotUnavailable(msg) from None

    if stream_id is not None and found.stream_id != stream_id:
        msg = (
            f"the metadata at {uri} is stream {found.stream_id}, not "
            f"{stream_id}: another stream's file at the same path. This "
            f"stream's history is local to the server's machine and cannot be "
            f"read from another one; give the stream an s3:// published "
            f"location to make it readable elsewhere."
        )
        raise SnapshotUnavailable(msg)

    return found


def _stamped(entry: Entry) -> bool:
    return _log.STAMP in (entry.system_schema.get("properties") or {})  # ty: ignore[unsupported-operator]


@dataclass
class _Piece:
    """One log the snapshot reads: its entry, and its table once opened."""

    entry: Entry
    table: _published.Table | None = None


class Snapshot:
    """A stream as of one point: its published tables, plus a broker tail.

    `end_offset` is exclusive: every row the snapshot holds is below it, and a
    reader carrying on live subscribes there.
    """

    __slots__ = (
        "_connection",
        "_manifest",
        "_pieces",
        "_s3",
        "_tail",
        "_ts",
        "end_offset",
        "metadata",
    )

    def __init__(
        self,
        metadata: Metadata,
        pieces: list[_Piece],
        connection: duckdb.DuckDBPyConnection,
        end_offset: int,
        *,
        tail: pa.Table | None,
        ts: int | None,
        manifest: pa.Table | None,
        s3: S3Options | None,
    ) -> None:
        self.metadata = metadata
        self._pieces = pieces
        self._connection = connection
        self.end_offset = end_offset
        self._tail = tail
        self._ts = ts
        self._manifest = manifest
        self._s3 = s3

    # -- reading ---------------------------------------------------------------

    def _open(self, piece: _Piece) -> _published.Table:
        if piece.table is None:
            published = piece.entry.published
            if published is None:  # pragma: no cover — `metadata` fills it in
                msg = f"log {piece.entry.name} has no published location"
                raise SnapshotUnavailable(msg)

            table = _published.Table.open(
                published, piece.entry.name, self._s3, shared=self._connection
            )
            expected = self._recorded(piece.entry)
            if expected is not None and table.record_count < expected:
                # **A retired log published short is a hole, not a smaller log.**
                # Its rows end below the next log's start, and a reader carrying
                # on from `end_offset` — catch-up does exactly that, stepping
                # over restore fences — would step over the missing rows the
                # same way and never know. The manifest's count is the whole
                # log's, read at retirement, so it is what a complete table holds.
                msg = (
                    f"log {piece.entry.name!r} is published short: its table "
                    f"holds {table.record_count} of the {expected} rows it had "
                    f"when it was retired. Publish it in full (the maintainer "
                    f"does, at its next sweep) and read again."
                )
                raise SnapshotUnavailable(msg)

            piece.table = table

        return piece.table

    def _recorded(self, entry: Entry) -> int | None:
        """A retired log's row count as the manifest has it, or None if unknown."""
        if self._manifest is None or entry is self.metadata.live_log:
            return None

        names = self._manifest.column(_manifest.KEY).to_pylist()
        if entry.name not in names:
            return None

        count = self._manifest.column("record_count")[names.index(entry.name)].as_py()
        return None if count is None else int(count)

    def _bounds(self, start: int | None, stop: int | None) -> str:
        terms = [f'"{_log.COLUMN}" < {min(stop or self.end_offset, self.end_offset)}']
        if start is not None:
            terms.append(f'"{_log.COLUMN}" >= {int(start)}')

        if self._ts is not None:
            terms.append(f'"{_log.STAMP}" <= {int(self._ts)}')

        return " AND ".join(terms)

    def _relevant(
        self,
        start: int | None,
        stop: int | None,
        filters: Sequence[_manifest.Term] = (),
    ) -> list[_Piece]:
        """The pieces whose offsets meet `[start, stop)` and whose bounds might match."""
        low = start if start is not None else -math.inf
        high = min(stop or self.end_offset, self.end_offset)
        pieces = [
            piece
            for piece in self._pieces
            if piece.entry.start_offset < high
            and (piece.entry.end_offset is None or piece.entry.end_offset > low)
        ]
        if not filters:
            return pieces

        kept = set(
            _manifest.prune(
                self._manifest, [p.entry.name for p in pieces], list(filters)
            )
        )
        return [piece for piece in pieces if piece.entry.name in kept]

    def _union(self, pieces: list[_Piece], where: str) -> str:
        parts = [
            f"SELECT * FROM {self._open(piece).relation()} WHERE {where}"
            for piece in pieces
        ]
        if self._tail is not None:
            self._connection.register("streamcast_tail", self._tail)
            parts.append(f"SELECT * FROM streamcast_tail WHERE {where}")

        if not parts:
            return "SELECT NULL::BIGINT AS litelink_offset WHERE FALSE"

        return " UNION ALL BY NAME ".join(parts)

    async def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
    ) -> pa.Table:
        """Rows in `[start_offset, end_offset)`, oldest first, as one Arrow table.

        `where` is SQL over the stream's columns. `filters` are
        `(column, operator, value)` terms, ANDed with `where`, and the ones
        that let a sealed log be skipped without opening it: they are pruned
        on the stream's manifest (#27) before any table is read.
        """

        def run() -> pa.Table:
            pieces = self._relevant(start_offset, end_offset, filters)
            conditions = [self._bounds(start_offset, end_offset)]
            if where is not None:
                conditions.append(f"({where})")

            conditions += [_term_sql(term) for term in filters]
            projection = (
                "*" if columns is None else ", ".join(f'"{c}"' for c in columns)
            )
            union = self._union(pieces, " AND ".join(conditions))
            return self._connection.execute(
                f'SELECT {projection} FROM ({union}) ORDER BY "{_log.COLUMN}"'
            ).to_arrow_table()

        return await asyncio.to_thread(run)

    async def sql(self, query: str) -> pa.Table:
        """`query` over the whole snapshot, which it reads as the table `log`."""

        def run() -> pa.Table:
            union = self._union(self._relevant(None, None), self._bounds(None, None))
            self._connection.execute(f"CREATE OR REPLACE TEMP VIEW log AS {union}")
            return self._connection.execute(query).to_arrow_table()

        return await asyncio.to_thread(run)

    async def rows(
        self, start: int, stop: int | None = None
    ) -> AsyncGenerator[tuple[int, dict[str, object]], None]:
        """`(offset, row)` for `[start, stop)`, oldest first, one batch at a time.

        Built by `_log.rows`, the function a server replays with, so a row
        read here is the row a subscriber would have been sent (I6). For a
        range too large to hold as one table, which is what catch-up reads.
        """
        high = min(stop or self.end_offset, self.end_offset)
        for piece in self._relevant(start, high):
            table = await asyncio.to_thread(self._open, piece)
            async for offset, row in _log.rows(table, start, high):
                yield offset, row

        if self._tail is not None:
            for record in self._tail.to_pylist():
                offset = record.pop(_log.COLUMN)
                if start <= offset < high:
                    yield offset, record

    async def floor(self) -> int | None:
        """The lowest offset the snapshot holds, or None if it holds none.

        Opens the oldest log with rows and no other: offsets increase across
        logs, so the first table with an extent is the answer.
        """
        for piece in self._pieces:
            table = await asyncio.to_thread(self._open, piece)
            if table.extent is not None:
                return table.extent[0]

        if self._tail is not None and self._tail.num_rows:
            return int(self._tail.column(_log.COLUMN)[0].as_py())

        return None

    async def close(self) -> None:
        await asyncio.to_thread(self._connection.close)

    async def __aenter__(self) -> Snapshot:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


def _term_sql(term: _manifest.Term) -> str:
    column, operator, value = term

    def literal(item: object) -> str:
        if item is None:
            return "NULL"

        if isinstance(item, bool):
            return "TRUE" if item else "FALSE"

        if isinstance(item, (int, float)):
            return repr(item) if math.isfinite(item) else f"'{item}'::DOUBLE"

        return "'" + str(item).replace("'", "''") + "'"

    if operator == "in":
        return f'"{column}" IN ({", ".join(literal(v) for v in value) or "NULL"})'  # ty: ignore[not-iterable]

    sql = "<>" if operator == "!=" else operator.replace("==", "=")
    return f'"{column}" {sql} {literal(value)}'


# -- building one --------------------------------------------------------------


async def snapshot(
    metadata_uri: str,
    *,
    as_of_offset: int | None = None,
    as_of_ts: int | None = None,
    broker: str | None = None,
    s3: S3Options | None = None,
    stream_id: str | None = None,
) -> Snapshot:
    """See `Stream.snapshot`."""
    if as_of_offset is not None and as_of_ts is not None:
        msg = "pass as_of_offset or as_of_ts, not both"
        raise ValueError(msg)

    if as_of_offset == LATEST and broker is None:
        msg = "as_of_offset=LATEST is the broker's frontier: pass broker="
        raise ValueError(msg)

    if as_of_offset is not None and as_of_offset < LATEST:
        msg = f"as_of_offset={as_of_offset} is negative; LATEST means the frontier"
        raise ValueError(msg)

    found = await asyncio.to_thread(metadata, metadata_uri, s3, stream_id)
    return (
        await asyncio.to_thread(
            _assemble, metadata_uri, found, as_of_offset, as_of_ts, s3
        )
        if broker is None or as_of_offset is None
        else await _with_tail(metadata_uri, found, as_of_offset, broker, s3)
    )


def _manifest_for(
    metadata_uri: str, found: Metadata, s3: S3Options | None
) -> pa.Table | None:
    if found.manifest is None:
        return None

    base = metadata_uri.rsplit("/", 1)[0]
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415 — only when there is one

        return pq.read_table(pa.BufferReader(_read(f"{base}/{found.manifest}", s3)))
    except FileNotFoundError:
        # Missing statistics never prune: every sealed log is read.
        return None


def _pieces(found: Metadata, as_of_ts: int | None) -> list[_Piece]:
    """The logs a snapshot as of `as_of_ts` reads, refusing what it cannot answer."""
    logs = list(found.logs)
    if as_of_ts is None:
        return [_Piece(entry) for entry in logs]

    kept = []
    for index, entry in enumerate(logs):
        if entry.start_ts is not None and entry.start_ts > as_of_ts:
            break  # this log and every later one begin after T

        if not _stamped(entry):
            # Wholly before T only if a later log, which does carry it, began
            # at or before T; otherwise T may fall inside this one.
            later = [e for e in logs[index + 1 :] if e.start_ts is not None]
            if not later or later[0].start_ts > as_of_ts:  # ty: ignore[unsupported-operator]
                msg = (
                    f"as of {as_of_ts} falls in log {entry.name!r}, which carries "
                    f"no {_log.STAMP}. Ask as of an offset instead, or migrate "
                    f"the stream with an unchanged schema to add the column."
                )
                raise SnapshotUnavailable(msg)

        kept.append(_Piece(entry))

    return kept


def _assemble(
    metadata_uri: str,
    found: Metadata,
    as_of_offset: int | None,
    as_of_ts: int | None,
    s3: S3Options | None,
) -> Snapshot:
    """Everything a snapshot needs but a broker: the pieces, the live pin, the end."""
    remote = any((entry.published or "").startswith("s3://") for entry in found.logs)
    connection = _published.connection(s3, remote=remote)
    try:
        pieces = _pieces(found, as_of_ts)
        live = next((p for p in pieces if p.entry is found.live_log), None)
        published_end = found.live_log.start_offset
        if live is not None:
            if _has_table(found.live_log, s3):
                live.table = _published.Table.open(
                    found.live_log.published or "",
                    found.live_log.name,
                    s3,
                    shared=connection,
                )
                if live.table.extent is not None:
                    published_end = live.table.extent[1]
            else:
                # Nothing published yet: the live log holds nothing a reader
                # here can see, and is not a piece to open.
                pieces.remove(live)
                live = None

        if as_of_ts is not None and live is not None and live.table is not None:
            newest = connection.execute(
                f'SELECT max("{_log.STAMP}") FROM {live.table.relation()}'
            ).fetchone()
            if newest is None or newest[0] is None or newest[0] < as_of_ts:
                msg = (
                    f"cannot serve as of {as_of_ts}: the live log "
                    f"{found.live_log.name!r} has published rows up to "
                    f"{_log.STAMP} {None if newest is None else newest[0]}, and rows "
                    f"after it may be earlier than {as_of_ts}. Ask as of an offset "
                    f"with broker=, or once more has been published."
                )
                raise SnapshotUnavailable(msg)

        end = published_end
        if as_of_offset is not None:
            if as_of_offset + 1 > published_end:
                msg = (
                    f"cannot serve as of {as_of_offset}: the published tables end "
                    f"at {published_end - 1}, and the rest is on the broker — "
                    f"pass broker= to read it"
                )
                raise SnapshotUnavailable(msg)

            end = as_of_offset + 1

        return Snapshot(
            found,
            pieces,
            connection,
            end,
            tail=None,
            ts=as_of_ts,
            manifest=_manifest_for(metadata_uri, found, s3),
            s3=s3,
        )
    except BaseException:
        connection.close()
        raise


def _has_table(entry: Entry, s3: S3Options | None) -> bool:
    """Whether the log has published anything yet: a hint file exists."""
    uri = (
        f"{(entry.published or '').rstrip('/')}/{entry.name}/metadata/version-hint.text"
    )
    try:
        _read(uri, s3)
    except FileNotFoundError:
        return False

    return True


async def _with_tail(
    metadata_uri: str,
    found: Metadata,
    as_of_offset: int,
    broker: str,
    s3: S3Options | None,
) -> Snapshot:
    """The published snapshot, then the broker's rows above it up to the point."""
    from streamcast import _client  # noqa: PLC0415 — the client imports this module

    published = await asyncio.to_thread(_assemble, metadata_uri, found, None, None, s3)
    start = published.end_offset
    try:
        if as_of_offset != LATEST and as_of_offset + 1 <= start:
            published.end_offset = as_of_offset + 1
            return published

        try:
            async with _client.connect(broker, offset=start) as sub:
                frontier = sub.info.end_offset
                if frontier is None:
                    msg = f"{broker} serves a stream with no log; it has no offsets"
                    raise SnapshotUnavailable(msg)

                end = frontier if as_of_offset == LATEST else as_of_offset + 1
                if end > frontier:
                    msg = (
                        f"cannot serve as of {as_of_offset}: the broker's "
                        f"frontier is {frontier - 1}"
                    )
                    raise SnapshotUnavailable(msg)

                records: list[dict[str, Any]] = []
                while start < end:
                    offset, row = await sub.recv()
                    if offset is None or offset >= end:
                        break

                    records.append({_log.COLUMN: offset, **row})
                    start = offset + 1

        except NotReplayable as refused:
            floor = refused.fields.get("earliest")
            msg = (
                f"cannot serve as of {as_of_offset}: the published tables end at "
                f"{published.end_offset - 1} and the broker's floor is "
                f"{floor if floor is not None else 'above it'} ({refused.why}). "
                f"Neither holds the rows between."
            )
            raise SnapshotUnavailable(msg) from refused

        published._tail = _tail_table(found.live_log, records)  # noqa: SLF001
        published.end_offset = end
        return published
    except BaseException:
        await published.close()
        raise


def _tail_table(live: Entry, records: list[dict[str, Any]]) -> pa.Table:
    """The broker's rows as Arrow, typed as the live log declares them."""
    declared = _schema.to_arrow(live.schema)
    schema = pa.schema([pa.field(_log.COLUMN, pa.int64(), nullable=False), *declared])
    return pa.Table.from_pylist(records, schema=schema)


__all__ = ["LATEST", "Snapshot", "SnapshotUnavailable", "metadata", "snapshot"]
