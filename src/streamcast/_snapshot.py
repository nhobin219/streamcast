"""A stream's history, read from any machine, as of a fixed point (#32).

    snapshot = await streamcast.Stream.snapshot(metadata_uri)
    table = await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()

A stream is a sequence of logs (`Stream.migrate`), each publishing an
ordinary Iceberg table, and the stream's metadata says which, in order,
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
  only: the broker is consulted as of an offset, not a time, so a T past what
  the live log has published is refused rather than answered short. Exact except across a server clock step, which can put
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
import collections
import math
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import pyarrow as pa
from litelink import S3Options

from streamcast import _log, _manifest, _published, _remote, _schema, _versions
from streamcast._errors import NotReplayable, StreamcastError
from streamcast._limits import MAX_TAIL
from streamcast._metadata import Metadata
from streamcast._published import DEFAULT_CACHE, ReadCache

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence

    import duckdb

    from streamcast._metadata import Entry

LATEST: Final = -1
"""`as_of_offset=LATEST`: up to the broker's frontier as of connect."""


class SnapshotUnavailable(StreamcastError):
    """A snapshot that cannot be served as asked: what is missing, and why."""


CACHE_BYTES: Final = 16 * 1024 * 1024
"""What the process keeps of the immutable metadata it has read, at most.

A version and its manifest are a few KB each, so this holds thousands of
streams' worth. Least recently used goes first, and nothing over a quarter of
it is kept, so one large manifest cannot push out everything else."""

_cache: collections.OrderedDict[tuple[str, tuple[object, ...]], bytes] = (
    collections.OrderedDict()
)
_cached_bytes = 0
_cache_lock = threading.Lock()


def _read(uri: str, s3_options: S3Options | None) -> bytes:
    """`uri`'s bytes, once per process for a versioned file on object storage.

    **Only what cannot change is cached** (#123): a metadata version or the
    manifest it names, whose names are never reused. The hint is read every
    time — it is what says a new version exists — and so is every plain copy.
    Nothing on local disk is cached: re-reading it costs no request.

    **Each entry is the credentials' that fetched it**, keyed by them and the
    endpoint as well as the URI. Keyed by the URI alone, a reader with wrong
    or narrower credentials would be handed what another fetched: an open
    that should fail would succeed, and one set of keys could read what only
    another may. litelink found the same in its own cache (litelink#185).
    """
    if uri.startswith("file://"):
        with open(_published.path(uri), "rb") as file:  # noqa: PTH123
            return file.read()

    cacheable = _versions.immutable(uri)
    entry = (uri, _identity(s3_options))
    if cacheable:
        with _cache_lock:
            if entry in _cache:
                _cache.move_to_end(entry)
                return _cache[entry]

    filesystem, key = _remote._filesystem(uri, s3_options)  # noqa: SLF001
    with filesystem.open_input_stream(key) as source:
        data = source.read()

    if cacheable and len(data) <= CACHE_BYTES // 4:
        _remember(entry, data)

    return data


def _identity(s3_options: S3Options | None) -> tuple[object, ...]:
    """Who a read is made as, and where to: what a cache entry belongs to.

    Resolved, so credentials from the environment are told apart too.
    """
    resolved = (s3_options or S3Options()).resolved()
    return (
        resolved.endpoint,
        resolved.region,
        resolved.access_key,
        resolved.secret_key,
    )


def _remember(key: tuple[str, tuple[object, ...]], data: bytes) -> None:
    global _cached_bytes  # noqa: PLW0603 — the cache is the process's
    with _cache_lock:
        if key in _cache:
            return

        _cache[key] = data
        _cached_bytes += len(data)
        while _cached_bytes > CACHE_BYTES:
            _, evicted = _cache.popitem(last=False)
            _cached_bytes -= len(evicted)


def _resolve(uri: str, s3_options: S3Options | None) -> str:
    """The metadata version a hint names, or `uri` itself if it is not a hint.

    A hint is what the greeting gives a reader. A `<stream>.metadata.json` URI
    — an older greeting's, or one written down — is read through the hint
    beside it (#124): the plain file there is a stream from before the
    versions, and one its first version has not deleted yet is stale. Read
    as it is only when there is no hint, for a stream never versioned.
    """
    hint = uri if uri.endswith(f"/{_versions.HINT}") else _versions.beside(uri)
    if hint is None:
        return uri

    try:
        named = _read(hint, s3_options).decode().strip()
    except FileNotFoundError:
        if hint == uri:
            raise

        return uri

    return f"{hint.rsplit('/', 1)[0]}/{named}"


def metadata(
    uri: str, s3_options: S3Options | None, stream_id: str | None = None
) -> Metadata:
    """The stream's metadata at `uri`, checked against `stream_id` if given.

    `stream_id` is the greeting's. A `file://` path that exists on two
    machines can name two streams, and reading the other one would return
    another stream's history without a word; the id is what tells them apart.
    """
    try:
        found = Metadata.from_json(
            _read(_resolve(uri, s3_options), s3_options).decode()
        )
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


def _from(start: int | None, piece: _Piece) -> int:
    """Where a read of `piece` starts: the caller's start, never below the log's.

    **The log's recorded start is the floor a reader sees.** Retention moves it
    up first and deletes the rows below it only after readers' grace
    (`_retention`), so a log can still hold rows the stream no longer has.
    """
    return (
        piece.entry.start_offset
        if start is None
        else max(start, piece.entry.start_offset)
    )


def _stamped(entry: Entry) -> bool:
    return _log.STAMP in (entry.system_schema.get("properties") or {})  # ty: ignore[unsupported-operator]


@dataclass
class _Piece:
    """One log the snapshot reads: its entry, and its table once opened."""

    entry: Entry
    table: _published.Table | None = None


@dataclass(frozen=True)
class _Frozen:
    """What one read sees of a snapshot `Live` moves: its tail, and its end."""

    tail: pa.Table | None
    end: int


class Reader:
    """A query's result as it streams: batch by batch, or whole.

        async with snapshot.sql("SELECT …") as result:
            async for batch in result:                 # pa.RecordBatch
                ...

        table = await snapshot.sql("SELECT …").read_all()

    The query runs once, when the result is first read — entering `async
    with`, the first batch, or `read_all` — on a DuckDB cursor of its own, so
    other reads of the same snapshot cannot end it short. Every DuckDB call is
    in a worker thread: the event loop never waits on the read. One batch is
    in memory at a time, unless `read_all` asks for every one.

    **Valid while its snapshot is open.** A batch asked for after the snapshot
    closed raises; it never ends the result early. Reading it to the end,
    `read_all`, `aclose` or leaving `async with` closes the cursor. A reader
    never read or closed keeps it, and keeps its snapshot's connection open,
    until it is.

    `Stream.sql` and `Stream.scan` return one with a snapshot of its own,
    opened at its first read and closed once it is done.
    """

    __slots__ = (
        "_cursor",
        "_done",
        "_opening",
        "_pending",
        "_plan",
        "_reader",
        "_schema",
        "_snapshot",
    )

    def __init__(
        self,
        snapshot: Snapshot | None,
        plan: Callable[[duckdb.DuckDBPyConnection], pa.RecordBatchReader] | None,
        *,
        opening: Callable[[], Awaitable[Reader]] | None = None,
    ) -> None:
        self._snapshot = snapshot
        self._plan = plan
        # For a reader with a snapshot of its own: opens it, and makes the
        # reader on it whose query this one runs.
        self._opening = opening
        self._reader: pa.RecordBatchReader | None = None
        self._cursor: duckdb.DuckDBPyConnection | None = None
        self._schema: pa.Schema | None = None
        self._pending: asyncio.Future[Any] | None = None
        self._done = False
        if snapshot is not None:
            # Counted from creation, not from the first read: a `Live` rebase
            # between the two must not close the base this was made against.
            snapshot._acquire()

    @classmethod
    def _owning(cls, opening: Callable[[], Awaitable[Reader]]) -> Reader:
        """A reader whose snapshot `opening` opens at the first read."""
        return cls(None, None, opening=opening)

    @property
    def schema(self) -> pa.Schema:
        """The result's schema, once the query has run."""
        if self._schema is None:
            msg = "the query has not run yet: enter `async with`, or read a batch"
            raise RuntimeError(msg)

        return self._schema

    async def read_all(self) -> pa.Table:
        """Every batch not yet read, as one table; then closed."""
        table = await self._run(lambda: self._started().read_all())
        await self.aclose()
        return table

    async def aclose(self) -> None:
        """Release the cursor. Idempotent."""
        if self._done:
            return

        self._done = True
        if self._pending is not None and not self._pending.done():
            # A read cancelled in its thread is still running there: the
            # cursor is closed under it only once it has returned.
            await asyncio.wait([self._pending])

        reader, cursor = self._reader, self._cursor
        self._reader = self._cursor = None
        try:
            if cursor is not None:
                await asyncio.to_thread(_close, reader, cursor)

        finally:
            if self._snapshot is not None:
                await self._snapshot._release()  # noqa: SLF001

    def __aiter__(self) -> Reader:
        return self

    async def __anext__(self) -> pa.RecordBatch:
        if self._done:
            raise StopAsyncIteration

        batch = await self._run(lambda: _log._next_batch(self._started()))  # noqa: SLF001
        if batch is None:
            await self.aclose()
            raise StopAsyncIteration

        return batch

    async def __aenter__(self) -> Reader:
        await self._run(self._started)
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def _started(self) -> pa.RecordBatchReader:
        """The query's reader, run on a new cursor the first time. In a thread."""
        if self._reader is None:
            assert self._snapshot is not None  # `_run` saw to it
            assert self._plan is not None
            cursor = self._snapshot._cursor()  # noqa: SLF001
            try:
                self._reader = self._plan(cursor)
            except BaseException:
                cursor.close()
                raise

            self._cursor = cursor
            self._schema = self._reader.schema

        return self._reader

    async def _open(self) -> None:
        """Open this reader's own snapshot, and take over the reader made on it."""
        assert self._opening is not None
        made = await self._opening()
        snapshot = made._snapshot  # noqa: SLF001
        assert snapshot is not None
        # Its count on the snapshot is this reader's now; it is never read.
        made._done = True  # noqa: SLF001
        self._snapshot, self._plan = snapshot, made._plan  # noqa: SLF001
        # Closed as soon as this reader is done, which is its only one.
        await snapshot._retire()  # noqa: SLF001

    async def _run(self, work: Callable[[], Any]) -> Any:
        if self._done:
            msg = "this result is closed"
            raise RuntimeError(msg)

        try:
            if self._snapshot is None:
                await self._open()

            assert self._snapshot is not None
            self._snapshot._check()  # noqa: SLF001
            self._pending = asyncio.ensure_future(asyncio.to_thread(work))
            return await asyncio.shield(self._pending)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.aclose()
            raise


def _close(
    reader: pa.RecordBatchReader | None, cursor: duckdb.DuckDBPyConnection
) -> None:
    try:
        if reader is not None:
            reader.close()

    finally:
        cursor.close()


class Snapshot:
    """A stream as of one point: its published tables, plus a broker tail.

    `end_offset` is exclusive: every row the snapshot holds is below it, and a
    reader carrying on live subscribes there.
    """

    __slots__ = (
        "_closed",
        "_connection",
        "_guard",
        "_manifest",
        "_pieces",
        "_readers",
        "_retiring",
        "_s3",
        "_shut",
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
        s3_options: S3Options | None,
    ) -> None:
        self.metadata = metadata
        self._pieces = pieces
        self._connection = connection
        self.end_offset = end_offset
        self._tail = tail
        self._ts = ts
        self._manifest = manifest
        self._s3 = s3_options
        # The connection is shared by every read, each from a worker thread:
        # held while a table is opened or a cursor taken from it.
        self._guard = threading.Lock()
        # Reads still open — readers, `rows` — which keep the connection open
        # past `close` or `_retire` until the last of them ends.
        self._readers = 0
        self._closed = False
        self._retiring = False
        self._shut = False

    # -- reading ---------------------------------------------------------------

    def _open(self, piece: _Piece) -> _published.Table:
        with self._guard:
            return self._opened(piece)

    def _opened(self, piece: _Piece) -> _published.Table:
        if piece.table is None:
            published = piece.entry.published
            if published is None:  # pragma: no cover — `metadata` fills it in
                msg = f"log {piece.entry.name} has no published location"
                raise SnapshotUnavailable(msg)

            table = _published.Table.open(
                published,
                piece.entry.name,
                self._s3,
                shared=self._connection,
                guard=self._guard,
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

    def _bounds(
        self, start: int | None, stop: int | None, *, stamped: bool, end: int
    ) -> str:
        """The snapshot's own limits, on columns one log is sure to have.

        The offset always; the `as_of_ts` bound only on a log that carries
        the stamp. `_pieces` keeps an unstamped log only when it lies wholly
        before the point, so every one of its rows is inside it.
        """
        terms = [f'"{_log.COLUMN}" < {min(stop or end, end)}']
        if start is not None:
            terms.append(f'"{_log.COLUMN}" >= {int(start)}')

        if self._ts is not None and stamped:
            terms.append(f'"{_log.STAMP}" <= {int(self._ts)}')

        return " AND ".join(terms)

    def _relevant(
        self,
        start: int | None,
        stop: int | None,
        filters: Sequence[_manifest.Term] = (),
        *,
        end: int,
    ) -> list[_Piece]:
        """The pieces whose offsets meet `[start, stop)` and whose bounds might match."""
        low = start if start is not None else -math.inf
        high = min(stop or end, end)
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

    def _select(
        self,
        cursor: duckdb.DuckDBPyConnection,
        frozen: _Frozen,
        start: int | None,
        stop: int | None,
        where: str | None,
        filters: Sequence[_manifest.Term],
    ) -> str:
        """The snapshot's rows in `[start, stop)` matching `where` and `filters`.

        Read on `cursor`, which is where the tail is registered: a cursor does
        not see what is registered on its parent. `frozen` is the tail and the
        end the reader was created with — a `Live` view moves both on the
        snapshot for each query, and an open reader must not see that.

        **The caller's conditions go OUTSIDE the union.** A log from before a
        migration added a column has no such column, so a condition on it
        inside that log's SELECT does not bind — the whole read fails. Over
        the union, the column is NULL there, which is what the union promises
        for every other read. The snapshot's own limits stay inside, where
        they are sure to bind and push down into each table's scan.
        """
        end = frozen.end
        pieces = self._relevant(start, stop, filters, end=end)
        parts = [
            f"SELECT * FROM {self._open(piece).relation()} "
            f"WHERE {self._bounds(_from(start, piece), stop, stamped=_stamped(piece.entry), end=end)}"
            for piece in pieces
        ]
        if frozen.tail is not None:
            # Broker rows: as of an offset only, so no stamp bound applies.
            cursor.register("streamcast_tail", frozen.tail)
            parts.append(
                f"SELECT * FROM streamcast_tail "
                f"WHERE {self._bounds(start, stop, stamped=False, end=end)}"
            )

        live = self.metadata.live_log
        if frozen.tail is None and not any(piece.entry is live for piece in pieces):
            # Nothing read from the live log — it has published nothing, and
            # there is no tail — but its columns still belong in the table,
            # typed as it declares them: a column a migration added would
            # otherwise be missing until the live log first publishes, and a
            # query naming it would be a binder error rather than NULLs. With
            # nothing else to read either, this is the whole (empty) table.
            cursor.register("streamcast_empty", _tail_table(self.metadata.live_log, []))
            parts.append("SELECT * FROM streamcast_empty")

        union = " UNION ALL BY NAME ".join(parts)
        conditions = [_term_sql(term) for term in filters]
        if where is not None:
            conditions.append(f"({where})")

        if not conditions:
            return union

        return f"SELECT * FROM ({union}) WHERE {' AND '.join(conditions)}"

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
        batch_size: int = _published.BATCH,
    ) -> Reader:
        """Rows in `[start_offset, end_offset)`, oldest first, as a `Reader`.

            table = await snapshot.scan(where="side = 1").read_all()

        `where` is SQL over the stream's columns. `filters` are
        `(column, operator, value)` terms, ANDed with `where`, and the ones
        that let a retired log be skipped without opening it: they are pruned
        on the stream's manifest (#27) before any table is read. Only
        `filters` prune; see `sql` for why `where` does not.
        """

        frozen = self._frozen()
        projection = "*" if columns is None else ", ".join(f'"{c}"' for c in columns)

        def plan(cursor: duckdb.DuckDBPyConnection) -> pa.RecordBatchReader:
            rows = self._select(
                cursor, frozen, start_offset, end_offset, where, filters
            )
            return cursor.execute(
                f'SELECT {projection} FROM ({rows}) ORDER BY "{_log.COLUMN}"'
            ).to_arrow_reader(batch_size)

        return Reader(self, plan)

    def sql(
        self,
        query: str,
        *,
        filters: Sequence[_manifest.Term] = (),
        start_offset: int | None = None,
        end_offset: int | None = None,
        batch_size: int = _published.BATCH,
    ) -> Reader:
        """`query` over the snapshot, which it reads as the table `log`, as a `Reader`.

            table = await snapshot.sql("SELECT side, count(*) FROM log GROUP BY side").read_all()

        `filters` and `[start_offset, end_offset)` narrow what `log` holds,
        exactly as they narrow `scan`: rows outside them are not in it, and a
        retired log they rule out is never opened. They are applied to the
        rows too, so the answer never depends on what the statistics happened
        to rule out.

        **The query itself does not prune**, though a `WHERE` in it says the
        same thing. Deriving terms from SQL means reading a predicate
        correctly in every case — `OR`, casts, functions, NULL semantics —
        and a misread does not fail: it drops a log that held matches and
        answers short, with no symptom. Until that is done soundly (#57), the
        pruning a caller wants is stated as `filters`.
        """

        frozen = self._frozen()

        def plan(cursor: duckdb.DuckDBPyConnection) -> pa.RecordBatchReader:
            rows = self._select(cursor, frozen, start_offset, end_offset, None, filters)
            # A temporary view is the cursor's own, so two readers' `log`
            # never replace each other.
            cursor.execute(f"CREATE OR REPLACE TEMP VIEW log AS {rows}")
            return cursor.execute(query).to_arrow_reader(batch_size)

        return Reader(self, plan)

    async def rows(
        self, start: int, stop: int | None = None
    ) -> AsyncGenerator[tuple[int, int | None, dict[str, object]], None]:
        """`(offset, ts, row)` for `[start, stop)`, oldest first, a batch at a time.

        Built by `_log.rows`, the function a server replays with, so a row
        read here is the row a subscriber would have been sent (I6). For a
        range too large to hold as one table, which is what catch-up reads.
        """
        self._check()
        frozen = self._frozen()
        high = min(stop or frozen.end, frozen.end)
        self._acquire()
        try:
            for piece in self._relevant(start, high, end=frozen.end):
                self._check()
                table = await asyncio.to_thread(self._open, piece)
                async for offset, ts, row in _log.rows(
                    table, _from(start, piece), high
                ):
                    # Each row, not each log: one closed mid-log raises at the
                    # next row rather than reading the rest of the log first.
                    self._check()
                    yield offset, ts, row

            if frozen.tail is not None:
                for record in frozen.tail.to_pylist():
                    offset = record.pop(_log.COLUMN)
                    ts = record.pop(_log.STAMP)
                    if start <= offset < high:
                        yield offset, ts, record

        finally:
            await self._release()

    async def floor(self) -> int | None:
        """The lowest offset the snapshot holds, or None if it holds none.

        Opens the oldest log with rows and no other: offsets increase across
        logs, so the first table with an extent is the answer.
        """
        for piece in self._pieces:
            table = await asyncio.to_thread(self._open, piece)
            if table.extent is not None and table.extent[1] > piece.entry.start_offset:
                # Where the log's rows begin, or where the stream says it
                # does, if retention moved that up (`_from`).
                return max(table.extent[0], piece.entry.start_offset)

        if self._tail is not None and self._tail.num_rows:
            return int(self._tail.column(_log.COLUMN)[0].as_py())

        return None

    async def close(self) -> None:
        """Close the snapshot. A reader still open raises at its next batch."""
        self._closed = True
        await self._shut_if_idle()

    async def _retire(self) -> None:
        """Close once the readers still open have finished, which carry on.

        What `Live` does with the base a rebase replaced: a query begun on it
        reads it to the end.
        """
        self._retiring = True
        await self._shut_if_idle()

    def _frozen(self) -> _Frozen:
        self._check()
        return _Frozen(self._tail, self.end_offset)

    def _check(self) -> None:
        if self._closed:
            msg = "this snapshot is closed: read it inside its `async with`"
            raise RuntimeError(msg)

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        with self._guard:
            return self._connection.cursor()

    def _acquire(self) -> None:
        self._readers += 1

    async def _release(self) -> None:
        self._readers -= 1
        await self._shut_if_idle()

    async def _shut_if_idle(self) -> None:
        if (self._closed or self._retiring) and self._readers == 0 and not self._shut:
            self._shut = True
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
    s3_options: S3Options | None = None,
    stream_id: str | None = None,
    max_tail: int = MAX_TAIL,
    cache: ReadCache = DEFAULT_CACHE,
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

    found = await asyncio.to_thread(metadata, metadata_uri, s3_options, stream_id)
    return (
        await asyncio.to_thread(
            _assemble, metadata_uri, found, as_of_offset, as_of_ts, s3_options, cache
        )
        if broker is None or as_of_offset is None
        else await _with_tail(
            metadata_uri, found, as_of_offset, broker, s3_options, max_tail, cache
        )
    )


def _manifest_for(
    metadata_uri: str, found: Metadata, s3_options: S3Options | None
) -> pa.Table | None:
    if found.manifest is None:
        return None

    try:
        import pyarrow.parquet as pq  # noqa: PLC0415 — only when there is one

        return pq.read_table(
            pa.BufferReader(
                _read(_versions.manifest_uri(metadata_uri, found.manifest), s3_options)
            )
        )
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
    s3_options: S3Options | None,
    cache: ReadCache = DEFAULT_CACHE,
) -> Snapshot:
    """Everything a snapshot needs but a broker: the pieces, the live pin, the end."""
    remote = any((entry.published or "").startswith("s3://") for entry in found.logs)
    connection = _published.connection(s3_options, remote=remote, cache=cache)
    try:
        pieces = _pieces(found, as_of_ts)
        live = next((p for p in pieces if p.entry is found.live_log), None)
        published_end = found.live_log.start_offset
        if live is not None:
            if _has_table(found.live_log, s3_options):
                live.table = _published.Table.open(
                    found.live_log.published or "",
                    found.live_log.name,
                    s3_options,
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
            manifest=_manifest_for(metadata_uri, found, s3_options),
            s3_options=s3_options,
        )
    except BaseException:
        connection.close()
        raise


def _has_table(entry: Entry, s3_options: S3Options | None) -> bool:
    """Whether the log has published anything yet: a hint file exists."""
    uri = (
        f"{(entry.published or '').rstrip('/')}/{entry.name}/metadata/version-hint.text"
    )
    try:
        _read(uri, s3_options)
    except FileNotFoundError:
        return False

    return True


async def _with_tail(
    metadata_uri: str,
    found: Metadata,
    as_of_offset: int,
    broker: str,
    s3_options: S3Options | None,
    max_tail: int,
    cache: ReadCache = DEFAULT_CACHE,
) -> Snapshot:
    """The published snapshot, then the broker's rows above it up to the point."""
    from streamcast import _client  # noqa: PLC0415 — the client imports this module

    published = await asyncio.to_thread(
        _assemble, metadata_uri, found, None, None, s3_options, cache
    )
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
                    offset, ts, row = await sub.recv()
                    if offset is None or offset >= end:
                        break

                    records.append({_log.COLUMN: offset, _log.STAMP: ts, **row})
                    start = offset + 1
                    if len(records) > max_tail:
                        msg = (
                            f"cannot serve as of {as_of_offset}: the broker holds "
                            f"more than max_tail={max_tail} rows the published "
                            f"tables do not — they end at {published.end_offset - 1}. "
                            f"Publishing is behind or stalled; ask as of a lower "
                            f"offset, raise max_tail, or wait for it to catch up."
                        )
                        raise SnapshotUnavailable(msg)

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
    """The broker's rows as Arrow, typed as the live log declares them.

    With `streamcast_ts` from each frame, so a tail row carries the stamp a
    published one does — null where the log has none.
    """
    declared = _schema.to_arrow(live.schema)
    schema = pa.schema(
        [
            pa.field(_log.COLUMN, pa.int64(), nullable=False),
            pa.field(_log.STAMP, pa.int64()),
            *declared,
        ]
    )
    return pa.Table.from_pylist(records, schema=schema)


__all__ = [
    "LATEST",
    "Snapshot",
    "SnapshotUnavailable",
    "metadata",
    "snapshot",
]
