"""`Stream.ingest`'s source: stamped on the way in, and explained when it fails.

litelink checks a bulk source in two places. Its schema up front, before any
offset is reserved — names, and whether the types cast — and its VALUES one
row group at a time, as each is prepared: a null in a required column, an
integer too wide for its column, a NaN. A value refused there stops the load
with whatever litelink says, and says nothing of where in the source it was.

**Checked again only on failure, and only what litelink might have been
holding.** A `RecordBatchReader` is read once, so the batches since the
previous row group are kept — about two of litelink's row groups, mostly
references to buffers it holds too — and nothing else is done with them
unless the load raises. A load that succeeds pays for a deque. One that fails
has the batch and row that broke it, found with the same casts litelink makes;
when they find nothing, litelink's message stands as it was, since a check
that disagrees with the one that actually failed must never replace it.
"""

from __future__ import annotations

import collections
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

from streamcast import _log

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class _Kept:
    index: int
    first_row: int
    batch: pa.RecordBatch


class Source:
    """The reader litelink loads: stamped if the log is, and remembered."""

    def __init__(
        self, reader: pa.RecordBatchReader, *, stamp: bool, keep_bytes: int
    ) -> None:
        self._source = reader
        self._stamp = stamp
        self._keep_bytes = keep_bytes
        self._kept: collections.deque[_Kept] = collections.deque()
        self._kept_bytes = 0
        field = pa.field(_log.STAMP, pa.int64(), nullable=False)
        self._field = field
        schema = reader.schema.append(field) if stamp else reader.schema
        self.reader = pa.RecordBatchReader.from_batches(schema, self._batches())

    def _batches(self) -> Iterator[pa.RecordBatch]:
        row = 0
        for index, batch in enumerate(self._source):
            if self._stamp:
                # The time it was loaded: when the server took these rows.
                now = time.time_ns() // 1_000
                stamp = pa.array([now] * batch.num_rows, pa.int64())
                batch = batch.append_column(self._field, stamp)  # noqa: PLW2901

            self._keep(_Kept(index, row, batch))
            row += batch.num_rows
            yield batch

    def _keep(self, kept: _Kept) -> None:
        self._kept.append(kept)
        self._kept_bytes += kept.batch.nbytes
        # The newest `keep_bytes` and the batch that crosses it: never fewer
        # than litelink's row group being prepared when it raised.
        while self._kept_bytes - self._kept[0].batch.nbytes >= self._keep_bytes:
            self._kept_bytes -= self._kept.popleft().batch.nbytes

    def locate(self, schema: pa.Schema) -> tuple[int, int, str] | None:
        """The first kept row litelink would refuse: `(batch, source row, why)`.

        `schema` is the log's, without its offset column. None if every kept
        row passes, which leaves litelink's own message to stand.
        """
        for kept in self._kept:
            found = _refused(kept.batch, schema)
            if found is not None:
                row, why = found
                return kept.index, kept.first_row + row, why

        return None


def _refused(batch: pa.RecordBatch, schema: pa.Schema) -> tuple[int, str] | None:
    """The first row of `batch` that does not fit `schema`, and why.

    pyarrow raises a plain `ValueError` for a null cast to a required column,
    and its own errors subclass `ValueError`, `TypeError` and
    `NotImplementedError`, so those three are what a refused cast raises.
    """
    shaped = batch.select(schema.names)
    try:
        cast = shaped.cast(schema)
    except (ValueError, TypeError, NotImplementedError) as exc:
        # The first row whose prefix no longer casts: a search, so it is a
        # few casts of slices rather than one per row.
        low, high = 0, batch.num_rows
        while high - low > 1:
            middle = (low + high) // 2
            if _casts(shaped.slice(0, middle), schema):
                low = middle
            else:
                high = middle

        return low, str(exc)

    for field in schema:
        if pa.types.is_floating(field.type):
            column = cast.column(field.name)
            finite = pc.fill_null(pc.is_finite(column), True)  # noqa: FBT003  # ty: ignore[unresolved-attribute]
            if not pc.all(finite).as_py():  # ty: ignore[unresolved-attribute]
                row = pc.index(finite, False).as_py()  # noqa: FBT003
                value = column[row].as_py()
                return row, f"column {field.name!r} cannot hold {value}"

    return None


def _casts(batch: pa.RecordBatch, schema: pa.Schema) -> bool:
    try:
        batch.cast(schema)
    except (ValueError, TypeError, NotImplementedError):
        return False

    return True
