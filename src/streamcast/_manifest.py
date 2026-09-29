"""Per-log statistics, so a reader can skip whole logs without opening them (#27).

A migrated stream is a sequence of logs, each its own Iceberg table. Iceberg's
own manifests prune FILES within a table; nothing in Iceberg says which
TABLES can be skipped. This is that summary, one level up — and named for the
same reason: an Iceberg manifest is per-file statistics for skipping files,
and this is per-log statistics for skipping logs.

    <stream>.manifest.parquet
    log     start_offset  end_offset  record_count  price                      side
    trades  1             1001        1000          {min, max, null_count,…}   {min, max, …}

**One row per SEALED log.** The live log is still being written, so its
statistics would always be behind; it is never pruned and has no row until a
migration seals it. **Wide**, one struct column per stream column, because a
long `(log, column, min, max)` layout cannot hold typed bounds for columns of
different types. A column's type is fixed for the life of the stream (#35),
so the union of struct columns is always well typed.

**Parquet, rewritten whole at each migration.** PyArrow and DuckDB read it
natively; migrations are rare, so the write is proportional to the log count
a few times a year and a reader makes one GET. `metadata.json`'s `manifest`
names it relative to itself, so the same pointer works for the local copy and
the archive's.

**Pruning is SOUND in one direction only.** Including a log that holds no
match is a wasted scan; excluding one that holds a match is a wrong answer
with no symptom. So every rule below fails towards include, and
`tests/test_manifest.py` checks every exclusion against DuckDB over generated
data — NULLs, NaNs, all-null columns, logs missing a column.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import pyarrow.parquet as pq

from streamcast import _remote, _schema

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from litelink import S3Options

    from streamcast._metadata import Entry

PRUNABLE: Final = frozenset(
    {pa.int32(), pa.int64(), pa.float32(), pa.float64(), pa.bool_()}
)
"""The column types a bound may prune on. Anything else cannot decide.

Strings are out on purpose: Iceberg truncates their bounds (16 characters by
default, the upper one incremented), and the comparison would have to match
DuckDB's byte order exactly. Binary and nested columns have no useful order.
"""

OPERATORS: Final = frozenset({"==", "<", "<=", ">", ">=", "in"})
"""What a term may use. Anything else cannot decide, and so includes."""

Term = tuple[str, str, object]
"""`(column, operator, value)` — one conjunct of the predicate being pruned for.

`value` is a list for `in`. Terms are ANDed: a log survives only if every term
might match it. A reader with an OR, or an operator not listed, passes fewer
terms — dropping a conjunct can only include more, never fewer.
"""


@dataclass(frozen=True, slots=True)
class ColumnStatistics:
    """One column's rollup across one log's data files (litelink#85).

    None anywhere means NOT KNOWN, never zero or empty: a file with no bound
    for the column, a count a file did not record. Every consumer treats
    unknown as "cannot prune".
    """

    min: object | None
    max: object | None
    null_count: int | None
    value_count: int | None
    nan_count: int | None = None
    """Floats only. Iceberg's bounds EXCLUDE NaN, so this is what says one is there."""


@dataclass(frozen=True, slots=True)
class LogStatistics:
    """A sealed log's statistics: its row count and each column's rollup."""

    record_count: int | None
    columns: Mapping[str, ColumnStatistics]


def columns(entries: Iterable[Entry]) -> dict[str, pa.DataType]:
    """The prunable columns across `entries`, user and system, with their types.

    Every name keeps one type across the stream (#35's rule), so a union keyed
    by name is well defined. First occurrence fixes the order.
    """
    found: dict[str, pa.DataType] = {}
    for entry in entries:
        for schema in (entry.schema, entry.system_schema):
            if not schema.get("properties"):
                continue

            for field in _schema.to_arrow(schema):
                if field.type in PRUNABLE:
                    found.setdefault(field.name, field.type)

    return found


def _struct(kind: pa.DataType) -> pa.DataType:
    fields = [
        pa.field("min", kind),
        pa.field("max", kind),
        pa.field("null_count", pa.int64()),
        pa.field("value_count", pa.int64()),
    ]
    if pa.types.is_floating(kind):
        fields.append(pa.field("nan_count", pa.int64()))

    return pa.struct(fields)


def build(sealed: Sequence[tuple[Entry, LogStatistics]]) -> pa.Table:
    """The manifest for `sealed`: one row per log, one struct per column.

    A column the log does not have, or has no statistics for, is a NULL
    struct — which is "no statistics", and never prunes.
    """
    kinds = columns(entry for entry, _ in sealed)
    schema = pa.schema(
        [
            pa.field("log", pa.string(), nullable=False),
            pa.field("start_offset", pa.int64(), nullable=False),
            pa.field("end_offset", pa.int64(), nullable=False),
            pa.field("record_count", pa.int64()),
            *(pa.field(name, _struct(kind)) for name, kind in kinds.items()),
        ]
    )

    rows = []
    for entry, statistics in sealed:
        row: dict[str, object] = {
            "log": entry.name,
            "start_offset": entry.start_offset,
            "end_offset": entry.end_offset,
            "record_count": statistics.record_count,
        }
        for name, kind in kinds.items():
            found = statistics.columns.get(name)
            if found is None:
                row[name] = None
                continue

            value = {
                "min": found.min,
                "max": found.max,
                "null_count": found.null_count,
                "value_count": found.value_count,
            }
            if pa.types.is_floating(kind):
                value["nan_count"] = found.nan_count

            row[name] = value

        rows.append(row)

    return pa.Table.from_pylist(rows, schema=schema)


# -- pruning -------------------------------------------------------------------


def prune(
    manifest: pa.Table | None, sealed: Sequence[str], terms: Sequence[Term]
) -> list[str]:
    """The logs in `sealed` that might hold a row matching every term.

    `sealed` is the READER's list — the `sealed_logs` of the `metadata.json`
    it read — and it is the authority: a manifest row for any other log is
    ignored, and a sealed log with no row is included. A manifest written by a
    migration that landed between the two reads can therefore add nothing.

    **Evaluated in Python rather than as a PyArrow expression**, and that is a
    soundness decision rather than a style one. A filter drops rows where the
    expression is NULL, and NULL here means "no statistics" — so the natural
    default of a vectorised filter would be to EXCLUDE a log it knows nothing
    about. There is one row per sealed log, so there is nothing to vectorise.
    """
    if manifest is None:
        return list(sealed)

    kinds = {
        field.name: field.type.field("min").type
        for field in manifest.schema
        if pa.types.is_struct(field.type)
    }
    rows = {row["log"]: row for row in manifest.to_pylist()}

    return [
        name
        for name in sealed
        if (row := rows.get(name)) is None
        or all(_may_match(row, term, kinds) for term in terms)
    ]


def _may_match(
    row: Mapping[str, object], term: Term, kinds: Mapping[str, pa.DataType]
) -> bool:
    """Whether a log with these statistics MIGHT hold a row matching `term`.

    True whenever it cannot decide. The comparisons are the rows DuckDB would
    return: a NULL matches no comparison, so bounds over the non-null values
    are enough — except for NaN, below.
    """
    column, operator, value = term
    kind = kinds.get(column)
    stats = row.get(column)
    if kind is None or operator not in OPERATORS or not isinstance(stats, dict):
        return True

    low, high = stats.get("min"), stats.get("max")
    if low is None or high is None:
        # An all-null column has no bounds, and a file that recorded none
        # makes the rollup's bound unknown. Neither says anything about rows.
        return True

    if pa.types.is_floating(kind):
        # **Iceberg's bounds exclude NaN, and DuckDB sorts NaN above every
        # float** — measured: `'nan'::DOUBLE > 5` is true, and so is
        # `NaN = NaN`, in a native table. Not everywhere: through
        # `read_parquet` or a registered Arrow table the same row does not
        # match, because the scan skips on NaN-free statistics or pushes the
        # filter into Arrow. A log holding a NaN can match `> v` whatever its
        # max says on at least one path, so a float column with NaNs (or an
        # unknown NaN count) does not prune at all.
        nans = stats.get("nan_count")
        if nans is None or nans > 0:
            return True

    values = value if operator == "in" else [value]
    if not isinstance(values, (list, tuple)):
        return True

    try:
        return any(_compare(operator, low, high, v) for v in values)
    except TypeError:
        # A value the column's type does not compare with — a string against
        # an integer column. Not this function's to reject: it cannot decide.
        return True


def _compare(operator: str, low: object, high: object, value: object) -> bool:
    """Whether some value in `[low, high]` satisfies `operator value`."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return True

    if operator in ("==", "in"):
        return low <= value <= high  # ty: ignore[unsupported-operator]

    if operator == "<":
        return low < value  # ty: ignore[unsupported-operator]

    if operator == "<=":
        return low <= value  # ty: ignore[unsupported-operator]

    if operator == ">":
        return high > value  # ty: ignore[unsupported-operator]

    return high >= value  # ty: ignore[unsupported-operator]


# -- the file ------------------------------------------------------------------


def name(stream: str) -> str:
    """The manifest's file name, which is also `metadata.json`'s pointer to it.

    Relative to the metadata file, so one pointer serves the local copy and the
    archive's alike.
    """
    return f"{stream}.manifest.parquet"


def save(root: str | os.PathLike[str], stream: str, manifest: pa.Table) -> Path:
    """Write it beside the metadata file, atomically, and return where."""
    target = Path(root) / name(stream)
    staging = target.with_suffix(".parquet.tmp")
    pq.write_table(manifest, staging)
    staging.replace(target)

    return target


def publish(
    archive: str, stream: str, manifest: pa.Table, s3: S3Options | None
) -> None:
    """Copy it to the archive, beside the logs and the metadata file."""
    uri = f"{archive.rstrip('/')}/{name(stream)}"
    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    with filesystem.open_output_stream(key) as sink:
        pq.write_table(manifest, sink)


def load(root: str | os.PathLike[str], stream: str) -> pa.Table | None:
    """The local manifest, or None if the stream has no sealed log yet."""
    target = Path(root) / name(stream)
    if not target.exists():
        return None

    return pq.read_table(target)


__all__ = [
    "OPERATORS",
    "PRUNABLE",
    "ColumnStatistics",
    "LogStatistics",
    "Term",
    "build",
    "columns",
    "load",
    "name",
    "prune",
    "publish",
    "save",
]
