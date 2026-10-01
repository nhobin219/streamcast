"""Per-log statistics, so a reader can skip whole logs without opening them (#27).

A migrated stream is a sequence of logs, each its own Iceberg table. Iceberg's
own manifests prune FILES within a table; nothing in Iceberg says which
TABLES can be skipped. This is that summary, one level up:

    <stream>.manifest.parquet
    log     start_offset  end_offset  record_count  price                      side
    trades  1             1001        1000          {min, max, null_count,…}   {min, max, …}

**The format and the pruning are litelink's** (`litelink.manifest`), which
prunes its own tiers with the same code: one entry per unit, keyed here by
`log`. It is sound in one direction only — every rule fails towards include —
and litelink's tests check every exclusion against DuckDB. What is
streamcast's is the file: which logs it describes, where it lives, and when it
is written.

**One row per SEALED log.** The live log is still being written, so its
statistics would always be behind; it has no row, and a reader passes it as
an entry with no statistics and an open end, which offsets alone can skip.

**Parquet, rewritten whole at each migration.** PyArrow and DuckDB read it
natively; migrations are rare, so the write is proportional to the log count
a few times a year and a reader makes one GET. `metadata.json`'s `manifest`
names it relative to itself, so the same pointer works for the local copy and
the published one.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pyarrow.parquet as pq
from litelink import manifest as _litelink

from streamcast import _remote

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa
    from litelink import S3Options, TierStatistics

KEY: Final = "log"
"""The unit column: a stream's manifest has one row per log."""

Term = _litelink.Term


def entry(
    name: str,
    start: int,
    end: int | None,
    schema: pa.Schema,
    statistics: TierStatistics,
) -> _litelink.Entry:
    """One log as a manifest entry: its offsets, its table's columns, its bounds."""
    return _litelink.Entry(name, start, end, schema, statistics)


def extend(previous: pa.Table | None, sealed: _litelink.Entry) -> pa.Table:
    """`previous` with a row for `sealed`, replacing any it already had."""
    return _litelink.extend(previous, sealed, key=KEY)


def prune(
    manifest: pa.Table | None, names: Sequence[str], terms: Sequence[Term]
) -> list[str]:
    """The logs in `names` that might hold a row matching every term, in order."""
    return _litelink.prune(manifest, names, terms, key=KEY)


# -- the file ------------------------------------------------------------------


def name(stream: str) -> str:
    """The manifest's file name, which is also `metadata.json`'s pointer to it.

    Relative to the metadata file, so one pointer serves the local copy and the
    published one alike.
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
    published: str, stream: str, manifest: pa.Table, s3: S3Options | None
) -> None:
    """Copy it beside the logs' published tables and the metadata file."""
    uri = f"{published.rstrip('/')}/{name(stream)}"
    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    with filesystem.open_output_stream(key) as sink:
        pq.write_table(manifest, sink)


def load(root: str | os.PathLike[str], stream: str) -> pa.Table | None:
    """The local manifest, or None if the stream has no sealed log yet."""
    target = Path(root) / name(stream)
    if not target.exists():
        return None

    return pq.read_table(target)


__all__ = ["KEY", "Term", "entry", "extend", "load", "name", "prune", "publish", "save"]
