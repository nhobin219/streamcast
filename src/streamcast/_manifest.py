"""Per-log statistics, so a reader can skip whole logs without opening them (#27).

A migrated stream is a sequence of logs, each its own Iceberg table. Iceberg's
own manifests prune FILES within a table; nothing in Iceberg says which
TABLES can be skipped. This is that summary, one level up:

    <stream>.metadata/00004-9f2c….manifest.parquet
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
a few times a year and a reader makes one GET. Each version of the metadata
names its own, beside it in the metadata directory (`_versions`), so the same
pointer works in the home and under the published prefix.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pyarrow.parquet as pq
from litelink import manifest as _litelink

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa
    from litelink import TierStatistics

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
    """The name a `Metadata` in hand gives its manifest, whichever it is.

    `_versions.load` hands a version's manifest back under it, and a commit
    writes the manifest under the version's own name instead. Also the file a
    stream from before the versions kept beside its metadata (`plain`).
    """
    return f"{stream}.manifest.parquet"


def load(root: str | os.PathLike[str], stream: str) -> pa.Table | None:
    """The local manifest, or None if the stream has no sealed log yet: the
    one the current version names, as `_metadata.load` reads that version."""
    from streamcast import _versions  # noqa: PLC0415 — it imports this module

    return _versions.load(root, stream)[2]


def plain(root: str | os.PathLike[str], stream: str) -> pa.Table | None:
    """The plain `<stream>.manifest.parquet` of a stream with no version yet."""
    target = Path(root) / name(stream)
    if not target.exists():
        return None

    return pq.read_table(target)


__all__ = [
    "KEY",
    "Term",
    "entry",
    "extend",
    "load",
    "name",
    "plain",
    "prune",
]
