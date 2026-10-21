"""Exactly-once publishing: each publisher's high-water mark, beside its rows.

A publisher that opts in (`publish(uri, publisher=...)`) numbers its rows
1, 2, 3, … with no holes, and the broker keeps, per publisher, the last seq it
appended and the offset that row got. A row at or below that mark is not
appended again; its reply carries the original offset if still known, or None.
The next seq is appended; anything above it is a gap and is refused. Gapless
because a hole can only mean lost rows or a sender bug, and papering over one
would hide exactly what this exists to stop.

**The mark is committed in the same SQLite transaction as the rows it
describes** -- litelink's buffer transaction (`log.transaction()`,
`extend(tx=)`) -- in a table of streamcast's own in the log's buffer,
`streamcast_publishers`. So it can never disagree with the log: a crash
before the commit loses both, after it keeps both. A second database would
need two commits, and a crash between them either drops rows or appends
duplicates. litelink owns everything else in that file; this module touches
only its own table, through the transaction litelink hands out.

**One buffer is one stream's log**, so the table has no stream column: the
publisher id is the key.

**The table holds the mark; the memory holds the window.** Each cached
publisher also keeps its last `window` committed ranges, `(first_seq, count,
first_offset)` -- a resend that regroups rows is still answered row by row.
The ranges are memory only. After a restart, or once an entry is evicted, a
duplicate AT the mark is answered with `last_offset` and an older one with
`null` -- and so is one repeating a row of the same commit whose entry was
evicted during it, which takes more publishers in one commit than the cache
holds. A sender that reads the greeting's `last_seq` deletes everything at
or below it instead of resending, so only a caller numbering rows by hand
meets a `null`. Storing the ranges would rewrite about 1.6 KB per publisher
in every commit to answer a question the greeting already settles.

**Everything here runs on the stream's writer thread** (`_writer`), which
owns the cache: it is read and written nowhere else, so it needs no lock.
The cache is filled from the table by key and moved only after a commit
returns, so it is never a second truth.

**Best effort across a lost buffer.** The table lives and dies with
`buffer.db`. A broker restart keeps it; a restore from a WAL replica brings it
back consistent with the rows it restored, minus the replication lag; a
restore from the published table alone loses it, because the published
table holds the log's rows and nothing else. A revive also starts without marks. `docs/SPEC.md` §6b.
"""

from __future__ import annotations

import collections
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final

from streamcast._errors import SequenceGap

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litelink import Transaction, WriteHandle

    from streamcast._writer import Job

TABLE: Final = "streamcast_publishers"

_DDL: Final = (
    f"CREATE TABLE IF NOT EXISTS {TABLE} ("
    "streamcast_publisher_id TEXT NOT NULL PRIMARY KEY, "
    "streamcast_last_seq INTEGER NOT NULL, "
    "streamcast_last_offset INTEGER NOT NULL)"
)
_EXISTS: Final = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?"
_READ: Final = (
    f"SELECT streamcast_last_seq, streamcast_last_offset FROM {TABLE} "
    "WHERE streamcast_publisher_id = ?"
)
_ALL: Final = (
    f"SELECT streamcast_publisher_id, streamcast_last_seq, streamcast_last_offset "
    f"FROM {TABLE}"
)
_UPSERT: Final = (
    f"INSERT INTO {TABLE} "
    "(streamcast_publisher_id, streamcast_last_seq, streamcast_last_offset) "
    "VALUES (?, ?, ?) ON CONFLICT (streamcast_publisher_id) DO UPDATE SET "
    "streamcast_last_seq = excluded.streamcast_last_seq, "
    "streamcast_last_offset = excluded.streamcast_last_offset"
)

MAX_PUBLISHER_ID_BYTES: Final = 128
"""The longest publisher id, in bytes of UTF-8. It goes in a URL and in
every commit's upsert, so it is a name, not a payload."""


def check_publisher_id(publisher: str) -> str:
    """`publisher`, or `ValueError` saying why it cannot name a sequence."""
    size = len(publisher.encode())
    if not 1 <= size <= MAX_PUBLISHER_ID_BYTES:
        msg = (
            f"a publisher id is 1 to {MAX_PUBLISHER_ID_BYTES} bytes of UTF-8; "
            f"got {size}"
        )
        raise ValueError(msg)

    return publisher


@dataclass(slots=True)
class Entry:
    """One publisher's mark, and the ranges this process committed for it."""

    last_seq: int
    last_offset: int | None
    ranges: collections.deque[tuple[int, int, int]] = field(repr=False)

    def offset(self, seq: int) -> int | None:
        """The offset `seq` was appended at, if this entry still knows it."""
        for first, count, first_offset in reversed(self.ranges):
            if first <= seq < first + count:
                return first_offset + (seq - first)

        return self.last_offset if seq == self.last_seq else None


class Publishers:
    """The writer thread's view of `streamcast_publishers`: a bounded cache.

    `capacity` entries at most, least recently used out first. Evicting is
    always safe, a connected publisher's included, because the table is the
    truth: the next row from it re-reads the mark by key, and only the ranges
    -- the original offsets of older duplicates -- are forgotten.
    """

    __slots__ = ("_capacity", "_entries", "_ready", "_window")

    def __init__(self, *, window: int, capacity: int) -> None:
        if window < 1 or capacity < 1:
            msg = f"window={window}, capacity={capacity}: each must be at least 1"
            raise ValueError(msg)

        self._window = window
        self._capacity = capacity
        self._entries: collections.OrderedDict[str, Entry] = collections.OrderedDict()
        self._ready = False

    def __len__(self) -> int:
        return len(self._entries)

    def entry(self, tx: Transaction, publisher: str) -> Entry:
        """`publisher`'s entry, read from the table on a miss."""
        if not self._ready:
            # Created on first use, inside a transaction that commits it:
            # a stream that never sees an exactly-once publisher gets no table.
            # Known to exist only once that transaction commits (`committed`),
            # so a rolled-back one creates it again next time.
            tx.execute(_DDL)

        found = self._entries.get(publisher)
        if found is None:
            rows = tx.execute(_READ, (publisher,))
            last_seq, last_offset = (
                (int(rows[0][0]), int(rows[0][1])) if rows else (0, None)
            )
            found = Entry(last_seq, last_offset, collections.deque(maxlen=self._window))
            self._entries[publisher] = found
            self._evict()

        self._entries.move_to_end(publisher)
        return found

    def committed(self) -> None:
        """A transaction that called `entry` committed: the table exists."""
        self._ready = True

    def plan(self, tx: Transaction, jobs: Sequence[Job]) -> None:
        """Decide each claimed job of a commit, in queue order. Writes nothing.

        Sets each job's `skip` (its leading rows already in the log), `prior`
        (their original offsets) and `gap` (a `SequenceGap` to fail it with).
        A job's rows from `skip` on are the ones to append. The marks move
        tentatively through the commit, so two jobs of one publisher in one
        group are judged in order; the cache itself moves only in `settle`,
        after the commit returns.
        """
        tentative: dict[str, int] = {}
        for job in jobs:
            if job.publisher is None:
                continue

            entry = self.entry(tx, job.publisher)
            last = tentative.get(job.publisher, entry.last_seq)
            count = len(job.rows)
            if job.seq > last + 1:
                job.gap = SequenceGap(last + 1, job.seq)
                continue

            skip = min(count, last - job.seq + 1)
            job.skip = skip
            job.prior = [entry.offset(job.seq + i) for i in range(skip)]
            if skip < count:
                tentative[job.publisher] = job.seq + count - 1

    @staticmethod
    def write(tx: Transaction, jobs: Sequence[Job], offsets: Sequence[int]) -> None:
        """Upsert the mark of every publisher whose rows are in this commit."""
        marks: dict[str, tuple[int, int]] = {}
        for job, first, count in _appended(jobs, offsets):
            assert job.publisher is not None
            marks[job.publisher] = (job.seq + job.skip + count - 1, first + count - 1)

        if marks:
            tx.executemany(
                _UPSERT, [(pid, seq, offset) for pid, (seq, offset) in marks.items()]
            )

    def settle(self, jobs: Sequence[Job], offsets: Sequence[int]) -> None:
        """The commit returned: move each publisher's cached mark to match."""
        for job, first, count in _appended(jobs, offsets):
            assert job.publisher is not None
            entry = self._entries.get(job.publisher)
            if entry is None:  # evicted mid-commit; the table has it
                continue

            first_seq = job.seq + job.skip
            entry.ranges.append((first_seq, count, first))
            entry.last_seq = first_seq + count - 1
            entry.last_offset = first + count - 1

        # A duplicate of a row an earlier job appended in THIS commit had no
        # offset to be answered with when `plan` ran; it has one now.
        for job in jobs:
            if job.publisher is None or None not in job.prior:
                continue

            entry = self._entries.get(job.publisher)
            if entry is not None:
                job.prior = [
                    entry.offset(job.seq + i) if offset is None else offset
                    for i, offset in enumerate(job.prior)
                ]

    def _evict(self) -> None:
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)


def _appended(
    jobs: Sequence[Job], offsets: Sequence[int]
) -> list[tuple[Job, int, int]]:
    """`(job, first_offset, count)` for each claimed job that appended rows.

    Every job's appended rows are adjacent in `offsets`, in queue order, the
    unclaimed ones' included, so the walk counts those too.
    """
    out: list[tuple[Job, int, int]] = []
    start = 0
    for job in jobs:
        if job.gap is not None:
            continue

        count = len(job.rows) - job.skip
        if count and job.publisher is not None:
            out.append((job, offsets[start], count))

        start += count

    return out


# -- migration ------------------------------------------------------------


def read_all(log: WriteHandle) -> list[tuple[str, int, int]]:
    """Every row of the table in `log`'s buffer; empty if it has none."""
    with log.transaction() as tx:
        if not tx.execute(_EXISTS, (TABLE,)):
            return []

        return [(str(p), int(s), int(o)) for p, s, o in tx.execute(_ALL)]


def write_all(log: WriteHandle, marks: Sequence[tuple[str, int, int]]) -> None:
    """Upsert `marks` into `log`'s table, in one transaction with no rows."""
    with log.transaction() as tx:
        tx.execute(_DDL)
        if marks:
            tx.executemany(_UPSERT, [tuple(mark) for mark in marks])


def carried(root: Path, name: str) -> Path:
    """Where `Stream.migrate` keeps the marks between retiring one log and
    committing the next: beside the stream's metadata, streamcast's own file.

    A retired log reopens read-only, and a read-only handle has no
    transaction to read the table through, so a migration that dies after the
    retire could not read the marks again. They are written here first.
    """
    return root / f"{name}.publishers.json"


def stash(path: Path, log: str, marks: Sequence[tuple[str, int, int]]) -> None:
    """Write `marks`, read from the log named `log`, to `path` durably: a
    temporary file, fsynced, renamed."""
    temporary = path.with_suffix(".json.tmp")
    with temporary.open("w") as handle:
        json.dump({"log": log, "marks": [list(mark) for mark in marks]}, handle)
        handle.flush()
        os.fsync(handle.fileno())

    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def unstash(path: Path, log: str) -> list[tuple[str, int, int]] | None:
    """The marks `stash` wrote from the log named `log`, or None.

    Keyed by the log they were read from, because a file a crash left behind
    can outlive its migration: a log retired some other way (`Stream.retire`)
    and then migrated never stashed its own, and an earlier log's marks
    applied to its successor would send every publisher back to an old seq.
    """
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None

    if not isinstance(raw, dict) or raw.get("log") != log:
        return None

    return [(str(p), int(s), int(o)) for p, s, o in raw["marks"]]


__all__ = [
    "MAX_PUBLISHER_ID_BYTES",
    "TABLE",
    "Entry",
    "Publishers",
    "carried",
    "check_publisher_id",
    "read_all",
    "stash",
    "unstash",
    "write_all",
]
