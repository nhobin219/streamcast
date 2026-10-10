"""Time-based retention: a stream's rows older than its window leave it (#118).

    streamcast.Stream.retain("trades", root="data", max_age=timedelta(days=30))

Off unless set, and then applied by the maintainer to the whole stream, oldest
first — the live log and every retired log, including one that is now only a
published table because its box is gone (litelink 0.12.3's `truncate` and
`delete` work from the published location alone).

**The floor is the smallest offset whose `streamcast_ts` is newer than the
cutoff**, `now - max_age`. Not the largest at or before it: `streamcast_ts` is
the server's clock, which can step backwards, so it is not monotonic in offset
order, and that rule would drop rows newer than the cutoff.

**Two halves, a grace apart, because litelink deletes a retired log's files at
once.** A reader still scanning them would lose files under it — its query
fails rather than answering wrong, but it fails. So:

1. `begin` publishes the floor: a new version of the stream's metadata in
   which the stream starts there. Retired logs wholly older than the cutoff
   are dropped from it, with their manifest rows; the first log kept starts
   at the floor. From then on a reader that resolves the metadata reads
   nothing below it. What is left to do is recorded in the same version
   (`Metadata.pending`), so a restart between the halves loses nothing.
2. `finish`, once `GRACE` has passed, deletes the dropped logs, truncates the
   retired log the floor cut through, and truncates the live log — whose own
   deletes wait out litelink's snapshot retention besides — then clears the
   record. Files are whole, so a log may keep rows just below the floor until
   the file holding them goes; no reader sees them.

One pass does one half: `finish` when a record is due, else `begin`. A held
claim on the live log raises `RuntimeError`, and the record stays for the next
pass.
"""

from __future__ import annotations

import dataclasses
import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Final

import litelink
import pyarrow as pa

from streamcast import _log, _manifest, _metadata, _published, _versions

if TYPE_CHECKING:
    from collections.abc import Callable

    from litelink import S3Options, WriteHandle

    from streamcast._metadata import Entry, Metadata

GRACE: Final = timedelta(hours=1)
"""How long a reader that resolved the metadata before a floor moved may keep
reading what is below it: the longest scan a reader is expected to run. As
long as litelink keeps a published snapshot by default."""

EVERY: Final = 3_600.0
"""Seconds between passes. A cutoff moves as fast as the clock and no faster,
and a pass reads each straddled log's newest stamps, so hourly is plenty."""


def now_us() -> int:
    return time.time_ns() // 1_000


def run(
    home: str | Path,
    stream: str,
    live: WriteHandle,
    *,
    s3_options: S3Options | None = None,
    now: int | None = None,
) -> Metadata | None:
    """One pass: the second half if one is due, else the first if there is
    anything to drop. Returns the metadata it committed, or None."""
    # The version read, from its own file, so each half commits only over it:
    # a `Stream.retain` or a migration that committed meanwhile makes this
    # pass a `Conflict`, retried on the next, rather than undone by it.
    read, metadata, manifest = _versions.load(home, stream)
    if metadata is None:
        return None

    at = now_us() if now is None else now
    if metadata.pending is not None:
        return finish(
            home,
            metadata,
            live,
            manifest=manifest,
            s3_options=s3_options,
            now=at,
            expected=read,
        )

    if metadata.retention is None:
        return None

    return begin(
        home,
        metadata,
        newest=_newest(live, s3_options),
        manifest=manifest,
        s3_options=s3_options,
        now=at,
        expected=read,
    )


def floor(
    metadata: Metadata, cutoff: int, newest: Callable[[Entry, int], int | None]
) -> tuple[int, tuple[Entry, ...]] | None:
    """Where the stream should start, and the retired logs wholly before it.

    `newest(entry, cutoff)` is the smallest offset in that log whose stamp is
    newer than `cutoff`, or None if it holds none. A retired log whose
    recorded span ends at or before the cutoff needs no question. None if
    nothing would move.
    """
    dropped: list[Entry] = []
    found = None
    for entry in metadata.sealed_logs:
        if entry.end_ts is not None and entry.end_ts <= cutoff:
            dropped.append(entry)
            continue

        first = newest(entry, cutoff)
        if first is None:
            dropped.append(entry)
            continue

        found = first
        break

    if found is None:
        found = newest(metadata.live_log, cutoff)
        if found is None:
            return None

    start = metadata.logs[0].start_offset
    if not dropped and found <= start:
        return None

    return found, tuple(dropped)


def begin(
    home: str | Path,
    metadata: Metadata,
    *,
    newest: Callable[[Entry, int], int | None],
    manifest: pa.Table | None = None,
    s3_options: S3Options | None = None,
    now: int,
    expected: object = _versions.UNCHECKED,
) -> Metadata | None:
    """The first half: publish the stream starting at its floor.

    `manifest` is the one the version `metadata` was read from names.
    """
    assert metadata.retention is not None  # the caller checked
    moved = floor(metadata, now - metadata.retention, newest)
    if moved is None:
        return None

    at, dropped = moved
    kept = list(metadata.sealed_logs[len(dropped) :])
    live = metadata.live_log
    if kept:
        kept[0] = dataclasses.replace(
            kept[0], start_offset=max(kept[0].start_offset, at)
        )
    else:
        live = dataclasses.replace(live, start_offset=max(live.start_offset, at))

    if manifest is not None:
        for entry in dropped:
            manifest = litelink.manifest.without(
                manifest, entry.name, key=_manifest.KEY
            )

        if (
            kept
            and kept[0].start_offset > metadata.sealed_logs[len(dropped)].start_offset
        ):
            # **The log the floor cuts through stops being counted now**, before
            # its truncate rather than after: a reader checks a retired log's
            # table against its count to catch one published short, and once
            # truncated it is short by design. Unknown, the check is skipped;
            # `finish` counts it again. Otherwise every read of it would fail
            # between the truncate and the commit that recounts it.
            manifest = _counted(manifest, kept[0].name, None)

    moved_to = dataclasses.replace(
        metadata,
        sealed_logs=tuple(kept),
        live_log=live,
        manifest=metadata.manifest if kept else None,
        pending=_metadata.Pending(
            floor=at,
            at=now,
            dropped=tuple(
                _metadata.Dropped(name=entry.name, published=entry.published)
                for entry in dropped
            ),
        ),
    )
    _versions.commit(
        home,
        moved_to,
        manifest=manifest if kept else None,
        published=live.published,
        s3_options=s3_options,
        expected=expected,
    )
    return moved_to


def finish(
    home: str | Path,
    metadata: Metadata,
    live: WriteHandle,
    *,
    manifest: pa.Table | None = None,
    s3_options: S3Options | None = None,
    now: int,
    expected: object = _versions.UNCHECKED,
) -> Metadata | None:
    """The second half, once readers have had their grace: delete, truncate.

    `manifest` is the one the version `metadata` was read from names.
    """
    pending = metadata.pending
    assert pending is not None  # the caller checked
    if now < pending.at + _us(GRACE):
        return None

    for dropped in pending.dropped:
        local = Path(home) / dropped.name
        if dropped.published is not None:
            litelink.delete(
                dropped.published,
                dropped.name,
                s3_options=s3_options,
                root=home if local.is_dir() else None,
            )

    recounted = None
    if metadata.sealed_logs:
        straddled = metadata.sealed_logs[0]
        if straddled.published is not None:
            if manifest is not None and _count(manifest, straddled.name) is not None:
                # Counted again since `begin` — a migration while this was
                # pending sealed the log the floor is in, with its count. Not
                # counted while it is truncated (see `begin`), so a version
                # without the count is committed first, and the rest of this
                # pass builds on that one.
                manifest = _counted(manifest, straddled.name, None)
                expected = _versions.commit(
                    home,
                    metadata,
                    manifest=manifest,
                    published=metadata.live_log.published,
                    s3_options=s3_options,
                    expected=expected,
                )

            litelink.truncate(
                straddled.published,
                straddled.name,
                below=pending.floor,
                s3_options=s3_options,
            )
            if manifest is not None and straddled.name in _names(manifest):
                # Counted again, as what the table holds now.
                opened = _published.Table.open(
                    straddled.published, straddled.name, s3_options
                )
                try:
                    count = opened.record_count
                finally:
                    opened.close()

                recounted = _counted(manifest, straddled.name, count)

    else:
        # The floor is in the live log. A held claim raises: the record stays,
        # and the next pass tries again.
        live.truncate(below=pending.floor)

    done = dataclasses.replace(metadata, pending=None)
    _versions.commit(
        home,
        done,
        manifest=recounted,
        published=metadata.live_log.published,
        s3_options=s3_options,
        expected=expected,
    )
    return done


def _names(manifest: pa.Table) -> list[str]:
    return manifest.column(_manifest.KEY).to_pylist()


def _count(manifest: pa.Table, name: str) -> int | None:
    """`name`'s row count in `manifest`, or None if it has no row or no count."""
    names = _names(manifest)
    if name not in names:
        return None

    return manifest.column("record_count")[names.index(name)].as_py()


def _counted(manifest: pa.Table, name: str, count: int | None) -> pa.Table:
    """`manifest` with `name`'s row counting `count` — None for unknown, which
    a reader's check for a table published short skips."""
    names = _names(manifest)
    if name not in names:
        return manifest

    counts = manifest.column("record_count").to_pylist()
    counts[names.index(name)] = count
    index = manifest.schema.get_field_index("record_count")
    return manifest.set_column(
        index, manifest.schema.field(index), pa.array(counts, pa.int64())
    )


def _newest(
    live: WriteHandle, s3_options: S3Options | None
) -> Callable[[Entry, int], int | None]:
    """`floor`'s question, asked of the live log through its handle and of a
    retired log through its published table."""

    def newest(entry: Entry, cutoff: int) -> int | None:
        if entry.end_offset is None:
            if _log.STAMP not in live.schema.names:
                return entry.start_offset  # no stamps: never older than anything

            # **Read before the query, not after.** The server appends while
            # this runs, in another process: a row landing between the two
            # would be counted below an end read afterwards, and dropped though
            # it is newer than anything. Read first, every row the query does
            # not see is at or above it.
            end = live.end_offset()
            table = live.sql(
                f'SELECT min("{_log.COLUMN}") AS first FROM log '
                f'WHERE "{_log.STAMP}" > {int(cutoff)}'
            ).read_all()
            first = table.column("first")[0].as_py()
            if first is None:
                # Nothing newer: the floor is past every row the log held.
                return end

            return int(first)

        if entry.published is None:
            return entry.start_offset  # nowhere to read it: kept

        opened = _published.Table.open(entry.published, entry.name, s3_options)
        try:
            if _log.STAMP not in opened.schema.names:
                return entry.start_offset

            row = opened._connection.execute(  # noqa: SLF001
                f'SELECT min("{_log.COLUMN}") FROM {opened.relation()} '
                f'WHERE "{_log.STAMP}" > {int(cutoff)}'
            ).fetchone()
        finally:
            opened.close()

        return None if row is None or row[0] is None else int(row[0])

    return newest


def _us(span: timedelta) -> int:
    return int(span.total_seconds() * 1_000_000)


__all__ = ["EVERY", "GRACE", "begin", "finish", "floor", "run"]
