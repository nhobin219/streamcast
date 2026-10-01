"""The logs a stream is made of, in order, and which one is being written.

A stream is one litelink log until it is migrated. `Stream.migrate` seals the
live log for good, creates the next one with the new schema starting at
exactly the offset the last one ended at, and records both here — so a stream
becomes a SEQUENCE of logs whose offsets are one dense, monotonic space.

    root/trades.metadata.json
    {"streamcast_metadata": 2, "stream": "trades", "stream_id": "6f1c…",
     "sealed_logs": [{"name": "trades", "published": "s3://bucket/prod",
                      "start_offset": 1, "end_offset": 1001,
                      "start_ts": 1790000000000000, "end_ts": 1790003600000000,
                      "schema": {...}, "system_schema": {...}}],
     "live_log": {"name": "trades-v2", "published": "s3://bucket/prod",
                  "start_offset": 1001, "start_ts": 1790003600000412,
                  "schema": {...}, "system_schema": {...}},
     "manifest": "trades.manifest.parquet"}

**Named after Iceberg's `metadata.json`**, which plays the same part for a
table: the JSON that says what it currently is. "Manifest" is Iceberg's word
for per-file statistics; the stream's per-log statistics are #27's, and
`manifest` is the pointer to them — null until there is a sealed log to
describe.

**Sealed and live are separate keys** because only sealed logs have
statistics and only they can be pruned: the structure says so, rather than
leaving a reader to remember that the last entry is special.

**Every durable stream has one, from its first `serve`** — see `ensure`. A
stream without a readable metadata file cannot be read by anything but its
own server, so `serve` writes it (and uploads it) or refuses to start.

**`stream_id` is minted once**, when the file is first written, and never
changes. Iceberg's `table-uuid` for the same reason: a `file://` path that
exists on two machines names two different streams, and the id is how a
reader tells them apart.

**Beside the logs, not inside one.** `root/trades` IS the first log's
directory, so a file in it would be a file inside a litelink log. And a copy
goes to `<archive>/trades.metadata.json`, because `Stream.restore` and a
remote reader have the archive and not this disk.

**Each entry says where its log's rows are read from**: `published`, the
prefix its published table sits under, at `<published>/<name>`. Per log,
because a stream without an `s3://` location publishes each log to a
directory inside that log. And its offsets and its `streamcast_ts` range
(`start_ts`, `end_ts`, microseconds), so a reader asking for a range or a
point in time skips whole logs without opening them; a bound not yet known is
null, and a null never skips.

**Version 1** is the same without `published`, `start_ts` and `end_ts`. It is
read, and `ensure` fills those in and rewrites the file at the next `serve`.

**Each entry records the schema its log was created with**, which is the
history the type rule is checked against: a column's type is fixed for the
life of the stream, including across its removal and re-addition, so the check
needs every schema the stream has had rather than only the current one.

**Written last, by atomic rename.** A migration that dies before this leaves
a new log directory the metadata does not name — an orphan that the next
`migrate` adopts if it is empty and has the requested shape, and refuses
otherwise. It never leaves a metadata file naming a log that does not exist.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Final

import litelink
import pyarrow.fs as pafs

from streamcast import _log, _remote, _schema

if TYPE_CHECKING:
    import pyarrow as pa
    from litelink import LogHandle, S3Options, TierStatistics

VERSION: Final = 2
READS: Final = frozenset({1, VERSION})


@dataclass(frozen=True, slots=True)
class Entry:
    """One log in the sequence."""

    name: str
    start_offset: int
    end_offset: int | None
    """Exclusive, and None for the log being written — its end is still moving."""
    schema: dict[str, object]
    """The APPLICATION's columns as JSON Schema, as `Stream.schema` publishes
    them. Without the system columns, which are owned rather than declared."""
    system_schema: dict[str, object]
    """The SYSTEM columns this log has, as JSON Schema (`_log.SYSTEM`).

    Per log, because it differs between them: a log from before a system
    column existed lacks it, and the one a migration created has all of them.
    Recorded so that is readable at a glance rather than by opening each log.
    """
    published: str | None = None
    """The prefix this log's published table is under. None only when read
    from a version-1 file, until `ensure` fills it in."""
    start_ts: int | None = None
    """The lowest `streamcast_ts` the log holds, or None if not known yet."""
    end_ts: int | None = None
    """The highest, for a sealed log; None for the live one, whose end moves."""


@dataclass(frozen=True, slots=True)
class Metadata:
    """A stream's logs: the sealed ones, oldest first, and the live one."""

    stream: str
    stream_id: str
    sealed_logs: tuple[Entry, ...]
    live_log: Entry
    """The log being written. Its `end_offset` is None: its end is moving."""
    manifest: str | None = None
    """Where the sealed logs' statistics are (#27), or None until there are any."""

    @property
    def logs(self) -> tuple[Entry, ...]:
        """Every log, oldest first, the live one last."""
        return (*self.sealed_logs, self.live_log)

    @property
    def current(self) -> Entry:
        return self.live_log

    @property
    def retired(self) -> tuple[Entry, ...]:
        return self.sealed_logs

    def advance(
        self,
        end: int,
        successor: Entry,
        *,
        span: tuple[int | None, int | None] = (None, None),
    ) -> Metadata:
        """The live log sealed at `end`, and `successor` live after it.

        `span` is the sealed log's `streamcast_ts` range, read from its
        statistics once it was retired.
        """
        head = self.live_log
        sealed = replace(
            head,
            end_offset=end,
            start_ts=head.start_ts if head.start_ts is not None else span[0],
            end_ts=span[1],
        )

        return replace(
            self, sealed_logs=(*self.sealed_logs, sealed), live_log=successor
        )

    def next_name(self) -> str:
        """`trades-v2`, `trades-v3`, … — named for its place in the sequence.

        A `.` cannot be the separator: litelink hands the name to Iceberg,
        which reads a dot as a namespace boundary and refuses the table.
        """
        return f"{self.stream}-v{len(self.logs) + 1}"

    def to_json(self) -> str:
        live = self.live_log
        return json.dumps(
            {
                "streamcast_metadata": VERSION,
                "stream": self.stream,
                "stream_id": self.stream_id,
                "sealed_logs": [
                    {
                        "name": entry.name,
                        "published": entry.published,
                        "start_offset": entry.start_offset,
                        "end_offset": entry.end_offset,
                        "start_ts": entry.start_ts,
                        "end_ts": entry.end_ts,
                        "schema": entry.schema,
                        "system_schema": entry.system_schema,
                    }
                    for entry in self.sealed_logs
                ],
                "live_log": {
                    "name": live.name,
                    "published": live.published,
                    "start_offset": live.start_offset,
                    "start_ts": live.start_ts,
                    "schema": live.schema,
                    "system_schema": live.system_schema,
                },
                "manifest": self.manifest,
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, text: str) -> Metadata:
        fields = json.loads(text)
        version = fields.get("streamcast_metadata")
        if version not in READS:
            msg = (
                f"stream metadata version {version!r}; this build reads "
                f"{', '.join(map(str, sorted(READS)))}"
            )
            raise ValueError(msg)

        live = fields["live_log"]
        return cls(
            stream=fields["stream"],
            stream_id=fields["stream_id"],
            sealed_logs=tuple(
                Entry(
                    name=entry["name"],
                    start_offset=int(entry["start_offset"]),
                    end_offset=int(entry["end_offset"]),
                    schema=entry["schema"],
                    system_schema=entry["system_schema"],
                    published=entry.get("published"),
                    start_ts=entry.get("start_ts"),
                    end_ts=entry.get("end_ts"),
                )
                for entry in fields["sealed_logs"]
            ),
            live_log=Entry(
                name=live["name"],
                start_offset=int(live["start_offset"]),
                end_offset=None,
                schema=live["schema"],
                system_schema=live["system_schema"],
                published=live.get("published"),
                start_ts=live.get("start_ts"),
            ),
            manifest=fields.get("manifest"),
        )


def describe(
    name: str, start: int, end: int | None, schema: pa.Schema, *, published: str
) -> Entry:
    """An entry for a log whose table schema is `schema`: both halves of it."""
    return Entry(
        name,
        start,
        end,
        _schema.from_arrow(_log.declared(schema)),
        _schema.from_arrow(_log.system(schema)),
        published=published,
    )


def span(statistics: TierStatistics) -> tuple[int | None, int | None]:
    """The `streamcast_ts` range in `statistics`, or Nones if it has none.

    None for a log with no stamp column, and for one whose rows are all still
    in the buffer — which no file's statistics cover yet.
    """
    stamp = statistics.columns.get(_log.STAMP)
    if stamp is None or stamp.min is None or stamp.max is None:
        return None, None

    return int(stamp.min), int(stamp.max)


def single(stream: str, log: LogHandle) -> Metadata:
    """The metadata of a stream that is one log — `log`, live — with a new id.

    What every stream gets at its first `serve`, including one whose log
    predates metadata files: writing this is how it gains one, and nothing
    about the log changes.

    The start is the lowest offset the log holds, since litelink does not
    publish the one it was created at: exact for any log not evicted dry, and
    the end offset of one that holds nothing yet.
    """
    start = _log.lowest(log)
    live = describe(
        log.name,
        log.end_offset() if start is None else start,
        None,
        log.schema,
        published=log.published,
    )
    return Metadata(
        stream=stream,
        stream_id=str(uuid.uuid4()),
        sealed_logs=(),
        live_log=replace(live, start_ts=span(log.column_statistics())[0]),
    )


def complete(metadata: Metadata, log: LogHandle) -> Metadata:
    """`metadata` with what a version-1 file lacks, and the live log's start.

    `log` is the live log. A sealed log's published prefix is read from its
    own directory if it is on this disk, and is otherwise the live log's: a
    stream without an `s3://` location keeps every log here, and one with it
    publishes them all under the same prefix.
    """
    root = Path(log.root)

    def located(entry: Entry) -> Entry:
        if entry.published is not None:
            return entry

        if entry.name == log.name:
            return replace(entry, published=log.published)

        if (root / entry.name).is_dir():
            with litelink.open(root, entry.name, read_only=True) as sealed:
                return replace(entry, published=sealed.published)

        return replace(entry, published=log.published)

    live = located(metadata.live_log)
    if live.start_ts is None:
        live = replace(live, start_ts=span(log.column_statistics())[0])

    return replace(
        metadata,
        sealed_logs=tuple(located(entry) for entry in metadata.sealed_logs),
        live_log=live,
    )


def path(root: str | os.PathLike[str], stream: str) -> Path:
    """Where a stream's metadata lives locally: beside its logs."""
    return Path(root) / f"{stream}.metadata.json"


def load(root: str | os.PathLike[str], stream: str) -> Metadata | None:
    """The stream's local metadata, or None if it has none yet."""
    try:
        text = path(root, stream).read_text()
    except FileNotFoundError:
        return None

    return Metadata.from_json(text)


def save(root: str | os.PathLike[str], metadata: Metadata) -> None:
    """Write it atomically: a reader sees the old metadata or the new, never half.

    fsynced before the rename and the directory after it, because the rename
    is what commits a migration — a metadata file that survived the crash as an
    empty file would name no current log at all.
    """
    target = path(root, metadata.stream)
    staging = target.with_suffix(".json.tmp")
    with staging.open("w") as file:
        file.write(metadata.to_json())
        file.flush()
        os.fsync(file.fileno())

    staging.replace(target)
    directory = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def remote(published: str) -> bool:
    """Whether a published location is off this machine, and so gets a copy.

    Every log has a published table since litelink 0.6: an `s3://` prefix,
    or by default a local directory inside the log. A local one is on this
    disk already, beside the metadata file, so there is nothing to upload.
    """
    return not published.startswith("file://")


def default_published(log: LogHandle) -> bool:
    """Whether `log` publishes to litelink's local default, inside its own directory."""
    return (
        log.published.rstrip("/") == (Path(log.root) / log.name / "published").as_uri()
    )


def uri(stream: str, log: LogHandle) -> str:
    """Where a reader finds `stream`'s metadata: the copy beside its published
    tables if they are remote, else this file, as an absolute `file://` URI —
    absolute because a relative root means nothing on another machine, or in
    another working directory."""
    if remote(log.published):
        return _uri(log.published, stream)

    return path(log.root, stream).resolve().as_uri()


def _uri(archive: str, stream: str) -> str:
    return f"{archive.rstrip('/')}/{stream}.metadata.json"


def publish(metadata: Metadata, archive: str, s3: S3Options | None) -> None:
    """Copy the metadata to the archive, beside the logs' own prefixes.

    Raises rather than logging: a stream whose archive does not name its
    current log is one `Stream.restore` would rebuild as the wrong log.
    """
    uri = _uri(archive, metadata.stream)
    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    with filesystem.open_output_stream(key) as stream:
        stream.write(metadata.to_json().encode())


def sync(metadata: Metadata, archive: str, s3: S3Options | None) -> None:
    """Make the archive's copy match `metadata`, uploading only if it differs.

    Run at every `serve`, so an upload that failed is repaired by the next
    start rather than by whoever notices, and a stream that has not changed
    costs one GET. Raises on any failure but absence — see `fetch`.
    """
    if fetch(archive, metadata.stream, s3) != metadata:
        publish(metadata, archive, s3)


def ensure(stream: str, log: LogHandle, s3: S3Options | None) -> Metadata:
    """The stream's metadata, written if it is not there and synced to the archive.

    **`serve` calls this for every durable stream before it listens**, and a
    failure is a failure to start: a stream whose metadata cannot be written,
    or cannot reach its archive, is one nothing else can read, and finding
    that out at the first remote read is finding it out too late.

    `log` must be the stream's LIVE log. A handle to any other — a retired log
    passed as `Stream(log=…)` — is refused, because serving it would append to
    a log the metadata says is sealed.
    """
    root = log.root
    found = load(root, stream)
    if found is None:
        found = single(stream, log)
        save(root, found)

    elif found.live_log.name == log.name:
        # Filled in where a version-1 file, or a live log with no rows when
        # it was written, left them unknown — and rewritten only if so.
        completed = complete(found, log)
        if completed != found:
            save(root, completed)
            found = completed

    else:
        msg = (
            f"stream {stream!r} is being served from log {log.name!r}, and its "
            f"metadata at {path(root, stream)} says the live log is "
            f"{found.live_log.name!r}. A sealed log is never written to again; "
            f"open the live one, or use Stream.new, which does."
        )
        raise ValueError(msg)

    if remote(log.published):
        sync(found, log.published, s3)

    return found


def fetch(archive: str, stream: str, s3: S3Options | None) -> Metadata | None:
    """The archive's copy, or None if there is none there.

    Only a MISSING object is None. Anything else — bad credentials, an
    unreachable endpoint — raises, because treating it as "one log" would
    restore the stream's first log as though it were its current one.
    """
    uri = _uri(archive, stream)
    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    try:
        with filesystem.open_input_stream(key) as source:
            text = source.read().decode()
    except FileNotFoundError:
        # **pyarrow says "not found" for a missing BUCKET too**, in exactly
        # the words it uses for a missing key — measured against rustfs. A
        # mistyped bucket would otherwise read as "no copy yet", and a restore
        # would go on to rebuild the stream's first log as though it were live.
        bucket = key.split("/", 1)[0]
        if filesystem.get_file_info(bucket).type == pafs.FileType.NotFound:
            msg = f"the archive's bucket {bucket!r} does not exist ({archive})"
            raise FileNotFoundError(msg) from None

        return None

    return Metadata.from_json(text)


def check_types(history: tuple[Entry, ...], declared: dict[str, object]) -> None:
    """Refuse a column whose type differs from any type it has had before.

    **A column's type is fixed for the life of the stream.** A reader of the
    whole stream unions its logs with `UNION ALL BY NAME`, and a name that
    changed type does not fail there — measured in DuckDB, `int64` with
    `double` becomes DOUBLE and silently loses integers past 2**53, and
    `int64` with `string` becomes VARCHAR, a different column under the old
    name. Widening is refused too: one rule with no exceptions is one a
    reader never has to look up.

    Removed columns are in the history as well, so re-adding a name takes the
    type it had. Adding and removing are otherwise free. There is no rename
    at this layer: a new name is a removal and an addition, and a read across
    the seam returns both columns. See `Stream.migrate`.
    """
    wanted = _schema.to_arrow(declared)
    for entry in history:
        before = _schema.to_arrow(entry.schema)
        for field in wanted:
            index = before.get_field_index(field.name)
            if index < 0:
                continue

            had = before.field(index).type
            if had != field.type:
                msg = (
                    f"column {field.name!r} was {had} in log {entry.name!r} and "
                    f"is declared {field.type} here. A column's type is fixed for "
                    f"the life of a stream, including after it is removed: its "
                    f"logs are read together with UNION ALL BY NAME, where a "
                    f"changed type silently coerces rather than failing. Declare "
                    f"it as {had}, or give the new column a new name."
                )
                raise ValueError(msg)


__all__ = [
    "Entry",
    "Metadata",
    "check_types",
    "describe",
    "ensure",
    "fetch",
    "load",
    "path",
    "publish",
    "save",
    "single",
    "sync",
]
