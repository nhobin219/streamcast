"""The logs a stream is made of, in order, and which one is being written.

A stream is one litelink log until it is migrated. `Stream.migrate` seals the
current log for good, creates the next one with the new schema starting at
exactly the offset the last one ended at, and records both here — so a stream
becomes a SEQUENCE of logs whose offsets are one dense, monotonic space.

    root/trades.metadata.json
    {"streamcast_metadata": 1, "stream": "trades",
     "logs": [{"name": "trades",    "start_offset": 1,    "end_offset": 1001,
               "schema": {...}, "system_schema": {...}},
              {"name": "trades-v2", "start_offset": 1001, "end_offset": null,
               "schema": {...}, "system_schema": {...}}]}

**Named after Iceberg's `metadata.json`**, which plays the same part for a
table: the JSON that says what it currently is. "Manifest" is Iceberg's word
for the per-file statistics, and those are #27's, in SQLite.

**No metadata file means one log**, named for the stream. That is every stream
created before migration existed, and it keeps working without a file being
written for it: the metadata appears at the first migration, and not before.

**Beside the logs, not inside one.** `root/trades` IS the first log's
directory, so a file in it would be a file inside a litelink log. And a copy
goes to `<archive>/trades.metadata.json`, because `Stream.restore` and a
remote reader have the archive and not this disk.

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
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from streamcast import _log, _remote, _schema

if TYPE_CHECKING:
    import pyarrow as pa
    from litelink import S3Options

VERSION: Final = 1


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


@dataclass(frozen=True, slots=True)
class Metadata:
    """A stream's logs, oldest first. The last is the one being written."""

    stream: str
    logs: tuple[Entry, ...]

    @property
    def current(self) -> Entry:
        return self.logs[-1]

    @property
    def retired(self) -> tuple[Entry, ...]:
        return self.logs[:-1]

    def next_name(self) -> str:
        """`trades-v2`, `trades-v3`, … — named for its place in the sequence.

        A `.` cannot be the separator: litelink hands the name to Iceberg,
        which reads a dot as a namespace boundary and refuses the table.
        """
        return f"{self.stream}-v{len(self.logs) + 1}"

    def to_json(self) -> str:
        return json.dumps(
            {
                "streamcast_metadata": VERSION,
                "stream": self.stream,
                "logs": [
                    {
                        "name": entry.name,
                        "start_offset": entry.start_offset,
                        "end_offset": entry.end_offset,
                        "schema": entry.schema,
                        "system_schema": entry.system_schema,
                    }
                    for entry in self.logs
                ],
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, text: str) -> Metadata:
        fields = json.loads(text)
        version = fields.get("streamcast_metadata")
        if version != VERSION:
            msg = f"stream metadata version {version!r}; this build reads {VERSION}"
            raise ValueError(msg)

        return cls(
            stream=fields["stream"],
            logs=tuple(
                Entry(
                    name=entry["name"],
                    start_offset=int(entry["start_offset"]),
                    end_offset=(
                        None
                        if entry["end_offset"] is None
                        else int(entry["end_offset"])
                    ),
                    schema=entry["schema"],
                    system_schema=entry["system_schema"],
                )
                for entry in fields["logs"]
            ),
        )


def describe(name: str, start: int, end: int | None, schema: pa.Schema) -> Entry:
    """An entry for a log whose table schema is `schema`: both halves of it."""
    return Entry(
        name,
        start,
        end,
        _schema.from_arrow(_log.declared(schema)),
        _schema.from_arrow(_log.system(schema)),
    )


def path(root: str | os.PathLike[str], stream: str) -> Path:
    """Where a stream's metadata lives locally: beside its logs."""
    return Path(root) / f"{stream}.metadata.json"


def load(root: str | os.PathLike[str], stream: str) -> Metadata | None:
    """The stream's metadata, or None for a stream that has never migrated."""
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


def fetch(archive: str, stream: str, s3: S3Options | None) -> Metadata | None:
    """The archive's copy, or None if this stream has never migrated.

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
    "fetch",
    "load",
    "path",
    "publish",
    "save",
]
