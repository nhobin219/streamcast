"""The logs a stream is made of, in order, and which one is being written.

A stream is one litelink log until it is migrated. `Stream.migrate` seals the
current log for good, creates the next one with the new schema starting at
exactly the offset the last one ended at, and records both here — so a stream
becomes a SEQUENCE of logs whose offsets are one dense, monotonic space.

    root/trades.manifest.json
    {"streamcast_manifest": 1, "stream": "trades",
     "logs": [{"name": "trades",    "start_offset": 1,    "end_offset": 1001,
               "schema": {...}},
              {"name": "trades-v2", "start_offset": 1001, "end_offset": null,
               "schema": {...}}]}

**No manifest means one log**, named for the stream. That is every stream
created before migration existed, and it keeps working without a file being
written for it: the manifest appears at the first migration, and not before.

**Beside the logs, not inside one.** `root/trades` IS the first log's
directory, so a file in it would be a file inside a litelink log. And a copy
goes to `<archive>/trades.manifest.json`, because `Stream.restore` and a
remote reader have the archive and not this disk.

**Each entry records the schema its log was created with**, which is the
history the type rule is checked against: a column's type is fixed for the
life of the stream, including across its removal and re-addition, so the check
needs every schema the stream has had rather than only the current one.

**Written last, by atomic rename.** A migration that dies before this leaves
a new log directory the manifest does not name — an orphan that the next
`migrate` adopts if it is empty and has the requested shape, and refuses
otherwise. It never leaves a manifest naming a log that does not exist.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from streamcast import _remote, _schema

if TYPE_CHECKING:
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
    them. Without `streamcast_ts`, which is owned rather than declared."""


@dataclass(frozen=True, slots=True)
class Manifest:
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
                "streamcast_manifest": VERSION,
                "stream": self.stream,
                "logs": [
                    {
                        "name": entry.name,
                        "start_offset": entry.start_offset,
                        "end_offset": entry.end_offset,
                        "schema": entry.schema,
                    }
                    for entry in self.logs
                ],
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, text: str) -> Manifest:
        fields = json.loads(text)
        version = fields.get("streamcast_manifest")
        if version != VERSION:
            msg = f"stream manifest version {version!r}; this build reads {VERSION}"
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
                )
                for entry in fields["logs"]
            ),
        )


def path(root: str | os.PathLike[str], stream: str) -> Path:
    """Where a stream's manifest lives locally: beside its logs."""
    return Path(root) / f"{stream}.manifest.json"


def load(root: str | os.PathLike[str], stream: str) -> Manifest | None:
    """The stream's manifest, or None for a stream that has never migrated."""
    try:
        text = path(root, stream).read_text()
    except FileNotFoundError:
        return None

    return Manifest.from_json(text)


def save(root: str | os.PathLike[str], manifest: Manifest) -> None:
    """Write it atomically: a reader sees the old manifest or the new, never half.

    fsynced before the rename and the directory after it, because the rename
    is what commits a migration — a manifest that survived the crash as an
    empty file would name no current log at all.
    """
    target = path(root, manifest.stream)
    staging = target.with_suffix(".json.tmp")
    with staging.open("w") as file:
        file.write(manifest.to_json())
        file.flush()
        os.fsync(file.fileno())

    staging.replace(target)
    directory = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _uri(archive: str, stream: str) -> str:
    return f"{archive.rstrip('/')}/{stream}.manifest.json"


def publish(manifest: Manifest, archive: str, s3: S3Options | None) -> None:
    """Copy the manifest to the archive, beside the logs' own prefixes.

    Raises rather than logging: a stream whose archive does not name its
    current log is one `Stream.restore` would rebuild as the wrong log.
    """
    uri = _uri(archive, manifest.stream)
    filesystem, key = _remote._filesystem(uri, s3)  # noqa: SLF001
    with filesystem.open_output_stream(key) as stream:
        stream.write(manifest.to_json().encode())


def fetch(archive: str, stream: str, s3: S3Options | None) -> Manifest | None:
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

    return Manifest.from_json(text)


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
    type it had. Adding and removing are otherwise free. **A rename cannot be
    detected** — it arrives as a removal and an addition — and is the one
    change this cannot stop; see `Stream.migrate`.
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
    "Manifest",
    "check_types",
    "fetch",
    "load",
    "path",
    "publish",
    "save",
]
