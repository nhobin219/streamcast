"""Versions of a stream's metadata, behind a hint: what lets a reader cache it.

    <stream>.metadata/
        version-hint.text                  "00004-9f2c….metadata.json"
        00004-9f2c….metadata.json          immutable: written once, never again
        00004-9f2c….manifest.parquet       immutable, named by the version that wrote it
        00003-51ab….metadata.json          the versions before, kept for a reader mid-open
    <stream>.metadata.json                 a plain copy of the current version (#124)
    <stream>.manifest.parquet              a plain copy of the current manifest

**Iceberg's own indirection, one level up** (#123). A versioned file's name is
never reused, so a reader that has read it once never needs to again: the only
read an open repeats is the hint, a few bytes. litelink caches its Iceberg
metadata by name for the same reason (litelink#185). The hint is the one file
rewritten in place, so it is the one never cached.

**Written in the order a reader needs**: the manifest a version names, then
the version, then the hint, so a hint never names a file that is not there
yet. The plain copies come last, for readers from before (#124 removes them).
The same layout sits in the stream's home and, when it publishes to S3, under
its published prefix, with the same file names in both.

**A version is a commit, and commits are rare**: a migration, a retirement, a
revival, a restore, or a `serve` that fills in what an older file left out.
So listing the directory to keep the last `KEEP` versions is cheap.
"""

from __future__ import annotations

import dataclasses
import io
import os
import re
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow.fs as pafs
import pyarrow.parquet as pq

from streamcast import _manifest, _metadata, _remote

if TYPE_CHECKING:
    import pyarrow as pa
    from litelink import S3Options

HINT: Final = "version-hint.text"
"""The one file in a stream's metadata directory that is rewritten."""

KEEP: Final = 10
"""Versions kept: the current one and the nine before, for a reader that read
the hint just before a commit and opens the version it named just after."""

_VERSION = re.compile(r"^(\d{5,})-[0-9a-f]{32}\.(metadata\.json|manifest\.parquet)$")


def directory(stream: str) -> str:
    """The metadata directory's name, beside the logs. Never a log's name:
    a log is named `<stream>` or `<stream>-vN`."""
    return f"{stream}.metadata"


def hint_uri(
    home: str | os.PathLike[str] | str, stream: str, published: str | None
) -> str:
    """Where a reader finds the hint: beside the published tables when they are
    remote, else in the stream's home, as an absolute `file://` URI."""
    if published is not None and _metadata.remote(published):
        return f"{published.rstrip('/')}/{directory(stream)}/{HINT}"

    return (Path(home) / directory(stream) / HINT).resolve().as_uri()


def immutable(uri: str) -> bool:
    """Whether `uri` names a versioned file, whose content never changes."""
    parent, _, name = uri.rpartition("/")
    return parent.endswith(".metadata") and _VERSION.match(name) is not None


# -- where the files go ----------------------------------------------------------


class _Store(Protocol):
    """A metadata directory and the home or prefix it sits in."""

    def read(self, name: str) -> bytes | None: ...

    def write(self, name: str, data: bytes) -> None: ...

    def names(self) -> list[str]: ...

    def delete(self, name: str) -> None: ...


class _Local:
    def __init__(self, home: Path, stream: str) -> None:
        self.top = home
        self.dir = home / directory(stream)

    def _path(self, name: str) -> Path:
        # The versions and the hint in the metadata directory; the plain
        # copies beside it, where readers from before look.
        versioned = _VERSION.match(name) is not None or name == HINT
        return (self.dir if versioned else self.top) / name

    def read(self, name: str) -> bytes | None:
        try:
            return self._path(name).read_bytes()
        except FileNotFoundError:
            return None

    def write(self, name: str, data: bytes) -> None:
        # Atomically, and durably: a hint that survived a crash as an empty
        # file would name no version at all.
        target = self._path(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = target.with_name(target.name + ".tmp")
        with staging.open("wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

        staging.replace(target)
        descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def names(self) -> list[str]:
        if not self.dir.is_dir():
            return []

        return [path.name for path in self.dir.iterdir() if path.is_file()]

    def delete(self, name: str) -> None:
        (self.dir / name).unlink(missing_ok=True)


class _Remote:
    def __init__(
        self, published: str, stream: str, s3_options: S3Options | None
    ) -> None:
        self.top = published.rstrip("/")
        self.dir = f"{self.top}/{directory(stream)}"
        self._s3 = s3_options

    def _uri(self, name: str) -> str:
        versioned = _VERSION.match(name) is not None or name == HINT
        return f"{self.dir if versioned else self.top}/{name}"

    def read(self, name: str) -> bytes | None:
        filesystem, key = _remote._filesystem(self._uri(name), self._s3)  # noqa: SLF001
        try:
            with filesystem.open_input_stream(key) as source:
                return source.read()
        except FileNotFoundError:
            return None

    def write(self, name: str, data: bytes) -> None:
        filesystem, key = _remote._filesystem(self._uri(name), self._s3)  # noqa: SLF001
        with filesystem.open_output_stream(key) as sink:
            sink.write(data)

    def names(self) -> list[str]:
        filesystem, key = _remote._filesystem(self.dir, self._s3)  # noqa: SLF001
        listed = filesystem.get_file_info(pafs.FileSelector(key, allow_not_found=True))
        return [info.base_name for info in listed if info.type == pafs.FileType.File]

    def delete(self, name: str) -> None:
        filesystem, key = _remote._filesystem(self._uri(name), self._s3)  # noqa: SLF001
        try:
            filesystem.delete_file(key)
        except FileNotFoundError:
            pass


# -- a commit --------------------------------------------------------------------


def current(store: _Store) -> tuple[int, str] | None:
    """The current version's number and file name, from the hint; None if none."""
    hint = store.read(HINT)
    if hint is None:
        return None

    name = hint.decode().strip()
    found = _VERSION.match(name)
    if found is None:  # pragma: no cover — only this module writes the hint
        msg = f"the metadata hint names {name!r}, which is not a version"
        raise ValueError(msg)

    return int(found.group(1)), name


def commit(
    home: str | os.PathLike[str],
    metadata: _metadata.Metadata,
    *,
    manifest: pa.Table | None = None,
    published: str | None = None,
    s3_options: S3Options | None = None,
    local: bool = True,
) -> None:
    """Write `metadata` as the stream's next version: in its home unless
    `local` is false, and under `published` when that is remote.

    `manifest` is a new manifest to go with it. Without one, the version names
    the manifest the current version does — carried over, since a manifest
    changes only with a sealed log.
    """
    stores: list[_Store] = []
    if local:
        stores.append(_Local(Path(home), metadata.stream))

    if published is not None and _metadata.remote(published):
        stores.append(_Remote(published, metadata.stream, s3_options))

    if not stores:
        return

    # One number for every store, so a version has one name wherever it is.
    latest = [found[0] for found in map(current, stores) if found is not None]
    tag = f"{max(latest, default=0) + 1:05d}-{uuid.uuid4().hex}"
    raw = None if manifest is None else _parquet(manifest)
    for store in stores:
        _write(store, metadata, tag, raw)


def _write(
    store: _Store, metadata: _metadata.Metadata, tag: str, manifest: bytes | None
) -> None:
    versioned = f"{tag}.metadata.json"
    if manifest is not None:
        named: str | None = f"{tag}.manifest.parquet"
        store.write(named, manifest)
    else:
        named = _carried(store, metadata, tag)

    # 1. the version, naming its manifest by the name it has beside it
    store.write(
        versioned, dataclasses.replace(metadata, manifest=named).to_json().encode()
    )
    # 2. the hint: from here on, readers open this version
    store.write(HINT, versioned.encode())
    # 3. the plain copies, for readers from before the hint (#124)
    if manifest is not None:
        store.write(_manifest.name(metadata.stream), manifest)

    store.write(f"{metadata.stream}.metadata.json", metadata.to_json().encode())
    _prune(store)


def _carried(store: _Store, metadata: _metadata.Metadata, tag: str) -> str | None:
    """The manifest this version names when it brings no new one.

    The current version's, if there is one. A stream versioned for the first
    time has only the plain copy: it is copied in under this version's name,
    so the version never names a file that can change. None if there is no
    manifest anywhere — a reader prunes nothing then, as before.
    """
    if metadata.manifest is None:
        return None

    found = current(store)
    if found is not None:
        previous = store.read(found[1])
        if previous is not None:
            named = _metadata.Metadata.from_json(previous.decode()).manifest
            if named is not None and _VERSION.match(named):
                return named

    plain = store.read(_manifest.name(metadata.stream))
    if plain is None:
        return None

    named = f"{tag}.manifest.parquet"
    store.write(named, plain)
    return named


def _prune(store: _Store) -> None:
    """Keep the newest `KEEP` versions and the manifests they name."""
    names = store.names()
    versions = sorted(
        (
            name
            for name in names
            if name.endswith(".metadata.json") and _VERSION.match(name)
        ),
        reverse=True,
    )
    kept = versions[:KEEP]
    named: set[str] = set()
    for name in kept:
        data = store.read(name)
        if data is not None:
            manifest = _metadata.Metadata.from_json(data.decode()).manifest
            if manifest is not None:
                named.add(manifest)

    for name in names:
        if not _VERSION.match(name):
            continue

        if name.endswith(".metadata.json") and name not in kept:
            store.delete(name)
        elif name.endswith(".manifest.parquet") and name not in named:
            store.delete(name)


def _parquet(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def missing(
    home: str | os.PathLike[str],
    stream: str,
    *,
    published: str | None = None,
    s3_options: S3Options | None = None,
) -> tuple[bool, bool]:
    """Whether the stream has no version yet: `(locally, published)`."""
    local = current(_Local(Path(home), stream)) is None
    remote = (
        published is not None
        and _metadata.remote(published)
        and current(_Remote(published, stream, s3_options)) is None
    )
    return local, remote


__all__ = [
    "HINT",
    "KEEP",
    "commit",
    "current",
    "directory",
    "hint_uri",
    "immutable",
    "missing",
]
