"""Versions of a stream's metadata, behind a hint: what lets a reader cache it.

    <stream>.metadata/
        version-hint.text                  "00004-9f2c….metadata.json"
        00004-9f2c….metadata.json          immutable: written once, never again
        00004-9f2c….manifest.parquet       immutable, named by the version that wrote it
        00003-51ab….metadata.json          the versions before, kept for a reader mid-open

**Iceberg's own indirection, one level up** (#123). A versioned file's name is
never reused, so a reader that has read it once never needs to again: the only
read an open repeats is the hint, a few bytes. litelink caches its Iceberg
metadata by name for the same reason (litelink#185). The hint is the one file
rewritten in place, so it is the one never cached.

**Written in the order a reader needs**: the manifest a version names, then
the version, then the hint, so a hint never names a file that is not there
yet. The same layout sits in the stream's home and, when it publishes to S3, under
its published prefix, with the same file names in both.

**A version is a commit, and commits are rare**: a migration, a retirement, a
revival, a restore, or a `serve` that fills in what an older file left out.
So listing the directory to keep the last `KEEP` versions is cheap.

**A stream from before the versions** has only `<stream>.metadata.json` and
`<stream>.manifest.parquet` beside its logs. They are read until its first
version is written, and that commit deletes them (#124): left behind, they
would answer a reader that still opened them with a stream as it was then.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import io
import os
import re
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from streamcast import _manifest, _metadata, _remote

if TYPE_CHECKING:
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
        self._path(name).unlink(missing_ok=True)


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


class Conflict(RuntimeError):
    """The stream's metadata changed since the caller read it: read it again."""


UNCHECKED: Final = object()
"""`commit`'s `expected` when the caller checks nothing."""


def load(
    home: str | os.PathLike[str], stream: str
) -> tuple[str | None, _metadata.Metadata | None, pa.Table | None]:
    """The current version's name, its metadata and the manifest it names,
    read from the versioned files — one consistent state.

    **Not the plain copies**, which are written after the hint: a reader
    between the two would pair the new version's name with the old version's
    content, and a compare-and-swap on that name would let a stale commit
    through. The metadata's `manifest` is given back as the plain name, which
    is what a commit expects to be handed. A stream with no version yet is
    read from its plain files, under the name None.
    """
    store = _Local(Path(home), stream)
    found = current(store)
    if found is None:
        return None, _metadata.plain(home, stream), _manifest.plain(home, stream)

    raw = store.read(found[1])
    if raw is None:  # pragma: no cover — a hint is written after its version
        msg = f"the hint names {found[1]}, which is not there"
        raise FileNotFoundError(msg)

    metadata = _metadata.Metadata.from_json(raw.decode())
    manifest = None
    if metadata.manifest is not None:
        data = store.read(metadata.manifest)
        manifest = None if data is None else pq.read_table(pa.BufferReader(data))
        metadata = dataclasses.replace(metadata, manifest=_manifest.name(stream))

    return found[1], metadata, manifest


def fetch(
    published: str, stream: str, s3_options: S3Options | None = None
) -> _metadata.Metadata | None:
    """The current version under a remote `published` prefix, as `load` reads
    the home's: through its hint, with `manifest` given back as the plain name.
    A stream from before the versions is read from its plain copy there.

    Only a MISSING stream is None. Anything else — bad credentials, an
    unreachable endpoint — raises, because treating it as "one log" would
    restore the stream's first log as though it were its current one.
    """
    store = _Remote(published, stream, s3_options)
    found = current(store)
    if found is None:
        raw = store.read(_metadata.plain_name(stream))
    else:
        raw = store.read(found[1])
        if raw is None:  # pragma: no cover — a hint is written after its version
            msg = f"the hint under {published} names {found[1]}, which is not there"
            raise FileNotFoundError(msg)

    if raw is None:
        # **pyarrow says "not found" for a missing BUCKET too**, in exactly
        # the words it uses for a missing key — measured against rustfs. A
        # mistyped bucket would otherwise read as "no copy yet", and a restore
        # would go on to rebuild the stream's first log as though it were live.
        filesystem, key = _remote._filesystem(store.dir, s3_options)  # noqa: SLF001
        bucket = key.split("/", 1)[0]
        if filesystem.get_file_info(bucket).type == pafs.FileType.NotFound:
            msg = f"the published location's bucket {bucket!r} does not exist ({published})"
            raise FileNotFoundError(msg)

        return None

    metadata = _metadata.Metadata.from_json(raw.decode())
    if metadata.manifest is not None:
        metadata = dataclasses.replace(metadata, manifest=_manifest.name(stream))

    return metadata


def manifest_uri(uri: str, manifest: str) -> str:
    """Where the manifest a version names is, for metadata opened at `uri`.

    Beside the version, in the metadata directory, when it is a versioned
    one — which is beside the hint, and not beside a `<stream>.metadata.json`
    URI read through the hint (`beside`). A plain one, a stream's from before
    the versions, is beside its plain metadata.
    """
    parent = uri.rpartition("/")[0]
    if _VERSION.match(manifest) and (hint := beside(uri)) is not None:
        parent = hint.rpartition("/")[0]

    return f"{parent}/{manifest}"


def beside(uri: str) -> str | None:
    """The hint beside a `<stream>.metadata.json` URI — where a stream from
    before the versions kept its metadata, and where a reader may still point
    — or None if `uri` is not one."""
    parent, _, name = uri.rpartition("/")
    if not name.endswith(".metadata.json") or _VERSION.match(name):
        return None

    return f"{parent}/{directory(name.removesuffix('.metadata.json'))}/{HINT}"


def version(home: str | os.PathLike[str], stream: str) -> str | None:
    """The name of the stream's current version in its home, or None."""
    found = current(_Local(Path(home), stream))
    return None if found is None else found[1]


@contextlib.contextmanager
def _locked(home: Path, stream: str):  # noqa: ANN202
    """Every local commit of a stream's metadata, one at a time on this box.

    A commit reads the current version and writes the next; two at once —
    a retention pass and a `Stream.retain`, say — would each build on what
    the other is replacing. An `flock`, held for the write, beside the hint.
    """
    directory_ = home / directory(stream)
    directory_.mkdir(parents=True, exist_ok=True)
    with (directory_ / ".lock").open("a") as file:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def commit(
    home: str | os.PathLike[str],
    metadata: _metadata.Metadata,
    *,
    manifest: pa.Table | None = None,
    published: str | None = None,
    s3_options: S3Options | None = None,
    local: bool = True,
    expected: object = UNCHECKED,
    published_first: bool = False,
) -> str | None:
    """Write `metadata` as the stream's next version: in its home unless
    `local` is false, and under `published` when that is remote.

    `manifest` is a new manifest to go with it. Without one, the version names
    the manifest the current version does — carried over, since a manifest
    changes only with a sealed log.

    **`expected` makes it a compare-and-swap**: the version name (`version`)
    the caller read `metadata` from, or None for none. If the home's current
    version is another by the time the lock is held, `Conflict` is raised and
    nothing is written — the caller's `metadata` was built on a version that
    is no longer current, and writing it would undo whatever replaced it.

    **`published_first` writes the published copy before the home's**, so a
    version in the home is one readers elsewhere were already shown: the home's
    write follows only once the remote one has succeeded. Retention needs that,
    because its grace is counted from the home's version (`_retention`).
    """
    if not local:
        return _commit(home, metadata, manifest, published, s3_options, local=False)

    with _locked(Path(home), metadata.stream):
        if expected is not UNCHECKED:
            found = version(home, metadata.stream)
            if found != expected:
                msg = (
                    f"stream {metadata.stream!r}'s metadata changed since it was "
                    f"read ({expected} is now {found}); read it again"
                )
                raise Conflict(msg)

        return _commit(
            home,
            metadata,
            manifest,
            published,
            s3_options,
            local=True,
            published_first=published_first,
        )


def _commit(
    home: str | os.PathLike[str],
    metadata: _metadata.Metadata,
    manifest: pa.Table | None,
    published: str | None,
    s3_options: S3Options | None,
    *,
    local: bool,
    published_first: bool = False,
) -> str | None:
    stores: list[_Store] = []
    if local:
        stores.append(_Local(Path(home), metadata.stream))

    if published is not None and _metadata.remote(published):
        stores.append(_Remote(published, metadata.stream, s3_options))

    if not stores:
        return None

    # One number for every store, so a version has one name wherever it is.
    latest = [found[0] for found in map(current, stores) if found is not None]
    tag = f"{max(latest, default=0) + 1:05d}-{uuid.uuid4().hex}"
    raw = None if manifest is None else _parquet(manifest)
    for store in reversed(stores) if published_first else stores:
        _write(store, metadata, tag, raw)

    return f"{tag}.metadata.json"


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
    # 3. the plain files a stream from before the versions had, if this is
    # its first: nothing reads them once there is a hint, and a reader that
    # still opened them would get the stream as it was. Best-effort, after
    # the commit has landed: one left behind is stale, not wrong for anyone
    # who reads the hint.
    for plain in (
        _metadata.plain_name(metadata.stream),
        _manifest.name(metadata.stream),
    ):
        with contextlib.suppress(OSError):
            store.delete(plain)

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
    "UNCHECKED",
    "Conflict",
    "beside",
    "commit",
    "load",
    "current",
    "directory",
    "fetch",
    "hint_uri",
    "immutable",
    "manifest_uri",
    "missing",
    "version",
]
