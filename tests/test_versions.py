"""Versions of a stream's metadata, behind a hint, and the reads they save (#123).

A reader that opens a stream again — a live view, every few seconds — reads
only `version-hint.text`: every version it names, and the manifest that version
names, is written once under a name never reused, so it is read once per
process. The plain `<stream>.metadata.json` stays, for readers from before.
"""

from __future__ import annotations

import json
import shutil
from typing import Any

import pyarrow.parquet as pq
import pytest

import streamcast
from streamcast import _manifest, _metadata, _remote, _snapshot, _versions
from tests.conftest import current_json

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}
WIDER: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "note": {"type": ["string", "null"]}},
    "required": ["i"],
}


async def served(root, serve, **kwargs: object) -> None:
    """A stream with three rows, served once: `serve` writes its first version."""
    stream = streamcast.Stream.new("t", root=root, schema=SCHEMA, **kwargs)  # ty: ignore[invalid-argument-type]
    try:
        await stream.send_many([{"i": i} for i in range(3)])
        async with serve(stream, maintain=False):
            pass
    finally:
        await stream.aclose()


async def migrated(root) -> None:
    await streamcast.Stream.migrate("t", root=root, schema=WIDER).aclose()


def from_before(home, metadata: _metadata.Metadata, manifest) -> None:
    """`home` as a build from before the versions left it: plain files only."""
    (home / "t.metadata.json").write_text(metadata.to_json())
    pq.write_table(manifest, home / "t.manifest.parquet")
    shutil.rmtree(home / "t.metadata")


def versions(home) -> list[str]:
    return sorted(
        path.name
        for path in (home / "t.metadata").iterdir()
        if path.name != _versions.HINT and not path.name.startswith(".")
    )


class TestACommit:
    async def test_serve_writes_the_first_version_and_nothing_beside_it(
        self, tmp_path, serve
    ):
        await served(tmp_path, serve)
        home = tmp_path / "t"
        hint = (home / "t.metadata" / _versions.HINT).read_text()

        assert hint.startswith("00001-") and hint.endswith(".metadata.json")
        assert versions(home) == [hint]
        assert not (home / "t.metadata.json").exists(), "no plain copy (#124)"

    async def test_a_migration_is_a_version_naming_its_own_manifest(
        self, tmp_path, serve
    ):
        await served(tmp_path, serve)
        await migrated(tmp_path)
        home = tmp_path / "t"
        hint = (home / "t.metadata" / _versions.HINT).read_text()
        version = json.loads((home / "t.metadata" / hint).read_text())

        assert hint.startswith("00002-")
        assert version["manifest"] == hint.replace(
            ".metadata.json", ".manifest.parquet"
        )
        assert (home / "t.metadata" / version["manifest"]).is_file()
        assert not (home / "t.manifest.parquet").exists(), "no plain copy (#124)"

    async def test_the_last_versions_are_kept_and_no_more(self, tmp_path, serve):
        await served(tmp_path, serve)
        home = tmp_path / "t"
        current = _metadata.load(home, "t")
        assert current is not None
        for _ in range(_versions.KEEP + 3):
            _versions.commit(home, current)

        kept = [name for name in versions(home) if name.endswith(".metadata.json")]
        assert len(kept) == _versions.KEEP
        assert kept[-1].startswith(f"{_versions.KEEP + 4:05d}-"), "the newest"

    async def test_a_stream_from_before_gets_its_first_version_at_serve(
        self, tmp_path, serve
    ):
        """Its plain files are read, carried into its first version — the
        manifest too — and then deleted: left, they would show a reader that
        still opened them the stream as it was."""
        await served(tmp_path, serve)
        await migrated(tmp_path)
        home = tmp_path / "t"
        before = _metadata.load(home, "t")
        manifest = _manifest.load(home, "t")
        assert before is not None
        assert manifest is not None
        from_before(home, before, manifest)

        stream = streamcast.Stream.new("t", root=tmp_path, schema=WIDER)
        try:
            async with serve(stream, maintain=False):
                pass
        finally:
            await stream.aclose()

        assert (home / "t.metadata" / _versions.HINT).is_file()
        assert _metadata.load(home, "t") == before
        assert _manifest.load(home, "t") == manifest
        assert not (home / "t.metadata.json").exists()
        assert not (home / "t.manifest.parquet").exists()

    async def test_a_stream_never_versioned_is_still_read(self, tmp_path, serve):
        """By a reader that has only its plain files — the stream's, until a
        build with the versions serves it."""
        await served(tmp_path, serve)
        await migrated(tmp_path)
        home = tmp_path / "t"
        before = _metadata.load(home, "t")
        manifest = _manifest.load(home, "t")
        assert before is not None
        assert manifest is not None
        from_before(home, before, manifest)

        plain = (home / "t.metadata.json").resolve().as_uri()
        async with await streamcast.Stream.snapshot(plain) as snap:
            assert [entry.name for entry in snap.metadata.logs] == ["t", "t-v2"]
            assert snap._manifest == manifest, "the plain one, beside it"  # noqa: SLF001


class TestTheGreeting:
    async def test_it_names_the_hint(self, tmp_path, serve):
        await served(tmp_path, serve)
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    info = sub.info

                named = stream.metadata_uri
        finally:
            await stream.aclose()

        hint = (tmp_path / "t" / "t.metadata" / _versions.HINT).resolve().as_uri()
        assert info.metadata == hint
        assert named == hint

    async def test_a_plain_uri_is_read_through_the_hint_beside_it(
        self, tmp_path, serve
    ):
        """`<stream>.metadata.json`, as an older greeting named it or a reader
        wrote it down: the current version, never a stale file at that path."""
        await served(tmp_path, serve)
        await migrated(tmp_path)
        home = tmp_path / "t"
        stale = _metadata.Metadata.from_json(
            json.dumps({**current_json(home, "t"), "sealed_logs": []})
        )
        (home / "t.metadata.json").write_text(stale.to_json())

        plain = (home / "t.metadata.json").resolve().as_uri()
        async with await streamcast.Stream.snapshot(plain) as snap:
            assert [entry.name for entry in snap.metadata.logs] == ["t", "t-v2"]
            assert snap._manifest is not None, "the version's, in its directory"  # noqa: SLF001


class Counting:
    """A filesystem that counts the objects read through it."""

    def __init__(self, filesystem, reads: list[str]) -> None:
        self._filesystem = filesystem
        self._reads = reads

    def open_input_stream(self, key):  # noqa: ANN001, ANN201
        self._reads.append(key)
        return self._filesystem.open_input_stream(key)

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(self._filesystem, name)


@pytest.mark.replication
class TestTheReadsItSaves:
    async def _opened(self, uri, s3, reads: list[str]) -> list[str]:
        reads.clear()
        async with await streamcast.Stream.snapshot(uri, s3_options=s3):
            pass

        # The stream's own files: its metadata directory and the plain copies.
        # Each log's table has an Iceberg hint of its own, read to find it.
        return sorted(
            key.rsplit("/", 1)[-1]
            for key in reads
            if "/t.metadata/" in key
            or key.endswith(("/t.metadata.json", "/t.manifest.parquet"))
        )

    async def test_a_second_open_reads_only_the_hint(
        self, tmp_path, serve, s3, bucket, monkeypatch
    ):
        await served(tmp_path, serve, published=bucket, s3_options=s3)
        await migrated(tmp_path)
        hint = f"{bucket}/t/t.metadata/{_versions.HINT}"
        _snapshot._cache.clear()  # noqa: SLF001
        reads: list[str] = []
        real = _remote._filesystem  # noqa: SLF001
        monkeypatch.setattr(
            _remote,
            "_filesystem",
            lambda uri, s3_options: (
                Counting(real(uri, s3_options)[0], reads),
                real(uri, s3_options)[1],
            ),
        )

        first = await self._opened(hint, s3, reads)
        assert _versions.HINT in first
        assert any(name.endswith(".metadata.json") for name in first)
        assert any(name.endswith(".manifest.parquet") for name in first)

        assert await self._opened(hint, s3, reads) == [_versions.HINT]

        # A plain URI, as an older greeting named it: through the hint too.
        plain = f"{bucket}/t/t.metadata.json"
        assert await self._opened(plain, s3, reads) == [_versions.HINT]

    async def test_a_new_version_is_read_when_the_hint_names_it(
        self, tmp_path, serve, s3, bucket
    ):
        await served(tmp_path, serve, published=bucket, s3_options=s3)
        hint = f"{bucket}/t/t.metadata/{_versions.HINT}"
        async with await streamcast.Stream.snapshot(hint, s3_options=s3) as snap:
            assert [entry.name for entry in snap.metadata.logs] == ["t"]

        await migrated(tmp_path)
        async with await streamcast.Stream.snapshot(hint, s3_options=s3) as snap:
            assert [entry.name for entry in snap.metadata.logs] == ["t", "t-v2"]

    async def test_other_credentials_are_not_served_what_these_read(
        self, tmp_path, serve, s3, bucket
    ):
        """Keyed by the URI alone, the cache would hand a reader with wrong
        keys what one with the right keys fetched (litelink#185). An open reads
        the hint uncached first, which fails for them anyway; this asks the
        cache directly, for the version it names."""
        await served(tmp_path, serve, published=bucket, s3_options=s3)
        hint = f"{bucket}/t/t.metadata/{_versions.HINT}"
        version = _snapshot._resolve(hint, s3)  # noqa: SLF001
        assert _snapshot._read(version, s3)  # noqa: SLF001 — cached now

        wrong = streamcast.S3Options(
            endpoint=s3.endpoint,
            region=s3.region,
            access_key="not-a-key",
            secret_key="not-a-secret",
        )
        with pytest.raises(OSError):  # noqa: PT011 — whatever S3 says
            _snapshot._read(version, wrong)  # noqa: SLF001


class TestTheCache:
    def test_only_versioned_files_are_immutable(self):
        base = "s3://b/p/t/t.metadata"
        assert _versions.immutable(f"{base}/00003-{'a' * 32}.metadata.json")
        assert _versions.immutable(f"{base}/00003-{'a' * 32}.manifest.parquet")
        assert not _versions.immutable(f"{base}/{_versions.HINT}")
        assert not _versions.immutable("s3://b/p/t/t.metadata.json")

    def test_it_is_bounded_least_recently_used_first(self, monkeypatch):
        monkeypatch.setattr(_snapshot, "CACHE_BYTES", 100)
        _snapshot._cache.clear()  # noqa: SLF001
        monkeypatch.setattr(_snapshot, "_cached_bytes", 0)
        who = _snapshot._identity(None)  # noqa: SLF001
        for name in "abcd":
            _snapshot._remember((name, who), b"x" * 25)  # noqa: SLF001

        _snapshot._remember(("e", who), b"x" * 25)  # noqa: SLF001
        assert [uri for uri, _ in _snapshot._cache] == ["b", "c", "d", "e"]  # noqa: SLF001
        _snapshot._cache.clear()  # noqa: SLF001
