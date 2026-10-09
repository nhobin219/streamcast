"""Where a stream's files live: a directory of its own, or the root it was made in.

A new stream lives in `root/<name>/`: its metadata, its manifest, and one
directory per log. A stream created before that layout keeps the one it had —
`root/<name>.metadata.json` beside `root/<name>/`, the first log's directory —
because an Iceberg table names its files by absolute path, so nothing can be
moved in place. The published side follows the same shape under its prefix.
"""

from __future__ import annotations

import shutil
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _log, _metadata

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


async def made(root, name: str = "t", **kwargs: object) -> None:
    """A stream with three rows and its metadata, then closed."""
    stream = streamcast.Stream.new(name, root=root, schema=SCHEMA, **kwargs)  # ty: ignore[invalid-argument-type]
    try:
        await stream.send_many([{"i": i} for i in range(3)])
        stream.ensure_metadata()
    finally:
        await stream.aclose()


async def legacy(root, name: str = "t", **kwargs: object) -> None:
    """A stream as an earlier version made it: its log directly in `root`."""
    log = litelink.new(
        root,
        name,
        schema=_log.with_system(streamcast.to_arrow(SCHEMA)),
        **kwargs,  # ty: ignore[invalid-argument-type]
    )
    stream = streamcast.Stream(name, log=log, owns_log=True)
    try:
        await stream.send_many([{"i": i} for i in range(3)])
        stream.ensure_metadata()
    finally:
        await stream.aclose()


class TestANewStream:
    async def test_it_lives_in_a_directory_of_its_own(self, tmp_path):
        await made(tmp_path)

        assert sorted(p.name for p in tmp_path.iterdir()) == ["t"]
        assert (tmp_path / "t" / "t.metadata.json").is_file()
        assert (tmp_path / "t" / "t" / "buffer.db").is_file(), "its first log"

    async def test_a_root_lists_one_entry_per_stream(self, tmp_path):
        for name in ("trades", "quotes", "orders"):
            await made(tmp_path, name)

        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "orders",
            "quotes",
            "trades",
        ]

    async def test_it_reopens_where_it_is(self, tmp_path):
        await made(tmp_path)
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            assert stream.end_offset == 4
            assert stream.metadata_uri == (
                (tmp_path / "t" / "t.metadata.json").resolve().as_uri()
            )
        finally:
            await stream.aclose()

    async def test_a_migration_adds_its_log_beside_the_first(self, tmp_path):
        await made(tmp_path)
        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=WIDER)
        await migrated.aclose()

        assert sorted(p.name for p in (tmp_path / "t").iterdir()) == [
            "t",
            "t-v2",
            "t.manifest.parquet",
            "t.metadata.json",
        ]


class TestAStreamFromBefore:
    async def test_it_keeps_its_layout_through_a_migration(self, tmp_path):
        await legacy(tmp_path)
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            assert stream.end_offset == 4, "found where it was"
        finally:
            await stream.aclose()

        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=WIDER)
        await migrated.aclose()

        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "t",
            "t-v2",
            "t.manifest.parquet",
            "t.metadata.json",
        ]

    async def test_a_log_without_its_metadata_is_found_too(self, tmp_path):
        """A stream never served has no metadata file yet: its log says where."""
        litelink.new(
            tmp_path, "flat", schema=_log.with_system(streamcast.to_arrow(SCHEMA))
        ).close()
        stream = streamcast.Stream.new("own", root=tmp_path, schema=SCHEMA)
        await stream.aclose()

        assert _metadata.home(tmp_path, "flat") == tmp_path
        assert _metadata.home(tmp_path, "own") == tmp_path / "own"
        assert _metadata.home(tmp_path, "new") == tmp_path / "new"


@pytest.mark.replication
class TestThePublishedSide:
    async def test_a_new_stream_publishes_under_a_prefix_of_its_own(
        self, tmp_path, s3, bucket
    ):
        await made(tmp_path / "a", published=bucket, s3_options=s3)
        stream = streamcast.Stream.new("t", root=tmp_path / "a", schema=SCHEMA)
        try:
            assert stream.metadata_uri == f"{bucket}/t/t.metadata.json"
        finally:
            await stream.aclose()

        assert _metadata.fetch(f"{bucket}/t", "t", s3) is not None
        assert _metadata.fetch(bucket, "t", s3) is None, "nothing at the prefix itself"

    async def test_restore_finds_it_and_rebuilds_it_in_its_layout(
        self, tmp_path, s3, bucket
    ):
        await made(tmp_path / "a", published=bucket, s3_options=s3)
        with litelink.open(tmp_path / "a" / "t", "t") as log:
            log.advance(flush=True)  # sealed, then published

        shutil.rmtree(tmp_path / "a")
        restored = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3
        )
        try:
            assert restored.log is not None
            assert restored.log.root == tmp_path / "b" / "t"
        finally:
            await restored.aclose()

    async def test_one_from_before_restores_as_it_was(self, tmp_path, s3, bucket):
        await legacy(tmp_path / "a", published=bucket, s3_options=s3)
        with litelink.open(tmp_path / "a", "t") as log:
            log.advance(flush=True)  # sealed, then published

        assert _metadata.fetch(bucket, "t", s3) is not None, "at the prefix itself"
        shutil.rmtree(tmp_path / "a")
        restored = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3
        )
        try:
            assert restored.log is not None
            assert restored.log.root == tmp_path / "b"
        finally:
            await restored.aclose()

    async def test_one_from_before_revives_as_it_was(self, tmp_path, s3, bucket):
        """A stream never changes layout: not even a revive on a new box,
        which makes a new log, moves it into the new one."""
        await legacy(tmp_path / "a", published=bucket, s3_options=s3)
        streamcast.Stream.retire("t", root=tmp_path / "a", s3_options=s3)
        shutil.rmtree(tmp_path / "a")

        revived = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3, revive=True
        )
        try:
            assert revived.log is not None
            assert (revived.log.root, revived.log.name) == (tmp_path / "b", "t-v2")
            assert revived.metadata_uri == f"{bucket}/t.metadata.json"
            assert _metadata.fetch(f"{bucket}/t", "t", s3) is None
        finally:
            await revived.aclose()

    async def test_a_new_one_revives_in_its_own_directory(self, tmp_path, s3, bucket):
        await made(tmp_path / "a", published=bucket, s3_options=s3)
        streamcast.Stream.retire("t", root=tmp_path / "a", s3_options=s3)
        shutil.rmtree(tmp_path / "a")

        revived = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3, revive=True
        )
        try:
            assert revived.log is not None
            assert (revived.log.root, revived.log.name) == (
                tmp_path / "b" / "t",
                "t-v2",
            )
            assert revived.metadata_uri == f"{bucket}/t/t.metadata.json"
        finally:
            await revived.aclose()
