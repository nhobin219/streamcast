"""A restored log's shape: the schema and `sort_by` the stream recorded, exactly.

litelink 0.10.1 checks a restore's schema and `sort_by` against the replica or
the published table, and needs them outright for a table no 0.10 publish
stamped — from which an Iceberg schema alone cannot rebuild the log: it keeps
no Arrow field metadata, where a binary column's wire encoding lives. So the
stream's metadata records both, and `Stream.restore` passes them on.
"""

from __future__ import annotations

import json
import shutil
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _metadata

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "trace": {"type": "string", "contentEncoding": "base16", "format": "bytes16"},
        "price": {"type": ["number", "null"]},
    },
    "required": ["event_ts", "trace"],
}
SORT_BY = ("event_ts",)


def row(i: int) -> dict[str, object]:
    return {"event_ts": 1_790_000_000_000_000 + i, "trace": bytes(16), "price": 1.5}


class TestTheMetadataRecordsTheSort:
    async def test_a_new_stream_records_it(self, tmp_path):
        stream = streamcast.Stream.new(
            "t", root=tmp_path, schema=SCHEMA, sort_by=SORT_BY
        )
        try:
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        metadata = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert metadata is not None
        assert metadata.live_log.sort_by == SORT_BY
        assert json.loads(
            _metadata.path(_metadata.home(tmp_path, "t"), "t").read_text()
        )["live_log"]["sort_by"] == list(SORT_BY)

    async def test_a_file_from_before_it_gets_it_at_the_next_serve(self, tmp_path):
        stream = streamcast.Stream.new(
            "t", root=tmp_path, schema=SCHEMA, sort_by=SORT_BY
        )
        try:
            stream.ensure_metadata()
            path = _metadata.path(_metadata.home(tmp_path, "t"), "t")
            older = json.loads(path.read_text())
            del older["live_log"]["sort_by"]
            path.write_text(json.dumps(older))
            # From before the versions too: the plain file is all there is.
            shutil.rmtree(_metadata.home(tmp_path, "t") / "t.metadata")
            before = _metadata.load(_metadata.home(tmp_path, "t"), "t")
            assert before is not None
            assert before.live_log.sort_by is None

            stream.ensure_metadata()  # what `serve` does at start
        finally:
            await stream.aclose()

        after = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert after is not None
        assert after.live_log.sort_by == SORT_BY

    async def test_the_recorded_shape_is_the_logs_exactly(self, tmp_path):
        """Binary encodings and system columns included: what Iceberg loses."""
        stream = streamcast.Stream.new(
            "t", root=tmp_path, schema=SCHEMA, sort_by=SORT_BY
        )
        try:
            stream.ensure_metadata()
            assert stream.log is not None
            actual = stream.log.schema
        finally:
            await stream.aclose()

        metadata = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert metadata is not None
        rebuilt = _metadata.shape(metadata.live_log)
        assert rebuilt.equals(actual, check_metadata=True)


@pytest.mark.replication
class TestRestoringIt:
    async def _lost_box(self, root, bucket, s3) -> None:
        """A stream with no WAL replication, published, and then its box gone."""
        stream = streamcast.Stream.new(
            "t",
            root=root,
            schema=SCHEMA,
            sort_by=SORT_BY,
            published=bucket,
            s3_options=s3,
        )
        try:
            await stream.send_many([row(i) for i in range(5)])
            assert stream.log is not None
            while stream.log.seal(flush=True) is not None:
                pass

            stream.log.publish(flush=True)
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        shutil.rmtree(root)

    async def test_the_shape_is_passed_from_the_metadata(
        self, tmp_path, s3, bucket, monkeypatch
    ):
        """litelink checks a given shape against the table EXACTLY: this
        passes only if the one rebuilt from the metadata is the log's. Both
        halves are recorded as passed, since a stamped table would supply them
        itself and hide one that was not."""
        await self._lost_box(tmp_path / "a", bucket, s3)
        passed: dict[str, object] = {}
        real = litelink.restore

        def recording(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            passed.update(kwargs)
            return real(*args, **kwargs)

        monkeypatch.setattr(litelink, "restore", recording)
        restored = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3
        )
        try:
            assert restored.log is not None
            assert restored.log.sort_by == SORT_BY
            trace = restored.log.schema.field("trace")
            assert trace.metadata == {b"streamcast.encoding": b"base16"}
            assert await restored.send(row(9)) is not None
        finally:
            await restored.aclose()

        assert passed["sort_by"] == SORT_BY
        schema = passed["schema"]
        assert schema is not None
        assert schema.field("trace").metadata == {b"streamcast.encoding": b"base16"}  # ty: ignore[unresolved-attribute]

    async def test_a_schema_that_disagrees_is_refused(self, tmp_path, s3, bucket):
        await self._lost_box(tmp_path / "a", bucket, s3)
        retyped = {
            **SCHEMA,
            "properties": {
                **SCHEMA["properties"],
                "price": {"type": ["string", "null"]},
            },
        }
        with pytest.raises(Exception, match="price"):
            streamcast.Stream.restore(
                "t",
                root=tmp_path / "b",
                published=bucket,
                s3_options=s3,
                schema=retyped,
            )

    async def test_config_replaces_the_restored_logs(self, tmp_path, s3, bucket):
        await self._lost_box(tmp_path / "a", bucket, s3)
        config = litelink.LogConfig(target_seal_size=123_456)
        restored = streamcast.Stream.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3, config=config
        )
        try:
            assert restored.log is not None
            assert restored.log.config.target_seal_size == 123_456
        finally:
            await restored.aclose()


class TestReviving:
    async def test_config_reaches_the_new_log(self, tmp_path):
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            await stream.send(row(0))
        finally:
            await stream.aclose()

        streamcast.Stream.retire("t", root=tmp_path)
        revived = streamcast.Stream.restore(
            "t",
            root=tmp_path,
            published=tmp_path.as_uri(),
            revive=True,
            config=litelink.LogConfig(target_seal_size=123_456),
        )
        try:
            assert revived.log is not None
            assert revived.log.config.target_seal_size == 123_456
        finally:
            await revived.aclose()
