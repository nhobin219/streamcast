"""The stream's metadata file: written by `serve` for every durable stream.

It is what a reader on another machine starts from (#32), so a stream without
one is a stream only its own server can read — which is why `serve` writes it,
uploads it, and refuses to start when it cannot.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _log, _metadata, _versions
from streamcast.asgi import asgi

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}
V2: dict[str, Any] = {
    **SCHEMA,
    "properties": {**SCHEMA["properties"], "venue": {"type": ["string", "null"]}},
}


def row(i: int, **extra: object) -> dict[str, object]:
    return {"event_ts": i, "price": float(i), **extra}


def written(root, stream: str = "trades") -> dict[str, Any]:
    return json.loads(_metadata.path(_metadata.home(root, stream), stream).read_text())


class TestServeWritesIt:
    async def test_a_new_stream_gets_one_at_its_first_serve(self, tmp_path, serve):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            assert not _metadata.path(
                _metadata.home(tmp_path, "trades"), "trades"
            ).exists(), "Stream.new stays offline; the file is serve's to write"
            async with serve(stream, maintain=False):
                pass

            found = written(tmp_path)
            assert set(found) == {
                "streamcast_metadata",
                "stream",
                "stream_id",
                "sealed_logs",
                "live_log",
                "manifest",
            }
            assert uuid.UUID(found["stream_id"])
            assert found["sealed_logs"] == []
            assert found["live_log"]["name"] == "trades"
            assert found["live_log"]["start_offset"] == 1
            assert "end_offset" not in found["live_log"], "the live log's end moves"
            assert found["manifest"] is None
        finally:
            await stream.aclose()

    async def test_a_log_from_before_metadata_files_gains_one(self, tmp_path, serve):
        # A pre-0.9 stream: a log at the stream's name and nothing beside it.
        with litelink.new(
            tmp_path, "trades", schema=streamcast.to_arrow(SCHEMA)
        ) as legacy:
            legacy.extend([row(i) for i in range(3)])

        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False):
                pass
        finally:
            await stream.aclose()

        found = written(tmp_path)
        assert found["live_log"]["start_offset"] == 1
        # It predates streamcast_ts, and the file says so.
        assert found["live_log"]["system_schema"]["properties"] == {}

    async def test_the_id_is_minted_once(self, tmp_path, serve):
        # A fresh `Stream.new` per serve, as a restart has: `serve` closes the
        # log a stream owns when it shuts down.
        async with serve(
            streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA),
            maintain=False,
        ):
            pass

        first = written(tmp_path)["stream_id"]

        async with serve(
            streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA),
            maintain=False,
        ):
            pass

        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        await migrated.aclose()

        assert written(tmp_path)["stream_id"] == first

    async def test_a_live_only_stream_has_none(self, tmp_path, serve):
        async with serve(streamcast.Stream("live"), maintain=False):
            pass

        assert list(tmp_path.iterdir()) == []

    async def test_the_asgi_app_writes_it_at_start(self, tmp_path):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            streams = asgi(stream, maintain=False, replicate=False)
            assert not _metadata.path(
                _metadata.home(tmp_path, "trades"), "trades"
            ).exists(), "constructed, often at import: no I/O yet"
            async with streams:
                assert _metadata.path(
                    _metadata.home(tmp_path, "trades"), "trades"
                ).exists()
        finally:
            await stream.aclose()


class TestMigrationUpdatesIt:
    async def test_the_live_log_is_sealed_into_the_list(self, tmp_path, serve):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        await stream.send_many([row(i) for i in range(5)])
        await stream.aclose()

        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        await migrated.aclose()

        found = written(tmp_path)
        assert [
            (e["name"], e["start_offset"], e["end_offset"])
            for e in found["sealed_logs"]
        ] == [("trades", 1, 6)]
        assert found["live_log"]["name"] == "trades-v2"
        assert found["live_log"]["start_offset"] == 6
        assert "venue" in found["live_log"]["schema"]["properties"]


class TestItNamesTheLiveLog:
    async def test_serving_a_sealed_log_is_refused(self, tmp_path, serve):
        """A sealed log is never written to again, and serve is what writes.

        litelink refuses a writer on a log `migrate` retired. A log sealed by
        a migration under litelink 0.5 was never retired and still opens, so
        `serve` checks the metadata too: here, a log it does not name as live.
        """
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        await stream.send(row(0))
        await stream.aclose()
        await streamcast.Stream.migrate("trades", root=tmp_path, schema=V2).aclose()

        with pytest.raises(litelink.RetiredError):
            litelink.open(_metadata.home(tmp_path, "trades"), "trades")

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        _versions.commit(
            _metadata.home(tmp_path, "trades"),
            dataclasses.replace(
                metadata,
                live_log=dataclasses.replace(metadata.live_log, name="trades-v3"),
            ),
        )
        with litelink.open(_metadata.home(tmp_path, "trades"), "trades-v2") as unnamed:
            with pytest.raises(ValueError, match="live log is 'trades-v3'"):
                async with serve(streamcast.Stream("trades", log=unnamed)):
                    pass

    async def test_a_handed_in_live_log_learns_its_seam(self, tmp_path, serve):
        """`Stream(log=…)` does no I/O, so the seam arrives at `serve`.

        Without it a subscribe below the seam would read the live log, find
        nothing there, and be served an empty replay — a silent hole.
        """
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        await stream.send_many([row(i) for i in range(5)])
        await stream.aclose()
        await streamcast.Stream.migrate("trades", root=tmp_path, schema=V2).aclose()

        with litelink.open(_metadata.home(tmp_path, "trades"), "trades-v2") as live:
            handed = streamcast.Stream("trades", log=live)
            async with serve(handed, maintain=False) as uri:
                assert handed.retired == (
                    (_metadata.home(tmp_path, "trades"), "trades"),
                )
                with pytest.raises(streamcast.NotReplayable) as raised:
                    await streamcast.connect(uri, offset=2)

        assert raised.value.why == "evicted"
        assert raised.value.fields["earliest"] == 6


@pytest.mark.replication
class TestThePublishedCopy:
    async def test_serve_uploads_it(self, tmp_path, serve, s3, bucket):
        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=SCHEMA, published=bucket, s3_options=s3
        )
        try:
            async with serve(stream, maintain=False, replicate=False):
                pass
        finally:
            await stream.aclose()

        assert _metadata.fetch(bucket, "trades", s3) == _metadata.load(
            tmp_path, "trades"
        )

    async def test_an_unchanged_stream_is_not_uploaded_again(
        self, tmp_path, serve, s3, bucket, monkeypatch
    ):
        first = streamcast.Stream.new(
            "trades", root=tmp_path, schema=SCHEMA, published=bucket, s3_options=s3
        )
        async with serve(first, maintain=False, replicate=False):
            pass

        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=SCHEMA, s3_options=s3
        )
        try:
            uploads: list[str] = []
            original = _metadata.publish
            monkeypatch.setattr(
                _metadata,
                "publish",
                lambda metadata, *a: (
                    uploads.append(metadata.stream),
                    original(metadata, *a),
                ),
            )
            async with serve(stream, maintain=False, replicate=False):
                pass
        finally:
            await stream.aclose()

        assert uploads == [], "one GET, and no PUT, for a stream that has not changed"

    async def test_serve_refuses_to_start_when_it_cannot_upload(
        self, tmp_path, serve, s3
    ):
        missing = f"s3://no-such-bucket-{uuid.uuid4().hex[:8]}/prefix"
        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=SCHEMA, published=missing, s3_options=s3
        )
        try:
            with pytest.raises(FileNotFoundError, match="does not exist"):
                async with serve(stream, maintain=False, replicate=False):
                    pass
        finally:
            await stream.aclose()

    async def test_a_missing_bucket_is_not_a_missing_copy(self, s3):
        # pyarrow reports both as "not found"; only the second means "none".
        with pytest.raises(FileNotFoundError, match="does not exist"):
            _metadata.fetch(f"s3://no-such-bucket-{uuid.uuid4().hex[:8]}/p", "t", s3)


def test_an_unknown_version_is_refused():
    with pytest.raises(ValueError, match="version"):
        _metadata.Metadata.from_json(json.dumps({"streamcast_metadata": 99}))


def test_system_schema_is_recorded_for_a_new_log(tmp_path):
    with litelink.new(
        tmp_path, "trades", schema=_log.with_system(streamcast.to_arrow(SCHEMA))
    ) as log:
        found = _metadata.single("trades", log)

    assert _log.STAMP in found.live_log.system_schema["properties"]  # ty: ignore[unsupported-operator]
