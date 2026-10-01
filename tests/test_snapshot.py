"""`Stream.snapshot`: a stream's history as of a fixed point, from any machine.

Written with a server's own calls — `Stream.new`, `send`, `migrate` — and read
back with nothing but the metadata file's URI, as another machine would.
"""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast import LATEST, SnapshotUnavailable, _manifest, _metadata

V1: dict[str, Any] = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}
V2: dict[str, Any] = {
    "type": "object",
    "properties": {**V1["properties"], "venue": {"type": ["string", "null"]}},
    "required": ["event_ts", "price"],
}


def row(i: int, **extra: object) -> dict[str, object]:
    return {"event_ts": i, "price": 100.0 + i, **extra}


def publish(log: litelink.WriteHandle) -> None:
    """Everything sent so far, sealed and in the published table."""
    while log.seal() is not None:
        pass

    log.publish(push_unsettled=True)


async def stream_of(
    root: Path,
    rows: int,
    *,
    published: str | None = None,
    s3: litelink.S3Options | None = None,
) -> streamcast.Stream:
    stream = streamcast.Stream.new(
        "trades", root=root, schema=V1, published=published, s3=s3
    )
    await stream.send_many([row(i) for i in range(rows)])
    return stream


async def served_once(stream: streamcast.Stream, serve) -> None:
    """`serve` writes the metadata file a reader resolves."""
    async with serve(stream, maintain=False):
        pass


def offsets(table) -> list[int]:
    return table.column("litelink_offset").to_pylist()


class TestOneLog:
    async def test_it_reads_everything_published_and_stops_there(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(5, 8)])  # not published
        await served_once(stream, serve)

        uri = _metadata.uri("trades", litelink.open(tmp_path, "trades", read_only=True))
        async with await streamcast.Stream.snapshot(uri) as snap:
            assert snap.end_offset == 6
            table = await snap.scan()

        assert offsets(table) == [1, 2, 3, 4, 5]
        assert table.column("price").to_pylist() == [100.0 + i for i in range(5)]

    async def test_as_of_an_offset_it_stops_at_that_offset(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        table = await streamcast.Stream.scan(uri, as_of_offset=3)
        assert offsets(table) == [1, 2, 3]

    async def test_past_the_published_end_it_needs_the_broker(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 3)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        with pytest.raises(SnapshotUnavailable, match="broker="):
            await streamcast.Stream.snapshot(uri, as_of_offset=10)


class TestTheBroker:
    async def test_latest_reads_the_tail_the_tables_do_not_hold(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 4)
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(4, 7)])  # only on the broker
        uri = stream.metadata_uri
        assert uri is not None

        async with serve(stream, maintain=False) as broker:
            async with await streamcast.Stream.snapshot(
                uri, as_of_offset=LATEST, broker=broker
            ) as snap:
                assert snap.end_offset == 8
                table = await snap.scan()
                streamed = [offset async for offset, _row in snap.rows(1)]

        assert offsets(table) == list(range(1, 8))
        assert table.column("price").to_pylist() == [100.0 + i for i in range(7)]
        assert streamed == list(range(1, 8))

    async def test_no_socket_when_the_tables_cover_it(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 4)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        # Nothing listens here: reaching for the broker would fail.
        table = await streamcast.Stream.scan(
            uri, as_of_offset=2, broker="ws://127.0.0.1:9/trades"
        )
        assert offsets(table) == [1, 2]

    async def test_a_gap_neither_side_holds_is_refused_with_both_numbers(
        self, tmp_path, serve
    ):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=V1, max_replay=2)
        await stream.send_many([row(i) for i in range(3)])
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(3, 20)])  # past max_replay
        uri = stream.metadata_uri
        assert uri is not None

        async with serve(stream, maintain=False) as broker:
            with pytest.raises(SnapshotUnavailable) as raised:
                await streamcast.Stream.snapshot(
                    uri, as_of_offset=LATEST, broker=broker
                )

        message = str(raised.value)
        assert "published tables end at 3" in message
        assert "Neither holds" in message


class TestAMigratedStream:
    async def test_its_logs_read_as_one_table(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        await stream.aclose()
        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send_many([row(i, venue="x") for i in range(5, 8)])
        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        table = await streamcast.Stream.scan(uri)

        # Dense across the seam, and the column the first log lacked is NULL there.
        assert offsets(table) == list(range(1, 9))
        assert table.column("venue").to_pylist() == [None] * 5 + ["x"] * 3

    async def test_a_filter_skips_a_sealed_log_without_opening_it(
        self, tmp_path, serve
    ):
        stream = await stream_of(tmp_path, 5)  # prices 100 .. 104
        await stream.aclose()
        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send_many([row(i, venue="x") for i in range(500, 503)])
        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        # The first log's table, gone: reading it would fail.
        with litelink.open(tmp_path, "trades", read_only=True) as old:
            shutil.rmtree(_published_dir(old.published, "trades"))

        async with await streamcast.Stream.snapshot(uri) as snap:
            table = await snap.scan(filters=[("price", ">", 500.0)])
            assert offsets(table) == [6, 7, 8]

            with pytest.raises(Exception, match="version-hint|No files found|IO Error"):
                await snap.scan()  # unfiltered: it is opened, and it is gone

    async def test_as_of_a_time_it_reads_what_was_stamped_by_then(
        self, tmp_path, serve
    ):
        # One send each, a millisecond apart: `send_many` stamps its whole
        # group with one `streamcast_ts`, which would leave nothing to divide.
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=V1)
        for i in range(5):
            await stream.send(row(i))
            await asyncio.sleep(0.001)

        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        everything = await streamcast.Stream.scan(uri)
        stamps = everything.column("streamcast_ts").to_pylist()
        middle = sorted(stamps)[2]

        assert len(set(stamps)) == 5, "the stamps must differ for this to test anything"

        table = await streamcast.Stream.scan(uri, as_of_ts=middle)
        assert all(ts <= middle for ts in table.column("streamcast_ts").to_pylist())
        assert table.num_rows == 3

        with pytest.raises(SnapshotUnavailable, match="published rows up to"):
            await streamcast.Stream.snapshot(uri, as_of_ts=max(stamps) + 1)

    async def test_a_retired_log_published_short_is_refused(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        await stream.aclose()
        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)
        # The manifest says the first log held one row more than its table does.
        manifest = _manifest.load(tmp_path, "trades")
        assert manifest is not None
        index = manifest.schema.get_field_index("record_count")
        counts = [count + 1 for count in manifest.column(index).to_pylist()]
        _manifest.save(
            tmp_path,
            "trades",
            manifest.set_column(index, "record_count", pa.array(counts, pa.int64())),
        )

        with pytest.raises(SnapshotUnavailable, match="holds 5 of the 6 rows"):
            await streamcast.Stream.scan(uri)


class TestTheMetadata:
    async def test_another_streams_file_at_the_same_path_is_refused(
        self, tmp_path, serve
    ):
        stream = await stream_of(tmp_path, 2)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        from streamcast import _snapshot

        with pytest.raises(SnapshotUnavailable, match="another stream's file"):
            await _snapshot.snapshot(uri, stream_id="not-this-one")

    async def test_a_missing_file_says_the_history_is_local(self, tmp_path):
        with pytest.raises(SnapshotUnavailable, match="local to the server's machine"):
            await streamcast.Stream.snapshot((tmp_path / "nope.metadata.json").as_uri())

    async def test_both_points_at_once_are_refused(self, tmp_path):
        with pytest.raises(ValueError, match="not both"):
            await streamcast.Stream.snapshot(
                (tmp_path / "x.metadata.json").as_uri(), as_of_offset=1, as_of_ts=1
            )


def _published_dir(published: str, name: str) -> Path:
    from streamcast import _published

    return Path(_published.path(f"{published}/{name}"))


@pytest.mark.replication
async def test_a_stream_published_to_s3_reads_from_there(tmp_path, s3, bucket, serve):
    stream = await stream_of(tmp_path, 4, published=bucket, s3=s3)
    assert stream.log is not None
    publish(stream.log)
    uri = stream.metadata_uri
    assert uri is not None
    assert uri.startswith(bucket)
    await served_once(stream, serve)

    table = await streamcast.Stream.scan(uri, s3=s3)
    assert offsets(table) == [1, 2, 3, 4]
