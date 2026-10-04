"""`Stream.adopt`: a stream continues from a published table nobody holds.

The case neither `new` nor `restore` covers: the old producer ran with
`wal_replication` off, published its log in full, and is gone. `new` refuses
the location (it holds a table) and `restore` has no replica to rebuild from.
What matters here is `migrate`'s properties at a seam nobody sealed: the
offsets stay one dense sequence, nothing the old producer issued is reissued,
and every reader of the stream sees both sides.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import streamcast
from streamcast import _metadata
from tests.test_migrate import V1, V2, evolve, row


async def _old_producer(root: Path, published: str, s3=None, count: int = 5) -> None:
    """A producer that published everything it had and was then taken down."""
    stream = streamcast.Stream.new(
        "trades", root=root, schema=V1, published=published, s3_options=s3
    )
    try:
        await stream.send_many([row(i) for i in range(count)])
        assert stream.log is not None
        stream.log.seal(flush=True)
        stream.log.publish(flush=True)
        assert stream.log.published_through() == count
    finally:
        await stream.aclose()
    shutil.rmtree(root)  # the box is gone; the table is all that is left


class TestItContinuesTheTable:
    async def test_the_new_log_starts_where_the_table_ends(self, tmp_path):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)

        stream = streamcast.Stream.adopt(
            "trades", root=tmp_path / "box_b", published=published, schema=V2
        )
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
            assert stream.log.end_offset() == 6, "one past the table's last row"
            assert stream.name == "trades"
            # The two rows sent now take the offsets after the seam, densely.
            assert await stream.send_many([row(5, venue="x"), row(6, venue="y")]) == [
                6,
                7,
            ]
        finally:
            await stream.aclose()

        metadata = _metadata.load(tmp_path / "box_b", "trades")
        assert metadata is not None
        (old,) = metadata.sealed_logs
        assert (old.name, old.start_offset, old.end_offset, old.published) == (
            "trades",
            1,
            6,
            published,
        )
        assert set(old.schema["properties"]) == {"event_ts", "price", "side"}
        assert metadata.live_log.name == "trades-v2"
        assert metadata.live_log.start_offset == 6
        assert "venue" in metadata.live_log.schema["properties"]
        assert metadata.manifest is None
        # (A `file://` location gets no published copy of the metadata; the S3
        # test below checks that one.)

    async def test_a_snapshot_reads_both_sides_of_the_seam(self, tmp_path):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        stream = streamcast.Stream.adopt(
            "trades", root=tmp_path / "box_b", published=published, schema=V2
        )
        try:
            await stream.send_many([row(5, venue="x"), row(6, venue="y")])
            stream.log.seal(flush=True)
            stream.log.publish(flush=True)
            stream.ensure_metadata()
            uri = stream.metadata_uri
            assert uri is not None
        finally:
            await stream.aclose()

        async with await streamcast.Stream.snapshot(uri) as snap:
            table = await snap.sql(
                "SELECT event_ts, price, venue FROM log ORDER BY event_ts"
            )
        assert table.num_rows == 7
        assert table.column("event_ts").to_pylist() == [
            1_790_000_000_000_000 + i for i in range(7)
        ]
        # The old table never had `venue`: null there, filled on the new side.
        assert table.column("venue").to_pylist() == [None] * 5 + ["x", "y"]

    async def test_a_subscriber_catching_up_from_the_start_gets_every_row(
        self, tmp_path, serve
    ):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        stream = streamcast.Stream.adopt(
            "trades", root=tmp_path / "box_b", published=published, schema=V2
        )
        async with serve(stream) as uri:
            await stream.send_many([row(5, venue="x"), row(6, venue="y")])
            async with streamcast.connect(uri, offset=1, catch_up=True) as sub:
                got = [await sub.recv() for _ in range(7)]
        assert [offset for offset, _ts, _row in got] == list(range(1, 8))
        assert [r["price"] for _o, _t, r in got] == [100.0 + i for i in range(7)]

    async def test_it_is_served_again_by_migrate_at_the_next_start(self, tmp_path):
        """The server's startup path is `migrate` (idempotent): an adopted stream
        is one it opens as-is, at its current log, with the seam intact."""
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        stream = streamcast.Stream.adopt(
            "trades", root=tmp_path / "box_b", published=published, schema=V2
        )
        await stream.aclose()
        again = streamcast.Stream.migrate("trades", root=tmp_path / "box_b", schema=V2)
        try:
            assert again.log is not None and again.log.name == "trades-v2"
            assert again.log.end_offset() == 6
        finally:
            await again.aclose()


class TestItRefuses:
    async def test_a_table_published_short_of_what_the_producer_said(self, tmp_path):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        with pytest.raises(ValueError, match="published short"):
            streamcast.Stream.adopt(
                "trades",
                root=tmp_path / "box_b",
                published=published,
                schema=V2,
                end_offset=9,
            )
        assert not (tmp_path / "box_b" / "trades-v2").exists(), "nothing was adopted"
        assert _metadata.load(tmp_path / "box_b", "trades") is None
        # And exactly right is accepted.
        stream = streamcast.Stream.adopt(
            "trades",
            root=tmp_path / "box_b",
            published=published,
            schema=V2,
            end_offset=6,
        )
        await stream.aclose()

    async def test_a_location_with_nothing_published(self, tmp_path):
        (
            tmp_path / "bucket"
        ).mkdir()  # the prefix exists; no table was ever published under it
        published = (tmp_path / "bucket").as_uri()
        with pytest.raises(FileNotFoundError, match="nothing to adopt"):
            streamcast.Stream.adopt(
                "trades", root=tmp_path / "box_b", published=published, schema=V2
            )

    async def test_a_box_that_already_holds_the_stream(self, tmp_path):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        stream = streamcast.Stream.adopt(
            "trades", root=tmp_path / "box_b", published=published, schema=V2
        )
        await stream.aclose()
        with pytest.raises(FileExistsError, match="Stream.migrate opens it"):
            streamcast.Stream.adopt(
                "trades", root=tmp_path / "box_b", published=published, schema=V2
            )

    async def test_a_column_whose_type_changed(self, tmp_path):
        published = (tmp_path / "bucket").as_uri()
        await _old_producer(tmp_path / "box_a", published)
        retyped = evolve(V1, price={"type": "integer"})
        with pytest.raises(ValueError, match="fixed for the life of a stream"):
            streamcast.Stream.adopt(
                "trades", root=tmp_path / "box_b", published=published, schema=retyped
            )
        assert not (tmp_path / "box_b" / "trades-v2").exists()


@pytest.mark.replication
class TestOnObjectStorage:
    async def test_the_same_round_trip_against_s3(self, tmp_path, s3, bucket, serve):
        await _old_producer(tmp_path / "box_a", bucket, s3)
        stream = streamcast.Stream.adopt(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            schema=V2,
            s3_options=s3,
            end_offset=6,
        )
        async with serve(stream) as uri:
            assert stream.log is not None and stream.log.end_offset() == 6
            await stream.send_many([row(5, venue="x")])
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                got = [await sub.recv() for _ in range(6)]
        assert [offset for offset, _ts, _row in got] == list(range(1, 7))
        assert _metadata.fetch(bucket, "trades", s3) == _metadata.load(
            tmp_path / "box_b", "trades"
        ), "published beside the tables, the same file"

    async def test_a_stream_migrated_once_is_adopted_from_its_metadata(
        self, tmp_path, s3, bucket
    ):
        """Adopted, served, lost again: the published metadata names the current
        log, whose table is sealed at its end, and the next name follows."""
        await _old_producer(tmp_path / "box_a", bucket, s3)
        stream = streamcast.Stream.adopt(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            schema=V2,
            s3_options=s3,
        )
        try:
            await stream.send_many([row(5, venue="x"), row(6, venue="y")])
            stream.log.seal(flush=True)
            stream.log.publish(flush=True)
        finally:
            await stream.aclose()
        shutil.rmtree(tmp_path / "box_b")

        V3 = evolve(V2, note={"type": ["string", "null"]})
        third = streamcast.Stream.adopt(
            "trades",
            root=tmp_path / "box_c",
            published=bucket,
            schema=V3,
            s3_options=s3,
        )
        try:
            assert third.log is not None and third.log.name == "trades-v3"
            assert third.log.end_offset() == 8
            metadata = _metadata.load(tmp_path / "box_c", "trades")
            assert [
                (e.name, e.start_offset, e.end_offset) for e in metadata.sealed_logs
            ] == [
                ("trades", 1, 6),
                ("trades-v2", 6, 8),
            ]
            assert _metadata.fetch(bucket, "trades", s3) == metadata
        finally:
            await third.aclose()
