"""`Stream.retire`: a stream finished for good, served read-only, and revived.

A stream that merely stops being written leaves its end on the box — the
trailing run `publish` holds back for compaction — and nothing refuses a late
write or says it is finished. Retiring publishes everything, refuses every
write, and records it in the metadata; `restore(..., revive=True)` undoes it on
a new log, here or on another box.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import litelink
import pytest

import streamcast
from streamcast import _metadata, _versions
from tests.conftest import current_json

SCHEMA = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}


async def produce(root: Path, count: int = 7, **kwargs: object) -> None:
    """A stream with `count` rows, written and then left: the server stopped."""
    stream = streamcast.Stream.new("t", root=root, schema=SCHEMA, **kwargs)  # ty: ignore[invalid-argument-type]
    try:
        await stream.send_many([{"i": i} for i in range(count)])
    finally:
        await stream.aclose()


class TestRetiring:
    async def test_it_publishes_what_stopping_would_leave_behind(self, tmp_path):
        await produce(tmp_path)
        with litelink.open(_metadata.home(tmp_path, "t"), "t", read_only=True) as log:
            # A plain stop: the rows are on this disk, none of them published.
            assert log.published_through() == 0

        retirement = streamcast.Stream.retire("t", root=tmp_path)

        assert retirement.end_offset == 8
        with litelink.open(_metadata.home(tmp_path, "t"), "t", read_only=True) as log:
            assert log.published_through() == 7, "every row, the tail included"

    async def test_the_metadata_records_it_and_old_builds_refuse_it(self, tmp_path):
        await produce(tmp_path)
        retirement = streamcast.Stream.retire("t", root=tmp_path)

        metadata = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert metadata is not None
        assert metadata.retirement == retirement
        assert retirement.start_ts is not None
        assert retirement.end_ts is not None
        # Version 3, so a build that predates retirement refuses the file
        # rather than taking the retired log for a migration that died.
        assert (
            current_json(_metadata.home(tmp_path, "t"), "t")["streamcast_metadata"] == 3
        )

    async def test_it_is_safe_to_run_again(self, tmp_path):
        await produce(tmp_path)
        first = streamcast.Stream.retire("t", root=tmp_path)
        assert streamcast.Stream.retire("t", root=tmp_path) == first

    async def test_a_retire_that_died_after_litelinks_step_finishes(self, tmp_path):
        """litelink retired the log, then the metadata was never written."""
        await produce(tmp_path)
        with litelink.open(_metadata.home(tmp_path, "t"), "t") as log:
            log.retire()

        retirement = streamcast.Stream.retire("t", root=tmp_path)
        assert retirement.end_offset == 8

    def test_a_stream_that_is_not_there_is_refused(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="no stream 't'"):
            streamcast.Stream.retire("t", root=tmp_path)


class TestARetiredStreamIsReadOnly:
    async def test_it_opens_read_only_and_refuses_a_send(self, tmp_path):
        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)

        for opened in (
            streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA),
            streamcast.Stream.migrate("t", root=tmp_path, schema=SCHEMA),
        ):
            try:
                assert opened.retirement is not None
                assert opened.end_offset == 8
                with pytest.raises(streamcast.StreamRetired, match="revive=True"):
                    await opened.send({"i": 99})
            finally:
                await opened.aclose()

    async def test_it_is_served_for_reading_and_refuses_a_publisher(
        self, tmp_path, serve
    ):
        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)

        async with serve(stream) as uri:
            # The history replays — from the published table, the only copy.
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert [(await sub.recv())[0] for _ in range(7)] == list(range(1, 8))

            # A publisher is refused by the stream, with when and where.
            with pytest.raises(streamcast.StreamRetired) as raised:
                async with streamcast.publish(uri):
                    pass

        assert raised.value.end_offset == 8
        assert raised.value.at is not None

    async def test_it_is_not_maintained(self, tmp_path, serve, monkeypatch):
        from streamcast import _server  # noqa: PLC0415

        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        planned: list[list[str]] = []
        real = _server._supervisors  # noqa: SLF001

        def recording(routes, maintain):  # noqa: ANN001, ANN202
            children = real(routes, maintain)
            planned.append([child.name for child in children])  # ty: ignore[unresolved-attribute]
            return children

        monkeypatch.setattr(_server, "_supervisors", recording)
        async with serve(stream):
            pass

        assert planned == [[]], "no maintainer for a finished log"


class TestReviving:
    async def test_restore_refuses_a_retired_stream_without_it(self, tmp_path):
        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        with pytest.raises(streamcast.StreamRetired, match="revive=True"):
            streamcast.Stream.restore("t", root=tmp_path, published=tmp_path.as_uri())

    async def test_it_continues_on_a_new_log_where_the_old_one_ended(
        self, tmp_path, serve
    ):
        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)

        revived = streamcast.Stream.restore(
            "t", root=tmp_path, published=tmp_path.as_uri(), revive=True
        )
        assert revived.retirement is None
        assert revived.log is not None
        assert revived.log.name == "t-v2"
        assert await revived.send_many([{"i": 7}, {"i": 8}]) == [8, 9], "dense"

        metadata = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert metadata is not None
        assert metadata.retirement is None
        assert [(e.name, e.start_offset, e.end_offset) for e in metadata.logs] == [
            ("t", 1, 8),
            ("t-v2", 8, None),
        ]
        # Back to version 2: nothing a build before retirement cannot read.
        assert (
            current_json(_metadata.home(tmp_path, "t"), "t")["streamcast_metadata"] == 2
        )

        async with serve(revived, maintain=False) as uri:
            async with streamcast.connect(uri, offset=1, catch_up=True) as sub:
                got = [(await sub.recv())[0] for _ in range(9)]

        assert got == list(range(1, 10)), "across the seam, from the table"

    async def test_a_revive_that_died_after_creating_its_log_carries_on(
        self, tmp_path, monkeypatch
    ):
        await produce(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        real = _versions.commit

        def failing(*_: object, **__: object) -> None:
            msg = "disk full, mid-revive"
            raise OSError(msg)

        monkeypatch.setattr(_versions, "commit", failing)
        with pytest.raises(OSError, match="mid-revive"):
            streamcast.Stream.restore(
                "t", root=tmp_path, published=tmp_path.as_uri(), revive=True
            )

        monkeypatch.setattr(_versions, "commit", real)
        revived = streamcast.Stream.restore(
            "t", root=tmp_path, published=tmp_path.as_uri(), revive=True
        )
        try:
            assert revived.log is not None
            assert (revived.log.name, revived.end_offset) == ("t-v2", 8)
        finally:
            await revived.aclose()


@pytest.mark.replication
class TestOnAnotherBox:
    async def test_retire_here_and_revive_there(self, tmp_path, s3, bucket, serve):
        """The planned move: nothing published is lost, and nothing is left."""
        box_a, box_b = tmp_path / "a", tmp_path / "b"
        await produce(box_a, published=bucket, s3_options=s3)
        streamcast.Stream.retire("t", root=box_a, s3_options=s3)
        published_copy = _versions.fetch(f"{bucket}/t", "t", s3)
        assert published_copy is not None
        assert published_copy.retirement is not None
        shutil.rmtree(box_a)  # the old box is gone

        revived = streamcast.Stream.restore(
            "t", root=box_b, published=bucket, s3_options=s3, revive=True
        )
        assert await revived.send_many([{"i": 7}]) == [8]
        async with serve(revived, maintain=False) as uri:
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                got = [(await sub.recv())[0] for _ in range(8)]

        assert got == list(range(1, 9))
        fetched = _versions.fetch(f"{bucket}/t", "t", s3)
        assert fetched is not None
        assert fetched.retirement is None, "flipped in the published copy too"

    async def test_a_stream_without_wal_replication_restores(
        self, tmp_path, s3, bucket
    ):
        """litelink rebuilds a log from its published table when there is no
        replica: fenced above anything the old log may have issued."""
        box_a, box_b = tmp_path / "a", tmp_path / "b"
        stream = streamcast.Stream.new(
            "t", root=box_a, schema=SCHEMA, published=bucket, s3_options=s3
        )
        try:
            await stream.send_many([{"i": i} for i in range(5)])
            assert stream.log is not None
            while stream.log.seal(flush=True) is not None:
                pass

            stream.log.publish(flush=True)
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        shutil.rmtree(box_a)

        restored = streamcast.Stream.restore(
            "t", root=box_b, published=bucket, s3_options=s3, published_reserve=1000
        )
        try:
            # Above the published end by the reserve the caller chose.
            assert restored.end_offset is not None
            assert restored.end_offset >= 6 + 1000
            [offset] = await restored.send_many([{"i": 5}])
            assert offset == restored.end_offset - 1
        finally:
            await restored.aclose()
