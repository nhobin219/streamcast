"""Starting a stream never replaces what its published prefix already holds.

Two ways a box that lost part of its disk used to overwrite the good copy in
object storage:

- **Its metadata gone, its log still here.** `serve`, `migrate`, `retire` and
  `retain` each described the log afresh, with a new `stream_id`, and
  uploaded that over the published copy: every consumer that records the id
  was refused, and a migrated stream's copy lost its sealed logs.
- **Its whole home gone.** `StreamHandle.open` created an empty stream at the
  prefix, and the first `serve` replaced the published metadata (and with WAL
  replication, the replica) with it.

The published copy is adopted in the first case, and `new` refuses in the
second, naming `StreamHandle.restore`.
"""

from __future__ import annotations

import shutil
from datetime import timedelta
from pathlib import Path
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _log, _metadata, _remote, _versions

pytestmark = pytest.mark.replication

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}
V2: dict[str, Any] = {
    **SCHEMA,
    "properties": {**SCHEMA["properties"], "note": {"type": ["string", "null"]}},
}


async def _served(root: Path, bucket: str, s3: object, *, rows: int = 20) -> str:
    """A stream with rows published and its metadata written and uploaded, as
    the broker's first start leaves it. Its `stream_id`."""
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=Path(root).absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="create_if_absent",
    )
    try:
        await stream.send_many([{"i": i} for i in range(rows)])
        log = stream.log
        assert log is not None
        while log.seal(flush=True) is not None:
            pass

        log.publish(flush=True)
        stream.ensure_metadata()
    finally:
        await stream.aclose()

    metadata = _metadata.load(_metadata.home(root, "t"), "t")
    assert metadata is not None
    return metadata.stream_id


def _lose_metadata(root: Path) -> None:
    """What a partly lost disk leaves: the logs, and none of the metadata."""
    home = _metadata.home(root, "t")
    shutil.rmtree(home / _versions.directory("t"))
    for plain in ("t.metadata.json", "t.manifest.parquet"):
        (home / plain).unlink(missing_ok=True)

    assert _metadata.load(home, "t") is None


def _published(bucket: str, s3: object) -> _metadata.Metadata:
    found = _versions.fetch(f"{bucket}/t", "t", s3)  # ty: ignore[invalid-argument-type]
    assert found is not None
    return found


class TestLostMetadataIsAdopted:
    async def test_serve_keeps_the_id(self, tmp_path, bucket, s3):
        first = await _served(tmp_path, bucket, s3)
        _lose_metadata(tmp_path)

        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            stream.ensure_metadata()  # what the broker does before it admits a connection
        finally:
            await stream.aclose()

        local = _metadata.load(_metadata.home(tmp_path, "t"), "t")
        assert local is not None
        assert local.stream_id == first
        assert _published(bucket, s3).stream_id == first

    async def test_migrate_keeps_the_id(self, tmp_path, bucket, s3):
        first = await _served(tmp_path, bucket, s3)
        _lose_metadata(tmp_path)

        stream = streamcast.StreamHandle.migrate(
            "t", root=tmp_path, schema=V2, s3_options=s3
        )
        await stream.aclose()

        published = _published(bucket, s3)
        assert published.stream_id == first
        assert [entry.name for entry in published.logs] == ["t", "t-v2"]

    async def test_retire_keeps_the_id(self, tmp_path, bucket, s3):
        first = await _served(tmp_path, bucket, s3)
        _lose_metadata(tmp_path)

        streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)

        published = _published(bucket, s3)
        assert published.stream_id == first
        assert published.retirement is not None

    async def test_retain_keeps_the_id(self, tmp_path, bucket, s3):

        first = await _served(tmp_path, bucket, s3)
        _lose_metadata(tmp_path)

        streamcast.StreamHandle.retain(
            "t", root=tmp_path, max_age=timedelta(days=1), s3_options=s3
        )

        published = _published(bucket, s3)
        assert published.stream_id == first
        assert published.retention is not None

    async def test_a_migrated_streams_sealed_logs_and_manifest_are_kept(
        self, tmp_path, bucket, s3
    ):
        first = await _served(tmp_path, bucket, s3)
        migrated = streamcast.StreamHandle.migrate(
            "t", root=tmp_path, schema=V2, s3_options=s3
        )
        try:
            migrated.ensure_metadata()
        finally:
            await migrated.aclose()

        before = _published(bucket, s3)
        assert before.manifest is not None
        _lose_metadata(tmp_path)

        # Only the first log is known to a home without metadata, and it is
        # sealed: refused, once the copy that says what follows it is home.
        with pytest.raises(ValueError, match="t-v2"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=V2),
                    storage=streamcast.Storage(
                        root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                    ),
                ),
                initialization="create_if_absent",
            )

        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=V2),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            assert stream.log is not None
            assert stream.log.name == "t-v2"
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        home = _metadata.home(tmp_path, "t")
        _, local, manifest = _versions.load(home, "t")
        assert local is not None
        assert local.stream_id == first
        assert [entry.name for entry in local.logs] == ["t", "t-v2"]
        assert manifest is not None, "the sealed log's statistics came back too"
        assert _published(bucket, s3) == before

    async def test_a_copy_naming_another_live_log_is_refused(
        self, tmp_path, bucket, s3
    ):
        await _served(tmp_path, bucket, s3)
        migrated = streamcast.StreamHandle.migrate(
            "t", root=tmp_path, schema=V2, s3_options=s3
        )
        try:
            migrated.ensure_metadata()
        finally:
            await migrated.aclose()

        before = _published(bucket, s3)
        _lose_metadata(tmp_path)

        # The first log is all a home without metadata knows to open, and the
        # copy says it is sealed: refused, never retired again over the copy.
        with pytest.raises(ValueError, match="t-v2"):
            streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)

        assert _published(bucket, s3) == before
        # The copy is in the home now, so the next run opens the live log.
        retirement = streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)
        assert _published(bucket, s3).retirement == retirement

    async def test_a_retired_stream_is_served_read_only_again(
        self, tmp_path, bucket, s3
    ):
        first = await _served(tmp_path, bucket, s3)
        streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)
        before = _published(bucket, s3)
        _lose_metadata(tmp_path)

        with pytest.raises(ValueError, match="run this again"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                    ),
                ),
                initialization="create_if_absent",
            )

        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            assert stream.retirement == before.retirement
        finally:
            await stream.aclose()

        assert _published(bucket, s3).stream_id == first

    async def test_a_stream_with_no_metadata_anywhere_gets_a_new_id(
        self, tmp_path, bucket, s3
    ):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        assert _published(bucket, s3).stream_id


class TestRecoveredRetirement:
    async def test_migrate_refuses_writes_and_revive_keeps_resume_offsets(
        self, tmp_path, bucket, s3, serve
    ):
        first = await _served(tmp_path, bucket, s3)
        retirement = streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)
        before = _published(bucket, s3)
        _lose_metadata(tmp_path)

        recovered = streamcast.StreamHandle.migrate(
            "t", root=tmp_path, schema=SCHEMA, s3_options=s3
        )
        try:
            # A recovery row at 21 followed by an explicit revive at the old
            # retirement frontier would acknowledge two different rows at 21.
            with pytest.raises(streamcast.StreamRetired):
                await recovered.send({"i": 100})

            assert recovered.retirement == retirement
            assert recovered.end_offset == 21
        finally:
            await recovered.aclose()

        home = _metadata.home(tmp_path, "t")
        assert not (home / "t-v2").exists()
        assert _published(bucket, s3) == before

        revived = streamcast.StreamHandle.restore(
            "t", root=tmp_path, published=bucket, s3_options=s3, revive=True
        )
        try:
            assert await revived.send({"i": 101}) == 21
            assert await revived.send({"i": 102}) == 22
            # A consumer that processed 21 and saved its next cursor at 22
            # receives the next distinct row, rather than skipping a reused 21.
            async with serve(
                revived,
                maintain=False,
            ) as uri:
                async with streamcast.connect(uri, offset=22) as sub:
                    offset, _ts, row = await sub.recv()
                    assert (offset, row) == (22, {"i": 102})

                log = revived.log
                assert log is not None
                while log.seal(flush=True) is not None:
                    pass

                log.publish(flush=True)
                metadata_uri = revived.metadata_uri
                assert metadata_uri is not None
                async with await streamcast.StreamHandle.snapshot(
                    metadata_uri, s3_options=s3
                ) as snapshot:
                    table = await snapshot.sql(
                        "SELECT litelink_offset, i FROM log ORDER BY litelink_offset"
                    ).read_all()

            assert table.column("litelink_offset").to_pylist() == list(range(1, 23))
            assert table.column("i").to_pylist() == [*range(20), 101, 102]
            assert _published(bucket, s3).stream_id == first
        finally:
            await revived.aclose()

    @pytest.mark.parametrize("crash", ["before_hint", "after_hint", "after_adopt"])
    async def test_migrate_retry_after_interrupted_adoption_stays_retired(
        self, tmp_path, bucket, s3, monkeypatch, crash
    ):
        await _served(tmp_path, bucket, s3)
        retirement = streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)
        before = _published(bucket, s3)
        _lose_metadata(tmp_path)
        home = _metadata.home(tmp_path, "t")

        with monkeypatch.context() as patch:
            if crash == "after_adopt":
                real_adopt = _versions.adopt

                def failed_adopt(*args, **kwargs):
                    adopted = real_adopt(*args, **kwargs)
                    assert adopted is not None and adopted.retirement == retirement
                    raise OSError("interrupted retirement adoption")

                patch.setattr(_versions, "adopt", failed_adopt)
            else:
                real_write = _versions._Local.write  # noqa: SLF001

                def failed_write(store, name, data):
                    if store.top == home and name == _versions.HINT:
                        if crash == "after_hint":
                            real_write(store, name, data)

                        raise OSError("interrupted retirement adoption")

                    real_write(store, name, data)

                patch.setattr(_versions._Local, "write", failed_write)  # noqa: SLF001

            with pytest.raises(OSError, match="interrupted retirement adoption"):
                streamcast.StreamHandle.migrate(
                    "t", root=tmp_path, schema=SCHEMA, s3_options=s3
                )

        # Before the hint, retry must recover again; after it, retirement is
        # already durable. Both that retry and every later startup refuse sends.
        for _ in range(2):
            recovered = streamcast.StreamHandle.migrate(
                "t", root=tmp_path, schema=SCHEMA, s3_options=s3
            )
            try:
                with pytest.raises(streamcast.StreamRetired):
                    await recovered.send({"i": 100})

                assert recovered.retirement == retirement
            finally:
                await recovered.aclose()

        assert not (home / "t-v2").exists()
        assert _published(bucket, s3) == before

    async def test_recovered_retirement_checks_the_declared_schema(
        self, tmp_path, bucket, s3
    ):
        await _served(tmp_path, bucket, s3)
        streamcast.StreamHandle.retire("t", root=tmp_path, s3_options=s3)
        before = _published(bucket, s3)
        _lose_metadata(tmp_path)

        with pytest.raises(ValueError, match="the retired stream"):
            streamcast.StreamHandle.migrate(
                "t", root=tmp_path, schema=V2, s3_options=s3
            )

        assert not (_metadata.home(tmp_path, "t") / "t-v2").exists()
        assert _published(bucket, s3) == before


class TestNewRefusesToReplaceAPublishedStream:
    async def test_a_served_stream(self, tmp_path, bucket, s3):
        first = await _served(tmp_path / "a", bucket, s3)

        with pytest.raises(FileExistsError, match=r"StreamHandle\.restore"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path / "b").absolute(),
                        published=bucket,
                        s3_options=s3,
                    ),
                ),
                initialization="create_if_absent",
            )

        assert not (tmp_path / "b" / "t").exists(), "nothing created on the new box"
        assert _published(bucket, s3).stream_id == first

        # And the restore it names brings the stream back, under its id.
        restored = streamcast.StreamHandle.restore(
            "t", root=tmp_path / "b", published=bucket, s3_options=s3
        )
        try:
            restored.ensure_metadata()
        finally:
            await restored.aclose()

        assert _published(bucket, s3).stream_id == first

    async def test_a_published_table_with_no_metadata_yet(self, tmp_path, bucket, s3):
        # Published and never served: no metadata at the prefix, only the table.
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path / "a").absolute(),
                    published=bucket,
                    s3_options=s3,
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            await stream.send_many([{"i": i} for i in range(5)])
            log = stream.log
            assert log is not None
            while log.seal(flush=True) is not None:
                pass

            log.publish(flush=True)
        finally:
            await stream.aclose()

        with pytest.raises(FileExistsError, match=r"StreamHandle\.restore"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path / "b").absolute(),
                        published=bucket,
                        s3_options=s3,
                    ),
                ),
                initialization="create_if_absent",
            )

    async def test_a_stream_served_before_its_first_publish(self, tmp_path, bucket, s3):
        # The metadata is at the prefix and the table is still empty: litelink
        # has nothing of its own to refuse, so this one is streamcast's.
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path / "a").absolute(),
                    published=bucket,
                    s3_options=s3,
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            await stream.send_many([{"i": i} for i in range(5)])
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        first = _published(bucket, s3).stream_id
        with pytest.raises(FileExistsError, match=r"StreamHandle\.restore"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path / "b").absolute(),
                        published=bucket,
                        s3_options=s3,
                    ),
                ),
                initialization="create_if_absent",
            )

        assert _published(bucket, s3).stream_id == first

    async def test_a_wal_replica_alone(self, tmp_path, bucket, s3):
        # Where litestream ships the log's buffer, with nothing else there yet.
        filesystem = _remote._filesystem(f"{bucket}/t/t/_wal/buffer.db/0000/x.ltx", s3)  # noqa: SLF001
        with filesystem[0].open_output_stream(filesystem[1]) as sink:
            sink.write(b"ltx")

        with pytest.raises(FileExistsError, match=r"StreamHandle\.restore"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                    ),
                ),
                initialization="create_if_absent",
            )

    async def test_a_stream_from_before_its_own_directory(self, tmp_path, bucket, s3):
        # Its log and its metadata at the prefix itself; a new box sees the
        # new layout, and must still find it there.
        log = litelink.new(
            tmp_path / "a",
            "t",
            schema=_log.with_system(streamcast.to_arrow(SCHEMA)),
            published=bucket,
            s3_options=s3,
        )
        stream = streamcast.StreamHandle("t", log=log, owns_log=True)
        try:
            await stream.send_many([{"i": i} for i in range(3)])
            stream.ensure_metadata()
        finally:
            await stream.aclose()

        assert _versions.fetch(bucket, "t", s3) is not None, "at the prefix itself"
        with pytest.raises(FileExistsError, match=r"StreamHandle\.restore"):
            streamcast.StreamHandle.open(
                streamcast.Stream(
                    spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                    storage=streamcast.Storage(
                        root=Path(tmp_path / "b").absolute(),
                        published=bucket,
                        s3_options=s3,
                    ),
                ),
                initialization="create_if_absent",
            )

    async def test_an_empty_prefix_is_created_on(self, tmp_path, bucket, s3):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        await stream.aclose()

    async def test_an_existing_home_opens_without_asking_the_prefix(
        self, tmp_path, bucket, s3, monkeypatch
    ):
        await _served(tmp_path, bucket, s3)

        def refused(*_: object, **__: object) -> None:
            raise AssertionError("an existing stream must open without a listing")

        monkeypatch.setattr(_versions, "occupied", refused)
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        await stream.aclose()
