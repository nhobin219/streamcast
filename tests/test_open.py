"""Opening a stream never creates or restores it without the caller asking."""

from __future__ import annotations

import inspect
import shutil
from typing import Any

import litelink
import pytest

from streamcast import (
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    _log,
    _metadata,
    _versions,
    to_arrow,
)

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}


async def test_missing_local_requires_create(tmp_path):
    for restore in (False, True):
        with pytest.raises(FileNotFoundError, match="create_if_absent"):
            StreamHandle.open(
                Stream(
                    spec=StreamSpec(name="t", schema=SCHEMA),
                    storage=Storage(root=tmp_path.absolute()),
                ),
                initialization=("restore_published" if restore else "open_existing"),
            )

    assert not (tmp_path / "t").exists()


@pytest.mark.parametrize("create", [False, True])
@pytest.mark.parametrize("restore", [False, True])
async def test_create_then_open_preserves_rows_and_checks_declaration(
    tmp_path, create, restore
):
    made = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute()),
        ),
        initialization="create_if_absent",
    )
    assert await made.send({"i": 7}) == 1
    await made.aclose()
    opened = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute()),
        ),
        initialization=(
            "restore_published"
            if restore
            else ("create_if_absent" if create else "open_existing")
        ),
    )
    assert opened.end_offset == 2
    await opened.aclose()
    wrong = {
        **SCHEMA,
        "properties": {"other": {"type": "integer"}},
        "required": ["other"],
    }
    with pytest.raises(ValueError, match="declares"):
        StreamHandle.open(
            Stream(
                spec=StreamSpec(name="t", schema=wrong),
                storage=Storage(root=tmp_path.absolute()),
            ),
            initialization="restore_published",
        )


async def test_new_is_removed_and_restore_is_opt_in():
    assert not hasattr(StreamHandle, "new")
    parameters = inspect.signature(StreamHandle.open).parameters
    assert tuple(parameters) == ("stream", "initialization")
    assert parameters["initialization"].default == "open_existing"


@pytest.mark.replication
async def test_empty_prefix_is_missing_unless_create(tmp_path, bucket, s3):
    for restore in (False, True):
        with pytest.raises(FileNotFoundError, match="create_if_absent"):
            StreamHandle.open(
                Stream(
                    spec=StreamSpec(name="t", schema=SCHEMA),
                    storage=Storage(
                        root=tmp_path.absolute(), published=bucket, s3_options=s3
                    ),
                ),
                initialization=("restore_published" if restore else "open_existing"),
            )

    with pytest.raises(FileNotFoundError, match="published"):
        StreamHandle.restore("t", root=tmp_path, published=bucket, s3_options=s3)

    made = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute(), published=bucket, s3_options=s3),
        ),
        initialization="restore_published",
    )
    await made.aclose()


@pytest.mark.replication
@pytest.mark.parametrize("create", [False, True])
async def test_fresh_machine_refuses_or_restores_replica(tmp_path, bucket, s3, create):
    made = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(
                root=(tmp_path / "a").absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="create_if_absent",
    )
    try:
        await made.send({"i": 7})
        assert made.log is not None
        while made.log.seal(flush=True) is not None:
            pass

        made.log.publish(flush=True)
        made.ensure_metadata()
        first = _metadata.load(_metadata.home(tmp_path / "a", "t"), "t")
        assert first is not None
        first_id = first.stream_id
    finally:
        await made.aclose()

    with pytest.raises(FileExistsError, match="restore_published"):
        StreamHandle.open(
            Stream(
                spec=StreamSpec(name="t", schema=SCHEMA),
                storage=Storage(
                    root=(tmp_path / "b").absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization=("create_if_absent" if create else "open_existing"),
        )

    assert not (tmp_path / "b").exists()
    restored = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA, replay_published=True),
            storage=Storage(
                root=(tmp_path / "b").absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="restore_published",
    )
    try:
        restored.ensure_metadata()
        metadata = _metadata.load(_metadata.home(tmp_path / "b", "t"), "t")
        assert metadata is not None and metadata.stream_id == first_id
        assert restored.end_offset is not None and restored.end_offset > 1
        assert restored.log is not None
        table = restored.log.scan(columns=["i"]).read_all()
        assert table.column("i").to_pylist() == [7]
    finally:
        await restored.aclose()


@pytest.mark.replication
async def test_unreachable_store_never_creates(tmp_path, bucket, s3, monkeypatch):

    def unavailable(*args, **kwargs):
        raise OSError("store unreachable")

    monkeypatch.setattr(_versions, "occupied", unavailable)
    for restore in (False, True):
        with pytest.raises(OSError, match="store unreachable"):
            StreamHandle.open(
                Stream(
                    spec=StreamSpec(name="t", schema=SCHEMA),
                    storage=Storage(
                        root=tmp_path.absolute(), published=bucket, s3_options=s3
                    ),
                ),
                initialization=("restore_published" if restore else "create_if_absent"),
            )

    assert not (tmp_path / "t").exists()


async def test_restore_empty_local_prefix_is_missing(tmp_path):
    published = tmp_path / "published"
    published.mkdir()
    with pytest.raises(FileNotFoundError, match="published"):
        StreamHandle.restore("t", root=tmp_path / "new", published=published.as_uri())

    assert not (tmp_path / "new").exists()


@pytest.mark.replication
@pytest.mark.parametrize("create", [False, True])
@pytest.mark.parametrize("restore", [False, True])
async def test_existing_log_opens_without_listing(
    tmp_path, bucket, s3, monkeypatch, create, restore
):

    made = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute(), published=bucket, s3_options=s3),
        ),
        initialization="create_if_absent",
    )
    assert await made.send({"i": 7}) == 1
    await made.aclose()

    def unexpected_listing(*args, **kwargs):
        raise AssertionError("an existing stream must not list the published prefix")

    monkeypatch.setattr(_versions, "occupied", unexpected_listing)
    opened = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute(), published=bucket, s3_options=s3),
        ),
        initialization=(
            "restore_published"
            if restore
            else ("create_if_absent" if create else "open_existing")
        ),
    )
    assert opened.end_offset == 2
    await opened.aclose()


@pytest.mark.replication
@pytest.mark.parametrize("legacy", [False, True])
async def test_open_restores_a_table_without_stream_metadata(
    tmp_path, bucket, s3, legacy
):
    if legacy:
        log = litelink.new(
            tmp_path / "a",
            "t",
            schema=_log.with_system(to_arrow(SCHEMA)),
            published=bucket,
            s3_options=s3,
        )
        made = StreamHandle("t", log=log, owns_log=True)
    else:
        made = StreamHandle.open(
            Stream(
                spec=StreamSpec(name="t", schema=SCHEMA),
                storage=Storage(
                    root=(tmp_path / "a").absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )

    try:
        await made.send({"i": 7})
        assert made.log is not None
        while made.log.seal(flush=True) is not None:
            pass

        made.log.publish(flush=True)
    finally:
        await made.aclose()

    restored = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(
                root=(tmp_path / "b").absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="restore_published",
    )
    try:
        assert restored.log is not None
        assert restored.log.scan(columns=["i"]).read_all().column("i").to_pylist() == [
            7
        ]
    finally:
        await restored.aclose()


@pytest.mark.replication
async def test_open_restores_a_missing_log_with_local_metadata(tmp_path, bucket, s3):

    made = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute(), published=bucket, s3_options=s3),
        ),
        initialization="create_if_absent",
    )
    try:
        await made.send({"i": 7})
        assert made.log is not None
        while made.log.seal(flush=True) is not None:
            pass

        made.log.publish(flush=True)
        made.ensure_metadata()
    finally:
        await made.aclose()

    home = _metadata.home(tmp_path, "t")
    metadata = _metadata.load(home, "t")
    assert metadata is not None
    shutil.rmtree(home / metadata.live_log.name)
    with pytest.raises(FileExistsError, match="restore_published"):
        StreamHandle.open(
            Stream(
                spec=StreamSpec(name="t", schema=SCHEMA),
                storage=Storage(
                    root=tmp_path.absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )

    restored = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="t", schema=SCHEMA),
            storage=Storage(root=tmp_path.absolute(), published=bucket, s3_options=s3),
        ),
        initialization="restore_published",
    )
    try:
        assert restored.log is not None
        assert restored.log.scan(columns=["i"]).read_all().column("i").to_pylist() == [
            7
        ]
        restored.ensure_metadata()
        after = _metadata.load(home, "t")
        assert after is not None and after.stream_id == metadata.stream_id
    finally:
        await restored.aclose()
