"""Definition forwarding, failed-open cleanup, and static handle ownership."""

from __future__ import annotations

from typing import get_args

import litelink
import pytest
from pydantic import ValidationError

import streamcast
from streamcast import (
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    _log,
    _stream,
    _versions,
)
from streamcast._spec import Initialization
from tests.conftest import trade
from tests.test_asgi import running

SCHEMA = {"type": "object", "properties": {"i": {"type": "integer"}}, "required": ["i"]}


@pytest.fixture
def definition(tmp_path):
    return Stream(
        spec=StreamSpec(
            name="events",
            schema=SCHEMA,
            sort_by=("i",),
            exactly_once=True,
            replay_published=True,
            group_commit=False,
            max_replay=29,
            max_publishers=7,
            max_backlog=17,
            max_inbound=19,
            max_in_flight=23,
            max_message_bytes=4096,
        ),
        storage=Storage(
            root=tmp_path / "data",
            published=(tmp_path / "published").as_uri(),
            replicate=True,
            config=litelink.LogConfig(target_seal_size=4096),
            s3_options=litelink.S3Options(
                access_key="test-key", secret_key="test-secret"
            ),
        ),
    )


def log_engine(definition, tmp_path, monkeypatch, initialization):
    """Use a real log while replacing only storage acquisition, never the factory."""
    log = litelink.new(
        tmp_path / "engine",
        "events",
        schema=_log.with_system(definition.spec.arrow),
        sort_by=("i",),
    )
    observed = {}

    def acquire(root, name, **kwargs):
        observed["open"] = (root, name, kwargs)
        if initialization == "restore_published":
            raise _stream._PublishedStream("published replica")

        return log, None

    def restore(root, name, **kwargs):
        observed["restore"] = (root, name, kwargs)
        return log

    monkeypatch.setattr(_stream, "_open_or_create", acquire)
    monkeypatch.setattr(_stream, "_open_retired", lambda *a, **k: None)
    monkeypatch.setattr(litelink, "restore", restore)
    monkeypatch.setattr(_versions, "occupied", lambda *a, **k: "published")
    return log, observed


@pytest.mark.parametrize("initialization", get_args(Initialization))
async def test_every_definition_field_reaches_each_initialization(
    definition, tmp_path, monkeypatch, initialization
):
    log, observed = log_engine(definition, tmp_path, monkeypatch, initialization)
    handle = StreamHandle.open(definition, initialization=initialization)
    try:
        assert handle.definition is definition
        assert handle.name == definition.spec.name
        assert handle.log is log
        assert handle.replicate is True
        assert handle._s3 is definition.storage.s3_options
        assert handle._owned is log
        for field in (
            "max_replay",
            "replay_published",
            "group_commit",
            "exactly_once",
            "max_backlog",
            "max_inbound",
            "max_in_flight",
            "max_message_bytes",
        ):
            assert getattr(handle, "_" + field) == getattr(definition.spec, field), (
                field
            )

        assert handle._writer is not None
        assert handle._writer._publishers is not None
        assert handle._writer._publishers._capacity == definition.spec.max_publishers
        root, name, args = observed[
            "restore" if initialization == "restore_published" else "open"
        ]
        assert root == definition.storage.root / "events"
        assert name == "events"
        assert args["sort_by"] == definition.spec.sort_by
        assert args["config"] is definition.storage.config
        assert args["s3_options"] is definition.storage.s3_options
        assert args["published"] == (tmp_path / "published" / "events").as_uri()
        if initialization == "restore_published":
            assert args["schema"] == _log.with_system(definition.spec.arrow)
            assert "replica_reserve" not in args and "published_reserve" not in args
        else:
            assert args["schema"] == definition.spec.schema_
            assert args["create"] is (initialization == "create_if_absent")
            assert args["prefix"] == definition.storage.published
    finally:
        await handle.aclose()


@pytest.mark.parametrize("initialization", get_args(Initialization))
def test_constructor_failure_closes_acquired_log(
    definition, tmp_path, monkeypatch, initialization
):
    log, _ = log_engine(definition, tmp_path, monkeypatch, initialization)
    closed = []
    close = log.close

    def track_close():
        closed.append(True)
        close()

    def fail(*args, **kwargs):
        raise RuntimeError("constructor failed")

    monkeypatch.setattr(log, "close", track_close)
    monkeypatch.setattr(StreamHandle, "__init__", fail)
    with pytest.raises(RuntimeError, match="constructor failed"):
        StreamHandle.open(definition, initialization=initialization)

    assert closed == [True]


def test_composed_definition_is_frozen_and_rejects_unknown_fields(definition):
    assert Stream.model_validate_json(definition.model_dump_json()) == definition
    with pytest.raises(ValidationError):
        definition.spec = definition.spec

    with pytest.raises(ValidationError):
        Stream(spec=definition.spec, storage=definition.storage, extra=True)  # ty: ignore[unknown-argument]


async def test_static_live_lent_and_owned_handles_share_lifespan(tmp_path, log):
    live = StreamHandle("live", schema=SCHEMA, max_backlog=13)
    lent = StreamHandle("lent", log=log, max_inbound=31)
    owned = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="owned", schema=SCHEMA),
            storage=Storage(root=tmp_path / "owned-data"),
        ),
        initialization="create_if_absent",
    )
    app = streamcast.create_app(
        streams=[live, lent, owned], config=streamcast.BrokerConfig(maintain=False)
    )
    async with running(app) as base:
        assert set(app.state.broker.generations) == {"live", "lent", "owned"}
        for handle, row in ((live, {"i": 1}), (lent, trade(0)), (owned, {"i": 2})):
            async with streamcast.connect(f"{base}/{handle.name}") as subscriber:
                offset = await handle.send(row)
                received_offset, _, received = await subscriber.recv()
                assert received_offset == offset
                assert received == row

        assert live.definition is lent.definition is None
        assert owned.definition is not None

    assert log.end_offset() == 2
    assert owned.log is not None
    with pytest.raises(Exception):  # noqa: B017  # litelink's closed SQLite handle
        owned.log.end_offset()


def test_unknown_initialization_is_rejected_before_storage(definition):
    with pytest.raises(ValueError, match="invalid initialization"):
        StreamHandle.open(definition, initialization="invalid")  # ty: ignore[invalid-argument-type]
