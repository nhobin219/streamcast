"""The definition factory, static serving and the ownership boundary."""

from __future__ import annotations

import shutil
from typing import Any, get_args

import litelink
import pytest
from pydantic import ValidationError

import streamcast
from streamcast._spec import Initialization

SCHEMA = {"type": "object", "properties": {"i": {"type": "integer"}}, "required": ["i"]}


def definition(root, published=None, options=None):
    return streamcast.Stream(
        spec=streamcast.StreamSpec(
            name="rows",
            schema=SCHEMA,
            sort_by=("i",),
            exactly_once=True,
            replay_published=True,
            group_commit=False,
            max_replay=17,
            max_publishers=3,
            max_backlog=19,
            max_inbound=23,
            max_in_flight=7,
            max_message_bytes=4096,
        ),
        storage=streamcast.Storage(
            root=root,
            published=published,
            replicate=published is not None,
            config=litelink.LogConfig(target_seal_size=2048),
            s3_options=options,
        ),
    )


@pytest.mark.parametrize(
    ("storage_kind", "initialization"),
    [
        ("local", "open_existing"),
        ("local", "create_if_absent"),
        *[
            pytest.param("s3", intent, marks=pytest.mark.replication)
            for intent in get_args(Initialization)
        ],
    ],
)
async def test_factory_observes_every_field_under_every_initialization(
    tmp_path, initialization, storage_kind, request, monkeypatch
):
    published = request.getfixturevalue("bucket") if storage_kind == "s3" else None
    options = request.getfixturevalue("s3") if storage_kind == "s3" else None
    restore_calls = []
    restore_log = litelink.restore

    def restoring(*args, **kwargs):
        restore_calls.append(kwargs)
        return restore_log(*args, **kwargs)

    monkeypatch.setattr(litelink, "restore", restoring)

    declared = definition(tmp_path / "data", published, options)
    if initialization != "create_if_absent":
        previous = streamcast.StreamHandle.open(
            declared, initialization="create_if_absent"
        )
        await previous.send({"i": 1})
        previous.ensure_metadata()
        assert previous.log is not None
        while previous.log.seal(flush=True) is not None:
            pass

        previous.log.publish(flush=True)
        await previous.aclose()
        if initialization == "restore_published":
            shutil.rmtree(declared.storage.root)

    handle = streamcast.StreamHandle.open(declared, initialization=initialization)
    try:
        if initialization == "restore_published":
            (restored,) = restore_calls
            assert restored["s3_options"] is declared.storage.s3_options
            assert restored["config"] == declared.storage.config
            assert restored["sort_by"] == declared.spec.sort_by

        else:
            assert restore_calls == []

        assert handle.definition is declared
        assert handle.name == declared.spec.name
        assert handle.log is not None
        assert handle.log.root == declared.storage.root / "rows"
        if published is not None:
            assert handle.log.published == published + "/rows"

        assert handle.log.config == declared.storage.config
        assert handle.log.sort_by == declared.spec.sort_by
        assert handle.schema is not None
        assert streamcast.to_arrow(handle.schema) == declared.spec.arrow
        assert handle._s3 is declared.storage.s3_options  # noqa: SLF001
        assert handle.replicate is declared.storage.replicate
        assert handle._exactly_once is True  # noqa: SLF001
        assert handle._replay_published is True  # noqa: SLF001
        assert handle._group_commit is False  # noqa: SLF001
        assert handle._max_replay == 17  # noqa: SLF001
        assert (
            handle._max_backlog,
            handle._max_inbound,
            handle._max_in_flight,
            handle._max_message_bytes,
        ) == (19, 23, 7, 4096)  # noqa: SLF001
        assert handle._writer is not None  # noqa: SLF001
        assert handle._writer._publishers is not None  # noqa: SLF001
        assert handle._writer._publishers._capacity == 3  # noqa: SLF001
        assert await handle.send({"i": 2}) is not None
    finally:
        await handle.aclose()


async def test_factory_closes_the_opened_log_when_constructor_validation_fails(
    tmp_path,
):
    logs = []

    class InvalidHandle(streamcast.StreamHandle):
        def __init__(self, *args: Any, log: Any, **kwargs: Any):
            logs.append(log)
            kwargs["max_backlog"] = 0
            super().__init__(*args, log=log, **kwargs)

    with pytest.raises(ValueError):
        InvalidHandle.open(definition(tmp_path), initialization="create_if_absent")

    (log,) = logs
    with pytest.raises(Exception, match="closed"):
        log.end_offset()


async def test_live_lent_and_owned_handles_serve_and_close_according_to_ownership(
    tmp_path, log, serve
):
    live = streamcast.StreamHandle("live")
    lent = streamcast.StreamHandle("lent", log=log)
    owned = streamcast.StreamHandle.open(
        definition(tmp_path / "owned"), initialization="create_if_absent"
    )
    owned_log = owned.log
    assert live.definition is lent.definition is None
    async with serve(live, lent, owned) as uri:
        base = uri.rsplit("/", 1)[0]
        for handle, row in (
            (live, {"i": 1}),
            (lent, {"event_ts": 1}),
            (owned, {"i": 2}),
        ):
            async with streamcast.connect(f"{base}/{handle.name}") as sub:
                sent = await handle.send(row)
                offset, _, received = await sub.recv()
                assert offset == sent
                assert all(received[k] == v for k, v in row.items())

    assert log.end_offset() == 2
    assert owned_log is not None
    with pytest.raises(Exception, match="closed"):
        owned_log.end_offset()


@pytest.mark.parametrize("failure", ["duplicate", "ceiling"])
async def test_app_validation_leaves_open_handles_with_the_caller(tmp_path, failure):
    owned = streamcast.StreamHandle.open(
        definition(tmp_path), initialization="create_if_absent"
    )
    try:
        reason = "both named" if failure == "duplicate" else "exceeds"
        with pytest.raises(ValueError, match=reason):
            streamcast.create_app(
                streams=[owned, owned] if failure == "duplicate" else [owned],
                config=streamcast.BrokerConfig(
                    transport_max_bytes=8192 if failure == "duplicate" else 1024
                ),
            )

        assert await owned.send({"i": 4}) == 1
    finally:
        await owned.aclose()


def test_definition_is_frozen_required_and_secret_free(tmp_path):
    declared = definition(tmp_path)
    assert set(declared.model_dump()) == {"spec", "storage"}
    assert "s3_options" not in declared.model_dump()["storage"]
    with pytest.raises(ValidationError):
        declared.spec = declared.spec

    for fields in (
        {},
        {"spec": declared.spec},
        {"storage": declared.storage},
        {**dict(declared), "extra": True},
    ):
        with pytest.raises(ValidationError):
            streamcast.Stream.model_validate(fields)


async def test_sidecars_group_static_handles_by_the_handle_flag(tmp_path, monkeypatch):
    from_runtime = []

    def sidecars(routes, replicate):
        from_runtime.append((set(routes), replicate))
        return []

    monkeypatch.setattr("streamcast._broker.runtime._sidecars", sidecars)
    first = streamcast.StreamHandle("first", replicate=True)
    second = streamcast.StreamHandle("second")
    app = streamcast.create_app(
        streams=[first, second], config=streamcast.BrokerConfig(maintain=False)
    )
    async with app.router.lifespan_context(app):
        assert from_runtime == [({"first"}, True), ({"second"}, False)]


async def test_partial_startup_closes_only_handles_already_wired(tmp_path, monkeypatch):
    handles = [
        streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name=name, schema=SCHEMA),
                storage=streamcast.Storage(root=tmp_path),
            ),
            initialization="create_if_absent",
        )
        for name in ("wired", "unwired")
    ]
    wired, unwired = handles
    original = streamcast.StreamHandle.ensure_metadata

    def fail_unwired(self):
        if self is unwired:
            raise RuntimeError("startup interrupted")

        return original(self)

    monkeypatch.setattr(streamcast.StreamHandle, "ensure_metadata", fail_unwired)
    app = streamcast.create_app(
        streams=handles, config=streamcast.BrokerConfig(maintain=False)
    )
    try:
        with pytest.raises(RuntimeError, match="startup interrupted"):
            async with app.router.lifespan_context(app):
                pytest.fail("startup must fail")

        assert wired.log is not None
        with pytest.raises(Exception, match="closed"):
            wired.log.end_offset()

        assert await unwired.send({"i": 1}) == 1
    finally:
        await unwired.aclose()
