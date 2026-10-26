"""Static/HTTP wiring parity and the persisted, credential-free definition."""

from __future__ import annotations

import copy
import shutil
from dataclasses import replace
from pathlib import Path
from typing import get_args

import litelink
import pytest
from pydantic import ValidationError

from streamcast import (
    BrokerConfig,
    HistoryGuard,
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    create_app,
)
from streamcast._broker.model import Fault
from streamcast._spec import Initialization
from tests.test_broker import (
    READER,
    SHAPE,
    definition,
    managed,
    put,
    serving,
)

pytest_plugins = ["tests.test_broker"]

OBSERVE = {
    "name": lambda h: h.name,
    "schema_": lambda h: h._declared,
    "sort_by": lambda h: tuple(h.log.sort_by),
    "exactly_once": lambda h: h._exactly_once,
    "replay_published": lambda h: h._replay_published,
    "group_commit": lambda h: h._group_commit,
    "max_replay": lambda h: h._max_replay,
    "max_publishers": lambda h: h._writer._publishers._capacity,
    "max_backlog": lambda h: h._max_backlog,
    "max_inbound": lambda h: h._max_inbound,
    "max_in_flight": lambda h: h._max_in_flight,
    "max_message_bytes": lambda h: h._max_message_bytes,
}
STORAGE_OBSERVE = {
    "root": lambda h: Path(h.log.root),
    "published": lambda h: h.log.published,
    "replicate": lambda h: h.replicate,
    "config": lambda h: h.log.config,
    "s3_options": lambda h: h._s3,
}


# Two real published tables exercise all factory branches, including disk loss.
@pytest.mark.slow
@pytest.mark.parametrize("initialization", get_args(Initialization))
async def test_every_field_static_and_managed(config, tmp_path, initialization):
    spec = StreamSpec(
        name="events",
        schema=SHAPE,
        sort_by=["value"],
        exactly_once=True,
        replay_published=True,
        group_commit=False,
        max_replay=29,
        max_publishers=7,
        max_backlog=17,
        max_inbound=19,
        max_in_flight=23,
        max_message_bytes=4096,
    )
    options = litelink.S3Options(access_key="test-key", secret_key="test-secret")
    table_config = litelink.LogConfig(target_seal_size=4096)
    handles = []
    definitions = []
    for path in ("static", "managed"):
        storage = Storage(
            root=tmp_path / path,
            published=(tmp_path / (path + "-published")).as_uri(),
            replicate=True,
            config=table_config,
            s3_options=options,
        )
        full = Stream(spec=spec, storage=storage)
        definitions.append(full)
        if initialization != "create_if_absent":
            seed = StreamHandle.open(full, initialization="create_if_absent")
            await seed.send({"value": 41})
            assert seed.log is not None
            seed.log.seal(flush=True)
            seed.log.publish(flush=True)
            await seed.aclose()
            if initialization == "restore_published":
                shutil.rmtree(storage.root)

    static = StreamHandle.open(definitions[0], initialization=initialization)
    # Static handles are served, rather than merely compared before admission.
    from_app = create_app(BrokerConfig(maintain=False), streams=static)
    async with from_app.router.lifespan_context(from_app):
        config = replace(config, storage_profiles={"local": definitions[1].storage})
        async with managed(config) as (owner, client, _):
            accepted = await put(
                owner,
                client,
                spec.name,
                spec.model_dump(mode="json"),
                initialization=initialization,
            )
            assert accepted.status_code == 202, accepted.text
            result = await put(
                owner,
                client,
                spec.name,
                {"state": "serving"},
                suffix="/desired",
                revision=1,
            )
            assert (
                owner.operations[result.json()["operation_id"]]["status"] == "succeeded"
            )
            remote = owner.generations[spec.name].stream
            handles = [static, remote]
            assert set(OBSERVE) == set(StreamSpec.model_fields)
            assert set(STORAGE_OBSERVE) == set(Storage.model_fields)
            for key, observe in OBSERVE.items():
                assert observe(static) == observe(remote), key
                expected = spec.arrow if key == "schema_" else getattr(spec, key)
                assert observe(remote) == expected, key

            for handle, full in zip(handles, definitions, strict=True):
                assert handle.log is not None
                assert full.storage.published is not None
                assert STORAGE_OBSERVE["root"](handle) == full.storage.root / spec.name
                assert (
                    STORAGE_OBSERVE["published"](handle)
                    == full.storage.published + "/events"
                )
                assert STORAGE_OBSERVE["replicate"](handle) is True
                assert STORAGE_OBSERVE["config"](handle) == table_config
                assert STORAGE_OBSERVE["s3_options"](handle) == options
                if initialization == "restore_published":
                    assert handle.log.scan(columns=["value"]).read_all().column(
                        "value"
                    ).to_pylist() == [41]
                    assert handle.end_offset is not None and handle.end_offset > 2

            got = await client.get("/control/v1/streams/events")
            assert Stream.model_validate(got.json()["stream"]) == definitions[1]


async def test_recorded_paths_current_keys_and_missing_profile(config, tmp_path):
    old_options = litelink.S3Options(access_key="old-key", secret_key="old-secret")
    new_options = litelink.S3Options(access_key="new-key", secret_key="new-secret")
    original = config.storage_profiles["local"].model_copy(
        update={"s3_options": old_options}
    )
    config = replace(config, storage_profiles={"local": original})
    async with managed(config) as (owner, client, _):
        await serving(owner, client)
        before = (await client.get("/control/v1/streams/events")).json()["stream"]
        assert before["spec"]["max_message_bytes"] is None
        assert "s3_options" not in before["storage"]
        for endpoint in ("/storage-profiles", "/storage-profiles/local", ""):
            discovered = await client.get("/control/v1" + endpoint)
            assert discovered.status_code == 200
            assert "old-key" not in discovered.text
            assert "old-secret" not in discovered.text
            assert "s3_options" not in discovered.text

    moved = Storage(root=tmp_path / "new-root", s3_options=new_options)
    async with managed(replace(config, storage_profiles={"local": moved})) as (
        owner,
        client,
        _,
    ):
        assert (await client.get("/control/v1/streams/events")).json()[
            "stream"
        ] == before
        opened = owner.generations["events"].stream
        assert opened._s3 == new_options
        assert opened.definition.storage.root == original.root
        assert not moved.root.exists()
        assert (await client.get("/control/v1/streams")).json()["streams"][0][
            "stream"
        ] == before

    async with managed(replace(config, storage_profiles={"other": moved})) as (
        owner,
        client,
        _,
    ):
        view = (await client.get("/control/v1/streams/events")).json()
        assert view["stream"] == before
        assert view["observed"]["error"] == "unknown_storage_profile"
        assert not owner.generations
        with pytest.raises(Fault, match="unknown_storage_profile"):
            owner.open_stream(owner.records["events"])

    registry_bytes = config.registry.read_bytes()
    for secret in (b"old-key", b"old-secret", b"new-key", b"new-secret"):
        assert secret not in registry_bytes


async def test_profiles_default_discovery_and_authorization(
    config, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path))
    default = replace(config, storage_profiles={})
    assert default.storage_profiles == {
        "default": Storage(root=tmp_path / ".streamcast/test")
    }
    assert not default.storage_profiles["default"].root.exists()
    async with managed(default) as (owner, client, _):
        profiles = await client.get("/control/v1/storage-profiles")
        assert profiles.json() == {
            "default": default.storage_profiles["default"].model_dump(mode="json")
        }
        single = await client.get("/control/v1/storage-profiles/default")
        assert single.json() == profiles.json()["default"]
        capabilities = (await client.get("/control/v1")).json()
        assert capabilities["storage_profiles"] == {
            "default": {"replicate": False, "publishes": False}
        }
        for path in (
            "/control/v1/storage-profiles",
            "/control/v1/storage-profiles/default",
        ):
            assert (
                await client.get(path, headers={"Authorization": "invalid"})
            ).status_code == 401

            assert (await client.get(path, headers=READER)).status_code == 403

        response = await client.put(
            "/control/v1/streams/events",
            json=definition(),
            headers={"Idempotency-Key": "default", "If-None-Match": "*"},
        )
        assert response.status_code == 202
        await owner.queue.join()
        assert owner.records["events"]["initialization"] == "open_existing"
        assert (
            owner.records["events"]["stream"]["storage"] == profiles.json()["default"]
        )
        bad = await put(owner, client, "other", definition(), storage_profile="missing")
        assert bad.status_code == 422
        assert bad.json()["error"]["available"] == ["default"]

    for broker_id in ("../escape", "bad/name", "", "x\n"):
        with pytest.raises(ValueError, match="broker identity"):
            BrokerConfig(broker_id=broker_id)

    for kwargs in ({"root": Path("relative")}, {"root": tmp_path, "replicate": True}):
        with pytest.raises(ValidationError):
            Storage.model_validate(kwargs)


@pytest.mark.parametrize(
    "bad,code",
    [
        ({"storage_profile": "local"}, "invalid_declaration"),
        ({"limits": {}}, "invalid_declaration"),
        ({"s3_options": {"secret_key": "sensitive"}}, "invalid_declaration"),
        ({"name": "other"}, "name_mismatch"),
        ({"max_in_flight": True}, "invalid_declaration"),
        ({"max_message_bytes": 2**30}, "invalid_declaration"),
    ],
)
async def test_invalid_declaration_is_sanitized(config, bad, code):
    async with managed(config) as (owner, client, _):
        response = await client.put(
            "/control/v1/streams/events",
            json=definition() | bad,
            params={"storage_profile": "local"},
            headers={"Idempotency-Key": "bad", "If-None-Match": "*"},
        )
        assert response.status_code == 422
        error = response.json()["error"]
        assert error["code"] == code
        if code == "invalid_declaration":
            assert error["errors"]
            assert all(set(item) == {"loc", "type"} for item in error["errors"])

        assert "sensitive" not in response.text
        assert not owner.records and not owner.operations


async def test_query_replace_receipts_and_normalized_retry(
    config, tmp_path, monkeypatch
):
    async with managed(config) as (owner, client, _):
        created = await put(owner, client, "events", definition(), key="create")
        assert created.status_code == 202
        mismatch = await put(
            owner, client, "events", definition(), key="create", storage_profile="other"
        )
        assert mismatch.status_code == 409
        assert mismatch.json()["error"]["code"] == "idempotency_mismatch"
        unchanged = copy.deepcopy(owner.records["events"])
        for profile in (None, "local"):
            params = {} if profile is None else {"storage_profile": profile}
            response = await client.put(
                "/control/v1/streams/events",
                json=definition(),
                params=params,
                headers={
                    "Idempotency-Key": "replace-" + str(profile),
                    "If-Match": '"1"',
                },
            )
            assert response.status_code == 200, response.text
            assert owner.records["events"] == unchanged

        changed = await put(
            owner, client, "events", definition(), revision=1, storage_profile="other"
        )
        assert changed.json()["error"]["code"] == "immutable_storage"
        assert changed.status_code == 409
        ignored = await put(
            owner,
            client,
            "events",
            definition(),
            revision=1,
            initialization="restore_published",
        )
        assert ignored.status_code == 200
        assert (
            owner.operations[ignored.json()["operation_id"]]["target"]["initialization"]
            == "create_if_absent"
        )
        assert owner.records["events"]["initialization"] == "create_if_absent"
        for params in (
            {"unknown": "x"},
            [("storage_profile", "local"), ("storage_profile", "other")],
            {"initialization": "invalid"},
        ):
            response = await client.put(
                "/control/v1/streams/other",
                json=definition(name="other"),
                params=params,
                headers={"Idempotency-Key": "query", "If-None-Match": "*"},
            )
            assert response.status_code == 422

        # Failed definition retry compares the normalized spec, including defaults.
        reconcile = owner.reconcile

        async def fail(record):
            raise OSError("injected")

        monkeypatch.setattr(owner, "reconcile", fail)
        failed = await put(owner, client, "failed", definition(), key="failed-create")
        operation = owner.operations[failed.json()["operation_id"]]
        assert operation["status"] == "failed"
        assert set(operation["target"]) == {"spec", "storage_profile", "initialization"}
        monkeypatch.setattr(owner, "reconcile", reconcile)
        retried = await put(
            owner, client, "failed", definition(), revision=1, key="retry"
        )
        assert retried.status_code == 202
        assert owner.operations[retried.json()["operation_id"]]["status"] == "succeeded"


@pytest.mark.parametrize("intent", ["create_if_absent", "restore_published"])
async def test_guard_refuses_any_creating_initialization(config, intent):
    config = replace(
        config, history_guards={"events": HistoryGuard("id", 1, {"value": 1})}
    )
    async with managed(config) as (owner, client, _):
        response = await put(
            owner, client, "events", definition(), initialization=intent
        )
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "history_prefix_protected"
        assert not owner.records


async def test_offline_paths_use_recorded_storage_and_current_keys(
    config, tmp_path, monkeypatch
):
    async with managed(config) as (owner, client, _):
        await serving(owner, client)

    options = litelink.S3Options(access_key="rotated", secret_key="rotated-secret")
    moved = Storage(root=tmp_path / "moved", s3_options=options)
    calls = []

    async def transition(record, storage, kind, target):
        calls.append((storage, kind))

    async def finalize(log, credentials, finish):
        assert Path(log.root).is_relative_to(config.storage_profiles["local"].root)
        assert credentials == options
        calls.append("finalized")

    monkeypatch.setattr("streamcast._broker.runtime.storage", transition)
    monkeypatch.setattr("streamcast._broker.runtime.with_replica", finalize)
    async with managed(replace(config, storage_profiles={"local": moved})) as (
        owner,
        client,
        _,
    ):
        response = await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        assert response.status_code == 202
        assert calls == ["finalized"]
        for kind in ("migration", "retire"):
            await owner.storage_transition(owner.records["events"], kind, {})
            storage, observed_kind = calls[-1]
            assert storage.root == config.storage_profiles["local"].root
            assert storage.s3_options == options
            assert observed_kind == kind

        assert not moved.root.exists()
