"""A v1 registry is backed up, converted atomically, and never opened implicitly."""

from __future__ import annotations

import copy
import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from streamcast import Storage, Stream, StreamHandle, StreamSpec, upgrade_registry
from streamcast._broker.registry import Lock, Registry, dumps
from streamcast._broker.runtime import BrokerRuntime
from tests.test_broker import SHAPE, managed, put

pytest_plugins = ["tests.test_broker"]

# The normalized 0.26 declaration and record, including resolved byte ceiling.
V1_DEFINITION: dict[str, Any] = {
    "schema": SHAPE,
    "sort_by": [],
    "storage_profile": "local",
    "initialization": "create_if_absent",
    "exactly_once": False,
    "replay_published": False,
    "group_commit": True,
    "max_replay": 100000,
    "limits": {
        "max_backlog": 8192,
        "max_inbound": 65536,
        "max_in_flight": 64,
        "max_message_bytes": 1048576,
    },
}
V1_RECORD: dict[str, Any] = {
    "name": "events",
    "definition": V1_DEFINITION,
    "desired": "stopped",
    "revision": 3,
    "stream_id": None,
    "pending": "pending",
    "profile_revision": "1",
}
V1_OPERATION: dict[str, Any] = {
    "id": "pending",
    "name": "events",
    "kind": "definition",
    "revision": 3,
    "target": {
        "schema": SHAPE,
        "storage_profile": "local",
        "initialization": "create_if_absent",
    },
    "status": "accepted",
    "error": None,
}


def seed_v1(config, records=None, operation=None):
    with sqlite3.connect(config.registry) as db:
        db.execute("UPDATE identity SET version=1")
        for record in records or [V1_RECORD]:
            db.execute(
                "INSERT INTO streams VALUES (?,?)", (record["name"], dumps(record))
            )

        db.execute(
            "INSERT INTO operations VALUES (?,?)",
            ("pending", dumps(operation or V1_OPERATION)),
        )
        db.execute(
            "INSERT INTO receipts VALUES (?,?,?,?)",
            (
                "admin",
                "pre-upgrade",
                "v1-digest",
                dumps({"status": 202, "operation_id": "pending", "revision": 3}),
            ),
        )


def state(path):
    with sqlite3.connect(path) as db:
        return {
            table: db.execute("SELECT * FROM " + table).fetchall()
            for table in ("identity", "streams", "operations", "receipts", "collection")
        }


async def test_explicit_upgrade_backup_pending_intent_and_receipts(config):
    seed_v1(config)
    original = state(config.registry)
    with pytest.raises(ValueError, match="registry version 1: run upgrade_registry"):
        Registry.open(config.registry, config.broker_id)

    upgrade_registry(config)
    backup = Path(str(config.registry) + ".v1")
    assert state(backup) == original
    converted = state(config.registry)
    assert converted["identity"] == [(2, config.broker_id)]
    assert converted["receipts"] == original["receipts"]
    record = json.loads(converted["streams"][0][1])
    assert set(record) == {
        "name",
        "stream",
        "storage_profile",
        "initialization",
        "desired",
        "revision",
        "stream_id",
        "pending",
    }
    full = Stream.model_validate(record["stream"])
    assert full.storage == config.storage_profiles["local"]
    assert full.spec.max_message_bytes == 1048576
    assert full.spec.max_publishers == 4096
    operation = json.loads(converted["operations"][0][1])
    assert operation["target"] == {
        "spec": record["stream"]["spec"],
        "storage_profile": "local",
        "initialization": "create_if_absent",
    }
    with pytest.raises(ValueError, match="v1 required"):
        upgrade_registry(config)

    assert state(backup) == original
    async with managed(config) as (owner, client, _):
        assert owner.operations["pending"]["status"] == "succeeded"
        assert owner.records["events"]["pending"] is None
        retry = await put(owner, client, "events", {"schema": SHAPE}, key="pre-upgrade")
        assert retry.status_code == 409
        assert retry.json()["error"]["code"] == "idempotency_mismatch"


@pytest.mark.parametrize("status", ["accepted", "failed"])
@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"limits": {"max_inbound": 17}},
        {
            "limits": {"max_message_bytes": 32768},
            "sort_by": ["value"],
            "exactly_once": True,
            "replay_published": True,
            "group_commit": False,
            "max_replay": None,
        },
    ],
)
async def test_upgrade_raw_pending_defaults_and_retry(
    config, monkeypatch, status, changes
):
    # 0.26 stores the raw PUT target, beside the resolved declaration. The
    # current host ceiling can differ from the one that accepted that PUT.
    raw = {"schema": SHAPE, "storage_profile": "local"} | changes
    record = copy.deepcopy(V1_RECORD)
    resolved = record["definition"]
    resolved.update({k: v for k, v in changes.items() if k != "limits"})
    resolved["initialization"] = "open_existing"
    resolved["limits"]["max_message_bytes"] = 32768
    resolved["limits"].update(changes.get("limits", {}))
    operation = copy.deepcopy(V1_OPERATION) | {"target": raw, "status": status}
    seed_v1(config, [record], operation)
    before = state(config.registry)

    upgrade_registry(config)

    after = state(config.registry)
    assert state(Path(str(config.registry) + ".v1")) == before
    assert after["receipts"] == before["receipts"]
    converted = json.loads(after["streams"][0][1])
    target = json.loads(after["operations"][0][1])["target"]
    assert target == {
        "spec": converted["stream"]["spec"],
        "storage_profile": "local",
        "initialization": "open_existing",
    }
    assert target["spec"] == {
        "name": "events",
        "schema": SHAPE,
        "sort_by": resolved["sort_by"],
        "exactly_once": resolved["exactly_once"],
        "replay_published": resolved["replay_published"],
        "group_commit": resolved["group_commit"],
        "max_replay": resolved["max_replay"],
        "max_publishers": 4096,
        **resolved["limits"],
    }

    async def fail_reconcile(self, record):
        raise RuntimeError("injected recovery failure")

    reconcile = BrokerRuntime.reconcile
    monkeypatch.setattr(BrokerRuntime, "reconcile", fail_reconcile)
    async with managed(config) as (owner, client, _):
        # Force recovery to fail so a v2 retry must match the converted intent.
        monkeypatch.setattr(BrokerRuntime, "reconcile", reconcile)
        assert owner.operations["pending"]["status"] == "failed"
        assert owner.records["events"]["pending"] == "pending"
        retry = await put(
            owner,
            client,
            "events",
            target["spec"],
            revision=3,
            key="v2-retry",
            initialization="open_existing",
        )
        assert retry.status_code == 202, retry.text
        assert owner.records["events"]["pending"] is None
        assert owner.operations[retry.json()["operation_id"]]["status"] == "succeeded"


@pytest.mark.parametrize("bad", ["profile", "identity", "conversion", "pending"])
async def test_upgrade_rolls_back_all_records_and_can_retry(config, tmp_path, bad):
    # The first record is valid and converted before the second fails.
    first = copy.deepcopy(V1_RECORD) | {"pending": None}
    second: dict[str, Any] = copy.deepcopy(V1_RECORD) | {
        "name": "other",
        "pending": None,
    }
    if bad == "profile":
        second["definition"]["storage_profile"] = "missing"
    elif bad == "identity":
        second["stream_id"] = "absent-on-disk"
    elif bad == "conversion":
        second["definition"]["limits"]["max_inbound"] = 0
    else:
        second["pending"] = "pending"
        first["pending"] = None

    seed_v1(config, [first, second])
    if bad == "pending":
        with sqlite3.connect(config.registry) as db:
            operation = copy.deepcopy(V1_OPERATION)
            operation["target"]["limits"] = {"max_inbound": 0}
            db.execute("UPDATE operations SET body=?", (dumps(operation),))

    original = state(config.registry)
    with pytest.raises(ValueError):
        upgrade_registry(config)

    assert state(config.registry) == original
    assert state(Path(str(config.registry) + ".v1")) == original
    # Repair v1 and rerun over an existing backup (including an interrupted copy).
    with sqlite3.connect(config.registry) as db:
        db.execute("DELETE FROM streams WHERE name='other'")
        db.execute("UPDATE streams SET body=?", (dumps(first),))

    repaired = state(config.registry)
    Path(str(config.registry) + ".v1").write_bytes(b"interrupted backup")
    upgrade_registry(config)
    assert state(Path(str(config.registry) + ".v1")) == repaired
    registry = Registry.open(config.registry, config.broker_id)
    try:
        assert registry.db.execute("SELECT version FROM identity").fetchone() == (2,)
    finally:
        registry.close()


async def test_bound_upgrade_checks_authoritative_metadata(config):
    full = Stream(
        spec=StreamSpec(name="events", schema=SHAPE),
        storage=config.storage_profiles["local"],
    )
    handle = StreamHandle.open(full, initialization="create_if_absent")
    handle.ensure_metadata()
    identity = handle._stream_id
    await handle.aclose()
    seed_v1(config, [copy.deepcopy(V1_RECORD) | {"stream_id": identity}])
    before = state(config.registry)
    with pytest.raises(ValueError, match="identity mismatch"):
        upgrade_registry(
            replace(
                config,
                storage_profiles={"local": Storage(root=full.storage.root / "wrong")},
            )
        )

    assert state(config.registry) == before
    upgrade_registry(config)
    registry = Registry.open(config.registry, config.broker_id)
    try:
        assert registry.load()[0]["events"]["stream_id"] == identity
    finally:
        registry.close()


def test_upgrade_uses_registry_lock_and_identity(config):
    seed_v1(config)
    original = state(config.registry)
    lock = Lock.open(config.registry.with_suffix(".db.lock"))
    try:
        with pytest.raises(BlockingIOError):
            upgrade_registry(config)
    finally:
        lock.close()

    assert state(config.registry) == original
    assert not Path(str(config.registry) + ".v1").exists()
    with pytest.raises(ValueError, match="identity mismatch"):
        upgrade_registry(replace(config, broker_id="wrong"))

    assert state(config.registry) == original
    assert not Path(str(config.registry) + ".v1").exists()
