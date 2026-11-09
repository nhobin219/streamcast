"""V3 folds intent without rewriting its active definition or rollback copy."""

from __future__ import annotations

import copy
import json
import os
import sqlite3
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from streamcast import upgrade_registry
from streamcast._broker import upgrade
from streamcast._broker.registry import Registry, dumps
from streamcast._broker.runtime import BrokerRuntime
from tests.test_broker import definition
from tests.test_registry_upgrade import seed_v1, state

pytest_plugins = ["tests.test_broker"]


def seed_v2(config, status="running"):
    seed_v1(config)
    with sqlite3.connect(config.registry) as db:
        upgrade._upgrade_v1_to_v2(db, config.registry, config)
        operation = json.loads(db.execute("SELECT body FROM operations").fetchone()[0])
        operation.update(
            status=status, error="previous_error" if status == "failed" else None
        )
        db.execute("UPDATE operations SET body=?", (dumps(operation),))

    return state(config.registry)


@pytest.mark.parametrize("status", ["accepted", "running", "failed"])
def test_fold_inspected_before_startup_and_durable_backup(config, monkeypatch, status):
    monkeypatch.setattr(upgrade, "VERSION", 3)
    original = seed_v2(config, status)
    record = json.loads(original["streams"][0][1])
    operation = json.loads(original["operations"][0][1])
    backup = Path(str(config.registry) + ".v2")
    backup.write_bytes(b"interrupted copy")
    fsync = os.fsync
    synced = []

    def watched(fd):
        synced.append(stat.S_ISREG(os.fstat(fd).st_mode))
        assert state(config.registry) == original
        assert state(backup) == original
        fsync(fd)

    monkeypatch.setattr(upgrade.os, "fsync", watched)
    upgrade_registry(config)
    assert synced == [True, False]
    converted = state(config.registry)
    assert set(converted) == {"identity", "streams", "collection"}
    assert converted["identity"] == [(3, config.broker_id)]
    assert converted["collection"] == original["collection"]
    expected = copy.deepcopy(record)
    expected["pending"] = {
        "kind": operation["kind"],
        "revision": operation["revision"],
        "target": operation["target"],
        "status": "failed" if status == "failed" else "accepted",
        "attempt": 1,
        "error": operation["error"],
    }
    assert json.loads(converted["streams"][0][1]) == expected
    assert state(backup) == original
    before = backup.read_bytes(), backup.stat().st_mtime_ns
    upgrade_registry(config)

    assert (backup.read_bytes(), backup.stat().st_mtime_ns) == before
    assert synced == [True, False]


class BeforeCommit(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        if sql == "UPDATE identity SET version = 3":
            raise OSError("injected before commit")

        return super().execute(sql, *args, **kwargs)


def test_fault_before_commit_rolls_back_tables_and_records(config, monkeypatch):
    original = seed_v2(config)
    connect = sqlite3.connect

    def injected(*args, **kwargs):
        return connect(*args, **kwargs, factory=BeforeCommit)

    monkeypatch.setattr(upgrade.sqlite3, "connect", injected)
    with pytest.raises(OSError, match="injected before commit"):
        upgrade_registry(config)

    monkeypatch.undo()
    assert state(config.registry) == original
    assert state(Path(str(config.registry) + ".v2")) == original
    upgrade_registry(config)
    assert state(config.registry)["identity"] == [(4, config.broker_id)]


def test_dangling_pending_aborts_with_source_and_backup_intact(config):
    seed_v2(config)
    with sqlite3.connect(config.registry) as db:
        db.execute("DELETE FROM operations")

    original = state(config.registry)
    with pytest.raises(
        ValueError, match="^registry upgrade: stream events pending operation missing$"
    ):
        upgrade_registry(config)

    assert state(config.registry) == original
    assert state(Path(str(config.registry) + ".v2")) == original


def test_v3_broker_refuses_v2_until_explicit_upgrade(config):
    seed_v2(config)
    with pytest.raises(ValueError, match="registry version 2: run upgrade_registry"):
        Registry.open(config.registry, config.broker_id)


@pytest.mark.parametrize("pending", [False, True])
async def test_default_maintenance_registry_and_pending_replay(
    config, pending, monkeypatch
):
    registry = Registry.open(config.registry, config.broker_id)
    owner = BrokerRuntime(config, registry)
    await owner.mutate(
        "events",
        "definition",
        definition(max_message_bytes=4096),
        None,
        "*",
        storage_profile="local",
        initialization="create_if_absent",
    )
    record = owner.records["events"]
    record["stream"]["spec"]["maintenance"] = {"dedicated": False}
    record["desired"] = "serving"
    if pending:
        record["pending"]["target"]["spec"]["maintenance"] = {}

    else:
        record["pending"] = None

    registry.update(record)
    registry.close()

    update = Registry.update
    writes = []

    def old_shape(registry, record):
        assert "maintenance" not in record["stream"]["spec"]
        intent = record["pending"]
        if intent is not None and intent["kind"] == "definition":
            assert "maintenance" not in intent["target"]["spec"]

        writes.append(copy.deepcopy(record))
        update(registry, record)

    monkeypatch.setattr(Registry, "update", old_shape)
    # Reading a default field does not re-admit an existing declaration
    # against a host's newly lowered transport ceiling.
    recovered = await BrokerRuntime.open(replace(config, transport_max_bytes=1024))
    try:
        assert writes
        assert recovered.observed["events"]["state"] == "serving"
        record = recovered.records["events"]
        assert record["pending"] is None
        assert "maintenance" not in record["stream"]["spec"]
        assert recovered.registry.load()[0]["events"] == record
        stream = recovered.generations["events"].stream
        assert await stream.send({"value": 1}) == 1
        assert stream.definition is not None
        assert stream.definition.spec.maintenance.dedicated is False
    finally:
        await recovered.close()


@pytest.mark.parametrize("pending", [False, True])
async def test_nondefault_maintenance_recovers_without_readmission(config, pending):
    config = replace(config, max_dedicated=1)
    registry = Registry.open(config.registry, config.broker_id)
    owner = BrokerRuntime(config, registry)
    await owner.mutate(
        "events",
        "definition",
        definition(maintenance={"dedicated": True, "compact_every": 9}),
        None,
        "*",
        storage_profile="local",
        initialization="create_if_absent",
    )
    record = copy.deepcopy(owner.records["events"])
    record["desired"] = "serving"
    if not pending:
        record["pending"] = None

    registry.update(record)
    registry.close()
    recovered = await BrokerRuntime.open(replace(config, max_dedicated=0))
    try:
        assert recovered.records["events"]["pending"] is None
        assert recovered.observed["events"]["state"] == "serving"
        assert recovered.generations["events"].maintenance.compact_every == 9
        assert recovered.dedicated_count() == 1
        assert await recovered.generations["events"].stream.send({"value": 1}) == 1
    finally:
        await recovered.close()
