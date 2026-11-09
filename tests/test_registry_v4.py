"""The one-time revision fence precedes the first possible forget."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from pathlib import Path

import pytest

from streamcast import upgrade_registry
from streamcast._broker import upgrade
from streamcast._broker.registry import Registry
from tests.test_broker import definition, managed, put
from tests.test_registry_upgrade import seed_v1, state

pytest_plugins = ["tests.test_broker"]


@pytest.mark.parametrize("source", [1, 2, 3])
@pytest.mark.parametrize("counter", [0, 8])
async def test_upgrade_initializes_high_water_once_and_next_create(
    config, source, counter
):
    seed_v1(config)
    with sqlite3.connect(config.registry) as db:
        if source >= 2:
            upgrade._upgrade_v1_to_v2(db, config.registry, config)

        if source == 3:
            upgrade._upgrade_v2_to_v3(db, config.registry, config)

        db.execute("UPDATE collection SET revision=?", (counter,))

    original = state(config.registry)
    with pytest.raises(
        ValueError, match=f"registry version {source}: run upgrade_registry"
    ):
        Registry.open(config.registry, config.broker_id)

    upgrade_registry(config)
    after = state(config.registry)
    assert after["identity"] == [(4, config.broker_id)]
    assert after["high_water"] == [(max(3, counter),)]
    assert after["collection"] == [(counter,)]
    assert json.loads(after["streams"][0][1])["revision"] == 3
    backup = Path(str(config.registry) + f".v{source}")
    assert state(backup) == original
    backup_bytes = backup.read_bytes(), backup.stat().st_mtime_ns
    upgrade_registry(config)
    assert state(config.registry) == after
    assert (backup.read_bytes(), backup.stat().st_mtime_ns) == backup_bytes
    async with managed(config) as (owner, client, _):
        created = await put(owner, client, "new", definition())
        assert created.status_code == 202
        assert created.json()["revision"] == max(3, counter) + 1
        assert owner.registry.high_water() == max(3, counter) + 1


class BeforeV4Commit(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        if sql == "UPDATE identity SET version = 4":
            raise OSError("injected before v4 commit")

        return super().execute(sql, *args, **kwargs)


def test_v4_failure_rolls_back_and_retry_preserves_source_backup(config, monkeypatch):
    seed_v1(config)
    with sqlite3.connect(config.registry) as db:
        upgrade._upgrade_v1_to_v2(db, config.registry, config)
        upgrade._upgrade_v2_to_v3(db, config.registry, config)

    original = state(config.registry)
    backup = Path(str(config.registry) + ".v3")
    backup.write_bytes(b"incomplete")
    connect = sqlite3.connect
    fsync = os.fsync
    synced = []

    def watched(fd):
        synced.append(stat.S_ISREG(os.fstat(fd).st_mode))
        assert state(config.registry) == original
        assert state(backup) == original
        fsync(fd)

    monkeypatch.setattr(upgrade.os, "fsync", watched)

    def failing(*args, **kwargs):
        return connect(*args, **kwargs, factory=BeforeV4Commit)

    monkeypatch.setattr(upgrade.sqlite3, "connect", failing)
    with pytest.raises(OSError, match="injected before v4 commit"):
        upgrade_registry(config)

    monkeypatch.undo()
    assert synced == [True, False]
    assert state(config.registry) == original
    assert state(backup) == original
    upgrade_registry(config)
    assert state(config.registry)["high_water"] == [(3,)]
    assert state(backup) == original


def test_registry_accept_never_lowers_high_water(config):
    registry = Registry.open(config.registry, config.broker_id)
    try:
        registry.accept({"name": "high", "revision": 9})
        registry.accept({"name": "low", "revision": 2})
        assert registry.high_water() == 9
        registry.forget("high")
        assert registry.high_water() == 9
        assert "high" not in registry.load()[0]
    finally:
        registry.close()
