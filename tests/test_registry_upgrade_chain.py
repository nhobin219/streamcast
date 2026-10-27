"""Each upgrade step writes its own version, independent of the current one."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from streamcast import upgrade_registry
from streamcast._broker import upgrade
from tests.test_broker import SHAPE
from tests.test_registry_upgrade import seed_v1, state

pytest_plugins = ["tests.test_broker"]


def test_v1_step_writes_literal_v2_shape(config, monkeypatch):
    seed_v1(config)
    original = state(config.registry)
    with sqlite3.connect(config.registry) as db:
        schema = db.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()

    # Changing the binding used by the upgrader must not relabel the v2 step.
    monkeypatch.setattr(upgrade, "VERSION", 3)
    upgrade_registry(config)

    converted = state(config.registry)
    assert converted["identity"] == [(2, config.broker_id)]
    with sqlite3.connect(config.registry) as db:
        assert (
            db.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall() == schema
        )

    spec = {
        "name": "events",
        "schema": SHAPE,
        "sort_by": [],
        "exactly_once": False,
        "replay_published": False,
        "group_commit": True,
        "max_replay": 100000,
        "max_publishers": 4096,
        "max_backlog": 8192,
        "max_inbound": 65536,
        "max_in_flight": 64,
        "max_message_bytes": 1048576,
    }
    assert len(converted["streams"]) == 1
    name, body = converted["streams"][0]
    assert name == "events"
    assert json.loads(body) == {
        "name": "events",
        "stream": {
            "spec": spec,
            "storage": config.storage_profiles["local"].model_dump(mode="json"),
        },
        "storage_profile": "local",
        "initialization": "create_if_absent",
        "desired": "stopped",
        "revision": 3,
        "stream_id": None,
        "pending": "pending",
    }
    assert len(converted["operations"]) == 1
    operation_id, body = converted["operations"][0]
    assert operation_id == "pending"
    assert json.loads(body) == {
        "id": "pending",
        "name": "events",
        "kind": "definition",
        "revision": 3,
        "target": {
            "spec": spec,
            "storage_profile": "local",
            "initialization": "create_if_absent",
        },
        "status": "accepted",
        "error": None,
    }
    assert converted["receipts"] == original["receipts"]
    assert converted["collection"] == original["collection"]
    assert state(Path(str(config.registry) + ".v1")) == original
