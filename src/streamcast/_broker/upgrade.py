"""Explicit, locked conversion of a v1 registry; startup never upgrades it."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from streamcast import _metadata
from streamcast._broker.model import BrokerConfig
from streamcast._broker.registry import VERSION, Lock, dumps
from streamcast._spec import Stream, StreamSpec


def _request(name: str, definition: dict) -> dict:
    spec = StreamSpec.model_validate(
        {
            key: value
            for key, value in definition.items()
            if key not in {"storage_profile", "initialization", "limits"}
        }
        | {"name": name}
        | definition["limits"]
    )
    return {
        "spec": spec.model_dump(mode="json"),
        "storage_profile": definition["storage_profile"],
        "initialization": definition["initialization"],
    }


def upgrade_registry(config: BrokerConfig) -> None:
    """Back up v1 to ``<registry>.v1`` and convert it in one transaction.

    Stop the broker first and configure the v1 profiles as named Storage
    objects. Bound identities must match disk. Downgrade restores the backup
    and loses subsequent changes. Receipts retain their pre-upgrade digests.
    """
    if config.registry is None:
        raise ValueError("a registry path is required")

    path = config.registry.resolve()
    lock = Lock.open(path.with_suffix(path.suffix + ".lock"))
    db = None
    try:
        if not path.is_file():
            raise FileNotFoundError("registry missing")

        db = sqlite3.connect(path)
        db.execute("PRAGMA synchronous=FULL")
        if db.execute("SELECT version, broker FROM identity").fetchall() != [
            (1, config.broker_id)
        ]:
            raise ValueError("registry identity mismatch: v1 required")

        backup_path = Path(str(path) + ".v1")
        # An interrupted SQLite copy need not be a readable database. The
        # locked source is still v1, so replace that incomplete backup first.
        backup_path.unlink(missing_ok=True)
        backup = sqlite3.connect(backup_path)
        try:
            db.backup(backup)
        finally:
            backup.close()

        with backup_path.open("rb") as handle:
            os.fsync(handle.fileno())

        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

        with db:
            db.execute("BEGIN IMMEDIATE")
            for name, body in db.execute("SELECT name, body FROM streams").fetchall():
                record = json.loads(body)
                target = _request(name, record["definition"])
                profile = config.storage_profiles.get(target["storage_profile"])
                if profile is None:
                    raise ValueError("unknown_storage_profile")

                if record["stream_id"] is not None:
                    metadata = _metadata.load(_metadata.home(profile.root, name), name)
                    if metadata is None or metadata.stream_id != record["stream_id"]:
                        raise ValueError("identity mismatch")

                converted = {
                    key: value
                    for key, value in record.items()
                    if key not in {"definition", "profile_revision"}
                } | {
                    "stream": Stream(
                        spec=StreamSpec.model_validate(target["spec"]), storage=profile
                    ).model_dump(mode="json"),
                    "storage_profile": target["storage_profile"],
                    "initialization": target["initialization"],
                }
                db.execute(
                    "UPDATE streams SET body=? WHERE name=?", (dumps(converted), name)
                )
                if record["pending"] is not None:
                    operation_id = record["pending"]
                    operation = json.loads(
                        db.execute(
                            "SELECT body FROM operations WHERE id=?", (operation_id,)
                        ).fetchone()[0]
                    )
                    if operation["kind"] == "definition":
                        operation["target"] = _request(name, operation["target"])
                        db.execute(
                            "UPDATE operations SET body=? WHERE id=?",
                            (dumps(operation), operation_id),
                        )

            db.execute("UPDATE identity SET version=?", (VERSION,))
    finally:
        if db is not None:
            db.close()

        lock.close()
