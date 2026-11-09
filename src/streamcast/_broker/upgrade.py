"""Explicit, locked registry upgrades; startup never upgrades it."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from streamcast import _metadata
from streamcast._broker.model import BrokerConfig
from streamcast._broker.registry import VERSION, Lock, dumps
from streamcast._spec import Stream, StreamSpec


def _request(name: str, definition: dict, max_message_bytes: int) -> dict:
    # Raw v1 operation targets omit defaults that stream records already hold.
    # StreamSpec shares the v1 defaults except for the byte ceiling, which
    # comes from the accepted record rather than the current host settings.
    spec = StreamSpec.model_validate(
        {
            key: value
            for key, value in definition.items()
            if key not in {"storage_profile", "initialization", "limits"}
        }
        | {"name": name, "max_message_bytes": max_message_bytes}
        | definition.get("limits", {})
    )
    return {
        "spec": spec.model_dump(mode="json"),
        "storage_profile": definition["storage_profile"],
        "initialization": definition.get("initialization", "open_existing"),
    }


def upgrade_registry(config: BrokerConfig) -> None:
    """Upgrade v1, v2 or v3 through individually backed-up, atomic steps.

    Stop the broker first. Each source version has its own fsynced backup
    (``<registry>.v1``, ``<registry>.v2`` or ``<registry>.v3``). Downgrade restores the matching
    backup and loses subsequent changes. V4 fences revisions across delete.
    A rerun at v4 leaves both the high-water mark and backups unchanged.
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
        steps = {1: _upgrade_v1_to_v2, 2: _upgrade_v2_to_v3, 3: _upgrade_v3_to_v4}
        identity = db.execute("SELECT version, broker FROM identity").fetchall()
        if identity == [(VERSION, config.broker_id)]:
            return

        if (
            len(identity) != 1
            or identity[0][1] != config.broker_id
            or identity[0][0] not in steps
            or identity[0][0] >= VERSION
        ):
            raise ValueError("registry identity mismatch: v1, v2 or v3 required")

        version = identity[0][0]
        while version < VERSION and version in steps:
            steps[version](db, path, config)
            version = db.execute("SELECT version FROM identity").fetchone()[0]

    finally:
        if db is not None:
            db.close()

        lock.close()


def _upgrade_v1_to_v2(db: sqlite3.Connection, path: Path, config: BrokerConfig) -> None:
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
            target = _request(
                name,
                record["definition"],
                record["definition"]["limits"]["max_message_bytes"],
            )
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
                    operation["target"] = _request(
                        name,
                        operation["target"],
                        target["spec"]["max_message_bytes"],
                    )
                    db.execute(
                        "UPDATE operations SET body=? WHERE id=?",
                        (dumps(operation), operation_id),
                    )

        db.execute("UPDATE identity SET version = 2")


def _upgrade_v2_to_v3(db: sqlite3.Connection, path: Path, config: BrokerConfig) -> None:
    backup_path = Path(str(path) + ".v2")
    # Only a still-v2 source can reach this step. An interrupted copy may be
    # replaced, but a committed v3 must never touch its rollback database.
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
            if record["pending"] is not None:
                row = db.execute(
                    "SELECT body FROM operations WHERE id=?", (record["pending"],)
                ).fetchone()
                if row is None:
                    raise ValueError(
                        f"registry upgrade: stream {name} pending operation missing"
                    )

                operation = json.loads(row[0])
                record["pending"] = {
                    "kind": operation["kind"],
                    "revision": operation["revision"],
                    "target": operation["target"],
                    "status": (
                        "failed" if operation["status"] == "failed" else "accepted"
                    ),
                    "attempt": 1,
                    "error": operation["error"],
                }
                db.execute(
                    "UPDATE streams SET body=? WHERE name=?", (dumps(record), name)
                )

        db.execute("DROP TABLE operations")
        db.execute("DROP TABLE receipts")
        db.execute("UPDATE identity SET version = 3")


def _upgrade_v3_to_v4(db: sqlite3.Connection, path: Path, config: BrokerConfig) -> None:
    backup_path = Path(str(path) + ".v3")
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
        # V3 cannot delete records, so the loaded maximum and collection
        # counter cover every revision it could have issued.
        mark = max(
            db.execute("SELECT revision FROM collection").fetchone()[0],
            max(
                (
                    json.loads(body)["revision"]
                    for (body,) in db.execute("SELECT body FROM streams")
                ),
                default=0,
            ),
        )
        db.execute("CREATE TABLE high_water (revision INTEGER)")
        db.execute("INSERT INTO high_water VALUES (?)", (mark,))
        db.execute("UPDATE identity SET version = 4")
