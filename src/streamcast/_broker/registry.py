"""Durable definitions and one pending intent per stream; never log authority."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import threading
from functools import wraps
from pathlib import Path
from typing import Any

VERSION = 4


def dumps(value: Any) -> str:
    # Property order matters to schema/wire order and target equality.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def serialized(call):
    @wraps(call)
    def invoke(self, *args, **kwargs):
        # SQLite's connection is shared across worker threads. Its own mutex
        # does not make a multi-statement Python transaction exclusive.
        with self.mutex:
            return call(self, *args, **kwargs)

    return invoke


class Lock:
    def __init__(self, handle: Any) -> None:
        self.handle = handle

    @classmethod
    def open(cls, path: Path) -> Lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+b")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            handle.close()
            raise

        return cls(handle)

    def close(self) -> None:
        self.handle.close()


class Registry:
    def __init__(self, db: sqlite3.Connection, lock: Lock | None) -> None:
        self.db = db
        self.lock = lock
        self.mutex = threading.RLock()

    @classmethod
    def memory(cls, broker_id: str) -> Registry:
        db = sqlite3.connect(":memory:", check_same_thread=False)
        cls.initialize(db, broker_id)
        return cls(db, None)

    @staticmethod
    def initialize(db, broker_id):
        with db:
            db.execute("CREATE TABLE identity (version INTEGER, broker TEXT)")
            db.execute("INSERT INTO identity VALUES (?, ?)", (VERSION, broker_id))
            db.execute(
                "CREATE TABLE streams (name TEXT PRIMARY KEY, body TEXT NOT NULL)"
            )
            db.execute("CREATE TABLE collection (revision INTEGER)")
            db.execute("INSERT INTO collection VALUES (0)")
            db.execute("CREATE TABLE high_water (revision INTEGER)")
            db.execute("INSERT INTO high_water VALUES (0)")

    @classmethod
    def open(cls, path: Path, broker_id: str, *, initialize: bool = False) -> Registry:
        path = path.resolve()
        lock = Lock.open(path.with_suffix(path.suffix + ".lock"))
        db = None
        try:
            if initialize:
                # Exclusive file creation distinguishes provisioning from a
                # restart with a lost registry. Never initialize on startup.
                with path.open("xb"):
                    pass

            elif not path.is_file():
                raise FileNotFoundError("registry missing")

            db = sqlite3.connect(path, check_same_thread=False)
            db.execute("PRAGMA synchronous=FULL")
            if initialize:
                cls.initialize(db, broker_id)

                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)

            identity = db.execute("SELECT version, broker FROM identity").fetchall()
            if identity in ([(1, broker_id)], [(2, broker_id)], [(3, broker_id)]):
                raise ValueError(
                    f"registry version {identity[0][0]}: run upgrade_registry"
                )

            if identity != [(VERSION, broker_id)]:
                raise ValueError("registry identity mismatch")

            return cls(db, lock)
        except BaseException:
            if db is not None:
                db.close()

            lock.close()
            raise

    @serialized
    def load(self) -> tuple[dict, int]:
        return (
            {
                name: json.loads(body)
                for name, body in self.db.execute("SELECT name, body FROM streams")
            },
            self.db.execute("SELECT revision FROM collection").fetchone()[0],
        )

    @serialized
    def high_water(self) -> int:
        return self.db.execute("SELECT revision FROM high_water").fetchone()[0]

    @serialized
    def accept(self, record: dict) -> None:
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO streams VALUES (?,?)",
                (record["name"], dumps(record)),
            )
            self.db.execute("UPDATE collection SET revision=revision+1")
            self.db.execute(
                "UPDATE high_water SET revision=max(revision, ?)",
                (record["revision"],),
            )

    @serialized
    def delete(self, name: str) -> None:
        with self.db:
            self.db.execute("DELETE FROM streams WHERE name=?", (name,))
            self.db.execute("UPDATE collection SET revision=revision+1")

    @serialized
    def update(self, record: dict) -> None:
        with self.db:
            self.db.execute(
                "UPDATE streams SET body=? WHERE name=?",
                (dumps(record), record["name"]),
            )

    @serialized
    def close(self) -> None:
        self.db.close()
        if self.lock is not None:
            self.lock.close()
