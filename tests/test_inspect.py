"""Stopped-writer inspection observes durable marks without opening a writer."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from streamcast import Storage, Stream, StreamHandle, StreamSpec, _dedupe, _inspect
from streamcast.__main__ import main
from streamcast._broker.registry import Lock

SHAPE = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
}


async def prepared(tmp_path):
    definition = Stream(
        spec=StreamSpec(name="events", schema=SHAPE), storage=Storage(root=tmp_path)
    )
    handle = StreamHandle.open(definition, initialization="create_if_absent")
    handle.ensure_metadata()
    await handle.send({"value": 1})
    await handle.send({"value": 2})
    assert handle.log is not None
    _dedupe.write_all(handle.log, [("z", 3, 2), ("a", 1, 1)])
    return handle


@pytest.mark.parametrize("owner", ["stream", "log", "alias"])
async def test_inspect_refuses_while_owner_lock_held(
    tmp_path, monkeypatch, capsys, owner
):
    handle = await prepared(tmp_path)
    home = Path(handle.log.root)
    name = handle.log.name
    await handle.aclose()
    if owner == "stream":
        lock_path = tmp_path / ".streamcast-owners/events.lock"
    else:
        lock_path = home / f".streamcast-log-owners/{name}.lock"

    lock = Lock.open(lock_path)
    reads = []

    def forbidden(*args, **kwargs):
        reads.append(args)
        pytest.fail("inspection read storage before refusing the active owner")

    monkeypatch.setattr(_inspect._metadata, "load", forbidden)
    monkeypatch.setattr(_inspect.sqlite3, "connect", forbidden)
    root = tmp_path
    if owner == "alias":
        root = tmp_path / "alias"
        root.symlink_to(home, target_is_directory=True)

    try:
        assert main(["inspect", "--log", str(root), "events", "--json"]) == 3
    finally:
        lock.close()

    assert reads == []
    output = capsys.readouterr()
    assert output.out == ""
    assert "owner" in output.err


async def test_inspect_marks_digest_matches_startup_report(tmp_path, capsys):
    handle = await prepared(tmp_path)
    startup = _inspect.startup_report(handle)
    home = Path(handle.log.root)
    db_path = home / handle.log.name / "buffer.db"
    await handle.aclose()
    before = db_path.read_bytes()
    plain = home / "events.metadata.json"
    stale = json.loads(plain.read_text())
    stale["stream_id"] = "stale-copy"
    plain.write_text(json.dumps(stale))
    assert main(["inspect", "--log", str(tmp_path), "events", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report == startup
    canonical = b'["a",1,1]\n["z",3,2]\n'
    assert report["marks_sha256"] == hashlib.sha256(canonical).hexdigest()
    assert report["marks_count"] == 2
    assert report["end_offset"] == 3  # exclusive, as in StreamHandle
    assert report["last_offset"] == 2
    assert report["buffer_rows"] == 2
    assert report["current_log"] == "events"
    assert report["stream_id"]
    assert report["sealed_through"] == report["published_through"] == 0
    assert db_path.read_bytes() == before


async def test_inspect_sqlite_is_read_only_and_tracks_watermarks(tmp_path, monkeypatch):
    handle = await prepared(tmp_path)
    log = handle.log
    log.seal(flush=True)
    log.publish(flush=True)
    log.evict()
    await handle.aclose()
    connect = sqlite3.connect
    modes = []

    def read_only(database, **kwargs):
        modes.append((database, kwargs))
        db = connect(database, **kwargs)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("CREATE TABLE forbidden (id INTEGER)")

        return db

    monkeypatch.setattr(_inspect.sqlite3, "connect", read_only)
    report = _inspect.inspect_log(tmp_path, "events")
    assert report["end_offset"] == 3
    assert report["buffer_rows"] == 0
    assert report["sealed_through"] == 2
    assert report["published_through"] == 2
    assert len(modes) == 1
    assert modes[0][0].endswith("?mode=ro")
    assert modes[0][1]["uri"] is True


def test_inspect_missing_never_creates(tmp_path):
    root = tmp_path / "absent"
    assert main(["inspect", "--log", str(root), "events", "--json"]) == 2
    assert not root.exists()
