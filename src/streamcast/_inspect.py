"""Stopped-owner inspection and the matching pre-admission startup report.

``end_offset`` is exclusive, like StreamHandle's frontier; ``last_offset``
is the last issued offset (zero on an empty log). Watermarks are inclusive.
SQLite is opened with mode=ro, never through litelink's recovering open path.
The broker can call ``startup_report`` while holding its existing owner locks,
with admission and maintenance fenced, to capture the same durable facts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import ExitStack, closing
from pathlib import Path
from typing import TYPE_CHECKING, Any

from streamcast import _dedupe, _metadata
from streamcast._broker.registry import Lock
from streamcast._spec import NAME

if TYPE_CHECKING:
    from streamcast._stream import StreamHandle


class OwnerHeld(Exception):
    """A live owner prevents a consistent stopped-writer report."""


def _report(home: Path, name: str) -> dict[str, Any]:
    metadata = _metadata.load(home, name)
    if metadata is None:
        raise ValueError("stream metadata is required")

    current = metadata.current.name
    if NAME.fullmatch(current) is None:
        raise ValueError("invalid current log name")

    uri = (home / current / "buffer.db").resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        db.execute("BEGIN")
        sequence = db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
        ).fetchone()
        last_offset = 0 if sequence is None else int(sequence[0])
        rows = int(db.execute("SELECT count(*) FROM buffer").fetchone()[0])
        published = db.execute(
            "SELECT v FROM meta WHERE k = 'published_through'"
        ).fetchone()
        published_through = 0 if published is None else int(published[0])
        sealed = db.execute(
            "SELECT max(end_offset) FROM extent WHERE rel_path IS NOT NULL"
        ).fetchone()[0]
        sealed_through = max(
            0 if sealed is None else int(sealed) - 1, published_through
        )
        count = 0
        digest = hashlib.sha256()
        if db.execute(_dedupe._EXISTS, (_dedupe.TABLE,)).fetchone() is not None:
            for p, s, o in db.execute(_dedupe._ALL + " ORDER BY 1, 2, 3"):
                mark = (str(p), int(s), int(o))
                digest.update(
                    (
                        json.dumps(mark, separators=(",", ":"), ensure_ascii=False)
                        + "\n"
                    ).encode()
                )
                count += 1

    return {
        "stream_id": metadata.stream_id,
        "current_log": current,
        "end_offset": last_offset + 1,
        "last_offset": last_offset,
        "buffer_rows": rows,
        "sealed_through": sealed_through,
        "published_through": published_through,
        "marks_sha256": digest.hexdigest(),
        "marks_count": count,
    }


def startup_report(stream: StreamHandle) -> dict[str, Any]:
    """Capture under the broker's owner locks, before admitting any writes."""
    if stream.log is None:
        raise ValueError("a durable stream is required")

    return _report(Path(stream.log.root), stream.name)


def inspect_log(root: Path, name: str) -> dict[str, Any]:
    if NAME.fullmatch(name) is None:
        raise ValueError("invalid stream name")

    root = root.resolve(strict=True)
    home = _metadata.home(root, name)
    if not home.is_dir():
        raise FileNotFoundError("stream home is missing")

    with ExitStack() as locks:

        def claim(path: Path) -> None:
            try:
                lock = Lock.open(path)
            except BlockingIOError:
                raise OwnerHeld("log owner lock is held") from None

            locks.callback(lock.close)

        claim(root / ".streamcast-owners" / f"{name}.lock")
        # Static aliases claim only physical logs. Enumerate directory names,
        # not metadata contents, so a live owner is refused before *any* data
        # read. A legacy shared home is conservatively locked as a whole.
        paths = {(home / name).resolve()}
        paths.update(
            child.resolve()
            for child in home.iterdir()
            if (child / "buffer.db").is_file()
        )
        for location in sorted(paths):
            claim(location.parent / ".streamcast-log-owners" / f"{location.name}.lock")

        return _report(home, name)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="streamcast inspect")
    parser.add_argument("--log", nargs=2, metavar=("ROOT", "NAME"), required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = inspect_log(Path(args.log[0]), args.log[1])
    except OwnerHeld:
        print("inspection refused: log owner lock is held", file=sys.stderr)
        return 3
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        print(
            "inspection failed: unavailable or invalid stream storage", file=sys.stderr
        )
        return 2

    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        for key, value in report.items():
            print(f"{key}: {value}")

    return 0
