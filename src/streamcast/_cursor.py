"""Where a consumer keeps the offset it finished with.

`streamcast.connect(uri, cursor=path)` loads it, resumes one above it, and
saves it as the consumer goes — which is the loop every consumer writes
identically, and `examples/consumer.py` wrote by hand before this existed.

**A file, not SQLite.** The question was raised and it is a reasonable one:
SQLite would make the write atomic and durable without thinking about it. The
answer is that `os.replace` is atomic on POSIX and Windows both, which closes
the corruption case in four lines — and the case SQLite would actually earn is
a different one: a consumer that wants its cursor committed in the SAME
transaction as its work. That only works if the cursor lives in the
consumer's OWN database, which is not a file this library can own. So the
cheap case is handled cheaply, and the expensive case is handed back with
`commit()` rather than half-served by a database that holds one integer and
brings a WAL, a shared-memory file and a lock protocol to do it.

**The cursor may lag the consumer. It must never lead it.** Everything here is
arranged around that asymmetry: a cursor behind the work re-delivers messages,
which is safe and visible; a cursor ahead of the work skips them for ever,
which is neither. So

* the offset is saved when the consumer asks for the NEXT message, not when it
  receives this one — coming back for another is the only evidence this
  library has that the last one was handled;
* saves are throttled, because an `os.replace` per message on a fast stream is
  three syscalls per message to record something that is allowed to be stale;
* a subscription that exits with an exception saves nothing, so the message
  whose handler raised is re-delivered.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Final

SAVE_EVERY: Final = 1.0
"""Seconds between saves while a stream is running.

The bound on how much a crash re-delivers, and it is a bound on RE-DELIVERY
rather than on loss — see the module docstring. One second of a 10,000
messages/s feed is 10,000 messages re-handled, which is why a consumer whose
work is not idempotent should be using `commit()` and its own transaction
instead.
"""


class Cursor:
    """One integer on disk: the last offset the consumer finished with."""

    __slots__ = ("_every", "_last", "_path", "_saved")

    def __init__(self, path: str | os.PathLike[str], every: float = SAVE_EVERY) -> None:
        self._path = Path(path)
        self._every = every
        self._last = 0.0
        self._saved: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> int | None:
        """The last offset finished with, or None if there is no usable file.

        **Unreadable is treated as absent, deliberately.** A cursor is a
        recovery hint; refusing to start because it is empty or truncated
        turns a torn write into an outage, when resuming from the beginning of
        what the log holds is both available and safe.
        """
        try:
            text = self._path.read_text().strip()
        except (OSError, ValueError):
            return None

        try:
            return int(text)
        except ValueError:
            return None

    def save(self, offset: int | None, *, force: bool = False) -> None:
        """Record `offset`, at most every `every` seconds unless forced.

        Atomic: written beside the target and renamed over it, so a reader
        sees the old value or the new one and never half of either.
        """
        if offset is None or offset == self._saved:
            return

        now = time.monotonic()
        if not force and now - self._last < self._every:
            return

        temporary = self._path.with_name(f"{self._path.name}.{os.getpid()}.tmp")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(str(offset))
        # Atomic on POSIX and on Windows. No fsync: this survives a process
        # crash, which is the case that happens, and paying ~400 us of fsync
        # per save to also survive a power cut would make the cursor the
        # slowest thing in the consumer.
        os.replace(temporary, self._path)
        self._saved = offset
        self._last = now


__all__ = ["SAVE_EVERY", "Cursor"]
