"""A keyed table log of orders, kept as a table in SQLite by a subscriber.

What state a stream holds is the application's to define. This one writes a
common shape, the keyed table log:

* **Each row is an order's whole state**, not a patch. An amended order is a
  second row, carrying every field again.
* **`order_id` is the key**, because this schema says so. streamcast has no
  keys; the offset is a row's only identity.
* **`deleted: true` retracts an order**, the standard log-compaction
  tombstone. It is a row like any other; streamcast has no delete.

Given that shape, the table the log stands for is "the last row by id, where
not deleted", and this file keeps it as a subscriber reads: an upsert per
row, or a delete for a tombstone.

**The view is its own cursor.** The offset it has applied is written in the
SAME transaction as the row, so the two cannot disagree: a view reopened after
a crash resumes at exactly `offset + 1`, having lost nothing and applied
nothing twice. `cursor=` could not promise that here — it saves as the loop
asks for the next row, which is a separate write from this one.

**A fork is a copy.** `fork()` copies the database into a private one at the
offset it has applied, which is all a branch needs to start from (see
`branches.py`).
"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import TYPE_CHECKING, Any

import streamcast

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

ORDER: dict[str, Any] = {
    "order_id": {"type": "integer"},
    "symbol": {"type": ["string", "null"]},
    "side": {"type": ["string", "null"]},
    "qty": {"type": ["integer", "null"]},
    "price": {"type": ["number", "null"]},
    "deleted": {"type": "boolean"},
}

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": ORDER,
    "required": ["order_id", "deleted"],
}

_TABLES = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    symbol TEXT, side TEXT, qty INTEGER, price REAL,
    at_offset INTEGER NOT NULL  -- the row this state came from
);
CREATE TABLE IF NOT EXISTS applied (
    one INTEGER PRIMARY KEY CHECK (one = 1),
    at_offset INTEGER NOT NULL
);
"""


class View:
    """Open orders by id, and the offset they are current to."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db
        self._changed = asyncio.Condition()

    @classmethod
    def open(cls, path: Path | str = ":memory:") -> View:
        db = sqlite3.connect(path)
        db.executescript(_TABLES)
        return cls(db)

    @property
    def offset(self) -> int | None:
        """The last offset applied, or None for a view that has applied nothing."""
        row = self.db.execute("SELECT at_offset FROM applied").fetchone()
        return None if row is None else row[0]

    def apply(self, offset: int, row: Mapping[str, Any]) -> None:
        """One row, and the offset it came from, in one transaction."""
        with self.db:
            if row["deleted"]:
                self.db.execute(
                    "DELETE FROM orders WHERE order_id = ?", (row["order_id"],)
                )
            else:
                self.db.execute(
                    "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        row["order_id"],
                        row["symbol"],
                        row["side"],
                        row["qty"],
                        row["price"],
                        offset,
                    ),
                )

            self.db.execute("INSERT OR REPLACE INTO applied VALUES (1, ?)", (offset,))

    def orders(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT order_id, symbol, side, qty, price FROM orders ORDER BY order_id"
        )
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor]

    def fork(self) -> View:
        """A private copy, current to the same offset."""
        copy = sqlite3.connect(":memory:")
        self.db.backup(copy)
        return View(copy)

    async def follow(self, uri: str, where: Mapping[str, Any] | None = None) -> None:
        """Apply every row after `offset`, then follow live, until cancelled.

        Cancelling is safe at any point: it lands at an `await`, between rows,
        never inside `apply`'s transaction.
        """
        start = streamcast.EARLIEST if self.offset is None else self.offset + 1
        async with streamcast.connect(uri, offset=start, where=where) as sub:
            await self._apply_all(sub)

    async def _apply_all(self, sub: streamcast.Subscription) -> None:
        async for offset, _ts, row in sub:
            # None only on a stream without a log, which has no past to
            # resume from and so nothing a view could be current to.
            assert offset is not None
            self.apply(offset, row)
            async with self._changed:
                self._changed.notify_all()

    async def reached(self, offset: int, timeout: float = 10.0) -> None:
        """Wait until this view has applied `offset`, which it must be sent.

        Bounded, because an offset outside this view's filter never arrives,
        and a demo that waits for one should say so rather than hang.
        """
        try:
            async with asyncio.timeout(timeout), self._changed:
                await self._changed.wait_for(
                    lambda: self.offset is not None and self.offset >= offset
                )

        except TimeoutError:
            msg = f"offset {offset} not applied in {timeout:g}s (at {self.offset})"
            raise TimeoutError(msg) from None
