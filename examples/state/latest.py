"""The latest-state subscriber: an order book kept current from a stream.

    uv run python -m examples.state.latest

A stream of order events — new, amended, cancelled, filled — and a subscriber
that keeps the open orders in SQLite (`view.py`). Along the way:

1. **The view resumes on its own.** It is stopped partway, as a crash would
   stop it, while orders keep arriving. Reopened, it reads its own offset and
   continues from the row after, because that offset was committed with the
   rows it covers.
2. **The log gives the same answer.** "Last row by id, where not deleted" is
   one window function over the stored table — what a subscriber with no
   state of its own, or an analyst, would run.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
from pathlib import Path
from typing import Any

import streamcast
from examples.state.view import SCHEMA, View


def order(
    order_id: int, symbol: str, side: str, qty: int, price: float
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "deleted": False,
    }


def gone(order_id: int) -> dict[str, Any]:
    """A tombstone: the order is closed, whether cancelled or filled."""
    return {
        "order_id": order_id,
        "symbol": None,
        "side": None,
        "qty": None,
        "price": None,
        "deleted": True,
    }


BEFORE = [
    order(1, "AAPL", "buy", 100, 190.0),
    order(2, "MSFT", "sell", 50, 410.0),
    order(3, "NVDA", "buy", 200, 120.0),
    order(1, "AAPL", "buy", 60, 190.0),  # amended: a partial fill
    gone(2),  # cancelled
]
AFTER = [
    order(4, "TSLA", "buy", 10, 250.0),
    gone(3),  # filled
    order(1, "AAPL", "buy", 60, 191.0),  # amended: a new price
]

# "Last row by id, where not deleted", over the whole log.
LATEST = """
SELECT order_id, symbol, side, qty, price FROM (
    SELECT *, row_number() OVER (
        PARTITION BY order_id ORDER BY litelink_offset DESC
    ) AS newest
    FROM log
)
WHERE newest = 1 AND NOT deleted
ORDER BY order_id
"""


async def stopped_after(view: View, uri: str, offset: int) -> None:
    """Follow until `offset` is applied, then stop, as a crash would."""
    following = asyncio.create_task(view.follow(uri))
    await view.reached(offset)
    following.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await following


async def main(root: Path) -> dict[str, Any]:
    stream = streamcast.Stream.new("orders", root=root, schema=SCHEMA)
    server = await streamcast.serve(stream, "127.0.0.1", 0, publish=True)
    uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/orders"
    path = root / "orders.db"
    try:
        async with streamcast.publish(uri) as publication:
            first = await publication.send_many(BEFORE)
            assert first[2] is not None

            view = View.open(path)
            await stopped_after(view, uri, first[2])
            stopped_at = view.offset
            view.db.close()

            # Orders keep arriving while the view is down.
            rest = await publication.send_many(AFTER)
            assert rest[-1] is not None

            view = View.open(path)
            resumed_after = view.offset
            assert resumed_after is not None
            await stopped_after(view, uri, rest[-1])
            book = view.orders()

        assert stream.log is not None
        from_log = stream.log.sql(LATEST).read_all().to_pylist()
    finally:
        server.close()
        await server.wait_closed()

    print(
        f"view stopped after offset {stopped_at}; reopened, it read {resumed_after} "
        f"from its own table and resumed at {resumed_after + 1}"
    )
    print("open orders, from the view:")
    for row in book:
        print(
            f"  {row['order_id']}  {row['symbol']:<5} {row['side']:<4} {row['qty']:>4} @ {row['price']}"
        )

    print(f"the log's window function agrees: {from_log == book}")
    return {
        "stopped_at": stopped_at,
        "resumed_after": resumed_after,
        "book": book,
        "from_log": from_log,
    }


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(main(Path(directory)))
