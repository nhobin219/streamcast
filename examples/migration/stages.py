"""The pipeline's streams, and the code each node runs.

Production is A -> B -> C:

* **A, `orders`**: raw orders.
* **B, `positions`**: each symbol's net position after every order.
* **C, `alerts`**: a row whenever a position reaches the limit.

The migration changes B's output schema: `position` becomes `net_qty`, and a
`notional` column is added. Its shadow runs beside production:

* **D, `positions_v2`**: B's code, changed to write the new schema.
* **E, `alerts_shadow`**: C's code, changed to read it. Its output schema is
  unchanged, so its rows should equal C's exactly.

Every output row carries `order_offset`, the offset in A it came from. That
is what makes old and new comparable row for row, by a join, rather than by
time.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

LIMIT = 100
"""A position this large, long or short, raises an alert."""


def _schema(**columns: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": columns, "required": list(columns)}


_INT = {"type": "integer"}
_TEXT = {"type": "string"}
_NUMBER = {"type": "number"}

ORDERS = _schema(symbol=_TEXT, side=_TEXT, qty=_INT, price=_NUMBER)
POSITIONS = _schema(order_offset=_INT, symbol=_TEXT, position=_INT)
POSITIONS_V2 = _schema(order_offset=_INT, symbol=_TEXT, net_qty=_INT, notional=_NUMBER)
ALERTS = _schema(order_offset=_INT, symbol=_TEXT, position=_INT)


def signed(order: dict[str, Any]) -> int:
    return order["qty"] if order["side"] == "buy" else -order["qty"]


def positions():  # noqa: ANN201 — a step, see `node.Step`
    """B: the net position of the order's symbol, after it."""
    held: defaultdict[str, int] = defaultdict(int)

    def step(offset: int, order: dict[str, Any]) -> list[dict[str, Any]]:
        held[order["symbol"]] += signed(order)
        return [
            {
                "order_offset": offset,
                "symbol": order["symbol"],
                "position": held[order["symbol"]],
            }
        ]

    return step


def positions_v2(*, bug: bool = False):  # noqa: ANN201
    """D: B, migrated. `bug` plays a migration that got sells wrong."""
    held: defaultdict[str, int] = defaultdict(int)

    def step(offset: int, order: dict[str, Any]) -> list[dict[str, Any]]:
        qty = abs(signed(order)) if bug else signed(order)
        held[order["symbol"]] += qty
        net = held[order["symbol"]]
        return [
            {
                "order_offset": offset,
                "symbol": order["symbol"],
                "net_qty": net,
                "notional": round(net * order["price"], 2),
            }
        ]

    return step


def alerts():  # noqa: ANN201
    """C: an alert when a position reaches the limit."""

    def step(_offset: int, row: dict[str, Any]) -> list[dict[str, Any]]:
        if abs(row["position"]) < LIMIT:
            return []

        return [
            {
                "order_offset": row["order_offset"],
                "symbol": row["symbol"],
                "position": row["position"],
            }
        ]

    return step


def alerts_v2():  # noqa: ANN201
    """E: C, reading the new schema and writing the old one."""

    def step(_offset: int, row: dict[str, Any]) -> list[dict[str, Any]]:
        if abs(row["net_qty"]) < LIMIT:
            return []

        return [
            {
                "order_offset": row["order_offset"],
                "symbol": row["symbol"],
                "position": row["net_qty"],
            }
        ]

    return step
