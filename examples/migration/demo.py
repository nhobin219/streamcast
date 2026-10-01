"""Test a schema migration against live production data, without touching it.

    uv run python -m examples.migration.demo
    uv run python -m examples.migration.demo --bug    # a migration that is wrong

Production is a pipeline, A -> B -> C (`stages.py`), and the migration
changes B's output schema. Rather than migrate B in place and hope, its
shadow runs beside it:

* **D** is B's code, changed to write the new schema to its own stream.
* **E** is C's code, changed to read it.

D subscribes to A from the start, so it rebuilds B's state from production's
whole history and then follows production live. Nothing in production
changes: D and E are just more subscribers, and a stream never waits on a
subscriber. This demo makes D slow on purpose, and production finishes long
before it does. A shadow slower still is dropped by the server and resumes
where it left off (`node.py`); production never waits for it either way.

**The test is a join.** The comparison is one more subscriber: it watches
the four output streams live, and DuckDB joins old against new, B against D
and C against E, matched on the order each row came from. An empty diff means
the migration does what production does; a non-empty one names the orders
where it does not. Each stream is also a table, so the same join can be run
later over the logs (`stream.log.sql`). Once the diff is empty, the cutover is
C reading D's stream, and B retiring with its history still queryable.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

import streamcast
from examples.migration import stages
from examples.migration.node import Node

if TYPE_CHECKING:
    from examples.migration.node import Step

SYMBOLS = ("AAPL", "MSFT", "NVDA")


def orders(count: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    return [
        {
            "symbol": rng.choice(SYMBOLS),
            "side": rng.choice(("buy", "sell")),
            "qty": rng.randint(1, 100),
            "price": round(rng.uniform(100, 500), 2),
        }
        for _ in range(count)
    ]


async def trade(publication: streamcast.Publication, rows: list[dict[str, Any]]) -> int:
    """Publish `rows` at a production-like pace, in small batches."""
    last = None
    for start in range(0, len(rows), 10):
        last = (await publication.send_many(rows[start : start + 10]))[-1]
        await asyncio.sleep(0.02)

    assert last is not None
    return last


def watching(rows: list[dict[str, Any]]) -> Step:
    """A step that keeps every row it is handed, and publishes nothing."""

    def step(_offset: int, row: dict[str, Any]) -> list[dict[str, Any]]:
        rows.append(row)
        return []

    return step


def diff(
    old: pa.Table, new: pa.Table, old_column: str, new_column: str
) -> list[dict[str, Any]]:
    """Orders whose outputs differ between old and new, or exist in only one."""
    found = duckdb.sql(
        f"""
        SELECT order_offset, old.symbol AS symbol,
               old.{old_column} AS old, new.{new_column} AS new
        FROM old FULL JOIN new USING (order_offset)
        WHERE old.{old_column} IS DISTINCT FROM new.{new_column}
        ORDER BY order_offset
        """
    )
    return [dict(zip(found.columns, row, strict=True)) for row in found.fetchall()]


async def main(root: Path, *, bug: bool = False, count: int = 200) -> dict[str, Any]:
    """Run the demo under `root`: `count` orders of history, then `count` live."""
    streams = {
        "orders": streamcast.Stream.new("orders", root=root, schema=stages.ORDERS),
        "positions": streamcast.Stream.new(
            "positions", root=root, schema=stages.POSITIONS
        ),
        "alerts": streamcast.Stream.new("alerts", root=root, schema=stages.ALERTS),
        "positions_v2": streamcast.Stream.new(
            "positions_v2", root=root, schema=stages.POSITIONS_V2
        ),
        "alerts_shadow": streamcast.Stream.new(
            "alerts_shadow", root=root, schema=stages.ALERTS
        ),
    }
    # No maintainer: a run this short never fills a log enough to seal it. A
    # pipeline that runs for real keeps `serve`'s default, which starts one.
    server = await streamcast.serve(
        streams.values(), "127.0.0.1", 0, publish=True, maintain=False
    )
    base = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    running: list[asyncio.Task[None]] = []
    try:
        async with contextlib.AsyncExitStack() as stack:
            publish = {
                name: await stack.enter_async_context(
                    streamcast.publish(f"{base}/{name}")
                )
                for name in streams
            }

            def start(node: Node) -> Node:
                running.append(asyncio.create_task(node.run()))
                return node

            # The comparison is one more subscriber: it watches the four output
            # streams live, from before anything is written, and keeps their
            # rows for the join at the end.
            outputs = {
                "positions": stages.POSITIONS,
                "alerts": stages.ALERTS,
                "positions_v2": stages.POSITIONS_V2,
                "alerts_shadow": stages.ALERTS,
            }
            seen: dict[str, list[dict[str, Any]]] = {name: [] for name in outputs}
            watchers = {
                name: start(
                    Node(f"watch {name}", f"{base}/{name}", watching(seen[name]), None)
                )
                for name in outputs
            }

            # Production, running over its history.
            b = start(
                Node("B", f"{base}/orders", stages.positions(), publish["positions"])
            )
            c = start(
                Node("C", f"{base}/positions", stages.alerts(), publish["alerts"])
            )
            past, live = orders(count, seed=1), orders(count, seed=2)
            history = await trade(publish["orders"], past)
            await b.reached(history)

            # The shadow, deployed beside it. D starts from the beginning of
            # A, and is slow on purpose: 8 ms a row, against production's 2.
            d = start(
                Node(
                    "D",
                    f"{base}/orders",
                    stages.positions_v2(bug=bug),
                    publish["positions_v2"],
                    delay=0.008,
                )
            )
            e = start(
                Node(
                    "E",
                    f"{base}/positions_v2",
                    stages.alerts_v2(),
                    publish["alerts_shadow"],
                )
            )

            # Production keeps trading.
            began = time.monotonic()
            last = await trade(publish["orders"], live)
            await b.reached(last)
            assert b.published is not None
            await c.reached(b.published)
            production_done = time.monotonic() - began
            shadow_behind = last - (d.done or 0)

            await d.reached(last)
            assert d.published is not None
            await e.reached(d.published)
            shadow_done = time.monotonic() - began

            # Until each watcher has every row its stream was written.
            for name, node in (
                ("positions", b),
                ("alerts", c),
                ("positions_v2", d),
                ("alerts_shadow", e),
            ):
                if node.published is not None:  # None: it published nothing
                    await watchers[name].reached(node.published)

        tables = {
            name: pa.Table.from_pylist(seen[name], schema=streamcast.to_arrow(schema))
            for name, schema in outputs.items()
        }
    finally:
        for task in running:
            task.cancel()

        for task in running:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        server.close()
        await server.wait_closed()

    positions_diff = diff(
        tables["positions"], tables["positions_v2"], "position", "net_qty"
    )
    alerts_diff = diff(
        tables["alerts"], tables["alerts_shadow"], "position", "position"
    )

    print(
        f"{len(past)} orders of history, then {len(live)} live. Production caught "
        f"up {production_done:.2f}s into the live run, the shadow "
        f"{shadow_behind} orders behind it."
    )
    print(f"the shadow caught up at {shadow_done:.2f}s")
    print(f"B against D: {len(positions_diff)} orders differ")
    print(f"C against E: {len(alerts_diff)} alerts differ")
    for row in (positions_diff + alerts_diff)[:5]:
        print(
            f"  order {row['order_offset']} {row['symbol']}: {row['old']} -> {row['new']}"
        )

    return {
        "orders": len(past) + len(live),
        "shadow_behind": shadow_behind,
        "positions_diff": positions_diff,
        "alerts_diff": alerts_diff,
        "alerts": tables["alerts"].num_rows,
        "notional": tables["positions_v2"].column("notional").to_pylist(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--bug", action="store_true", help="deploy a migration that is wrong"
    )
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(main(Path(directory), bug=arguments.bug))
