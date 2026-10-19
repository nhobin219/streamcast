"""Exactly-once end to end: an outbox, an inbox, and a crash in between.

    uv run python -m examples.outbox_inbox.demo
    uv run python -m examples.outbox_inbox.demo --count 50 --crash-at 13

Three processes' worth of code, each with its own SQLite database, talking to
one broker over sockets as separate processes would:

* **The producer** places orders. Each order is a row in its own table and a
  row in its outbox, in ONE transaction of its database
  (`streamcast.outbox`): an order is never placed without being published,
  nor published without being placed. It stops halfway with rows still in
  its outbox, as a process that dies would, and a second run ships them.
* **The enricher** reads orders through an inbox (`streamcast.inbox`), keeps
  a running total per symbol, and publishes each enriched order through an
  outbox -- the input row's delete, the total and the output row in ONE
  transaction. It crashes once in the middle of that transaction, and is
  started again: the order it was handling is delivered again, nothing it
  had enqueued in the failed transaction exists, and the totals are right.
* **The consumer** reads the enriched stream through an inbox into a table
  keyed by order id.

Each stream is served with `exactly_once=True`, so the broker appends each
publisher's seq once (`docs/SPEC.md` §6b), and each database holds its own
outbox and inbox (§6c). At the end every order is in the enriched stream's log
exactly once, in the consumer's table exactly once, and the enricher's totals
equal the producer's orders summed. `main` returns what it saw, for
`tests/test_outbox_inbox_example.py`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import tempfile
from pathlib import Path
from typing import Any

import streamcast

SYMBOLS = ("AAPL", "MSFT", "NVDA")

ORDERS = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "symbol": {"type": "string"},
        "qty": {"type": "integer"},
    },
    "required": ["order_id", "symbol", "qty"],
}

ENRICHED = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "symbol": {"type": "string"},
        "qty": {"type": "integer"},
        "running_total": {"type": "integer"},
    },
    "required": ["order_id", "symbol", "qty", "running_total"],
}

POLL = 0.05
"""Seconds between each outbox's and inbox's looks at its own tables:
quicker than the default once a second, so the demo finishes promptly."""


class Crash(Exception):
    """The enricher dying mid-transaction."""


def order(i: int) -> dict[str, Any]:
    return {"order_id": i, "symbol": SYMBOLS[i % len(SYMBOLS)], "qty": 1 + i % 5}


async def produce(uri: str, store: streamcast.Store, ids: range) -> None:
    """Place `ids`: each order and its outbox row in one transaction, then
    wait until the broker holds every one."""
    async with streamcast.outbox(
        uri, publisher="producer", store=store, poll_every=POLL
    ) as out:
        for i in ids:
            async with store.transaction() as tx:
                await tx.execute("INSERT INTO placed VALUES (?)", (i,))
                await out.send(order(i), tx=tx)

        await out.flush(timeout=30)


async def produce_and_die(uri: str, store: streamcast.Store, ids: range) -> None:
    """Place `ids` in one transaction that commits after the sender is gone:
    a process that died between its COMMIT and shipping. The rows are safe in
    the outbox, and the next run's sender ships them."""
    async with store.transaction() as tx:
        async with streamcast.outbox(uri, publisher="producer", store=store) as out:
            for i in ids:
                await tx.execute("INSERT INTO placed VALUES (?)", (i,))
                await out.send(order(i), tx=tx)


async def enrich(
    orders_uri: str,
    enriched_uri: str,
    store: streamcast.Store,
    *,
    start: int | None,
    until: int,
    crash_at: int | None,
) -> None:
    """Consume, transform and publish, one transaction per order.

    `start` places a new subscriber (the orders were published before it
    first ran); a restart passes None and resumes from where it received.
    """
    async with store.transaction() as tx:
        await tx.execute(
            "CREATE TABLE IF NOT EXISTS totals (symbol TEXT PRIMARY KEY, total INTEGER)"
        )

    async with (
        streamcast.inbox(
            orders_uri,
            subscriber="enricher",
            store=store,
            offset=start,
            poll_every=POLL,
        ) as inbox,
        streamcast.outbox(
            enriched_uri, publisher="enricher", store=store, poll_every=POLL
        ) as out,
    ):
        async for _offset, _ts, row, tx in inbox:
            await tx.execute(
                "INSERT INTO totals VALUES (?, ?) ON CONFLICT (symbol) "
                "DO UPDATE SET total = total + excluded.total",
                (row["symbol"], row["qty"]),
            )
            [(total,)] = await tx.fetch(
                "SELECT total FROM totals WHERE symbol = ?", (row["symbol"],)
            )
            await out.send({**row, "running_total": total}, tx=tx)
            if row["order_id"] == crash_at:
                # After the work and the send, before COMMIT: all of it rolls
                # back, and the order is delivered again on the next start.
                raise Crash(row["order_id"])

            if row["order_id"] == until:
                await inbox.commit()
                await out.flush(timeout=30)
                return


async def consume(uri: str, store: streamcast.Store, *, until: int) -> None:
    async with store.transaction() as tx:
        await tx.execute(
            "CREATE TABLE IF NOT EXISTS seen (order_id INTEGER PRIMARY KEY, total INTEGER)"
        )

    async with streamcast.inbox(
        uri, subscriber="consumer", store=store, offset=1, poll_every=POLL
    ) as inbox:
        async for _offset, _ts, row, tx in inbox:
            # A primary key: a duplicate delivery would fail this insert.
            await tx.execute(
                "INSERT INTO seen VALUES (?, ?)",
                (row["order_id"], row["running_total"]),
            )
            if row["order_id"] == until:
                return


async def main(
    root: Path, *, count: int = 20, crash_at: int | None = 7
) -> dict[str, Any]:
    """Run it all under `root`; what each side saw."""
    orders_stream = streamcast.Stream.new(
        "orders", root=root / "broker", schema=ORDERS, exactly_once=True
    )
    enriched_stream = streamcast.Stream.new(
        "enriched", root=root / "broker", schema=ENRICHED, exactly_once=True
    )
    server = await streamcast.serve(
        [orders_stream, enriched_stream], "127.0.0.1", 0, maintain=False
    )
    port = server.sockets[0].getsockname()[1]
    orders_uri = f"ws://127.0.0.1:{port}/orders"
    enriched_uri = f"ws://127.0.0.1:{port}/enriched"
    last = count - 1
    producer = await streamcast.SqliteStore.open(root / "producer.db")
    enricher = await streamcast.SqliteStore.open(root / "enricher.db")
    consumer = await streamcast.SqliteStore.open(root / "consumer.db")
    try:
        async with producer.transaction() as tx:
            await tx.execute("CREATE TABLE placed (order_id INTEGER PRIMARY KEY)")

        # The producer's first run dies with half its orders unshipped.
        await produce_and_die(orders_uri, producer, range(count // 2))
        async with producer.transaction() as tx:
            [(stranded,)] = await tx.fetch("SELECT count(*) FROM streamcast_outbox")

        print(f"producer: died with {stranded} order(s) still in its outbox")
        await produce(orders_uri, producer, range(count // 2, count))
        print(f"producer: {count} orders placed and published")

        crashed = False
        start: int | None = 1
        if crash_at is not None:
            try:
                await enrich(
                    orders_uri,
                    enriched_uri,
                    enricher,
                    start=start,
                    until=last,
                    crash_at=crash_at,
                )
            except Crash:
                crashed = True
                start = None  # it has a saved position now
                print(f"enricher: crashed handling order {crash_at}; starting again")

        await enrich(
            orders_uri, enriched_uri, enricher, start=start, until=last, crash_at=None
        )
        print("enricher: every order enriched")
        await consume(enriched_uri, consumer, until=last)
        print("consumer: every enriched order received")

        enriched_log = enriched_stream.log
        assert enriched_log is not None
        logged = enriched_log.sql('SELECT order_id FROM log ORDER BY "litelink_offset"')
        in_log = [row["order_id"] for row in logged.read_all().to_pylist()]
        async with consumer.transaction() as tx:
            seen = [
                int(i)
                for (i,) in await tx.fetch("SELECT order_id FROM seen ORDER BY 1")
            ]

        async with enricher.transaction() as tx:
            totals = {
                str(s): int(t)
                for s, t in await tx.fetch("SELECT symbol, total FROM totals")
            }

        expected: dict[str, int] = {}
        for i in range(count):
            placed = order(i)
            expected[placed["symbol"]] = (
                expected.get(placed["symbol"], 0) + placed["qty"]
            )

        result = {
            "stranded": int(stranded),
            "crashed": crashed,
            "in_log": in_log,
            "seen": seen,
            "totals": totals,
            "expected_totals": expected,
        }
        once = sorted(in_log) == list(range(count))
        print(
            f"enriched log: {len(in_log)} rows, each order once: {once}; "
            f"totals right: {totals == expected}"
        )
        return result
    finally:
        for store in (producer, enricher, consumer):
            await store.close()

        server.close()
        await server.wait_closed()
        for stream in (orders_stream, enriched_stream):
            with contextlib.suppress(Exception):
                await stream.aclose()


def cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--count", type=int, default=20, help="orders to place")
    parser.add_argument(
        "--crash-at",
        type=int,
        default=7,
        help="the order the enricher crashes on, once (default 7; -1 for none)",
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as root:
        asyncio.run(
            main(
                Path(root),
                count=args.count,
                crash_at=None if args.crash_at < 0 else args.crash_at,
            )
        )


if __name__ == "__main__":
    cli()
