"""A producer: Bitstamp's live BTC/USD order book, as a keyed table log.

    just demo book    # a broker, this, and the page that subscribes

Bitstamp publishes every change to its BTC/USD order book over an
unauthenticated websocket: an order created, changed (partly filled) or
deleted, each with the order's id. That is a keyed table log already, so this
publishes it as one, to the broker's `orders` stream (`schema.json`):

* `order_id` is the key;
* a created or changed order is its whole current state;
* a deleted order is a tombstone, `deleted: true`.

The subscriber is `index.html`, a browser page. It keeps the table the log
stands for, the last row by `order_id` where not deleted, in AG Grid as rows
arrive, about a hundred a second. A reload replays the stream from the start
and rebuilds the same book; a dropped connection resumes after the last row
it applied. It shows the orders placed since the producer started: one
resting before then appears only if it changes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
from typing import Any

import websockets

import streamcast

FEED = "wss://ws.bitstamp.net"
SUBSCRIBE = json.dumps(
    {"event": "bts:subscribe", "data": {"channel": "live_orders_btcusd"}}
)
EVENTS = ("order_created", "order_changed", "order_deleted")


def row(event: str, order: dict[str, Any]) -> dict[str, Any]:
    """One feed event as one row of the keyed table log."""
    deleted = event == "order_deleted"
    return {
        "order_id": int(order["id"]),
        "side": None if deleted else ("buy" if order["order_type"] == 0 else "sell"),
        "price": None if deleted else float(order["price"]),
        "amount": None if deleted else float(order["amount"]),
        "event_ts": int(order["microtimestamp"]),
        "deleted": deleted,
    }


async def relay(publication: streamcast.Publication) -> None:
    """Bitstamp's order events to the broker, reconnecting as needed."""
    async for feed in websockets.connect(FEED):
        try:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                frame = json.loads(message)
                if frame.get("event") in EVENTS:
                    await publication.send(row(frame["event"], frame["data"]))

        except websockets.ConnectionClosed:
            print("upstream dropped; reconnecting", flush=True)


async def main(uri: str) -> None:
    async with streamcast.publish(uri) as publication:
        print(f"publishing Bitstamp's BTC/USD orders to {uri}", flush=True)
        await relay(publication)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", default="ws://127.0.0.1:8767/orders")
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.uri))
