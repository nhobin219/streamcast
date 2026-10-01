"""A producer: Bitstamp's live BTC/USD trades, published to a broker.

    just demo                    # this, a broker, and a consumer
    uv run python -m examples.trades.producer --uri ws://127.0.0.1:8765/trades

Bitstamp publishes trades over an unauthenticated websocket, so there is
nothing to configure and no credentials to set. This holds **one** connection
to it and publishes every trade to the broker, which serves every consumer on
the box from its log. That is the whole idea, and the reason it is not one
connection to Bitstamp per consumer.

**The parse happens once, here.** Each frame becomes a row of the stream's
schema (`schema.json`), so consumers receive the row, not the frame, and the
log is a table: `SELECT max(price) FROM log` works, and Iceberg statistics
prune a query for one minute of trades. Frames that are not trades — the
subscription ack, the reconnect notices — have no row and stop here, which is
the feed handler's job in every tickerplant.

`publish` returns once the broker has the row durably, and the broker never
awaits a consumer, so a dashboard that stops reading cannot slow the strategy
sitting beside it.
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
CHANNEL = "live_trades_btcusd"
SUBSCRIBE = json.dumps({"event": "bts:subscribe", "data": {"channel": CHANNEL}})


def row(trade: dict[str, Any]) -> dict[str, Any]:
    """One trade frame, as a row of `schema.json`."""
    return {
        "event_ts": int(trade["microtimestamp"]),
        "trade_id": int(trade["id"]),
        "price": float(trade["price"]),
        "amount": float(trade["amount"]),
        "side": int(trade["type"]),
    }


async def relay(publication: streamcast.Publication) -> None:
    """One upstream connection, reconnecting for as long as this runs.

    `websockets.connect` as an async iterator reconnects with backoff, and
    the broker's offsets simply continue across an upstream blip.
    """
    async for feed in websockets.connect(FEED):
        try:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                frame = json.loads(message)
                if frame.get("event") == "trade":
                    await publication.send(row(frame["data"]))

        except websockets.ConnectionClosed:
            print("upstream dropped; reconnecting", flush=True)


async def main(uri: str) -> None:
    async with streamcast.publish(uri) as publication:
        print(f"publishing {CHANNEL} to {uri}", flush=True)
        await relay(publication)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", default="ws://127.0.0.1:8765/trades")
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.uri))
