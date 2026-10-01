"""A live order book, kept by a browser page from a stream: `just demo book`.

Bitstamp publishes every change to its BTC/USD order book over an
unauthenticated websocket: an order created, changed (partly filled) or
deleted, each with the order's id. That is a keyed table log already, so this
relays it as one, into a stream called `orders`:

* `order_id` is the key;
* a created or changed order is its whole current state;
* a deleted order is a tombstone, `deleted: true`.

`book.html` is served from the stream's own port and subscribes to it. The
page keeps the table the log stands for, the last row by `order_id` where not
deleted, in AG Grid as rows arrive, about a hundred a second. A reload
replays the stream from the start and rebuilds the same book, then goes live;
a dropped connection resumes after the last row it applied.

It shows the orders placed since the demo started: one resting before then
appears only if it changes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import tempfile
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import websockets

import streamcast

if TYPE_CHECKING:
    from websockets.asyncio.server import ServerConnection
    from websockets.http11 import Request, Response

FEED = "wss://ws.bitstamp.net"
SUBSCRIBE = json.dumps(
    {"event": "bts:subscribe", "data": {"channel": "live_orders_btcusd"}}
)
PAGE = (Path(__file__).parent / "book.html").read_text()

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "side": {"type": ["string", "null"]},
        "price": {"type": ["number", "null"]},
        "amount": {"type": ["number", "null"]},
        "event_ts": {"type": "integer"},  # microseconds, as the feed sends them
        "deleted": {"type": "boolean"},
    },
    "required": ["order_id", "event_ts", "deleted"],
}


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


async def relay(stream: streamcast.Stream) -> None:
    """Bitstamp's order events into the stream, reconnecting as needed."""
    async for feed in websockets.connect(FEED):
        try:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                frame = json.loads(message)
                if frame.get("event") in (
                    "order_created",
                    "order_changed",
                    "order_deleted",
                ):
                    await stream.send(row(frame["event"], frame["data"]))

        except websockets.ConnectionClosed:
            print("  upstream dropped; reconnecting", flush=True)


def page(connection: ServerConnection, request: Request) -> Response | None:
    """`book.html` at `/`; every other path is the stream's to answer."""
    if request.path not in ("/", "/index.html"):
        return None

    response = connection.respond(HTTPStatus.OK, PAGE)
    del response.headers["Content-Type"]
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    return response


async def main(root: Path, host: str, port: int) -> None:
    stream = streamcast.Stream.new("orders", root=root, schema=SCHEMA)
    async with streamcast.serve(stream, host, port, process_request=page):
        print(f"open http://{host}:{port}/   Ctrl-C to stop", flush=True)
        await relay(stream)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument(
        "--root", type=Path, help="keep the log here (default: a temporary directory)"
    )
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with (
        contextlib.suppress(KeyboardInterrupt),
        tempfile.TemporaryDirectory() as directory,
    ):
        asyncio.run(
            main(arguments.root or Path(directory), arguments.host, arguments.port)
        )
