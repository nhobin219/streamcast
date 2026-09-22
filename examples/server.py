"""A live public feed, fanned out to everything on this box.

    just demo                  # this, against Bitstamp BTC/USD trades
    just demo-consumer         # in another terminal, and a third, and a fourth

Bitstamp publishes trades over an unauthenticated websocket, so there is
nothing to configure and no credentials to set. This holds **one** connection
to it and serves every consumer on the box from that — which is the whole
idea, and the reason it is not six connections to Bitstamp.

Every trade goes into a litelink log before any subscriber sees it, so a
consumer that stops and starts again resumes from where it left off rather
than from now. Stop `demo-consumer`, leave it stopped, start it again, and
watch it replay the gap.

**The schema below is this demo's, not streamcast's.** Every field the feed
sends that is worth a column gets one, because that is what makes the log a
table rather than a pile of frames: `SELECT max(price) FROM log` works,
Iceberg statistics prune a query for one minute of trades, and a subscriber
receives the row rather than a blob to parse. Frames that are not trades —
the subscription ack, the reconnect notices — have no row and are dropped
here, which is the feed handler's job in every tickerplant.

`--no-log` is the other end of the range: a pure multicaster, no litelink, no
replay, and `?offset=` refused outright. Right when the stream is a cache
nobody resumes; wrong the first time a consumer restarts.

The loop is two calls:

    message = await feed.recv()
    await stream.send(message)

`send` returns once the message is durable, and it never awaits a consumer —
so a dashboard that stops reading cannot slow the strategy sitting beside it.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
from pathlib import Path

import litelink
import pyarrow as pa
import websockets

import streamcast

FEED = "wss://ws.bitstamp.net"
CHANNEL = "live_trades_btcusd"
SUBSCRIBE = json.dumps({"event": "bts:subscribe", "data": {"channel": CHANNEL}})

SCHEMA = pa.schema(
    [
        # Microseconds, as the feed sends them. Leading column of `sort_by`,
        # so a bounded query on it prunes whole files.
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        # 0 buy, 1 sell, as the feed spells it.
        pa.field("side", pa.int64()),
    ]
)


def row(trade: dict) -> dict:
    """One frame, as columns. The parse happens ONCE, here.

    That is the difference between this and a blob multicaster: every
    subscriber receives the parsed row, so nobody downstream parses it again,
    and the log is queryable by anything that can read Iceberg.
    """
    return {
        "event_ts": int(trade["microtimestamp"]),
        "trade_id": int(trade["id"]),
        "price": float(trade["price"]),
        "amount": float(trade["amount"]),
        "side": int(trade["type"]),
    }


async def publish(stream: streamcast.Stream) -> None:
    """One upstream connection, reconnecting for as long as the server runs.

    `websockets.connect` as an async iterator reconnects with backoff, which
    is the behaviour a server wants: subscribers stay attached across an
    upstream blip, and with a log attached they do not even see it — the
    offsets simply continue.
    """
    async for feed in websockets.connect(FEED):
        try:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                frame = json.loads(message)
                # The subscription ack and the reconnect notices are not
                # trades and have no row, so they stop here. A typed log is
                # what forces that decision to be made once, by the feed
                # handler, instead of by every consumer independently.
                if frame.get("event") != "trade":
                    continue

                await stream.send(row(frame["data"]))

        except websockets.ConnectionClosed:
            print("  upstream dropped; reconnecting")


async def report(stream: streamcast.Stream) -> None:
    """A line every five seconds, and nothing when nothing is happening.

    `end_offset` is None on a server run with `--no-log`: nothing assigns
    offsets there, so there is no count to report and the line says only how
    many subscribers are attached.
    """
    last = stream.end_offset
    while True:
        await asyncio.sleep(5)
        now = stream.end_offset
        if now is None or last is None:
            print(f"  live-only  {stream.subscribers} subscriber(s)")
            last = now
            continue

        if now != last:
            print(
                f"  offset {now - 1:,}  (+{now - last} in 5s)  "
                f"{stream.subscribers} subscriber(s)"
            )
            last = now


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("streamcast-data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="live-only: no durable tier, and `?offset=` is refused",
    )
    args = parser.parse_args()

    # `new` takes the shape and `open` takes none of it, so a restart against
    # an existing log continues its offsets rather than restarting them.
    log = None
    if not args.no_log:
        try:
            log = litelink.open(args.root, "trades")
        except FileNotFoundError:
            log = litelink.new(
                args.root, "trades", schema=SCHEMA, sort_by=("event_ts",)
            )

    with contextlib.ExitStack() as closing:
        if log is not None:
            closing.enter_context(log)

        stream = streamcast.Stream("trades", log=log)
        async with streamcast.serve(stream, args.host, args.port):
            where = "durable" if log else "live-only"
            print(f"serving {CHANNEL} ({where}) at ws://{args.host}:{args.port}/trades")
            print(f"  resuming from offset {stream.end_offset}")
            print("  subscribe:  just demo-consumer")

            stopping = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stopping.set)

            work = [
                asyncio.create_task(publish(stream)),
                asyncio.create_task(report(stream)),
            ]
            try:
                await stopping.wait()
            finally:
                for task in work:
                    task.cancel()

                await asyncio.gather(*work, return_exceptions=True)
                # Subscribers get a 1001 rather than a reset, so their
                # `async for` ends cleanly instead of raising.
                await stream.aclose()

        stopped = stream.end_offset
        where = f"offset {stopped - 1:,}" if stopped is not None else "live-only"
        print(f"\nstopped at {where}")
        if log is not None:
            # Sealing is litelink's, not streamcast's — and it is the one
            # thing worth doing on the way out, because an orderly shutdown
            # is the right moment to close the open group.
            while log.seal() is not None:
                pass


if __name__ == "__main__":
    asyncio.run(main())
