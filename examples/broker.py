"""A live public feed, fanned out to everything on this box.

    just demo                  # this, against Bitstamp BTC/USD trades
    just demo-consumer         # in another terminal, and a third, and a fourth

Bitstamp publishes trades over an unauthenticated websocket, so there is
nothing to configure and no credentials to set. This holds **one** connection
to it and serves every consumer on the box from that — which is the whole
idea, and the reason it is not six connections to Bitstamp.

Every message goes into a litelink log before any subscriber sees it, so a
consumer that stops and starts again resumes from where it left off rather
than from now. Stop `demo-consumer`, leave it stopped, start it again, and
watch it replay the gap.

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
import websockets

import streamcast

FEED = "wss://ws.bitstamp.net"
CHANNEL = "live_trades_btcusd"
SUBSCRIBE = json.dumps({"event": "bts:subscribe", "data": {"channel": CHANNEL}})


async def publish(stream: streamcast.Stream) -> None:
    """One upstream connection, reconnecting for as long as the broker runs.

    `websockets.connect` as an async iterator reconnects with backoff, which
    is the behaviour a broker wants: subscribers stay attached across an
    upstream blip, and with a log attached they do not even see it — the
    offsets simply continue.
    """
    async for feed in websockets.connect(FEED):
        try:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                # The subscription ack and the reconnect notices go down the
                # stream too, deliberately: this is a multicaster, and what
                # the exchange said is what every consumer should see. An
                # application that wants them filtered filters them, once,
                # rather than each consumer guessing.
                await stream.send(message)

        except websockets.ConnectionClosed:
            print("  upstream dropped; reconnecting")


async def report(stream: streamcast.Stream) -> None:
    """A line every five seconds, and nothing when nothing is happening."""
    last = stream.end_offset
    while True:
        await asyncio.sleep(5)
        now = stream.end_offset
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
            log = litelink.new(args.root, "trades", schema=streamcast.SCHEMA)

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

        print(f"\nstopped at offset {stream.end_offset - 1:,}")
        if log is not None:
            # Sealing is litelink's, not streamcast's — and it is the one
            # thing worth doing on the way out, because an orderly shutdown
            # is the right moment to close the open group.
            while log.seal() is not None:
                pass


if __name__ == "__main__":
    asyncio.run(main())
