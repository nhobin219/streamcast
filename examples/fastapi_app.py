"""A FastAPI service that also serves a stream, on the port it already has.

    just demo-fastapi                                    # terminal 1
    just demo-consumer --uri ws://127.0.0.1:8000/streams/trades   # terminal 2

**You do not call `serve()`.** `serve` and `asgi` are two transports for the
same `Stream`, and a mounted app uses one of them:

    serve(stream, host, port)   owns a socket        -> a standalone server
    asgi(stream)                owns no socket       -> mounted in your app

The `Stream` is the thing either way. It holds the offsets, the log and the
fan-out; the transport only carries frames. So everything below is ordinary
FastAPI, with three lines that are not.

Run it with any ASGI server:

    uvicorn examples.fastapi_app:app --port 8000
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

import streamcast
from streamcast.asgi import asgi

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer", "format": "int64"},
        "symbol": {"type": "string"},
        "price": {"type": "number"},
    },
    "required": ["event_ts", "symbol", "price"],
}

# 1. The stream, created once at import. `new` opens the log at this root, or
#    opens what is already there — the same call `examples/server.py` makes.
#    Module level because both the lifespan and the mount need the same object.
trades = streamcast.Stream.new("trades", root=Path("streamcast-data"), schema=SCHEMA)

# 2. The transport. No host, no port: the ASGI server this runs under owns the
#    socket. `publish=True` lets producers on other machines write to it.
streams = asgi(trades, publish=True)


async def _feed() -> None:
    """Stand-in for whatever actually produces rows.

    In a real service this is the upstream websocket, the queue consumer, or
    the handler that just took an order — anything that calls `trades.send`.
    """
    price = 85_000.0
    while True:
        price += random.uniform(-25, 25)  # noqa: S311 — a demo feed
        await trades.send(
            {
                "event_ts": int(asyncio.get_running_loop().time() * 1e6),
                "symbol": "BTC/USD",
                "price": round(price, 2),
            }
        )
        await asyncio.sleep(0.5)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """3. `async with streams` — and it is not optional.

    This is what starts the maintainer that seals the log, and what closes the
    log on the way out. Starlette does not run a mounted sub-app's lifespan, so
    nothing else will do it: without this the log grows with nothing sealing
    it, which is the one failure the library says out loud it must never allow.
    """
    async with streams:
        producing = asyncio.create_task(_feed())
        try:
            yield

        finally:
            producing.cancel()
            await asyncio.gather(producing, return_exceptions=True)


app = FastAPI(lifespan=lifespan, title="a service that also serves a stream")


@app.get("/health")
async def health() -> dict[str, Any]:
    """An ordinary route, to make the point that this is an ordinary app.

    It reads the stream's own state — the offsets are on the `Stream`, not on
    the transport, so nothing here has to ask the websocket layer anything.
    """
    return {
        "stream": trades.name,
        "end_offset": trades.end_offset,
        "subscribers": trades.subscribers,
        "durable": trades.durable,
    }


# 4. Mounted. Subscribers now connect to ws://host/streams/trades, and the
#    name stays `trades` — the prefix belongs to the mount, not to the stream.
app.mount("/streams", streams)
