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
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

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


@app.get("/stats")
async def info() -> dict[str, Any]:
    """The facts, unprocessed. `serve(stats=True)` serves this same object.

    The offsets are on the `Stream`, not on the transport, so nothing here
    has to ask the websocket layer anything.
    """
    return asdict(trades.stats)


# How long this application considers silence acceptable. A DOMAIN number: a
# stream that publishes once a day would use 26 hours and be right to.
STALE_AFTER_S = 10.0


@app.get("/health")
async def health() -> JSONResponse:
    """The verdict, which is **this application's** to make, not the library's.

    This is the split worth copying. `stream.stats` reports numbers and never
    classifies them, because a freshness threshold is domain knowledge — the
    demo feed sends twice a second, so ten seconds of silence is broken; a
    stream that publishes once a day at 00:20 UTC is healthy after 23 hours.
    A threshold chosen inside the library would be wrong for one of them and
    would look authoritative to whoever read it.

    Note what `uptime_s` is doing: at startup nothing has been sent by this
    process, so `last_send_age_s` is None while `end_offset` may be in the
    millions. Without the uptime check a fresh restart would report unhealthy
    until the first row arrived.
    """
    stats = trades.stats
    if stats.last_send_age_s is None:
        fresh = stats.uptime_s < STALE_AFTER_S  # just started, nothing yet
    else:
        fresh = stats.last_send_age_s < STALE_AFTER_S

    return JSONResponse(
        {"status": "ok" if fresh else "stale", **asdict(stats)},
        status_code=200 if fresh else 503,
    )


# 4. Mounted. Subscribers now connect to ws://host/streams/trades, and the
#    name stays `trades` — the prefix belongs to the mount, not to the stream.
app.mount("/streams", streams)
