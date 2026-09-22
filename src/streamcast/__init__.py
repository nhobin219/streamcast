"""A WebSocket multicaster with replay.

One process holds the upstream subscription; every consumer on the box reads
from it. That is the whole idea, and it exists because the alternative — every
consumer opening its own connection to the exchange — runs into a subscription
limit, pays N times the bandwidth, and gives each consumer a stream that can
quietly differ from its neighbour's. One connection in, one stream out, the
same bytes to everyone.

With a litelink log attached it stops being a fan-out and becomes a
tickerplant — kx's term for a process that captures a feed, writes it to a
log file, and publishes it to registered subscribers, which is this one almost
exactly (https://code.kx.com/q/architecture/). Every message is durable
*before* any subscriber sees it, which means an offset is a resume cursor: a
consumer that falls behind, crashes, or is restarted reconnects with the last
offset it processed and the server replays the gap out of the log before
switching it to live — with no window in which a message is in neither place. `docs/SPEC.md` §3 is where that partition
is argued; `Stream` is where it is enforced.

**The API is `websockets` with one modification.** `serve` and `connect` have
the same shapes and pass their keywords through; the difference is that
iterating a subscription yields `(offset, row)` rather than `message`, because
the offset is what makes a reconnect a resume.

**The schema is yours.** streamcast declares no columns — the log is an
ordinary litelink table with whatever shape you gave it, so every column
prunes, compresses, and is queryable from any Iceberg engine. `send` takes a
row, subscribers receive that row, and the parse happens once at the publisher
rather than once per consumer.

.. code-block:: python

    # server
    log = litelink.new("data", "trades", schema=SCHEMA, sort_by=("event_ts",))
    stream = streamcast.Stream("trades", log=log)

    async with streamcast.serve(stream, "localhost", 8765):
        async for frame in exchange_feed:
            await stream.send(parse(frame))      # a row

    # consumer
    async with streamcast.connect("ws://localhost:8765/trades", offset=123) as sub:
        async for offset, msg in sub:
            ...

**The object model is two classes and two functions.** `Stream` is the
broadcast — offsets, subscribers, replay — and holds no socket. `serve` puts
it behind a port; `connect` reads it. `Subscription` is what a consumer holds,
and it is read-only: it has no `send`, rather than a `send` that raises, for
the reason litelink's read handles have no `append`.

``EARLIEST`` is the offset that means "everything the log still holds".

Every frame on the wire is JSON text — the greeting, then an ``[offset, msg]``
pair per message — so ``wscat ws://localhost:8765/trades?offset=0`` is a working
subscriber with no client library at all. **``msg`` is the row the publisher
sent and nothing else**: no offset key, no injected metadata, so a subscriber
can log it, forward it or append it to another stream whole.
"""

from importlib.metadata import PackageNotFoundError, version

from streamcast._client import Subscription, connect
from streamcast._errors import (
    Close,
    NotReplayable,
    ProtocolError,
    StreamcastError,
    StreamNotFound,
    TooSlow,
)
from streamcast._protocol import EARLIEST, Greeting
from streamcast._server import serve
from streamcast._stream import MAX_BACKLOG, MAX_REPLAY, Stream

try:
    __version__ = version("streamcast")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "EARLIEST",
    "MAX_BACKLOG",
    "MAX_REPLAY",
    "Close",
    "Greeting",
    "NotReplayable",
    "ProtocolError",
    "Stream",
    "StreamNotFound",
    "StreamcastError",
    "Subscription",
    "TooSlow",
    "__version__",
    "connect",
    "serve",
]
