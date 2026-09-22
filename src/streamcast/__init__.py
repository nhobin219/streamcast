"""A WebSocket multicaster with a durable log behind it.

One process holds the upstream subscription; every consumer on the box reads
from it. That is the whole idea, and it exists because the alternative — every
consumer opening its own connection to the exchange — runs into a subscription
limit, pays N times the bandwidth, and gives each consumer a stream that can
quietly differ from its neighbour's. One connection in, one stream out, the
same bytes to everyone.

With a litelink log attached it stops being a fan-out and becomes a
tickerplant: every message is durable *before* any subscriber sees it, which
means an offset is a resume cursor. A consumer that falls behind, crashes, or
is restarted reconnects with the last offset it processed and the broker
replays the gap out of the log before switching it to live — with no window in
which a message is in neither place. `docs/SPEC.md` §3 is where that partition
is argued; `Stream` is where it is enforced.

**The API is `websockets` with one modification.** `serve` and `connect` have
the same shapes and pass their keywords through; the difference is that
iterating a subscription yields `(offset, message)` rather than `message`,
because the offset is what makes a reconnect a resume.

.. code-block:: python

    # broker
    log = litelink.new("data", "trades", schema=streamcast.SCHEMA)
    stream = streamcast.Stream("trades", log=log)

    async with streamcast.serve(stream, "localhost", 8765):
        async for message in exchange_feed:
            await stream.send(message)

    # consumer
    async with streamcast.connect("ws://localhost:8765/trades", offset=123) as sub:
        async for offset, message in sub:
            ...

**The object model is two classes and two functions.** `Stream` is the
broadcast — offsets, subscribers, replay — and holds no socket. `serve` puts
it behind a port; `connect` reads it. `Subscription` is what a consumer holds,
and it is read-only: it has no `send`, rather than a `send` that raises, for
the reason litelink's read handles have no `append`.

``SCHEMA`` and ``EARLIEST`` are exported because they appear in calls a user
writes — the first is what a streamcast log is created with, the second is the
offset that means "everything you still have".
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
from streamcast._log import SCHEMA
from streamcast._protocol import BINARY, EARLIEST, TEXT, Greeting
from streamcast._server import serve
from streamcast._stream import MAX_BACKLOG, MAX_REPLAY, Stream

try:
    __version__ = version("streamcast")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "BINARY",
    "EARLIEST",
    "MAX_BACKLOG",
    "MAX_REPLAY",
    "SCHEMA",
    "TEXT",
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
