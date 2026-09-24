"""A durable JSON WebSocket pubsub framework built on litelink.

Publishers write, subscribers read, and every message is appended to a
litelink log — an Iceberg table on disk — before any subscriber sees it. A
server holds the upstream connection and every subscriber reads from it,
receiving the same bytes in the same order from one `encode` call.

**The log is the analytical table**, which is why litelink is underneath this
rather than an append-only file. The usual shape is a message log in one
system and an analytical store in another with a pipeline between them; here
there is no extraction step and no second copy, so the Parquet a message was
appended to is the Parquet DuckDB or any other Iceberg engine reads.

A streamcast server is a Python WebSocket tickerplant: a process that captures
a feed, optionally writes it to a log, and publishes it to registered
subscribers (https://code.kx.com/q/architecture/).

With a log attached, every message is durable *before* any subscriber sees it,
so a consumer that falls behind, crashes or restarts reconnects with the last
offset it processed and the server replays the gap before switching it to
live, with no window in which a message is in neither place. Without one the
fan-out is identical, offsets are `null`, and `?offset=` is refused.
`docs/SPEC.md` §3 argues that partition; `Stream` enforces it.

**`serve` and `connect` are the `websockets` API, with three deviations.** They
have the same shapes and pass their keywords through, and `serve` returns an
object that proxies `websockets.Server`. What differs: iterating a
subscription yields `(offset, row)` rather than `message`, because the offset
is what makes a reconnect a resume; a subscription is read-only, with no
`send` rather than a `send` that raises; and `compression` defaults to None
here where `websockets` defaults to `"deflate"`, because permessage-deflate
compresses once per subscriber a frame this encodes once.

`publish` has no `websockets` counterpart — WebSocket defines frames, not
verbs, so both publishing and subscribing here are URL conventions on top of
it. It is shaped like `connect` so it reads the same way.

**The schema is yours, declared in JSON Schema.** streamcast declares no
columns — the log is an ordinary litelink table with whatever shape you gave
it, so every column prunes, compresses, and is queryable from any Iceberg
engine. The wire is JSON, so the columns are declared in JSON Schema and
converted here (`to_arrow`, `from_arrow`); `send` takes a row, subscribers
receive that row, and the parse happens once at the publisher rather than once
per consumer.

.. code-block:: python

    # server — `new` creates the log at data/trades, or opens what is there
    stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA,
                               sort_by=("event_ts",))

    # Fan-out, sealing, compaction and WAL shipping: all of it, one call.
    async with streamcast.serve(stream, "localhost", 8765):
        async for frame in upstream:
            await stream.send(parse(frame))      # a row

    # consumer — `cursor` keeps the resume point, so a restart is a resume
    async with streamcast.connect(
        "ws://localhost:8765/trades", cursor=".trades.offset", catch_up=True
    ) as sub:
        async for offset, msg in sub:
            ...

**`serve` starts everything the stream needs.** A log that nobody seals grows
for ever, and a WAL nobody ships is not replicated, so `serve` runs the
maintainer in a subprocess and litestream as an flock-guarded sidecar. Both
are keyword-controlled (`maintain=`, `replicate=`) for when you run your own.

**The object model is two classes and two functions.** `Stream` is the
broadcast — offsets, subscribers, replay — and holds no socket. `serve` puts
it behind a port; `connect` reads it. `Subscription` is what a consumer holds,
and it is read-only: it has no `send`, rather than a `send` that raises, for
the reason litelink's read handles have no `append`.

**Recovery is three keywords on `connect`.** `cursor=` keeps the last handled
offset on disk; `cursor_uri=` ships it to object storage so a consumer can
resume on another box; `catch_up=True` reads the gap from the log's archive
when a consumer has fallen past what the server will replay, then picks the
socket up where the archive ended.

``EARLIEST`` is the offset that means "everything the log still holds".

Every frame on the wire is JSON text — the greeting, then an ``[offset, msg]``
pair per message — so ``wscat ws://localhost:8765/trades?offset=0`` is a working
subscriber with no client library at all. **``msg`` is the row the publisher
sent and nothing else**: no offset key, no injected metadata, so a subscriber
can log it, forward it or append it to another stream whole.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink import S3Options

from streamcast._catchup import CatchUpUnavailable
from streamcast._client import Subscription, connect
from streamcast._cursor import Cursor
from streamcast._errors import (
    Close,
    NotReplayable,
    ProtocolError,
    Rejected,
    StreamcastError,
    StreamNotFound,
    TooSlow,
)
from streamcast._maintain import Maintain
from streamcast._protocol import EARLIEST, Greeting, LogInfo
from streamcast._publish import Publication, publish
from streamcast._schema import from_arrow, to_arrow
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
    "CatchUpUnavailable",
    "Close",
    "Cursor",
    "Greeting",
    "LogInfo",
    "Maintain",
    "S3Options",
    "NotReplayable",
    "ProtocolError",
    "publish",
    "Rejected",
    "Publication",
    "Stream",
    "StreamNotFound",
    "StreamcastError",
    "Subscription",
    "TooSlow",
    "__version__",
    "connect",
    "from_arrow",
    "serve",
    "to_arrow",
]
