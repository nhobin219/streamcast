"""A JSON WebSocket pub/sub broker for structured rows.

Publishers submit rows to one broker, which assigns offsets and appends to a
litelink log before broadcasting. Subscribers receive `[offset, ts, row]` in
order and resume by offset. A stream without a log uses null offsets and
cannot replay. The log is also an Iceberg table, readable without the broker.

Declare columns in JSON Schema; `Stream.new` creates or reopens the log.
`serve` runs the maintainer and, with `replicate=True`, the WAL sidecar.
`Stream.send` publishes locally; `publish` does it over a socket. `connect`
returns a read-only subscription. Both ends pass transport keywords through
to `websockets`, with compression off by default because encoding is shared
while permessage-deflate compresses once per connection.

    stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA)
    async with streamcast.serve(stream, "localhost", 8765):
        async for frame in upstream:
            await stream.send(parse(frame))

    async with streamcast.connect(
        "ws://localhost:8765/trades", cursor=".trades.offset", catch_up=True
    ) as sub:
        async for offset, ts, row in sub:
            ...

`cursor` saves the consumer's handled offset; `cursor_uri` mirrors it to S3;
`catch_up=True` reads published tables before reconnecting when the server
cannot replay the gap. File cursors give at-least-once processing. For work
in a database, `inbox` and `outbox` commit receipt, state changes and outgoing
rows in the caller's transaction. Output deduplication needs a stream opened
with `exactly_once=True` and retains the buffer-loss limits in `docs/SPEC.md` §6b.

`Stream.snapshot` reads a fixed published point, optionally completed from
the broker; `Stream.live` keeps a view current. `docs/API.md` is the call
reference; `docs/SPEC.md` records the wire and ordering invariants.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink import S3Options

from streamcast._catchup import CatchUpUnavailable
from streamcast._client import Subscription, connect
from streamcast._cursor import Cursor
from streamcast._errors import (
    AcknowledgedRowsLost,
    Close,
    ExactlyOnceUnavailable,
    IngestFailed,
    NotReplayable,
    ProtocolError,
    Rejected,
    SequenceGap,
    StoreBehindBroker,
    StreamcastError,
    StreamMismatch,
    StreamNotFound,
    StreamRetired,
    Superseded,
    TooSlow,
)
from streamcast._inbox import Inbox, inbox
from streamcast._limits import (
    DEDUPE_WINDOW,
    INBOX_MAX_BYTES,
    INBOX_RESUME_AT,
    MAX_BACKLOG,
    MAX_PUBLISHERS,
    PAGE_BYTES,
    PAGE_ROWS,
    SEND_BATCH,
)
from streamcast._live import Live
from streamcast._maintain import Maintain
from streamcast._outbox import Outbox, outbox
from streamcast._protocol import EARLIEST, Greeting, PublisherMark
from streamcast._publish import Publication, publish
from streamcast._schema import from_arrow, to_arrow
from streamcast._server import serve
from streamcast._snapshot import LATEST, Reader, Snapshot, SnapshotUnavailable
from streamcast._stats import Stats
from streamcast._store import PostgresStore, SqliteStore, Store, Transaction, store_ddl
from streamcast._stream import MAX_REPLAY, Stream

try:
    __version__ = version("streamcast")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "DEDUPE_WINDOW",
    "EARLIEST",
    "INBOX_MAX_BYTES",
    "INBOX_RESUME_AT",
    "LATEST",
    "MAX_BACKLOG",
    "MAX_PUBLISHERS",
    "MAX_REPLAY",
    "PAGE_BYTES",
    "PAGE_ROWS",
    "SEND_BATCH",
    "AcknowledgedRowsLost",
    "CatchUpUnavailable",
    "Live",
    "Reader",
    "Snapshot",
    "SnapshotUnavailable",
    "Close",
    "Cursor",
    "ExactlyOnceUnavailable",
    "Greeting",
    "Inbox",
    "IngestFailed",
    "Maintain",
    "S3Options",
    "NotReplayable",
    "Outbox",
    "PostgresStore",
    "ProtocolError",
    "publish",
    "Rejected",
    "Publication",
    "PublisherMark",
    "SequenceGap",
    "SqliteStore",
    "Stats",
    "Store",
    "StoreBehindBroker",
    "Stream",
    "StreamMismatch",
    "StreamNotFound",
    "StreamRetired",
    "StreamcastError",
    "Subscription",
    "Superseded",
    "TooSlow",
    "Transaction",
    "__version__",
    "connect",
    "from_arrow",
    "inbox",
    "outbox",
    "serve",
    "store_ddl",
    "to_arrow",
]
