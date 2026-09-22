# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and why;
this says what you can call.

```python
import streamcast
from streamcast import EARLIEST, SCHEMA, Stream, Subscription, __version__
```

The object model is **two classes and two functions**. `Stream` is the broadcast —
offsets, subscribers, replay — and holds no socket. `serve` puts it behind a port;
`connect` reads it from the other end. `Subscription` is what a consumer holds.

```
Stream            send · send_many · end_offset · subscribers · durable · aclose
  │
  ├─ serve(…)     ─► websockets.Server
  └─ connect(…)   ─► Subscription      recv · __aiter__ · offset · info · close
```

`SCHEMA` and `EARLIEST` are exported because they appear in calls you write: the first
is what a streamcast log is created with, the second is the offset that means
"everything you still have". `Greeting`, `Close` and the five exception types are
exported because they appear in what you catch and inspect.

## The API is `websockets`, with one modification

`serve` and `connect` have the same shapes and pass every keyword through. Two things
differ:

**Iterating a subscription yields `(offset, msg)`**, not `message`. The offset is the
only thing that makes a reconnect a resume rather than a restart, and a subscriber that
has to ask for it separately will forget to. `msg` is a `dict` over your declared columns
**and nothing else** — the offset is framing, not data, and never appears inside it.

**A subscription is read-only.** It has no `send` — rather than a `send` that raises —
because publishing is `Stream.send` in the server's own process. Nothing inherits a
method it has to refuse.

One default differs: **`compression` is `None` here and `"deflate"` there.**
permessage-deflate keeps a 32 KB compressor per connection, so a frame this library
deliberately encodes once is then compressed once *per subscriber* — the wrong trade on
the LAN this is built for. Pass `compression="deflate"` for subscribers across a WAN.

## `Stream`

```python
streamcast.Stream(name="", *, log=None, max_backlog=8192, max_replay=100_000)
```

`name` is where it is served: `"trades"` at `/trades`, `""` at `/`. It is the name's only
home — routing and the greeting both read it, so they cannot disagree.

**`log` is what separates a multicaster from a [tickerplant](https://code.kx.com/q/architecture/).** Without it nothing assigns
offsets at all — `send` returns None and every frame carries `null` — so `?offset=` is
refused outright rather than appearing to work until the day a subscriber needs it. With
it, every row is durable *before* any subscriber sees it.

```python
log = litelink.new("data", "trades", schema=SCHEMA, sort_by=("event_ts",))
stream = streamcast.Stream("trades", log=log)
```

**Any shape of log works** — the schema is yours, and `Stream` reads its column order once
at construction to fix the key order on the wire. The `Stream` does not close the log: you
opened it, you close it.

`max_backlog` is messages, not bytes (see [`SPEC.md`](SPEC.md) §4). `max_replay` bounds
how far back a subscribe may ask. **Size them against each other**: a replay streams
while live messages queue behind it.

### Publishing

```python
await stream.send(row: Row) -> int                      # Row = Mapping[str, object]
await stream.send_many(rows: Iterable[Row]) -> list[int]
```

`Row` is litelink's — the same mapping `litelink.append` takes, over your declared
columns. litelink validates it, so a wrong type or an unknown column raises here naming
the column, and **nothing is broadcast**.

Both return the assigned offsets, and with a log attached **the rows are durable when the
call returns** — one SQLite transaction at `synchronous=FULL`.

`send_many` commits the whole group in one transaction: *measured*, 1,707 us per message
one at a time against 10 us at a group of 100. That call size is the write-throughput
lever and it is a call-site choice; no setting tunes it.

Each row in a group still gets its own offset and its own frame, so a subscriber cannot
tell a group from the same rows sent singly. That is deliberate — batching is the server's
durability decision, and making it visible on the wire would make every subscriber's
parser depend on how the publisher happened to poll.

**Neither awaits a consumer**, and today neither awaits at all. See `SPEC.md` §3 for why
that is a correctness property rather than a performance note — and for the one hazard it
creates: a publish loop with no `await` of its own starves every subscriber.

The frame is the row as JSON text, encoded once with msgspec and shared by every
subscriber (I6). **Key order comes from the log's schema, not from your dict**, which is
what makes a replay of a row byte-identical to what went out live.

### Observing

```python
stream.end_offset -> int     # the offset the next message will be assigned
stream.subscribers -> int    # how many are attached right now
stream.durable -> bool       # whether a log is attached
stream.name -> str
stream.log -> WriteHandle | None
```

`end_offset` is the same quantity as litelink's `end_offset()`, and deliberately the same
name — but an attribute here and a call there, because litelink reads it from SQLite and
this is the counter `send` already maintains.

```python
await stream.aclose(reason="server shutting down") -> None
```

Drops every subscriber with a 1001, concurrently. **Does not close the log.**

## `serve`

```python
streamcast.serve(streams, host=None, port=None, **websockets_kwargs) -> Server
```

`streams` is a `Stream` or an iterable of them. Returns exactly what `websockets.serve`
returns, so all three forms work:

```python
async with streamcast.serve(stream, "127.0.0.1", 8765):
    ...

server = await streamcast.serve([trades, quotes], "0.0.0.0", 8765)
await server.serve_forever()
```

Routing is by `Stream.name`. Two streams with one name raise `ValueError` at `serve`
rather than resolving: whichever lost would be unreachable, and the subscriber that
wanted it would get somebody else's messages — which looks like working software.

`host=None` binds every interface, exactly as `websockets` does. Pass `"127.0.0.1"` for a
server that should only serve its own box, which is the case this library is built for.
TLS is `ssl=`; authentication is `process_request=`. See [`SECURITY.md`](../SECURITY.md).

The server never calls `recv` on a subscription. A client that sends anyway fills its own
receive buffer, stops being able to send, and is closed by the keepalive.

## `connect`

```python
streamcast.connect(uri, *, offset=None, **websockets_kwargs) -> Subscription
```

Awaitable and an async context manager, like `websockets.connect`.

```python
async with streamcast.connect("ws://localhost:8765/trades", offset=123) as stream:
    async for offset, msg in stream:
        ...
```

`offset` is the resume point:

| | |
|---|---|
| **absent** | live only, from now |
| `streamcast.EARLIEST` | everything the log still holds |
| an integer | resume from it, **inclusive** |

It may be written into the URI instead — `ws://localhost:8765/trades?offset=123`, which is
what makes `wscat` a working subscriber — but never both. Given both, `connect` raises
rather than picking: two values that disagree is a resume from the wrong place, and
neither is more likely to be the intended one.

**The greeting is awaited before `connect` returns**, so entering the block means the
server accepted the subscribe. A refused offset raises here, not minutes later inside
some `recv`.

### `Subscription`

```python
await sub.recv() -> tuple[int | None, dict[str, object]]
async for offset, msg in sub: ...
await sub.close(code=1000, reason="") -> None

sub.offset -> int | None          # the last offset RECEIVED — the resume cursor
sub.info -> Greeting              # what the server said at subscribe
sub.connection -> ClientConnection  # the websockets object, unwrapped
```

`msg` is a `dict` over the stream's declared columns — **exactly** the row the publisher
sent, with no offset key and nothing else injected, so it can be logged, forwarded or
appended to another stream whole. There is nothing to parse: the server's table is typed,
so the parse happened once at the publisher.

`offset` is `None` on a stream with no log. Nothing assigned one, and `?offset=` is
refused on such a stream, so there is nothing to resume from.

Iteration **stops** on a normal close (1000/1001) and **raises** on anything else — the
same contract as iterating a `websockets` connection, with the refusals below filling in
for what a bare code cannot say.

`info` is the greeting: `end_offset` (the server's frontier at subscribe), `replay` (the
`[start, end)` about to be replayed, or `None`), `durable` (whether these offsets survive
a server restart), `stream`, `version`.

## Resuming

```python
offset = None
while True:
    try:
        async with streamcast.connect(uri, offset=offset) as stream:
            async for offset, msg in stream:
                handle(msg)

    except (ConnectionClosed, OSError, streamcast.TooSlow):
        offset = None if offset is None else offset + 1
```

The `+ 1` belongs at the call site rather than inside a helper, because only the caller
knows whether the last message was actually *processed* — a subscriber that persists its
work resumes from what it committed, not from what it received. `examples/consumer.py` is
this with a cursor file.

## Refusals

```
StreamcastError
├── ProtocolError      the peer is not speaking streamcast, or sent a bad subscribe
├── StreamNotFound     nothing served at that path; `.serves` lists what is
├── NotReplayable      that offset cannot be served; `.why` says which of five
└── TooSlow            dropped for falling behind; `.offset` is where to resume
```

`NotReplayable.why` is the one worth branching on, because the next move differs:

| `.why` | what to do |
|---|---|
| `not_durable` | drop `offset=`, or give the server a log |
| `empty` | subscribe live; there is nothing to replay yet |
| `ahead` | your cursor is above the server's frontier — it was restored or rebuilt |
| `too_old` | read the log directly for the gap, then subscribe from where you stopped |
| `evicted` | the rows are gone from the log; read the archive, or accept the gap |

`.fields` carries whatever numbers survived the close frame — `offset`, `earliest`,
`behind`, `max_replay`, `end_offset` — and `str(exc)` is a sentence built from them.

`Close` is the code enum: `BAD_REQUEST` 4400, `NO_SUCH_STREAM` 4404, `NOT_REPLAYABLE`
4416, `TOO_SLOW` 4429.

## The schema is yours

streamcast declares no columns. Create the log the way litelink's own example does — every
field the feed sends that is worth a column gets one:

```python
SCHEMA = pa.schema([
    pa.field("event_ts", pa.int64(), nullable=False),   # microseconds, as the feed sends
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("side", pa.int64()),
])

log = litelink.new("data", "trades", schema=SCHEMA, sort_by=("event_ts",))
```

**`sort_by` is a read-shape decision, not a knob** — only a leading column prunes, and
changing it later rewrites every file. litelink's default is offset order, which is right
when every query you will run is an offset range.

That is what puts litelink underneath this rather than an append-only file:

| | |
|---|---|
| **pruning** | Iceberg statistics per column, so a bounded query never reads the rest |
| **compression** | a `price` column of float64 compresses against its neighbours |
| **the archive** | any Iceberg engine reads it as a table, with nothing installed |
| **the replay** | rows come off Arrow as columns, not as strings to re-parse |

A log written by streamcast is an ordinary litelink log, so everything litelink offers
applies unchanged: `scan`, `sql`, archiving to S3, WAL replication, and reading it from
another machine with `litelink.snapshot`. Reading the log directly is how you go further
back than `max_replay`.

```python
with litelink.open("data", "trades", read_only=True) as reader:
    reader.sql("SELECT count(*), max(price) FROM log").read_all()
```

Note litelink's own caution about opening a reader in the writer's process — a separate
process is the supported shape.

**A frame that is not a row has nowhere to go.** Subscription acks, heartbeats and
reconnect notices are dropped by the feed handler, which is the same division of labour a
kdb tickerplant has: the feed handler parses, the plant stores typed rows.

## On the wire

Every frame is JSON text. The greeting, then one object per row:

```
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":[1200,1861],"durable":true}
[1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

No binary header, no length prefix, no payload kind. `wscat ws://localhost:8765/trades?offset=0`
is a working subscriber, and a consumer in any language needs a JSON parser rather than
this document.

Encoding is [msgspec](https://github.com/jcrist/msgspec), which sits on the hot path in
both directions — every publish encodes a row, every replayed row re-encodes one. Measured
on a six-column trade row: **0.285 us against 5.815 us** for stdlib `json` to encode
(20.4x), and 0.386 against 4.989 to decode (12.9x). The ratio narrows as one large string
column comes to dominate a frame; `just bench` prints both for your own shape.
