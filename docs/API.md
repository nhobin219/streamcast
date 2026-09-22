# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and why;
this says what you can call.

```python
import streamcast
from streamcast import Cursor, EARLIEST, Maintain, Stream, Subscription, to_arrow
```

**A durable stream needs no other import.** Declare the columns in JSON Schema and
`Stream` creates the log, while `serve` maintains and replicates it:

```python
stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA)

async with streamcast.serve(stream, "localhost", 8765):
    await stream.send({"event_ts": ..., "price": ...})
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

## The API is `websockets`, with three deviations

`serve` and `connect` have the same shapes and pass every keyword through — `ssl`,
`ping_interval`, `process_request`, `max_queue` and the rest behave exactly as they do
there — and `serve` returns an object that proxies `websockets.Server` (`sockets`,
`serve_forever`, `connections`, `is_serving`). Three things differ:

**Iterating a subscription yields `(offset, msg)`**, not `message`. The offset is the
only thing that makes a reconnect a resume rather than a restart, and a subscriber that
has to ask for it separately will forget to. `msg` is a `dict` over your declared columns
**and nothing else** — the offset is framing, not data, and never appears inside it.

**A subscription is read-only.** It has no `send` — rather than a `send` that raises —
because publishing is `Stream.send` in the server's own process. Nothing inherits a
method it has to refuse.

**`compression` is `None` here and `"deflate"` there.** permessage-deflate is per
connection while the encode is shared: `send` encodes a frame once and hands the same
bytes to every subscriber, and deflate compresses those identical bytes once per
subscriber. Measured on a six-column trade row:

| subscribers | CPU per message, off | on |
|---|---|---|
| 1 | 0.564 µs | 4.02 µs |
| 6 | 0.564 µs | 21.3 µs |
| 50 | 0.564 µs | 173 µs |
| 200 | 0.564 µs | 691 µs |

At 50 subscribers and 30,000 msg/s that asks for 1.5M compressions/s where a core
manages roughly 290k: the server goes CPU-bound and `max_backlog` drops the subscribers
that fall behind. Off, the cost is bandwidth instead — 26.9 against 4.6 Mbit/s per
subscriber, since deflate is **5.8× smaller** here (112 bytes to 19).

Pass `compression="deflate"` when bandwidth costs more than CPU, which is a WAN with few
subscribers. There is no middle setting: without context takeover — the variant that
would let one compressed frame be shared across connections — the same frames compress
1.1×.

## `Stream`

```python
streamcast.Stream(name="", *, log=None, owns_log=False,
                  max_backlog=8192, max_replay=100_000)

streamcast.Stream.new(name="", *, root, schema,          # creates or opens the log
                      sort_by=None, config=None, archive=None, s3=None,
                      replay_archive=False,
                      max_backlog=8192, max_replay=100_000)
```

`name` is where it is served: `"trades"` at `/trades`, `""` at `/`. It is the name's only
home — routing and the greeting both read it, so they cannot disagree.

**Two ways to give it a log, and they are separate calls.** `Stream.new(root=, schema=)`
creates or opens one — `new` the first time, `open` every time after, which is the
try/except every caller otherwise writes — and takes litelink's own `new()` keywords
(`sort_by`, `config`, `archive`, `s3`) with the stream's name fed through. The plain
initialiser takes a handle you already opened.

The split follows litelink, whose own handles say it outright: *"the initialiser takes
already built collaborators and does no I/O, so a test can substitute any of them."*
`Stream(...)` builds nothing and touches no disk; `Stream.new(...)` is where the I/O is.
It also deletes two hand-written errors — `new` has no `log=` parameter and `root`/`schema`
are required, so the bad combinations are refused by the signature rather than checked at
runtime.

**`log` is what makes an offset a resume cursor**, and it is optional — a
[tickerplant](https://code.kx.com/q/architecture/)'s log is optional too, and some kdb
implementations omit it. Without one nothing assigns
offsets at all — `send` returns None and every frame carries `null` — so `?offset=` is
refused outright rather than appearing to work until the day a subscriber needs it. With
it, every row is durable *before* any subscriber sees it.

```python
stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA,
                               sort_by=("event_ts",))
```

**Any shape of log works** — the schema is yours, and `Stream` reads its column order once
at construction to fix the key order on the wire.

**Who closes it depends on who opened it.** `aclose` closes a log the `Stream` owns —
which `Stream.new` sets — and never one passed to the initialiser, since that one stays
yours, for a process that also reads or writes it through litelink. Pass `owns_log=True`
alongside `log=` to hand over the lifetime of a handle you opened. An existing log is checked against
a declared `schema=` rather than adopted, because a declaration that disagreed with the
disk would be silently ignored and every `send` validated against columns the caller never
wrote down.

`max_backlog` is messages, not bytes (see [`SPEC.md`](SPEC.md) §4). `max_replay` bounds
how far back a subscribe may ask; **`None` removes the bound**, so nothing is ever refused
as `too_old`. With `replay_archive=True` that makes the server a complete gateway to the
log — any language can replay the whole stream over a plain WebSocket, with no litelink and
no credentials of its own. The cost is that a long replay accumulates live messages behind
it and `max_backlog` is what drops the subscriber, so size the two together. **Size them against each other**: a replay streams
while live messages queue behind it. The defaults are exported as
`streamcast.MAX_BACKLOG` and `streamcast.MAX_REPLAY`, for a caller that wants to scale
from them rather than restate them.

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
streamcast.serve(streams, host=None, port=None, *, maintain=True,
                 **websockets_kwargs) -> Server
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

### `maintain`

**`maintain=True` starts one maintainer subprocess per stream that has a log**, and stops
it when the server closes. Without it, nothing in this library ever calls litelink's
`seal_due()` — measured on 100,000 rows (~14 MB, past the 8 MiB seal target): the buffer
held every one of them, the table held zero Parquet files, and `buffer.db` was 15.7 MB and
growing. litelink says it plainly: *"A maintainer is not optional."*

```python
streamcast.serve(stream, host, port)                           # a maintainer per log
streamcast.serve(stream, host, port, maintain=False)           # you run your own
streamcast.serve(stream, host, port,
                 maintain=streamcast.Maintain(seal_every=0.1, maintain_every=30))
```

`Maintain` is a frozen dataclass of `seal_every` (0.25 s) and `maintain_every` (10 s). The
cadences differ by 40x because the costs do: `seal_due` is an indexed read of one row when
there is nothing to seal, while `maintain` reads table metadata to compact, evict and
expire.

**It is always a subprocess, and there is deliberately no thread option.** A seal is
CPU-bound pure Python, so it starves a thread sharing its interpreter even holding no
lock — litelink measured appends running 45.2 ms behind an in-process seal. On a fan-out
server that is 45 ms with nothing fanned out and no keepalive answered, which surfaces as
latency spikes that look like a network problem.

It dies with the server, which is safe because litelink allows one writer: nothing is
appending, so an unsealed buffer is not growing. The other direction is supervised — a
maintainer that exits while the server runs is restarted with backoff, because losing it
silently returns the server to never sealing.

`maintain=False` is right when you run litelink's own four-process shape
(`examples/adsb/`, one process per storage role), or when the log is shared with something
else that sweeps it. `python -m streamcast maintain --root PATH --name NAME` is the same
loop, runnable by hand.

### `replicate`

**`replicate=True` runs litestream** for any stream whose log has `wal_replication` on,
which is what makes that log survive losing its machine. `serve` is the one thing you
start; there is no second process to remember.

```python
log = litelink.new(root, "trades", schema=SCHEMA,
                   config=litelink.LogConfig(wal_replication=True),
                   archive="s3://bucket/prefix")

async with streamcast.serve(streamcast.Stream("trades", log=log), host, port):
    ...        # sealing, compaction, and WAL shipping all running
```

`wal_replication` is opt-in on the log, so for almost every deployment this starts
nothing. `replicate=False` opts out, for someone running their own more finely tuned
litestream.

**Two litestream instances on one database is the thing litestream forbids**, and the
sidecar is built around not doing it:

- an `flock` taken non-blocking and held for the server's life, on a file **beside the
  log** — litelink's own example locks `log.root`, which is the *parent* and shared
  between streams;
- `PR_SET_PDEATHSIG` on the child, so a `SIGKILL` of the server cannot orphan it — a
  handler cannot cover `SIGKILL`, only the kernel can;
- a server that cannot take the lock **stands by and retries**, so one started beside a
  dying one takes over when the kernel frees it.

Replication lives exactly as long as the writer, which is the property litelink's example
cannot offer — it hangs the sidecar off the maintainer, and notes that a writer without
one has `wal_replication=True` and no replication. That is also why `replicate` is its own
argument rather than part of `maintain`.

A missing litestream binary raises `SidecarUnavailable` at `serve`, not at the first
missed push: a server that came up and replicated nothing would leave you believing you
had protection you did not have. The wheel bundles one, so this is only reachable on a
platform litelink ships no binary for.

### Credentials

**Both subprocesses resolve credentials from the environment**, because litelink never
persists them — its model is the ordinary AWS chain at the point of use, so a profile,
instance metadata or SSO all work untouched. An `S3Options` you passed to `litelink.new`
does **not** reach them.

```bash
AWS_ENDPOINT_URL=...  AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...   # the maintainer
LITESTREAM_ACCESS_KEY_ID=...  LITESTREAM_SECRET_ACCESS_KEY=...           # litestream
```

litestream reads its own pair rather than the AWS ones, which is why the config litelink
generates carries no secret and is safe to commit. On AWS with an instance role, none of
this needs setting.

The server never calls `recv` on a subscription. A client that sends anyway fills its own
receive buffer, stops being able to send, and is closed by the keepalive.

## `connect`

```python
streamcast.connect(uri, *, offset=<unset>, cursor=None, cursor_uri=None,
                   s3=None, upload_every=30.0, catch_up=False,
                   catch_up_retries=3, archive=None,
                   **websockets_kwargs) -> Subscription
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
await sub.close(code=1000, reason="") -> None   # drains what is in flight
sub.commit(offset=None) -> None   # save the cursor now — see Resuming


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
a server restart), `schema` (the stream's columns as JSON Schema), `archive` (where its
log is archived, which is what `catch_up` reads), `stream`, `version`.

## Resuming

**`cursor=path` is the whole of it.** The file holds the last offset finished with; the
subscription loads it at connect, resumes one above, and saves as the loop runs.

```python
async with streamcast.connect(uri, cursor=".trades.offset") as stream:
    async for offset, msg in stream:
        handle(msg)
```

Stop the consumer, start it again, and it picks up where it stopped.

| `cursor` | `offset` | resumes from |
|---|---|---|
| — | — | live, from now |
| — | `N` | `N`, inclusive |
| path | — | the file, one above |
| path | `N` | `N` — the file is overridden, and still updated |
| path | `None` | live, from now — the file is ignored, and still updated |

`offset`'s default is a sentinel rather than `None`, because with a cursor those last two
rows have to be different things: *not given* means "use the file", `None` means "ignore
it and take the live stream".

**The cursor lags deliberately, and must never lead or rewind.** A cursor behind the work
re-delivers, which is safe and visible; a cursor ahead of it skips messages for ever,
which is neither. So four things are arranged around that:

- it advances when you ask for the **next** message, not when you receive this one —
  coming back for another is the only evidence the library has that the last was handled;
- saves are throttled to once a second, because an atomic rename per message is three
  syscalls to record something allowed to be stale;
- a block that exits with an **exception** saves nothing, so the message whose handler
  raised is re-delivered;
- the automatic save only ever moves **forward**. Not for ordering reasons: within a
  subscription that is TCP's guarantee — one connection, one byte stream, in order — so
  keeping a consumer's offset and resuming from it is safe on its own. The rewind this
  guards is an operator's, not a network's: an explicit `offset=` sharing a cursor file.
  A one-off `connect(uri, offset=1, cursor=path)` beside a production run wrote 1, 2, 3
  over a file that said 400. `offset=` now overrides the *read* and cannot corrupt the
  *write*.

  `recv` separately refuses a frame whose offset did not increase. That one is for the
  **catch-up join**, where the rows below the socket came from object storage and the
  splice depends on `Catcher.start` and the server's replay window agreeing on an
  inclusive/exclusive boundary — a place TCP says nothing about. It doubles as an
  assertion on the replay/live partition. It does not span a reconnect.

`commit(offset)` **is** allowed to move it backwards, because that is you stating what is
durable and correcting an optimistic value downward is the point. It also cancels the
clean-exit save, so leaving the block does not undo it.

**A batching consumer should not use the automatic save at all.** It advances as you read,
which for a batch is ahead of what you have flushed. Drive `streamcast.Cursor` yourself —
it is exported for exactly that — and save only what you have committed.

```python
sub.commit()          # force a save now — for a batching or non-idempotent consumer
sub.commit(offset)    # ...at an offset you actually committed
```

### `catch_up` — when the server will not replay that far back

A consumer that has been down long enough falls past `max_replay`, and the server refuses
with `NotReplayable(why="too_old")`. The rows are not gone — they are in the log's archive
— but getting them means knowing where that is, opening litelink, scanning it without
running out of memory, and working out where to resume the socket. `catch_up=True` does
all of it:

```python
async with streamcast.connect(uri, cursor=".trades.offset", catch_up=True) as stream:
    async for offset, msg in stream:
        handle(msg)
```

The consumer sees one stream. Underneath, rows below the server's window come from object
storage and the rest from the socket.

**Nothing is connected while the archive is read**, and that is the part that matters.
Holding the socket open through the read would make the server queue for a subscriber that
will not take a message until it has pulled millions of rows out of S3 — and `max_backlog`
is 8,192, so it would be dropped with `TooSlow` before the catch-up finished, failing
exactly the consumers that need it. Tested at `max_backlog=16`, catching up 20,000 rows.

So it is a **loop**: read the archive, try to connect at the offset it reached, and if the
server has moved on far enough to refuse again, read the newly archived rows and try once
more. It converges when the archive gets inside the server's window.
`catch_up_retries` (default 3) bounds it for when that never happens, and the failure says
which of three things to change.

| | |
|---|---|
| `catch_up=False` *(default)* | the plain `NotReplayable` refusal; handle the gap yourself |
| `catch_up_retries=3` | rounds of read-then-connect before giving up |
| `archive="s3://bucket/prefix"` | override where to read; otherwise taken from the refusal, or from the greeting |
| `s3=streamcast.S3Options(...)` | credentials; otherwise the environment |

**Where the archive location comes from.** The refusal carries it when it fits — a close
frame is 123 bytes and the numbers that make the message readable are ordered first, so a
long bucket URI is what drops. The greeting carries it too, with no such limit, so a client
that did not get it from the refusal spends one throwaway connection asking. An explicit
`archive=` beats both.

**Failures land at `connect`, not at the first `recv`.** The archive is opened before the
subscription is handed back, so unreadable credentials raise where you called `connect`
rather than minutes later from whatever line read next. The message is long on purpose —
it names what was tried, which credential source, and four ways out:

```
cannot read stream 'trades' from s3://market-data/prod to catch up.

  tried:       endpoint http://..., region us-east-1
  credentials: the ambient credential chain (profile, instance metadata, SSO)
  underlying:  OSError: ...

  * On AWS, the usual fix is an instance role or profile that can GET and LIST under ...
  * Elsewhere, set AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, ...
  * `catch_up=False` turns this back into the plain NotReplayable refusal ...
  * To skip the gap and accept the loss, reconnect with offset=streamcast.EARLIEST ...
```

**What it cannot fix.** If the archive's frontier is itself below the server's window,
a range exists that neither holds — the server has forgotten it and the archive never
received it. That is reported with both numbers rather than half-served, because a
consumer that silently resumed above the gap would have lost data and been told it
recovered.

Only `too_old` and `evicted` are recoverable this way. `not_durable`, `empty` and `ahead`
are a stream with no log, a log with nothing in it, and a cursor from the future; reading
object storage fixes none of them.

### `cursor_uri` — resuming on another box

A local cursor recovers a consumer that restarted. It does not recover one whose machine
is gone, which is what this is for — the same idea as the server's WAL replication, one
layer out:

```python
async with streamcast.connect(
    uri,
    cursor=".trades.offset",
    cursor_uri="s3://streamcast/consumer1/",   # trailing / = prefix; name appended
) as stream:
    ...
```

A **daemon thread** uploads the cursor every `upload_every` seconds (30 by default) and
once more on a clean exit. It is a thread rather than the event loop or `asyncio.to_thread`
because the PUT is blocking and best-effort, and that pool — `min(32, cpu + 4)` — is what
every replay scan uses.

**On connect the local cursor wins**; the remote is read only when there is no local one.
That is the disaster-recovery case, and the only one where a copy that lags by up to
`upload_every` should decide. When it is used, the value is written down locally too, so a
second restart on the new box needs no bucket.

Credentials resolve from the environment and `s3=streamcast.S3Options(...)` overrides them
— litelink's model, so a profile, instance metadata or SSO all work untouched. The upload
goes through pyarrow, which litelink already brings, so this adds no dependency.

**What it deliberately does not solve.** It is periodic, so recovering on another box
re-delivers whatever happened in the last `upload_every`. Two consumers sharing one key is
last-writer-wins and is not supported. Re-delivery is the safe direction, and a consumer
needing exactly-once wants its cursor in the same transaction as its work — its own
database, not this.

Logging is `logging.getLogger("streamcast.cursor")` at DEBUG. Failures to reach the bucket
are logged and never raised: a disaster-recovery convenience must not become a dependency
of the stream. A *configuration* error — a `cursor_uri` that is not `s3://` — raises at
`connect`, because a consumer that believed it was shipping a cursor and never was is the
failure this whole feature exists to prevent.

### Storage

**It is a file, not SQLite**, written beside the target and renamed over it — atomic on
POSIX and Windows both. SQLite was considered and is the wrong tool for one integer: the
case it would genuinely earn is a cursor committed in the *same transaction* as the
consumer's work, and that only works in the consumer's own database, which is not a file
this library can own. `commit()` is the hand-back for that.

An unreadable or empty file is treated as absent rather than as an error — a cursor is a
recovery hint, and refusing to start because a save was torn turns a crash into an outage.

## Refusals

```
StreamcastError
├── ProtocolError      the peer is not speaking streamcast, or sent a bad subscribe
├── StreamNotFound     nothing served at that path; `.serves` lists what is
├── NotReplayable      that offset cannot be served; `.why` says which of five
├── CatchUpUnavailable `catch_up=True` could not read the gap; says what to change
└── TooSlow            dropped for falling behind; `.offset` is where to resume
```

`NotReplayable.why` is the one worth branching on, because the next move differs:

| `.why` | what to do |
|---|---|
| `not_durable` | drop `offset=`, or give the server a log |
| `empty` | subscribe live; there is nothing to replay yet |
| `ahead` | your cursor is above the server's frontier — it was restored or rebuilt |
| `too_old` | `catch_up=True`, or read the log directly and subscribe from where you stopped |
| `evicted` | below what the scan's tier holds; `catch_up=True` if the archive goes back further, else accept the gap |

`.fields` carries whatever numbers survived the close frame — `offset`, `earliest`,
`behind`, `max_replay`, `end_offset` — and `str(exc)` is a sentence built from them.

`Close` is the code enum: `BAD_REQUEST` 4400, `NO_SUCH_STREAM` 4404, `NOT_REPLAYABLE`
4416, `TOO_SLOW` 4429.

## The schema is yours

streamcast declares no columns — you do, in **JSON Schema**, because the wire is JSON and
this is a library about JSON websockets:

```python
SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},                   # int64
        "price": {"type": "number"},                       # float64
        "amount": {"type": "number"},
        "side": {"type": "integer", "format": "int32"},    # narrower, on purpose
        "tag": {"type": ["string", "null"]},               # nullable
    },
    "required": ["event_ts", "price", "amount", "side"],
}

stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA, sort_by=("event_ts",))
```

`Stream` creates the log at `root/name` if it is not there and opens it if it is — the
`new`/`open` dance every server writes — and **closes it on `aclose`, because it opened
it.** A log you open yourself and pass as `log=` stays yours, closed by you. Passing both
is refused.

An existing log whose columns disagree with the declaration is **refused, not adopted**:
litelink fixes a log's shape at creation and `open` takes none of it, so a disagreement
would otherwise be ignored and every send validated against columns you never wrote down.

| JSON | `format` | Arrow |
|---|---|---|
| `boolean` | — | `bool` |
| `integer` | — or `int64` | `int64` |
| `integer` | `int32` | `int32` |
| `number` | — or `double` | `float64` |
| `number` | `float` | `float32` |
| `string` | — | `string` |

**`format` carries the width, because JSON Schema does not.** `integer` does not choose
between int32 and int64; left out, the wider of each pair wins — a feed that overflows an
int32 is a silent wrong answer, while one that would have fitted costs four bytes a row.

### `required` is presence; `"null"` in a type is the value

These are different rules, and conflating them is the easy mistake. Verified against a
real validator (`tests/test_schema.py` asserts this table against `jsonschema`, and CI
runs it):

| schema | `{"c":"x"}` | `{"c":null}` | `{}` |
|---|---|---|---|
| required + `"string"` | valid | **invalid** — by `type` | **invalid** — by `required` |
| required + `["string","null"]` | valid | valid | invalid — by `required` |
| optional + `"string"` | valid | **invalid** — by `type` | valid |
| optional + `["string","null"]` | valid | valid | valid |

A schema whose columns are all nullable is the shape for **best-effort capture** — parse
what you can, keep the raw message in its own column, and never lose a message to a feed
that changed. The README has the pattern.

Arrow has two states, not four — a column is nullable or it is not, and there is no
"absent", because **a row that omits a column stores NULL**. So:

```
nullable = (not in `required`) or ("null" in its type)
```

Three of those rows map exactly. The fourth — **optional with a non-null type — is
refused**, not widened. It means "may be absent, but never null when present", which a
stream cannot express, and accepting it would make streamcast take rows your own declared
schema rejects. The message names both fixes: mark it `required`, or add `"null"` to its
type.

So the rule is: **every property is either required with a plain type, or nullable through
its type.** And everything the greeting publishes is something `to_arrow` would accept,
because a nullable column is published as `["string","null"]` rather than merely left out
of `required` — a subscriber validating against it needs to know the value may be null.

`required` *is* enforced, one library down: it becomes `nullable=False` and
`litelink.append` refuses both a missing column and an explicit `None`, telling them
apart.

Anything litelink cannot store is refused **here**, where the message names JSON Schema's
vocabulary rather than Arrow's: nested objects, arrays, `date-time` (store epoch
integers), `byte`/`binary`, and the narrow integer widths Iceberg would widen silently.

```python
streamcast.to_arrow(SCHEMA)     -> pa.Schema      # if you want the litelink schema
streamcast.from_arrow(schema)   -> dict           # what the greeting publishes
```

**The greeting carries the schema**, so a subscriber in another language reads the columns
without this repo:

```json
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":null,"durable":true,
 "schema":{"type":"object","properties":{"event_ts":{"type":"integer","format":"int64"}}}}
```

Widths are stated explicitly on the way out, so what a subscriber reads back is what the
column actually is.

**A caveat JSON cannot fix:** integers beyond 2^53 do not survive every parser. Python and
msgspec carry int64 exactly; a JavaScript subscriber silently rounds. A nanosecond
`event_ts` is past it — microseconds, which the examples use, are not.

### Or bring your own litelink log

```python
log = litelink.new("data", "trades", schema=streamcast.to_arrow(SCHEMA),
                   sort_by=("event_ts",))
stream = streamcast.Stream("trades", log=log)
```

Which is what you want for `litelink.restore`, or a log shared with something else.

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

Every frame is JSON text. The greeting, then a **two-element pair** per message — the
offset, then the row:

```
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":[1200,1861],
 "archive":"s3://market-data/prod","schema":{...},"durable":true}
[1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

(The greeting is one line on the wire; it is wrapped here to fit.)

The offset is **positional**, not a key in the object — `const [offset, msg] =
JSON.parse(frame)` — so `msg` is the publisher's row and nothing else, with no column to
strip before forwarding it. No binary header, no length prefix, no payload kind.

`wscat ws://localhost:8765/trades?offset=0` is a working subscriber, and a consumer in any
language needs a JSON parser rather than this document.

Encoding is [msgspec](https://github.com/jcrist/msgspec), which sits on the hot path in
both directions — every publish encodes a row, every replayed row re-encodes one. Measured
on a six-column trade row: **0.285 us against 5.815 us** for stdlib `json` to encode
(20.4x), and 0.386 against 4.989 to decode (12.9x). The ratio narrows as one large string
column comes to dominate a frame; `just bench` prints both for your own shape.
