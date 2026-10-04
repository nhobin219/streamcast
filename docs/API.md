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
  └─ connect(…)   ─► Subscription      recv · recv_many · batches · __aiter__ · offset · info · close
```

`SCHEMA` and `EARLIEST` are exported because they appear in calls you write: the first
is what a streamcast log is created with, the second is the offset that means
"everything you still have". `LATEST` is the snapshot point that means "the broker's
frontier". `Greeting`, `Close`, `Snapshot` and the exception types are exported because
they appear in what you catch and inspect.

## The API is `websockets`, with three deviations

`serve` and `connect` are `websockets`' own: same names, same shapes, every keyword passed
through — `ssl`, `ping_interval`, `process_request`, `max_queue` and the rest behave
exactly as they do there — and `serve` returns an object that proxies `websockets.Server`
(`sockets`, `serve_forever`, `connections`, `is_serving`). Three things differ:

**Iterating a subscription yields `(offset, ts, msg)`**, not `message`. The offset is the
only thing that makes a reconnect a resume rather than a restart, and a subscriber that
has to ask for it separately will forget to. `ts` is `streamcast_ts`, when the server took
the row. `msg` is a `dict` over your declared columns **and nothing else** — the offset
and the stamp are framing, not data, and never appear inside it.

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
streamcast.Stream(name="", *, log=None, owns_log=False, schema=None,
                  max_replay=100_000, group_commit=True)

streamcast.Stream.new(name="", *, root, schema,          # creates or opens the log
                      sort_by=None, config=None, published=None, s3_options=None,
                      replay_published=False, group_commit=True,
                      max_replay=100_000)

streamcast.Stream.migrate(name="", *, root, schema, ...)  # a new log, a new schema
streamcast.Stream.restore(name="", *, root, published,    # on a box without the log
                          s3_options=None, schema=None, sort_by=None, config=None,
                          replica_reserve=None, published_reserve=None,
                          revive=False, ...)
streamcast.Stream.retire(name="", *, root, s3_options=None)  # for good; returns the
                                                            # record: .at, .end_offset
```

`name` is where it is served: `"trades"` at `/trades`, `""` at `/`. It is the name's only
home — routing and the greeting both read it, so they cannot disagree.

**Two ways to give it a log, and they are separate calls.** `Stream.new(root=, schema=)`
creates or opens one — `new` the first time, `open` every time after, which is the
try/except every caller otherwise writes — and takes litelink's own `new()` keywords
(`sort_by`, `config`, `published`, `s3_options`) with the stream's name fed through. The plain
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

**A stream without a log can still declare a schema**, and then it accepts exactly the rows a
stream with a log would:

```python
stream = streamcast.Stream("trades", schema=SCHEMA)   # no log, still checked
```

Every row goes through litelink's own `validate_row` — the same checks `append` makes — so a
wrong type, an unknown or missing column, or a NaN or infinity raises with the message a log
would give, and nothing is sent to anyone. The greeting publishes the schema, frames follow
its column order, `where=` is checked against it, and binary and map columns work as they do
with a log. Attaching a log later changes nothing about what is accepted. The check costs
~4–14 µs a row (it depends on the machine), which a stream that declares nothing doesn't pay.
Passing `schema=` with `log=` is refused: a log carries its own.

**Without a schema a stream checks nothing**, on purpose: it is a shape-agnostic relay. One
consequence: JSON has no NaN or infinity, so a non-finite float sent on such a stream reaches
subscribers as `null`. Declare a schema to have it refused instead.

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

`max_replay` bounds
how far back a subscribe may ask; **`None` removes the bound**, so nothing is ever refused
as `too_old`. With `replay_published=True` that makes the server a complete gateway to the
log — any language can replay the whole stream over a plain WebSocket, with no litelink and
no credentials of its own. The cost is that a long replay accumulates live messages behind
it and `serve`'s `max_backlog` is what drops the subscriber, so size the two together. **Size them against each other**: a replay streams
while live messages queue behind it. The defaults are exported as
`streamcast.MAX_BACKLOG` and `streamcast.MAX_REPLAY`, for a caller that wants to scale
from them rather than restate them.

`group_commit` (on by default) lets the stream commit sends from several publishers in one
transaction when they queue behind a commit in flight — more throughput under concurrent
publishers. `group_commit=False` makes each send its own commit. See Publishing; the
greeting says which a stream makes.

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

**Neither awaits a consumer.** With a log, each awaits its own commit, which runs on the
stream's writer thread so no other stream on the broker waits on this one's disk; without
one, neither awaits at all. See `SPEC.md` §3 for the ordering that is a correctness
property rather than a performance note — and for the one hazard on a stream with no log:
a publish loop with no `await` of its own starves every subscriber.

**Sends that queue behind a commit share the next one**, by default: under concurrent
publishers that is the throughput — measured 4,232 rows/s from 8 publishers against
~1,300 committed one at a time. Every row is still durable before its send returns and
before any subscriber sees it, in the same order, and each send's rows stay adjacent. A
lone publisher never waits for a group to form. `group_commit=False` makes each send its
own commit; the greeting's `group_commit` says which guarantee a stream makes.

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
                 replicate=False, max_backlog=8192,
                 max_inbound=65_536, max_in_flight=64,
                 **websockets_kwargs) -> Server
```

Runs a **log-backed pub/sub broker**: publishers on one side, subscribers on the other,
neither owning the process. It owns the log, assigns every offset, and is litelink's
single writer.

Routing is exact-match on `Stream.name`. There is no topic hierarchy and no wildcard
subscription, and that is a consequence rather than an omission: a subscriber resumes by
offset, an offset belongs to one log, so a subscription spanning streams would need a
cursor per stream.

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

**Three keywords bound every queue the server keeps**, so neither a slow consumer, a slow
disk nor a fast publisher can run it out of memory. They are the serving process's
settings, not the streams', so a restart may change them:

| keyword | bounds | at the bound |
|---|---|---|
| `max_backlog` (8,192) | frames a subscriber may fall behind | it is dropped with `TooSlow` |
| `max_inbound` (65,536) | rows a durable stream may have queued for commit, across every publisher | a local `send` waits; a remote publisher's connection stops being read, so TCP holds it back |
| `max_in_flight` (64) | replies owed one publisher connection | the connection stops being read |

Each takes one int for every stream, or a map from stream name to int that names exactly
the streams served — a missing name or one not served raises `ValueError` at `serve`, as
does a bound below 1:

```python
streamcast.serve([trades, quotes], host, port,
                 max_inbound={"trades": 262_144, "quotes": 65_536}, max_backlog=16_384)
```

`max_backlog` is messages, not bytes (see [`SPEC.md`](SPEC.md) §4). Nothing at
`max_inbound` is refused, and a batch larger than it is let in alone, when nothing else is
queued.

`host=None` binds every interface, exactly as `websockets` does. Pass `"127.0.0.1"` for a
server that should only serve its own box, which is the case this library is built for.
TLS is `ssl=`; authentication is `process_request=`. See [`SECURITY.md`](../SECURITY.md).

### The metadata file

Before it listens, `serve` writes `root/<stream>.metadata.json` for every stream with a log
that doesn't have one yet, and, when the log publishes to S3, makes sure
`<published>/<stream>.metadata.json` matches it. That costs one GET, plus a PUT only when
something changed. **If either fails, `serve` raises instead of starting.** The file is
what lets anything other than this server read the stream (#32), so a broken one is found
at deploy. The ASGI app does the same when its lifespan starts.

```json
{"streamcast_metadata": 2, "stream": "trades", "stream_id": "6f1c…",
 "sealed_logs": [], "live_log": {"name": "trades", "published": "s3://…",
 "start_offset": 1, "start_ts": 1790038800123456, …}, "manifest": null}
```

The upload uses the `s3_options=` the stream was created with (`Stream.new`, `Stream.migrate`,
`Stream.restore`, or `Stream(log=…, s3_options=…)`), and otherwise the environment. A stream from
an earlier release gets its file on its first `serve`, and a version-1 file is rewritten
as version 2. A `Stream(log=…)` handed a log the
file says is sealed is refused.

### `maintain`

**`maintain=True` starts one maintainer covering every stream that has a log** — a set of
five subprocesses, one per role of litelink's recommended split — and stops them when the
server closes:

| role | runs | every |
|---|---|---|
| `seal` | `seal()` | 0.25 s |
| `compact` | `compact()` | 10 s |
| `publish` | `publish()` | 10 s |
| `clean` | `evict()`, `reclaim("buffer")`, `reclaim("staging")`, `sweep("staging")` | 10 s |
| `clean-published` | `reclaim("published")`, `sweep("published")` | 60 s |

A step gets its own process when it is heavy on CPU or the network, so a minute-long push
to a slow bucket never delays a seal, and compaction never competes with it for an
interpreter. One set for the server, not one per log: each process is a full interpreter
with litelink, pyarrow, pyiceberg and duckdb loaded — 149 MB RSS measured — so the set
costs five of those, shared by every log.

`Maintain(dedicated=("trades",))` gives a named log a set of its own, for one busy enough
that its compaction would hold up the others'. Names are the **log's**, not the
route; a name this server does not serve with a log raises at `serve` rather than being
ignored, and naming every log starts no shared maintainer at all. Without it, nothing in this library ever calls litelink's
`seal()` — measured on 100,000 rows (~14 MB, past the 8 MiB seal target): the buffer
held every one of them, the table held zero Parquet files, and `buffer.db` was 15.7 MB and
growing. litelink says it plainly: *"A maintainer is not optional."*

```python
streamcast.serve(streams, host, port)                          # one set of five, all logs
streamcast.serve(stream, host, port, maintain=False)           # you run your own
streamcast.serve(stream, host, port,
                 maintain=streamcast.Maintain(seal_every=0.1, publish_every=30))
```

`Maintain` is a frozen dataclass of one cadence per role — `seal_every`, `compact_every`,
`publish_every`, `clean_every`, `clean_published_every`, with the defaults above — and
`dedicated`. The cadences differ by orders of magnitude because the costs do: `seal` is an
indexed read of one row when there is nothing to seal, compaction rewrites files, and the
published table's cleanup lists a bucket. **`clean` is the only role that deletes buffer
rows already in staging** (litelink's `evict("buffer")`): `seal` and `publish` move data
and delete nothing, so a maintainer without it grows `buffer.db` without bound.

**It is always a subprocess, and there is deliberately no thread option.** A seal is
CPU-bound pure Python, so it starves a thread sharing its interpreter even holding no
lock — litelink measured appends running 45.2 ms behind an in-process seal. On a fan-out
server that is 45 ms with nothing fanned out and no keepalive answered, which surfaces as
latency spikes that look like a network problem.

It dies with the server, which is safe because litelink allows one writer: nothing is
appending, so an unsealed buffer is not growing. On Linux that includes a `SIGKILL` of the
server: the child is started with `PR_SET_PDEATHSIG`, as the sidecar is below. Elsewhere
only an orderly shutdown stops it. Both children run in sessions of their own, so a
terminal's Ctrl-C reaches the server alone, and the server stops them. The other direction is supervised — a
maintainer that exits while the server runs is restarted with backoff, because losing it
silently returns the server to never sealing.

`maintain=False` is right when you run the roles yourself — litelink's `examples/adsb/`, or
one container per role — or when the log is shared with something else that sweeps it.
`python -m streamcast maintain --role ROLE --every SECONDS --log PATH NAME` is one role's
loop, runnable by hand; repeat `--log` to sweep several from one process, which is what
`serve` does for each role.

A migrated stream's retired logs are not maintained: `retire()` published, evicted and
swept them completely, and litelink refuses them a writer.

### `replicate`

**`replicate=True` runs litestream** for any stream whose log has `wal_replication` on,
which is what makes that log survive losing its machine. `serve` is the one thing you
start; there is no second process to remember.

```python
log = litelink.new(root, "trades", schema=SCHEMA,
                   config=litelink.LogConfig(wal_replication=True),
                   published="s3://bucket/prefix")

async with streamcast.serve(
    streamcast.Stream("trades", log=log), host, port, replicate=True
):
    ...        # sealing, compaction, publishing, and WAL shipping all running
```

**It is off by default** — opt in with `replicate=True`. `wal_replication` is opt-in on the
log too, so for almost every deployment there is nothing to replicate anyway. A log that
has `wal_replication` on, served without `replicate=True`, gets a `UserWarning` naming it
at every start — one line — because silence would leave you believing the log is
protected. That includes running your own, more finely tuned litestream, where the line
is a reminder rather than a fault.

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

## `Stream.stats`

```python
stream.stats        # -> Stats
```

Counters this object already holds — no log query, no socket, nothing that can block or
fail. Poll it as often as you like.

| field | |
|---|---|
| `name`, `durable` | which stream, and whether it has a log |
| `end_offset` | the offset the next message gets, or None with no log |
| `subscribers` | attached right now |
| `started_ts`, `uptime_s` | when this `Stream` was constructed, and how long ago |
| `last_send_ts`, `last_send_age_s` | the most recent `send`, or **None** |

**Ages come from `time.monotonic`; timestamps are wall clock.** An age is therefore
immune to an NTP step and means the same thing on a machine whose clock disagrees with
yours, while the timestamps are what a human reads and what correlates with your own
logs. A caller subtracting `last_send_ts` from its own `time.time()` would be measuring
clock skew as much as staleness, which is why both are published.

**`last_send_*` is None until this process sends**, and that means *not in this process*
rather than *never*: the log may hold millions of rows from before the last restart.
`uptime_s` is what disambiguates, and a health check needs both — see the `/health` route
in `examples/fastapi_app.py`.

### What it deliberately does not carry

**No `status`.** Freshness is domain knowledge and a threshold here would be wrong for
someone while looking authoritative. Classification belongs to the application.

**No `rows_1m`.** Read `end_offset` twice and you have the rate over the window you
actually care about. A window chosen here is the same mistake as a threshold.

**No `maintain` / `replicate`.** A `Stream` does not own its children — `serve` does, and
a mounted app's object does — so it would be answering for something it cannot see.

### `serve(stats=True)`

Serves the same payload for every stream on the port `serve` already listens on, at
`/stats`, or at a path of your own: `stats="/_internal/streams"`. `served_at` is included so
a caller can measure its own clock skew.

**On by default, unlike `publish=`**, and the asymmetry is deliberate. `publish` grants
writes that nothing else on the port grants. This discloses strictly *less* than the
socket beside it: a wrong-path connect is answered with `serves=` naming every stream,
the greeting carries `end_offset`, and anyone who can reach the port can subscribe and
read every row in full. Everything here but the subscriber count is derivable by
subscribing, so gating it would protect nothing while leaving a quiet stream
undiagnosable by default — the failure it exists to fix.

A server that needs this private needs the port private; one that needs the port public
has already published the names. `stats=False` turns it off.

A `process_request` of your own composes rather than being overwritten: yours is called
for every path but this one, sync or async.

A mounted ASGI app gets no equivalent and needs none — it has routes already, and
`stream.stats` is the object to write one over.

## `asgi`

```python
from streamcast.asgi import asgi

asgi(streams, *, maintain=True, replicate=False,
     max_backlog=8192, max_inbound=65_536, max_in_flight=64)
```

The same streams behind an ASGI app, for a service that already has one. Needs the extra:
`pip install 'streamcast[asgi]'`, which adds Starlette and nothing else — not FastAPI,
which is a layer above what this uses, and not uvicorn, which is the deployer's choice.

```python
streams = asgi([trades, quotes])

@asynccontextmanager
async def lifespan(app):
    async with streams:
        yield

app = FastAPI(lifespan=lifespan)
app.mount("/streams", streams)
```

`serve` and `asgi` are two transports for the same `Stream`; a mounted app calls one of
them and never both. [`examples/fastapi_app.py`](../examples/fastapi_app.py) is a
complete service that runs.

Returns an object that is both an ASGI application and an async context manager. It takes
the `serve` keywords that are about the streams, and none of the ones about a socket: no
`host`, `port`, `ssl`, `compression` or `ping_interval`, because those belong to the server
the host app is running and restating them here would be two places to set one thing.

**`async with` is what starts the maintainers and closes the streams, and it is
required.** It is the mounted twin of what `serve`'s `wait_closed` does: children
stopped, then `aclose` on each stream, which closes a log `Stream.new` opened and leaves
a handed-in handle alone. Starlette does not run
a mounted sub-app's lifespan — documented behaviour, not a bug — so an app relying on
lifespan events alone starts no maintainer once mounted, and a log with nothing sealing it
buffers every row it ever receives. The app handles `lifespan` too, for the case where it is
run directly rather than mounted; both routes are idempotent.

Routing, refusals and close codes are identical to `serve`'s, including the 4400/4404/4410/
4416/4429 reasons, with one difference ASGI forces: the handshake is **accepted** before a
refusal can be sent, because a close code only exists on an accepted connection.
`websockets` refuses after its handshake too, so what a client sees is the same; rejecting
the upgrade instead would turn every refusal into an HTTP 403 with the reason discarded.

### What moves to the ASGI server

| | `serve` | mounted |
|---|---|---|
| keepalive | `ping_interval=20` — a dead peer surfaces as a close in ~40s | uvicorn's `--ws-ping-interval` |
| compression | off; the encode is shared across subscribers, deflate is per connection | the host app's setting |
| TLS, binding, HTTP/2 | `serve`'s keywords | the host app's |

Compression is the one that bites. `serve` turns permessage-deflate off because a frame is
encoded once and handed to every subscriber, while deflate then compresses those identical
bytes once per connection: 4 µs of CPU per message at one subscriber against 691 µs at 200.
A host app that enables it globally pays that, and the symptom is a CPU-bound server
dropping subscribers for falling behind — an outage that reads like a bug.

### The transport boundary

`_transport.Peer` names what the stream layer uses from a connection — `send`, `close`,
`wait_closed`, and `async for` — and nothing else. `_stream` and `_subscriber` are written
against that Protocol, so both transports are ordinary callers and neither knows the other
exists.

The adapter runs one reader task per connection, which `serve` does not need: `websockets`
reads in the background, while ASGI delivers a disconnect as a message nobody sees until
`receive()` is called. A subscription never reads, so without that task a subscriber who
walked away from a quiet stream would never be noticed — the pump parks in `queue.get()`
and the `Subscriber` stays in the fan-out set for ever.

## `publish`

```python
streamcast.publish(uri, *, cursor=None, cursor_uri=None, s3_options=None,
                   upload_every=30.0, max_in_flight=64,
                   **websockets_kwargs) -> Publication

await producer.send(row) -> int | None          # durable, then fanned out
await producer.send_many(rows) -> list          # ONE transaction for the group
await producer.submit(row) -> Future[int | None]        # written; the future: durable
await producer.submit_many(rows) -> Future[list]        # the same, for a group
producer.info -> Greeting                       # incl. the stream's schema
producer.connection -> ClientConnection
producer.resumed_from -> int | None              # the offset last acked, from `cursor`
producer.commit(offset=None) -> None            # save the cursor now
await producer.close(code=1000, reason="") -> None
```

Publishing from a process that is not the server's.

**This one is not a `websockets` function.** WebSocket has no publish — RFC 6455 defines
frames, not verbs, and `websockets` exposes `serve`, `connect` and `broadcast`. Pub/sub
protocols layered over WebSocket do define one (WAMP's `PUBLISH`, MQTT's), but streamcast
implements none of them: this is shaped like `connect` — awaitable, async context manager,
keywords passed through — so it reads the same way, and the request is a query parameter
rather than a message type, exactly as `?offset=` is for a subscribe. The server appends
with the same `Stream.send` / `send_many` a local publisher calls, so `send` returns once the row is
durable and `send_many` is the same throughput lever it is locally.

```python
async with streamcast.publish("ws://localhost:8765/trades") as producer:
    offset = await producer.send({"event_ts": 1790038800123456, "price": 85565.0})
```

**Every served stream takes publishers.** Whether a stream can be written is the
stream's to say, not the server's: a retired one refuses a publisher with 4410
(`StreamRetired`). There is no authentication yet, so a port any client can reach is
one any client can read and write.

The URI is the stream's, the same string `connect` takes — `?publish` is added by the
client, so one address in a config file serves both ends.

| | |
|---|---|
| **one writer** | any number of publishers, one `WriteHandle`, held by the server. Offsets stay contiguous and a batch stays one commit with publishers racing — I1 is what makes that free |
| **`Rejected`** | a row the schema refuses, with litelink's message naming the column. Nothing committed; the connection stays open and the next row works |
| **all or nothing** | a rejected row in a `send_many` commits none of the group, because it is one transaction |
| **pipelined** | up to `max_in_flight` (64) frames unanswered per connection, answered in the order sent, so a reply needs no correlation id. `send` waits for its own; `submit` does not |

**`cursor=` records the offset this publisher was last acknowledged for**, and
`cursor_uri=` ships it to object storage so a producer can resume on another box — the
same two keywords `connect` takes. It does not resume by itself: a producer cursor says
where this publisher got to, not what it should send next, which is its own outbox.
`resumed_from` reports it and you act on it. Saves are throttled to once a second and
settled on a clean exit; `commit()` forces one, `commit(offset)` states what you consider
settled. A cursor that lags only widens the recovery replay; one that leads would skip
rows and duplicate them.

**`submit` is the lever for one publisher.** A loop of `await producer.send(row)` waits a
round trip per row: about 1,000 rows/s on localhost, less over any real network. A loop of
`await producer.submit(row)` keeps up to `max_in_flight` rows on the wire; the server
queues each as it reads it, so rows that arrive while a commit is in flight share the next
(`group_commit`) — measured 8,198 rows/s from one publisher, against 998 with `send`.
`submit` returns once the frame is written, waiting first if the window is full, with a
future that resolves to the offset once the row is durable or raises what `send` would.
Acknowledgements come back in the order rows were sent, and `close()` waits for the ones
still owed. The server bounds what it will owe one connection with
`serve(max_in_flight=)`, 64 by default; a client allowed more is held at the socket.

```python
async with streamcast.publish("ws://localhost:8765/trades") as producer:
    futures = [await producer.submit(row) for row in rows]
    offsets = await asyncio.gather(*futures)   # whenever you need them
```

**Publishing is at-least-once under retry.** A row is durable when `send` returns, but if
the connection drops before the reply arrives the publisher cannot tell whether the append
happened — and with `submit`, up to `max_in_flight` rows may be in that state at once. Carry a publisher key and a per-publisher sequence as columns and recovery
becomes a query against the log — [`SPEC.md`](SPEC.md) §6b has the pattern and the
arithmetic.

## `connect`

```python
streamcast.connect(uri, *, offset=<unset>, cursor=None, cursor_uri=None,
                   s3_options=None, upload_every=30.0, catch_up=False,
                   catch_up_retries=3, metadata=None,
                   **websockets_kwargs) -> Subscription
```

Awaitable and an async context manager, like `websockets.connect`.

```python
async with streamcast.connect("ws://localhost:8765/trades", offset=123) as stream:
    async for offset, ts, msg in stream:
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
await sub.recv() -> tuple[int | None, int | None, dict[str, object]]   # (offset, ts, msg)
async for offset, ts, msg in sub: ...
await sub.recv_many(limit=500) -> list[tuple]   # at least one row, then what has arrived
async for batch in sub.batches(limit=500): ...  # recv_many until an ordinary close
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

`ts` is `streamcast_ts`: when the server took the row, in UTC microseconds — the value
its log stored, so a replayed or caught-up row carries the same `ts` it carried live. A
stream with no log sends its send time. `None` only for a row of a log created before the
column existed.

Iteration **stops** on a normal close (1000/1001) and **raises** on anything else — the
same contract as iterating a `websockets` connection, with the refusals below filling in
for what a bare code cannot say.

**`recv_many` takes what has already arrived, and never waits for more.** It waits for
the first row as `recv` does, then takes every row already on the connection, up to
`limit` — the rule group commit follows on the publishing side. A consumer that keeps up
gets small batches at `recv`'s latency; one that falls behind gets everything that
queued, which costs it less per row, so it catches up and the batches shrink again.
Catch-up rows are in memory once read, so a catch-up's batches come full.

```python
async for batch in sub.batches(limit=500):
    await database.insert_many(row for _offset, _ts, row in batch)
```

The same rows in the same order as `recv`, checked the same way. The cursor counts a
batch as handled when the next one is asked for, so a handler that raises reads the whole
batch again. If the connection ends partway through, the rows before it are returned and
the next call raises the refusal, whose resume offset is the last row you were given.

`info` is the greeting: `end_offset` (the server's frontier at subscribe), `replay` (the
`[start, end)` about to be replayed, or `None`), `durable` (whether these offsets survive
a server restart), `group_commit` (whether sends may share a commit; false means each
send is its own), `schema` (the stream's columns as JSON Schema), `metadata`,
`stream_id`, `stream`, `version`.

**`info.metadata` is enough to read the stream's history yourself.** It is the URI of the
stream's metadata file, which is what [`Stream.snapshot`](#reading-a-streams-history)
takes, and `stream_id` is the id that file records:

```python
table = await streamcast.Stream.scan(sub.info.metadata, as_of_offset=sub.info.end_offset)
```

Both are `None` when the stream has no log. Credentials are never published — they are
the reader's own, resolved from its environment the way litelink resolves them. A stream
that publishes to a local directory names a `file://` URI, which reads only on the
server's machine; anywhere else the read says so.

### `where=` — filtering a subscription

```python
connect(uri, where={"ticker": "AAPL"})              # equality
connect(uri, where={"ticker": ["AAPL", "NVDA"]})    # membership
connect(uri, where={"ticker": "AAPL", "qty": 100})  # AND
connect(uri, where={})                              # no restriction
```

A JSON object of column to value, carried as `?where=` so `wscat` can use it too. Scalars
compare by equality; a list means membership, unambiguously, because `_schema` refuses
array columns outright — a column can never hold one. Terms combine with AND.

There is no expression language and no `eval`: the predicate comes from a client over a
socket. Refused with a 4400, naming the problem:

| | |
|---|---|
| a column the schema does not have | `where names column 'tikcer', which this stream does not have` |
| a value that is not a scalar or list of them | `where['ticker'] is {...}; a filter value is a scalar` |
| an empty list | `where['ticker'] is an empty list, which matches nothing` |
| `?publish&where=` | `publish takes no where=; a publisher receives nothing` |

Refused rather than accepted-and-silent, because a filter is the one request whose failure
mode is indistinguishable from a quiet stream.

**Cost.** The predicate is compiled once at subscribe and specialised on its arity, then
runs per message per filtered subscriber against the dict `send` was called with. Measured
on a four-column row: 162 ns for one term, 260 ns for two, against 961 ns for the
`msgspec` encode already on the path. The encode stays shared — a filter decides whether
to enqueue the frame, not what to build — so filtering never costs a second serialisation.

**The replay is filtered identically**, through the same predicate. `_log.rows` already
yields dicts, so there is one implementation and nothing to diverge; pushing the predicate
into the scan's SQL would prune more but Python and SQL disagree about coercion and NULL,
and that disagreement would surface as a resume delivering what the live stream did not.

**`replay` in the greeting becomes an upper bound.** Unfiltered, `end - start` is exactly
what arrives before live; filtered, the count is unknown until the scan runs, so the window
bounds it. The recovery loop in [`SPEC.md`](SPEC.md) §6b reads a count and therefore wants
an unfiltered subscription.

`Greeting.where` echoes the applied filter, so a subscriber can confirm the server
understood the predicate rather than assume it.

On a **live-only** stream with no `schema=`, there are no declared columns, so no name can be checked and any
is accepted. A typo silently matches nothing there — a property of having no schema rather
than of the filter.

## Resuming

**`cursor=path` is the whole of it.** The file holds the last offset finished with; the
subscription loads it at connect, resumes one above, and saves as the loop runs.

```python
async with streamcast.connect(uri, cursor=".trades.offset") as stream:
    async for offset, ts, msg in stream:
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

**A consumer that batches with `recv_many` or `batches` can use the automatic save**: it
counts a batch as handled when the next is asked for. One that builds its own batches out
of `recv` should not — that save advances per row read, ahead of what it has flushed — and
calls `commit()` once each batch is durable, or drives `streamcast.Cursor` itself.

```python
sub.commit()          # force a save now — for a batching or non-idempotent consumer
sub.commit(offset)    # ...at an offset you actually committed
```

### `catch_up` — when the server will not replay that far back

A consumer that has been down long enough falls past `max_replay`, and the server refuses
with `NotReplayable(why="too_old")`. The rows are not gone — they are in the stream's
published tables — but getting them means knowing where those are, reading them without
running out of memory, and working out where to resume the socket. `catch_up=True` does
all of it:

```python
async with streamcast.connect(uri, cursor=".trades.offset", catch_up=True) as stream:
    async for offset, ts, msg in stream:
        handle(msg)
```

The consumer sees one stream. Underneath, rows below the server's window come from object
storage and the rest from the socket.

It is built on [`Stream.snapshot`](#reading-a-streams-history): each round takes a
snapshot of everything published above the consumer's offset, streams its rows, and
connects at the offset the snapshot ended at.

**Nothing is connected while the tables are read**, and that is the part that matters.
Holding the socket open through the read would make the server queue for a subscriber that
will not take a message until it has pulled millions of rows out of S3 — and `max_backlog`
is 8,192, so it would be dropped with `TooSlow` before the catch-up finished, failing
exactly the consumers that need it. Tested at `max_backlog=16`, catching up 20,000 rows.

So it is a **loop**: read the published tables, try to connect at the offset they reached,
and if the server has moved on far enough to refuse again, read the newly published rows
and try once more. It converges when the published tables get inside the server's window.
`catch_up_retries` (default 3) bounds it for when that never happens, and the failure says
which of three things to change.

| | |
|---|---|
| `catch_up=False` *(default)* | the plain `NotReplayable` refusal; handle the gap yourself |
| `catch_up_retries=3` | rounds of read-then-connect before giving up |
| `metadata="s3://bucket/prefix/trades.metadata.json"` | the metadata file to read from; otherwise the greeting's |
| `s3_options=streamcast.S3Options(...)` | credentials; otherwise the environment |

**Where the metadata file comes from.** The greeting names it, with the stream's id, so a
refused client spends one throwaway connection asking. The refusal does not: a close frame
is 123 bytes and a bucket URI does not fit beside the numbers. The id is checked against
the file, so a file that belongs to another stream is refused rather than read. An
explicit `metadata=` wins, and is trusted to be the file the caller meant.

**Failures land at `connect`, not at the first `recv`.** The first snapshot is opened before the
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

**What it cannot fix.** If the published tables end below the server's window, a range
exists that neither holds — the server has forgotten it and it was never published. The
same is true at the other end: tables that start above the consumer's offset do not hold
the rows it asked for. That is reported with both numbers rather than half-served, because a
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
    cursor_uri="s3://streamcast/consumer1/stream.offset",
) as stream:
    ...
```

**`cursor_uri` names the object, not the prefix holding it.** The local path and the
remote key are independent — `.trades.offset` locally and `…/consumer1/stream.offset`
remotely is an ordinary pairing — and a URI ending in `/` is refused rather than completed
with the local filename. Deriving one from the other meant renaming a local file moved the
remote object, and one `cursor_uri` shared by two consumers with different local names
wrote to two places while reading as one setting.

A **daemon thread** uploads the cursor every `upload_every` seconds (30 by default) and
once more on a clean exit. It is a thread rather than the event loop or `asyncio.to_thread`
because the PUT is blocking and best-effort, and that pool — `min(32, cpu + 4)` — is what
every replay scan uses.

**On connect the local cursor wins**; the remote is read only when there is no local one.
That is the disaster-recovery case, and the only one where a copy that lags by up to
`upload_every` should decide. When it is used, the value is written down locally too, so a
second restart on the new box needs no bucket.

Credentials resolve from the environment and `s3_options=streamcast.S3Options(...)` overrides them
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
├── Rejected           a published row the schema refuses; nothing committed
├── StreamRetired      the stream is retired and takes no rows; `.at`, `.end_offset`
└── TooSlow            dropped for falling behind; `.offset` is where to resume
```

`NotReplayable.why` is the one worth branching on, because the next move differs:

| `.why` | what to do |
|---|---|
| `not_durable` | drop `offset=`, or give the server a log |
| `empty` | subscribe live; there is nothing to replay yet |
| `ahead` | your cursor is above the server's frontier — it was restored or rebuilt |
| `too_old` | `catch_up=True`, or read the log directly and subscribe from where you stopped |
| `evicted` | below what the scan's tier holds; `catch_up=True` if the published tables go back further, else accept the gap |

`.fields` carries whatever numbers survived the close frame — `offset`, `earliest`,
`behind`, `max_replay`, `end_offset` — and `str(exc)` is a sentence built from them.

`Close` is the code enum: `BAD_REQUEST` 4400, `NO_SUCH_STREAM` 4404, `RETIRED` 4410,
`NOT_REPLAYABLE` 4416, `TOO_SLOW` 4429.

## The schema is yours

You declare the columns, in **JSON Schema**, because the wire is JSON and this is a
library about JSON websockets:

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

**The table carries two columns you did not declare.** `litelink_offset` is litelink's, and
every frame carries it as its offset. **`streamcast_ts`** is streamcast's: the time the server
took the row, in UTC microseconds — so `streamcast_ts - event_ts` is feed latency per row,
live as `ts - msg["event_ts"]` and queryable over the whole history. Every frame carries it
as its `ts`, the value the log stored, so a replay sends what the live frame did; the
greeting's `schema` leaves it out. A stream with no log sends its send time. `send_many`
gives its whole group one value, because the group commits as one transaction. It is wall
clock, so a clock step on the server shows in it.

The name is reserved. A declaration that uses it is refused, and so is a row that
supplies it. A log created before the column existed opens unchanged and is not stamped,
and a log you pass as `log=` is stamped only if its schema has the column — each log's
`system_schema` in the metadata file says which.

| JSON | `format` | Arrow |
|---|---|---|
| `boolean` | — | `bool` |
| `integer` | — or `int64` | `int64` |
| `integer` | `int32` | `int32` |
| `number` | — or `double` | `float64` |
| `number` | `float` | `float32` |
| `string` | — | `string` |
| `string` + `contentEncoding: "base16"` or `"base64"` | — or `bytesN` | `binary`, or `fixed_size_binary(N)` |
| `object` + `properties` | — | `struct` |
| `object` + `additionalProperties: {…}` | — | `map<string, …>` |
| `array` + `items` | — | `list` |

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
vocabulary rather than Arrow's: `date-time` (store epoch integers), the narrow and unsigned
integer widths Iceberg would widen or can't hold, and unions.

### Binary and nested columns

```python
SCHEMA = {
    "type": "object",
    "properties": {
        "trace_id": {"type": "string", "contentEncoding": "base16", "format": "bytes16"},
        "payload": {"type": ["string", "null"], "contentEncoding": "base64"},
        "attrs": {"type": "object", "additionalProperties": {"type": "string"}},
        "res": {"type": "object", "properties": {"service": {"type": "string"}},
                "required": ["service"], "additionalProperties": False},
        "tags": {"type": ["array", "null"], "items": {"type": "string"}},
    },
    "required": ["trace_id", "attrs", "res"],
}
```

- **Binary** is `bytes` in Python and text on the wire, in the column's `contentEncoding`:
  `base16` (hex, as OTLP/JSON writes trace and span IDs) or `base64` (a third smaller).
  `format: "bytesN"` fixes the size. A remote publisher sends the text; `send` takes
  `bytes`; a `Subscription` hands you `bytes` back, and so does catch-up.
- **A map is a JSON object** from string keys. Pass a `dict` to `send`: a list of pairs is
  refused, because it would replay as a different frame than it was sent as.
- **`where=`** works on binary columns, with the value given as text in the column's
  encoding (`where={"trace_id": "4bf92f35…"}`). Struct, list and map columns can't be
  filtered on.
- A `binary` column is for small values. Large payloads wait for litelink's blob fields
  (#40).

```python
streamcast.to_arrow(SCHEMA)     -> pa.Schema      # if you want the litelink schema
streamcast.from_arrow(schema)   -> dict           # what the greeting publishes
```

**The greeting carries the schema**, so a subscriber in another language reads the columns
without this repo:

```json
{"streamcast":4,"stream":"trades","end_offset":1861,"replay":null,"durable":true,
 "schema":{"type":"object","properties":{"event_ts":{"type":"integer","format":"int64"}}}}
```

Widths are stated explicitly on the way out, so what a subscriber reads back is what the
column actually is.

**A caveat JSON cannot fix:** integers beyond 2^53 do not survive every parser. Python and
msgspec carry int64 exactly; a JavaScript subscriber silently rounds. A nanosecond
`event_ts` is past it — microseconds, which the examples use, are not.

### Changing the schema: `Stream.migrate`

A log's shape is fixed when it is created, so a new schema means a new log. With the
server stopped:

```python
stream = streamcast.Stream.migrate("trades", root="data", schema=SCHEMA_V2)
async with streamcast.serve(stream, "localhost", 8765):
    ...
```

The current log is retired: sealed for good, published in full, and refusing writers from
then on. Then `trades-v2` is created with the new schema, starting at exactly the offset
the old log ended at. `root/trades.metadata.json` (and a copy beside the published tables,
when they are on S3) records the sequence. A migration that dies partway finishes on the
rerun.
Offsets carry on as one dense sequence. `Stream.new` and `Stream.restore` open whichever
log the metadata names as current.

It is **idempotent**: a stream whose current log already has that schema and every
system column streamcast defines today is opened rather than migrated again, so the call
can live in your startup. Migrating with an *unchanged* schema is therefore the upgrade: a
log created before a system column existed (`streamcast_ts`, or one added in a later
release) moves onto a log that has it. `metadata.json` records each log's
`system_schema` beside its `schema`, so which logs have which is readable at a glance.

**What may change:** columns added, columns removed, nullability. **What may not:** a
column's type, ever, including after it has been removed. Re-adding a name takes the type
it had. A stream's logs are read together with `UNION ALL BY NAME`, where a changed type
coerces silently (`int64` beside `string` becomes a string column) rather than failing.
Widening is refused too.

### Finishing a stream: `Stream.retire`

A stream that just stops being written isn't finished: its last rows stay on the box,
because `publish` holds back the trailing run for compaction, and nothing refuses a late
write. With the server stopped:

```python
streamcast.Stream.retire("trades", root="data")                 # finished
stream = streamcast.Stream.restore(
    "trades", root="data", published="s3://market-data/prod", revive=True,
)                                                                # and undone
```

Retiring publishes every row (the trailing run included), retires the log in litelink so
it refuses writers for good, and records it in `metadata.json`: when (`at`, UTC
microseconds), where the log ended, its `sort_by` and its `streamcast_ts` span, locally
and beside the tables. A retire that dies partway finishes on the rerun.

**A retired stream is read-only, and still served.** `new` and `migrate` open its log for
reading and set `stream.retirement`. Subscribers replay and catch up, always from the
published table, its only copy; a `live` view sees nothing new. A local `send` raises
`StreamRetired`, a publisher is refused with 4410, and it gets no maintainer or sidecar.

**`restore(..., revive=True)` undoes it**, on the same box or another one: the stream
continues on its next log, `trades-v2` or later, starting at exactly the retired end, with
the same columns and sort. Retiring published everything first, so reviving loses nothing,
and nothing is needed from the old box. Without `revive=True`, `restore` refuses a
retired stream.

A retired stream's `metadata.json` is version 3, so a streamcast build from before
retirement refuses it, rather than taking the retired log for a migration that died and
quietly starting the next one. Reviving writes version 2 again.

**There is no rename.** Changing `price` to `px` removes one column and adds another.
Nothing is backfilled or merged, so a read across the seam returns both, each null in the
logs that did not have it. Combining them (`coalesce(px, price)`) is up to your
application.

**The server replays only the current log.** A consumer that was caught up when the
server stopped resumes at the seam with nothing lost. One further behind is refused
`evicted`, with `earliest` at the seam, and `catch_up=True` bridges it: it reads through
`Stream.snapshot`, which reads every log of the stream. `offset=EARLIEST` means the start
of the current log.

`sort_by` and `config` default to the current log's. `s3_options=` is what the metadata file is
uploaded with, both here and at `serve`.

Each migration also adds the retired log to `<stream>.manifest.parquet`: per-column bounds
and counts, read from the log's own Iceberg statistics without opening a data file. It is
what lets a reader of the whole stream skip logs that can't match (#32). The metadata file
points to it.

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
| **the published tables** | any Iceberg engine reads them, with nothing installed |
| **the replay** | rows come off Arrow as columns, not as strings to re-parse |

A log written by streamcast is an ordinary litelink log, so everything litelink offers
applies unchanged: `scan`, `sql`, publishing to S3 and WAL replication. A whole stream —
every log a migration left behind — reads from another machine with
[`Stream.snapshot`](#reading-a-streams-history).

```python
with litelink.open("data", "trades", read_only=True) as reader:
    reader.sql("SELECT count(*), max(price) FROM log").read_all()
```

Note litelink's own caution about opening a reader in the writer's process — a separate
process is the supported shape.

**A frame that is not a row has nowhere to go.** Subscription acks, heartbeats and
reconnect notices are dropped by the feed handler, which is the same division of labour a
kdb tickerplant has: the feed handler parses, the plant stores typed rows.

## Reading a stream's history

```python
await streamcast.Stream.snapshot(metadata_uri, *, as_of_offset=None, as_of_ts=None,
                                 broker=None, s3_options=None,
                                 max_tail=1_000_000, memory_cache=True,
                                 disk_cache=False, cache_key=None,
                                 disk_cache_volume_limit=0.8) -> Snapshot
await streamcast.Stream.scan(metadata_uri, *, <the same>, columns=None, where=None,
                             filters=(), start_offset=None, end_offset=None) -> pa.Table
await streamcast.Stream.sql(metadata_uri, query, *, <the same>, filters=(),
                            start_offset=None, end_offset=None) -> pa.Table

stream.metadata_uri -> str | None     # on the server
sub.info.metadata -> str | None       # on a subscriber
```

A subscription delivers rows one at a time, in order. To ask a question of a stream — an
aggregate, a join, a scan of last Tuesday — read it as a table. There are two readers, and
both present every log the stream has been through (migrations included) as one table,
`log`, with the same `scan` and `sql`:

| | `Stream.snapshot` | `Stream.live` |
|---|---|---|
| **answers as of** | one fixed point, chosen when it opens | the newest row, at every query |
| **reads** | the published tables | the published tables, plus the broker's rows as they arrive |
| **needs the server** | no — offline unless asked for rows not yet published | yes, always |
| **holds in memory** | the query's result (and the broker's rows, if asked for them) | the rows not yet published |
| **addressed by** | the stream's metadata file | the broker's URI |

A stream is a sequence of logs — one per schema, after `Stream.migrate` — and its
metadata file says which, in order, where each is published, and the offsets and
`streamcast_ts` range each holds. `Stream.snapshot` reads that file and then the published
tables, on the reader's own machine with its own credentials. **Given the S3 metadata file,
it never touches the server**:

```python
s3 = streamcast.S3Options(region="us-east-1")    # or the environment / AWS profile

async with await streamcast.Stream.snapshot(
    "s3://market-data/prod/trades.metadata.json", s3_options=s3
) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side")
```

The broker is consulted only for rows no table holds yet, and only when asked: a point
past the published end needs `broker=`, and is refused without it. Every other read is a
read of files.

```python
async with await streamcast.Stream.snapshot(sub.info.metadata) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side")
    await snapshot.scan(columns=["price"], filters=[("price", ">", 500.0)])
    async for offset, ts, row in snapshot.rows(1):
        ...
```

`Stream.scan` and `Stream.sql` are the one-shot forms: open, read once, close.

**A fixed point, three ways to name it** — at most one of them:

| | |
|---|---|
| *(neither)* | everything published. The live log's unpublished tail is left out, and no broker is involved |
| `as_of_offset=N` | every row up to and including offset `N`. Past what is published the rest comes from `broker=`, and is refused without it |
| `as_of_offset=streamcast.LATEST` | the broker's frontier when it connects; needs `broker=` |
| `as_of_ts=T` | every row whose `streamcast_ts` is at most `T`. Published rows only — the broker is consulted as of an offset, not a time — so a `T` past what is published is refused rather than answered short |

`snapshot.end_offset` is exclusive: every row the snapshot holds is below it, and a reader
carrying on live subscribes there.

| | |
|---|---|
| `scan(columns=, where=, filters=, start_offset=, end_offset=)` | rows in `[start_offset, end_offset)`, oldest first, as one Arrow table |
| `sql(query, filters=, start_offset=, end_offset=)` | `query` over the snapshot, which it reads as the table `log`, holding only the rows in `[start_offset, end_offset)` that match `filters` |
| `rows(start, stop=None)` | one `(offset, ts, row)` at a time, built exactly as a server replays them |
| `close()` | or `async with` |

`where` is SQL over the stream's columns. `filters` are `(column, operator, value)` terms,
ANDed with `where`. Across a migration the logs read as one table with `UNION ALL BY
NAME`, so a column a log lacked reads as null there, in a condition as in a result.

**`filters=` and `where=` both filter rows, but only `filters=` can skip a log.** The
result is the same either way; what differs is how much gets read.

- **`filters=`** is a list of `(column, operator, value)` terms — `==`, `<`, `<=`, `>`, `>=`,
  `in` — ANDed together. Because each term is that simple, it can be checked against every
  log's per-column min and max (`<stream>.manifest.parquet`) *before the log is opened*: a
  log whose prices all sit below 85,000 is never read. The terms are then applied to the
  rows too, so what you get never depends on what was skipped. Numeric and boolean columns
  skip; others still filter, they just can't rule out a log.
- **`where=`** (and a `WHERE` in `sql`'s query) is any SQL expression: arithmetic, `OR`,
  functions, other columns. It is applied to the rows of every log in range, and skips
  none. Reading arbitrary SQL well enough to rule logs out is easy to get subtly wrong —
  an `OR` or a cast misread drops a log that held matches, and the answer comes back short
  with nothing to show for it — so it isn't attempted
  ([#57](https://github.com/nhobin219/streamcast/issues/57)).

So state the cheap, prunable part of a predicate as `filters=` and the rest as `where=`.
Offset bounds (`start_offset=`, `end_offset=`) skip logs the same way `filters=` does.
Within a log, DuckDB's own Parquet statistics still prune files for both.

```python
await snapshot.scan(
    columns=["event_ts", "price"],
    filters=[("price", ">", 85_000.0)],            # skips whole logs that can't match
    where="side = 1 AND amount * price > 1000",    # any SQL, applied to rows
)
await snapshot.sql("SELECT max(price) FROM log WHERE side = 1",
                   filters=[("price", ">", 85_000.0)])   # narrows the table `log`
```

**Correct or it raises `SnapshotUnavailable`.** A missing metadata file, a file whose
`stream_id` is not the one the greeting named, a retired log whose table holds fewer rows
than it had when it was retired, and a range neither the tables nor the broker holds, all
raise. The last one names both numbers. A short answer to an analytical question is a
wrong number, not an error, so it is never given.

**`max_tail` bounds what a snapshot reads from the broker**: the rows the published tables
do not hold yet, which it keeps in memory. That stays small while publishing keeps up; past
`max_tail` rows (1,000,000 by default, counted as read) the snapshot is refused, saying
publishing is behind, rather than running the reader out of memory.

**Caching what is read is litelink's, and yours to choose.** The four keywords are
`litelink.duckdb_connection`'s, with its defaults, and the same on `snapshot`, `scan`,
`sql` and `live` — the reads that repeat:

| keyword | default | what it does |
|---|---|---|
| `memory_cache` | on | DuckDB's external file cache: what was read stays in memory for the process |
| `disk_cache` | off | `s3://` reads kept on disk by `cache_httpfs`, across restarts |
| `cache_key` | `None` | the disk cache's directory: relative to litelink's cache root (`cache_key="<stream id>"` is `~/.cache/litelink/<stream id>`), absolute as given, or `None` for its `default` |
| `disk_cache_volume_limit` | 0.8 | how full the disk cache's volume may get, everything on it counted |

Nothing chooses a key for you, because only you know what deserves a cache of its own: a
stream id, a team, a job. Readers asking for different settings read through different
DuckDB databases. A disk cache earns its keep where reads cross a network to object
storage; against a store on the same machine, it measured no faster than reading it
again. `connect` takes none of them: a catch-up reads the gap to its cursor once, never
the same files twice, so it reads with litelink's defaults, as does a `live` view's
catch-up at open or after a dropped connection.

**A cached reader still sees every publish.** Each read resolves the table's current
metadata with `litelink.current_metadata`, outside DuckDB's caches: through a disk-cached
connection, `version-hint.text` would pin a reader to an old snapshot (litelink#141).
Everything the hint names is written once, and caches safely.

**Where the metadata file is.** With an `s3://` published location, the copy beside the
tables, which reads from anywhere. Without one, every log publishes under its own
directory and the URI is the local `file://` file, which reads only on the server's
machine. On another machine the read says so and suggests an `s3://` location.

### Keeping it current: `Stream.live`

```python
await streamcast.Stream.live(broker, *, s3_options=None, rebase_every=10.0, where=None,
                             start_offset=None, max_tail=1_000_000, memory_cache=True,
                             disk_cache=False, cache_key=None,
                             disk_cache_volume_limit=0.8) -> Live

async with await streamcast.Stream.live("ws://localhost:8765/trades") as live:
    await live.wait_for(offset)                  # until that row is visible
    await live.wait_for(ts=t)                    # until every row stamped by t is
    await live.sql("SELECT side, sum(amount) FROM log GROUP BY side")
    await live.scan(columns=["price"], filters=[("price", ">", 500.0)])
    live.end_offset                              # one above the newest row a query sees
```

A `Live` is a `Snapshot` kept moving — **real-time analytics on a stream in one line**:
the published tables as a base, and the broker's rows appended in memory as they arrive,
so every `scan` and `sql` answers as of the newest row received. It is **online by
design**: it takes only the broker's address, because a view of the stream *now* has to be
listening to it. For an offline read, or a fixed point to come back to, use a snapshot. They take the same arguments as on a `Snapshot`, and the rows read the same
— the broker's carry their `streamcast_ts` like published ones.

| | |
|---|---|
| **open** | one connection for the greeting, a published snapshot of the metadata file it names, then a subscription at the snapshot's end with `catch_up=True`: no gap and no duplicate at the join |
| **where** | the broker is the only address. Its greeting names the metadata file and the stream's id, read again at every reconnect, so the view follows the stream as the broker serves it now. A stream with no log has nothing published and raises `ValueError` |
| **memory** | only what is not yet published. Every `rebase_every` seconds, and after every reconnect, the base is re-pinned to what is published now and the rows it covers are dropped. If publishing stalls, the view stops at `max_tail` rows (1,000,000 by default) and its next query raises, rather than holding whatever a stalled publisher leaves it |
| **reading** | a background task only appends; rows become Arrow when a query asks, and a query runs in a thread, so a slow one never stalls the socket |
| **drops** | a closed connection, `TooSlow` or a network error reconnects from the last row received, with catch-up and a capped backoff |
| **`wait_for`** | exactly one of `offset` (that row has arrived) or `ts=` (every row stamped at or before it has). A time is proven only by a row stamped after it, so on a quiet stream `wait_for(ts=)` waits for the next row: bound it with `asyncio.timeout` where the stream can go idle. Published rows count. Refused on a log without `streamcast_ts` |
| **`where=`** | narrows the view as `connect(where=)` narrows a subscription, on both sides of the join: the server sends only matching rows, and the published tables are read with the same terms as `filters=`, so every query sees only matches and retired logs that can't match are skipped. Equality and membership over **non-null scalars on scalar columns** only — `None` means "is null" to the subscription and matches nothing in SQL, and a binary column is text on the wire and bytes in the table — so those are refused at open; filter them in the query instead. With `where=`, `wait_for(offset)` returns once a *matching* row at or past it has arrived |
| **`start_offset=`** | the lowest offset any query sees. `streamcast.LATEST` is the broker's frontier at open: a view of what happens from now. Rows still move to the published tables at each rebase, read from there above the start, so memory stays the publish lag |
| **failures** | anything reconnecting can't fix is kept and raised by the next query or `wait_for`, so a broken view never answers from stale data |

Queries run one at a time: each reads one DuckDB connection, which cannot run two.

## On the wire

Every frame is a text frame of JSON. The greeting, then a **three-element array** per
message: the offset, the time the server took the row, then the row. Reading a row in another language, including binary columns, is
spelled out step by step in [SPEC §2](SPEC.md#reading-a-row-in-another-language).

```
{"streamcast":4,"stream":"trades","end_offset":1861,"replay":[1200,1861],
 "metadata":"s3://market-data/prod/trades.metadata.json",
 "stream_id":"5f0c…","schema":{...},"durable":true}
[1861,1790038800124001,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

(The greeting is one line on the wire; it is wrapped here to fit.)

The offset and the stamp are **positional**, not keys in the object —
`const [offset, ts, msg] = JSON.parse(frame)` — so `msg` is the publisher's row and nothing
else, with no column to strip before forwarding it. `offset` is `null` on a stream with no
log; `ts` is `null` only for a row of a log created before `streamcast_ts` existed. No binary header, no length prefix, no payload kind.

`wscat ws://localhost:8765/trades?offset=0` is a working subscriber, and a consumer in any
language needs a JSON parser rather than this document.

Encoding is [msgspec](https://github.com/jcrist/msgspec), which sits on the hot path in
both directions — every publish encodes a row, every replayed row re-encodes one. Measured
on a six-column trade row: **0.285 us against 5.815 us** for stdlib `json` to encode
(20.4x), and 0.386 against 4.989 to decode (12.9x). The ratio narrows as one large string
column comes to dominate a frame; `just bench` prints both for your own shape.
