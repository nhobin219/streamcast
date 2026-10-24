# API

Everything public, on one page. [`SPEC.md`](SPEC.md) says what the system is and why;
this says what you can call.

```python
import streamcast
from streamcast import Cursor, EARLIEST, Maintain, Stream, Subscription, to_arrow
```

Declare columns in JSON Schema and open a `Stream`; the canonical FastAPI
application owns serving, maintenance and shutdown. Clients use `connect` and
`publish`, with `websockets` transport keywords. Server transport settings belong
to the ASGI host, rather than the stream runtime.

**Iterating a subscription yields `(offset, ts, msg)`**, not `message`. The offset is the
only thing that makes a reconnect a resume rather than a restart, and a subscriber that
has to ask for it separately will forget to. `ts` is `streamcast_ts`, when the server took
the row. `msg` is a `dict` over your declared columns **and nothing else** — the offset
and the stamp are framing, not data, and never appear inside it.

**A subscription is read-only.** It has no `send` — rather than a `send` that raises —
because publishing uses `Stream.send` locally or `publish` remotely. Nothing inherits a
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
                  max_replay=100_000, group_commit=True, exactly_once=False,
                  max_publishers=4096)

streamcast.Stream.open(name="", *, root, schema, create=False, restore=False,
                      sort_by=None, config=None, published=None, s3_options=None,
                      replay_published=False, group_commit=True,
                      max_replay=100_000, exactly_once=False)

streamcast.Stream.migrate(name="", *, root, schema, ...)  # a new log, a new schema
streamcast.Stream.restore(name="", *, root, published,    # on a box without the log
                          s3_options=None, schema=None, sort_by=None, config=None,
                          replica_reserve=None, published_reserve=None,
                          revive=False, ...)
streamcast.Stream.retire(name="", *, root, s3_options=None)  # for good; returns the
                                                            # record: .at, .end_offset
streamcast.Stream.retain(name="", *, root, max_age, s3_options=None)  # timedelta or None
streamcast.Stream.ingest(name="", source, *, root, s3_options=None,  # Arrow, in bulk;
                         publish=True, flush=None)                    # -> (start, end)
```

`name` is where it is served: `"trades"` at `/trades`, `""` at `/`. It is the name's only
home — routing and the greeting both read it, so they cannot disagree.

**`Stream.open` opens an existing log and checks its declaration.** With
`create=True` it creates only when neither disk nor published prefix holds the stream,
passing `sort_by`, `config`, `published` and `s3_options` to litelink on creation.
A missing local log with a published replica raises `FileExistsError` unless
`restore=True` is passed; it then restores before opening. An absent stream raises
`FileNotFoundError` unless `create=True`. An unreachable store propagates its error.
`Stream.restore` on an empty prefix raises `FileNotFoundError`.
Stop the old writer before restoring on another machine.
 An existing log keeps its stored configuration
and published location. The plain
initialiser takes a handle you already opened and reads its frontier. `s3_options` is
passed to litelink on creation and reopening, and retained for metadata uploads and child
processes. Credentials are not persisted: pass the options again on each open, or use the
environment.

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
stream = streamcast.Stream.open("trades", create=True, root="data", schema=SCHEMA,
                               sort_by=("event_ts",))
```

**Any shape of log works** — the schema is yours, and `Stream` reads its column order once
at construction to fix the key order on the wire.

**Who closes it depends on who opened it.** `aclose` closes a log the `Stream` owns —
which `Stream.open` sets — and never one passed to the initialiser, since that one stays
yours, for a process that also reads or writes it through litelink. Pass `owns_log=True`
alongside `log=` to hand over the lifetime of a handle you opened. An existing log is checked against
a declared `schema=` rather than adopted, because a declaration that disagreed with the
disk would be silently ignored and every `send` validated against columns the caller never
wrote down.

`max_replay` bounds how many rows a subscribe may replay; **`None` removes the bound**,
so nothing is ever refused as `too_old`. With `replay_published=True` that makes the
server a complete gateway to the log — any language can replay the retained history over
a plain WebSocket, with no litelink and no credentials of its own. A long replay
accumulates live messages behind it; size `create_app`'s `max_backlog` against that backlog.
The defaults are exported as `streamcast.MAX_BACKLOG` and `streamcast.MAX_REPLAY`, for a
caller that wants to scale from them rather than restate them.

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

Drops every subscriber with a 1001, concurrently, drains the writer and closes a log the
stream owns. A handle passed with `owns_log=False` stays open.

## `create_app` and `BrokerConfig`

```python
streamcast.create_app(config=None, *, streams=()) -> fastapi.FastAPI
streamcast.BrokerConfig(
    maintain=True, replicate=False, max_backlog=8192, max_inbound=65_536,
    max_in_flight=64, max_message_bytes=None, transport_max_bytes=1<<20,
    subscriber_yield_every=64, stats=True, ...)
```

This is the sole broker application and runtime. `streams` is a static `Stream`
or iterable; a registry configuration adds dynamic management to the same
runtime. Construction opens no files or sockets. Static streams cannot be
changed through HTTP, and their names must not overlap registered definitions.
Names route exactly: no wildcard subscription or hierarchy. Duplicate static
names are rejected. Existing logs are preserved; startup does not reset offsets.

FastAPI and uvicorn are dependencies, and uvicorn is the supported server. Run
**one worker**, lifespan enabled, without reload or pre-fork workers: the
lifespan owns each stream's writer and storage locks, so a second worker would
be a second writer. A second worker over the same logs fails to start on
their ownership locks (`BlockingIOError`); one over live-only streams, which
have nothing to lock, would start and split publishers from subscribers.
Multiple workers are not supported. Set the server's
frame ceiling to `transport_max_bytes` too: application validation happens after
ASGI transport allocation. TLS, bind address, keepalive and compression belong
to that server. Disable permessage-deflate for shared-encode fan-out unless its
per-connection CPU cost is justified for the deployment.

```python
import uvicorn
app = streamcast.create_app(streams=[trades, quotes])
# Equivalent CLI: uvicorn module:app --workers 1 --lifespan on \
#     --ws-per-message-deflate false --ws-max-size 1048576
uvicorn.run(app, host="127.0.0.1", port=8765, workers=1,
            lifespan="on", ws_per_message_deflate=False, ws_max_size=1<<20)
```

The top-level lifespan owns admission, connections, replay tasks, writers and
children. Static streams and registry-backed streams use that same owner.
Shutdown fences writes, closes peers, joins replay and writer work, then reaps
children and closes owned handles. Borrowed log handles remain the caller's.

Startup waits for the children to report ready, up to `readiness_timeout` (30
seconds), and fails rather than serve without a maintainer: five interpreters
loading pyarrow and DuckDB can take most of that on a small loaded host. Static
streams share the maintainer group with managed ones, so a managed stream's
transition, which replaces the group, also fences a static stream's writes
until the new group is ready, rather than accept rows no maintainer will seal.
A local `stream.send` then raises `ValueError("stream unavailable")`; retry
after a moment. A publisher connection is closed with 1001, as at a stop, with
nothing of the refused frame queued: `publish` raises `ConnectionClosed` and an
`outbox` reconnects and resends. It is never answered `rejected`, which an
outbox would drop from the caller's database.

| setting | default | bounds |
|---|---|---|
| `max_backlog` | 8,192 | queued frames per subscriber; overflow drops that subscriber |
| `max_inbound` | 65,536 | rows queued for commit; producers wait at capacity |
| `max_in_flight` | 64 | outstanding replies per publisher connection |
| `max_message_bytes` | `transport_max_bytes` | each incoming publisher frame |
| `subscriber_yield_every` | 64 | positive number of outbound data frames between scheduling checkpoints |

The four per-stream bounds take a positive integer or an exact name-to-integer
map for static streams. Managed definitions carry their own bounds below.
`transport_max_bytes` is a positive application-wide ceiling; per-stream frame
limits must fit beneath it. A batch larger than `max_inbound` enters alone.

`subscriber_yield_every` is how many data frames a subscriber is sent before
its task yields to the event loop. A send to a writable socket need not
suspend, so without the checkpoint one subscriber's ready replay would run
every other task on the loop — publishers included — out until it finished.
Replay and live delivery share one counter; 1 yields after every frame, and
there is no setting that never yields. The checkpoint is outside the atomic
commit, delivery and subscribe partition, so it changes neither frame order nor
durability. 64 is provisional: the measurement that chooses the default is not
made yet. Static streams take it from `BrokerConfig`; managed streams too.

### Mounting

A mount does not start a child lifespan. The host enters it explicitly:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
streams = streamcast.create_app(streams=[trades, quotes])
@asynccontextmanager
async def lifespan(app):
    async with streams.router.lifespan_context(streams):
        yield
app = FastAPI(lifespan=lifespan)
app.mount("/streams", streams)
```

The child owns the same runtime whether direct or mounted. Never start another
application over those streams. See `examples/fastapi_app.py`.

### Where a stream's files live

Each stream has a directory of its own under `root`, so a root serving many streams lists
one entry per stream:

```
data/                                s3://bucket/prefix/
  trades/                              trades/
    trades.metadata/                     trades.metadata/
      version-hint.text                    version-hint.text
      00004-9f2c….metadata.json            00004-9f2c….metadata.json
      00004-9f2c….manifest.parquet         00004-9f2c….manifest.parquet
    trades.metadata.json                 trades.metadata.json      a plain copy
    trades.manifest.parquet              trades.manifest.parquet
    trades/          the first log       trades/       its published table
    trades-v2/       after a migrate     trades-v2/
  quotes/                              quotes/
```

**A stream never changes layout.** One created before 0.18 keeps the one it has, for
good: `data/trades.metadata/` beside `data/trades/`, which is its first log's
directory, with `<published>/trades.metadata/` and `<published>/trades/` on the
published side. Every log it adds keeps that layout too, through migrations, restores on
another box and revivals. A new stream starts in its own directory and stays there.
Which layout a stream has is read from what's on disk, or from where `restore` finds its
metadata, and both layouts are served alike.

To move a stream to the new layout, create a new stream and load the old one's rows into it
with [`Stream.ingest`](#loading-rows-in-bulk-streamingest).

### The metadata file

Before it listens, `create_app` writes the first version of the metadata, in
`root/<stream>/<stream>.metadata/`, for every stream with a log that doesn't have one yet,
and, when the log publishes to S3, makes sure the current version under
`<published>/<stream>/<stream>.metadata/` matches it. That costs one GET, plus a PUT only when
something changed. **If either fails, `create_app` raises instead of starting.** The file is
what lets anything other than this server read the stream (#32), so a broken one is found
at deploy. The ASGI app does the same when its lifespan starts.

```json
{"streamcast_metadata": 2, "stream": "trades", "stream_id": "6f1c…",
 "sealed_logs": [], "live_log": {"name": "trades", "published": "s3://…",
 "start_offset": 1, "start_ts": 1790038800123456, …}, "manifest": null}
```

**Each change is a new version, behind a hint**, as an Iceberg table's metadata is. A
migration, retirement, revival or restore writes an immutable
`<stream>.metadata/<n>-<id>.metadata.json`, plus a new manifest beside it when there is
one, and then rewrites `<stream>.metadata/version-hint.text` to name it. The last ten
versions are kept. A reader opens the hint, which the greeting gives as `metadata_hint`. The
version and manifest it names are never rewritten, so a reader that opens the stream again,
such as a `Live` view every few seconds, reads only the hint, a few bytes. A reader
caches each file per process, per set of credentials, up to 16 MiB in all.

**A plain copy of the current version** is written beside the versions after each commit,
`<stream>.metadata.json` and `<stream>.manifest.parquet`, for clients from before the hint,
which open the greeting's `metadata`. A stream from before 0.18 has only those, read until
its first version is written. `Stream.snapshot` and `connect(metadata=)` take a
`<stream>.metadata.json` URI too, and read it through the hint beside it.

Clients accept protocol 4 or 5; the server sends 4. `info.metadata` is the best URI
in the greeting: the hint when one is present. The rolling-upgrade pairs exercised
by CI are listed in [`tests/test_interop.py`](../tests/test_interop.py); the 0.20.1
[CHANGELOG](../CHANGELOG.md) entry describes the upgrade order around 0.20.0.

The upload uses the `s3_options=` the stream was created with (`Stream.open`, `Stream.migrate`,
`Stream.restore`, or `Stream(log=…, s3_options=…)`), and otherwise the environment. A stream from
an earlier release gets its file on its first `create_app`, and a version-1 file is rewritten
as version 2. A `Stream(log=…)` handed a log the
file says is sealed is refused.

### `maintain`

**`maintain=True` starts one maintainer covering every stream that has a log** — a set of
five subprocesses, one per role of litelink's recommended split — and stops them when the
server closes:

| role | runs | every |
|---|---|---|
| `seal` | `seal()`, and `seal(flush=True)` once per `flush_every` | 0.25 s |
| `compact` | `compact()` | 10 s |
| `publish` | `publish()`, and `publish(flush=True)` once per `flush_every` | 10 s |
| `clean` | `evict()`, `reclaim("buffer")`, `reclaim("staging")`, `sweep("staging")` | 10 s |
| `clean-published` | `reclaim("published")`, `sweep("published")` | 60 s |

A step gets its own process when it is heavy on CPU or the network, so a minute-long push
to a slow bucket never delays a seal, and compaction never competes with it for an
interpreter. One set for the server, not one per log: each process is a full interpreter
with litelink, pyarrow, pyiceberg and duckdb loaded — 149 MB RSS measured — so the set
costs five of those, shared by every log.

`Maintain(dedicated=("trades",))` gives a named log a set of its own, for one busy enough
that its compaction would hold up the others'. Names are the **log's**, not the
route; a name this server does not serve with a log raises at `create_app` rather than being
ignored, and naming every log starts no shared maintainer at all. Without it, nothing in this library ever calls litelink's
`seal()` — measured on 100,000 rows (~14 MB, past the 8 MiB seal target): the buffer
held every one of them, the table held zero Parquet files, and `buffer.db` was 15.7 MB and
growing. litelink says it plainly: *"A maintainer is not optional."*

```python
streamcast.create_app(streams=streams)                          # one set of five, all logs
streamcast.create_app(streams=stream, config=streamcast.BrokerConfig(maintain=False))           # you run your own
streamcast.create_app(streams=stream, config=streamcast.BrokerConfig(
    maintain=streamcast.Maintain(seal_every=0.1, publish_every=30)))
```

`Maintain` is a frozen dataclass of one cadence per role — `seal_every`, `compact_every`,
`publish_every`, `clean_every`, `clean_published_every`, with the defaults above —
`flush_every`, and `dedicated`. The cadences differ by orders of magnitude because the costs do: `seal` is an
indexed read of one row when there is nothing to seal, compaction rewrites files, and the
published table's cleanup lists a bucket. **`clean` is the only role that deletes buffer
rows already in staging** (litelink's `evict("buffer")`): `seal` and `publish` move data
and delete nothing, so a maintainer without it grows `buffer.db` without bound.

**`flush_every` (60 s) is the RPO without WAL replication, and how far the published table
trails the writer.** A plain `seal()` cuts only at litelink's `target_seal_size` and a plain
`publish()` pushes only finished files — 512 MiB on disk since litelink 0.11 — so unflushed,
the published table trails by up to a whole file per log: days, on a stream of 100 rows a
second, all of it on this machine only. Once per `flush_every` the seal role passes
`flush=True`, cutting whatever is buffered, and the publish role does too a few seconds
later, pushing what was cut. A row then reaches the published table about `flush_every +
publish_every` after it is written, plus the upload. Every other pass is unflushed, so files
still end up at litelink's targets: what a flushed publish pushes early is swapped for the
finished file later, at the cost of uploading those rows twice and one published commit per
log per flush. Flushes fall on wall-clock multiples of the interval, so the two processes
agree on them across restarts. `flush_every=None` never flushes, leaving RPO to WAL
replication (`replicate=True`), which bounds it in seconds either way. Compaction is never
flushed.

The published table is what a catch-up, a snapshot and a `Live` view read, so this is also
their freshness: a `Live` view holds every row not yet published in memory, up to
`max_tail`.

```python
streamcast.create_app(streams=stream, config=streamcast.BrokerConfig(
    maintain=streamcast.Maintain(flush_every=900)))  # 15 min
```

**A graceful stop fences writes before draining**, then stops the seal role,
which seals with `flush=True`, and the publish role, which publishes with
`flush=True`, then the remaining children. The publish role gets 20 seconds
before it is killed; other roles get 5 seconds each. The host owns the overall
shutdown budget. `flush_every=None` skips the publish flush. Managed transitions
also verify final publication before relinquishing ownership.

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
`create_app` does for each role.

A migrated stream's retired logs are not maintained: `retire()` published, evicted and
swept them completely, and litelink refuses them a writer.

### `replicate`

**`replicate=True` runs litestream** for any stream whose log has `wal_replication` on,
which is what makes that log survive losing its machine. `create_app` is the one thing you
start; there is no second process to remember.

```python
log = litelink.new(root, "trades", schema=SCHEMA,
                   config=litelink.LogConfig(wal_replication=True),
                   published="s3://bucket/prefix")

app = streamcast.create_app(
    streams=streamcast.Stream("trades", log=log),
    config=streamcast.BrokerConfig(replicate=True),
)
# Serve app with one ASGI worker and lifespan enabled.
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

A missing litestream binary raises `SidecarUnavailable` when the app's lifespan starts, not at the first
missed push: a server that came up and replicated nothing would leave you believing you
had protection you did not have. The wheel bundles one, so this is only reachable on a
platform litelink ships no binary for.

### Credentials

**Both subprocesses get the stream's `S3Options`, through their environment.** litelink
never persists credentials, so a child that opens a log by name has no other way to learn
them. `create_app` starts each child with the variables below set from the `S3Options` the
`Stream` was given (`Stream.open(..., create=True, s3_options=)`), laid over the server's own
environment. Explicit keys also remove an inherited `AWS_SESSION_TOKEN`, which belongs to
other keys. A field you leave unset is left to that environment, so a profile, instance
metadata or SSO all work untouched, and a stream with no `S3Options` resolves in its
children exactly as it does in the server. The environment is used rather than the command
line because only the process's own user can read it, while anyone on the box can see a
command line in `ps`.

```bash
AWS_ENDPOINT_URL=...  AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=...  # the maintainer
LITESTREAM_ACCESS_KEY_ID=...  LITESTREAM_SECRET_ACCESS_KEY=...                         # litestream
```

One environment holds one set of options, so streams given different `S3Options` are
maintained, and replicated, by processes of their own: one per role per set.

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

**No `maintain` / `replicate`.** A `Stream` does not own its children — `create_app` does, and
a mounted app's object does — so it would be answering for something it cannot see.

### Statistics

`BrokerConfig(stats=True)` serves `/stats` on the same listener; a string
selects another absolute path and False disables it. `stats.read` grants filter
individual streams. `served_at` permits clock-skew measurement. `Stream.stats`
is the same per-stream object for application-specific health decisions.

## Dynamic management

Use root exports `BrokerConfig`, `StorageProfile`, `Authorization`,
`HistoryGuard`, `initialize_registry` and `create_app`. Provision a registry
once, explicitly, then start the application; startup never creates one:

```python
config = streamcast.BrokerConfig(
    broker_id="primary", registry=Path("state/broker.db"),
    profiles={"local": streamcast.StorageProfile("1", Path("data"))},
    authorization=authorization,
)
streamcast.initialize_registry(config)  # once; existing files are refused
app = streamcast.create_app(config)     # optionally streams=[static_stream]
```

### Ownership and recovery

The top-level lifespan owns one runtime: registry, operation worker, readiness
monitor, stream generations, connections, writer threads, maintenance roles,
and replication sidecars. It acquires an exclusive registry lock and holds
canonical logical-stream and physical-log locks even for stopped definitions.
Profile/layout/symlink aliases cannot create a second managed owner. These
are local cooperative locks, **not a distributed writer lease**; writers outside this runtime and other hosts must be stopped by the deployment owner first.
Hard-link aliases, lock-file deletion, and external mutation of managed logs
are unsupported. Keep the registry and log directories on durable local
storage that supports SQLite fsync and advisory locks.

Startup requires the explicitly provisioned registry with the configured
broker identity. It replays pending intent before reconciling other desired
states. It never initializes a missing registry or recreates a bound missing
log. A new definition starts stopped and creates no log. First serving may
create only an unbound `create_if_absent` declaration. Once a stream identity
is bound, disk and authoritative version-hint metadata must match it.

Each accepted mutation atomically commits the declaration/desired state,
operation and idempotency receipt before returning 202. HTTP disconnect or
cancellation does not cancel accepted work. Operations are serialized; reads
and data traffic do not wait on the control serialization lock. Blocking
storage work runs off the event loop. Subprocess creation runs on the
lifespan thread so its parent-death relationship survives worker completion.

Stop fences new reads and writes, closes connections, joins replay workers
including cancelled scans, drains the writer, stops and reaps children,
seals/publishes, closes handles, then releases ownership. A failed final
publication is an operation failure; handles and locks are retained rather
than handed to another owner. The process must exit and recover. Repeated
ASGI cancellation joins owned startup/shutdown tasks before returning.

Maintenance roles have fixed group membership. A stream transition replaces
the shared group: all writes are fenced while old children are reaped and
new children become ready. Neighbor stream objects and connections stay in
place; their write admission resumes after readiness. A maintainer reports
its PID only after opening its whole group. A replication sidecar must own
its logs and accept a connection on its control socket. This establishes
local readiness, **not remote object-store durability**. Startup/replacement
has a bounded readiness timeout (30 seconds); later child unavailability
fences writes after that deadline. Reads may continue. There is no promise
of uninterrupted writes during membership changes.

The host owns shutdown deadlines. Killing a process before draining may
leave accepted intent to recover on the next boot. An unavailable or failed
operation never proves a destructive transition had no effect.

### Host policy and authorization

`StorageProfile(revision, root, published=None, config=None, s3_options=None,
replicate=False)` holds host-only locations, litelink configuration and
credentials. Its immutable revision is bound into each declaration; changing
a profile revision requires a reviewed host handoff. HTTP never supplies
paths, URLs, credentials, WAL settings, or maintenance policy. Public profile
data contains revision, replication/maintenance flags and replay scope only.
WAL-replicated logs require a replication-enabled profile.

`BrokerConfig` also accepts `history_guards={}`, `transport_max_bytes=1<<20`,
`control_max_bytes=256*1024`, `max_pending=32`, `readiness_timeout=30.0` and
`readiness_every=0.25`. Bounds must be positive; a per-stream frame limit
cannot exceed the transport ceiling. The runtime bounds pending operations
and in-flight control tasks. It returns 429 with `Retry-After: 1` rather than
growing an unbounded queue. Durable operation and receipt history is retained;
there is no automatic eviction.

The injected `Authorization` protocol has two asynchronous methods:

```python
async def authenticate(scope) -> str | None: ...
async def allows(principal: str, action: str, resource: str) -> bool: ...
```

The principal is a stable nonsecret identity. Actions are `data.read`,
`data.publish`, `stats.read`, `control.read`, `control.mutate` and
`control.migrate`. Resources are exact stream names, with `"*"` for collection
and operation discovery. Discovery also filters each returned stream by its
individual grant. Operation lookup requires both the discovery grant and
the target stream grant.

Authentication and the applicable grant precede body parsing, stream lookup
and WebSocket upgrade. Unauthenticated requests receive 401 with a Basic
challenge; authenticated denials receive 403. A URI's percent-escaped user
information supplies ordinary Basic authorization to WebSocket clients and
to HTTP stats clients; credentials in query parameters are not interpreted.
The host validates Basic credentials or any other authentication mechanism,
trusted ingress, Origin and proxy headers. Registry hosting requires an explicit authorization policy. Static-only hosting defaults to public data and stats access; supply an authorization policy to restrict it. Do not log credential-bearing URIs.

### HTTP contract, version 1

All paths below are relative to the application mount. Requests and
responses are JSON. Mutations require `Content-Type: application/json` and
`Idempotency-Key` (1–128 printable ASCII characters, no spaces).
Missing keys receive 428; malformed keys receive 400. Duplicate
JSON keys, nonfinite numbers and malformed JSON receive 400. Bodies over the
configured control limit receive 413 before the application accumulates them.
Framework validation is supplemented by the existing schema compiler;
body reading is explicit so authorization and byte limits precede parsing.
The application exposes no Swagger, ReDoc or unauthenticated OpenAPI route.

Declarations describe the physical schema supported by `to_arrow`, rather
than a general JSON Schema validator. Unknown keywords, including `enum`,
`minimum` and `maximum`, are rejected with 422. A controller adopting an
existing application schema can explicitly declare
`streamcast.from_arrow(streamcast.to_arrow(application_schema))`. This
preserves column order, widths, nullability and nested encoding; it does not
enforce discarded application constraints. The application remains responsible
for those constraints, as with the static stream API. Review that conversion
before adoption rather than silently stripping keywords in the broker.

| Method and path | Result |
| --- | --- |
| `GET /control/v1` | Contract version, broker/boot identities, bounds, public profiles, protected names, wire versions. |
| `GET /control/v1/streams?limit=100&cursor=...` | Name-ordered page: `revision`, `streams`, nullable `next_cursor`. Limit is 1–1000. |
| `GET /control/v1/streams/{name}` | Definition, desired/observed state, revision, identity, pending operation and public profile; `ETag`. |
| `PUT /control/v1/streams/{name}` | Create or replace the full declaration. Create requires `If-None-Match: *`; replace requires `If-Match`. |
| `PUT /control/v1/streams/{name}/desired` | `{"state":"serving"}`, `"stopped"`, or terminal `"retired"`; requires `If-Match`. |
| `POST /control/v1/streams/{name}/migrations` | `stream_id`, target `schema` and target `sort_by`; requires `If-Match` and `control.migrate`. |
| `GET /control/v1/operations/{id}` | Operation `id`, `name`, `kind`, `revision`, `target`, `status`, nullable `error`. |
| `GET /stats` | The existing stats payload, filtered by `stats.read`. |

Names match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`. The ETag is the exact quoted
decimal revision, for example `"3"`. Missing preconditions receive 428; a
stale or malformed precondition receives 412. Both precondition headers
together are refused. Collection cursors bind the collection revision and
last name; a mutation invalidates an older cursor with 409. Restart from the
first page. A no-op does not change collection or stream revision.

A declaration is a JSON object with no unknown keys:

```json
{
  "schema": {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"]
  },
  "sort_by": [],
  "storage_profile": "local",
  "initialization": "open_existing",
  "exactly_once": false,
  "replay_published": false,
  "max_replay": 100000,
  "group_commit": true,
  "limits": {
    "max_backlog": 8192,
    "max_inbound": 65536,
    "max_in_flight": 64,
    "max_message_bytes": 1048576
  }
}
```

`schema` and `storage_profile` are required. Other keys default as shown
except `limits`, whose authoritative defaults are `_limits` and the host's
transport ceiling. `initialization` may also be `create_if_absent`;
`max_replay` may be null (unbounded) or a nonnegative integer. Limits are
positive integers. Boolean fields accept JSON booleans only.

Schema property order is significant. The schema compiler in
[`API.md`](API.md#stream) defines supported types, nullability, integer and
float formats, binary, arrays, structs and maps. The control API rejects
unsupported constraints and unknown keywords instead of accepting ignored
validation. `title` and `description` are annotations. Explicit
`additionalProperties: true` is refused. Reserved `streamcast_ts` and
`litelink_offset` columns are refused. Sort columns must be distinct declared
column names. Row validation and protocol framing remain the existing core's.

Non-schema definition changes require observed `stopped`. A bound stream's
storage profile cannot change. Schema/order/sort changes require an explicit
migration, with the current stream identity and both desired and observed
stopped (or a failed stopped transition). Migration keeps the active
definition separate from the durable target until metadata and target
schema are verified. Failed irreversible intent cannot be overwritten with
a different target. Retrying that exact intent uses a new idempotency key
and current ETag; reusing the original key returns its original receipt.
Retirement is terminal and leaves replay available without writes.

A mutation returns 202 with `status`, `operation_id`, `revision`, an ETag and
`Location: /control/v1/operations/{id}`. Poll the operation: `accepted` →
`running` → `succeeded` or `failed`. A satisfied no-op returns 200 and a
succeeded operation without a new generation. A receipt is scoped to the
principal and broker registry and hashes method, resource, ordered JSON body
and preconditions. An identical retry returns the original status/body/
Location **before checking today's ETag**. Different intent under that key
receives 409. The controller persists keys across network retries and polls
to completion; 202 alone does not establish readiness.

Observed state carries `state`, `boot_id`, `generation`, `revision`, `at`
(Unix seconds), `read_admission`, `write_admission`, and `children_ready`.
These are observations of this boot, not durable proof of current health.
Errors use `{"error":{"code":...,"message":...,"retryable":false,
"operation_id":null}}`; messages contain no storage paths, credentials or
rows. Statuses include 400 malformed input, 401 authentication, 403 grants
or history guard, 404 unknown resources, 409 conflict, 412 preconditions,
413 size, 415 media type, 422 unsupported declaration, 428 preconditions
required, 429 capacity, and 503 unavailable. 429/503 are retryable and include
`Retry-After: 1`. A failed operation reports `transition_failed`; inspect
host-side evidence before retrying an irreversible operation.

### Data consumer contract and protected history

Data remains `ws[s]://host/{name}`; publishing uses `?publish` and, for
exactly-once streams, `&publisher={stable_id}`. Subscriptions support the
existing `offset`, projection and filtering query semantics. Wire readers
accept versions 4 and 5; greetings still write 4. Rows are text JSON
`[offset, timestamp_us, application_row]`. Existing publisher sequence,
duplicate ACK, refusal, retirement, replay and per-subscriber backpressure
rules are unchanged. There is no new client database or archive dependency.
Unknown/inactive streams close 4404 after authorized upgrade, malformed data
requests 4400, and oversized frames 1009. A stop closes connections with
1001; clients reconnect with their durable cursor and publisher identity.

For an application whose thread/history reader starts at `offset=0`, current
log replay is insufficient after migration or retention. Such a deployment
**must inject** `history_guards={name: HistoryGuard(stream_id, known_offset,
known_row)}` from a handoff audit. Before data admission the broker verifies
the authoritative identity, no migration/retention/pending metadata, live
origin at offset one, exactly-once publishing, published replay, unbounded
replay, a dense complete prefix through the current tail, and the captured
old row. It scans history through the same broker replay machinery; this can
make startup expensive for large histories. No latency claim is made for
that audit. Protected migration and retirement are rejected **before
journaling** even with control grants. An already migrated/trimmed stream fails admission;
it needs a separately reviewed multi-log history design. Removing a host
guard or mutating storage outside this owner invalidates that assurance.

### Integration responsibilities

The library owns runtime, protocol, durable intent and local lifecycle
invariants. A deployment controller owns declaration reconciliation,
idempotency keys, ETags and operation polling. The host operator owns storage
profiles, authentication, ingress, shutdown budgets, retained-state handoff,
and stopping previous writers. Each consuming application owns its stream
bindings, stable producer identities, durable cursors, frame sizing and
reconnect behavior. Release and adoption require those owners' acceptance,
supported-version interoperability, real replication checks and a reviewed
consumer change. The canonical application replaces the standalone and static ASGI hosting APIs. This is a breaking API change; release and adoption require separate review.


## `publish`

```python
streamcast.publish(uri, *, publisher=None, max_in_flight=64,
                   **websockets_kwargs) -> Publication

await producer.send(row) -> int | None          # durable, then fanned out
await producer.send_many(rows) -> list          # ONE transaction for the group
await producer.submit(row) -> Future[int | None]        # written; the future: durable
await producer.submit_many(rows) -> Future[list]        # the same, for a group
producer.info -> Greeting                       # incl. the stream's schema
producer.connection -> ClientConnection
await producer.close(code=1000, reason="") -> None
```

Publishing from a process that is not the server's.

**This one is not a `websockets` function.** WebSocket has no publish — RFC 6455 defines
frames, not verbs, and `websockets` exposes `create_app`, `connect` and `broadcast`. Pub/sub
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

**`submit` is the lever for one publisher.** A loop of `await producer.send(row)` waits a
round trip per row: about 1,000 rows/s on localhost, less over any real network. A loop of
`await producer.submit(row)` keeps up to `max_in_flight` frames on the wire; the server
queues each as it reads it, so rows that arrive while a commit is in flight share the next
(`group_commit`) — measured 8,198 rows/s from one publisher, against 998 with `send`.
`submit` returns once the frame is written, waiting first if the window is full, with a
future that resolves to the offset once the row is durable or raises what `send` would.
Acknowledgements come back in the order rows were sent, and `close()` waits for the ones
still owed. The server bounds what it will owe one connection with
`BrokerConfig(max_in_flight=)`, 64 by default; a client allowed more is held at the socket.

```python
async with streamcast.publish("ws://localhost:8765/trades") as producer:
    futures = [await producer.submit(row) for row in rows]
    offsets = await asyncio.gather(*futures)   # whenever you need them
```

A frame may contain a batch. **A plain publisher is at-least-once under retry.** A row
is durable when `send` returns, but if the connection drops before the reply arrives the
publisher cannot tell whether the append happened — and with `submit`, up to
`max_in_flight` frames may be in that state at once. **`publisher=` makes it exactly
once** — see below and [`SPEC.md`](SPEC.md) §6b.

### Exactly-once: `publisher=`

```python
stream = streamcast.Stream.open("orders", create=True, root="data", schema=SCHEMA, exactly_once=True)

async with streamcast.publish(uri, publisher="enricher") as out:
    out.info.publisher -> PublisherMark(id, last_seq, last_offset)   # where the broker has us
    await out.send(row, seq=n)                  # rows 1, 2, 3, … with no holes
    await out.send_many(rows, seq=n)            # rows n, n + 1, …
```

The publisher names one ordered sequence (1 to 128 bytes of UTF-8, stable across
restarts, distinct per parallel producer) and numbers its rows with no holes. The broker
keeps the last seq it appended for that publisher, committed in the same transaction as
the rows in a table of its own in the log's buffer, and judges every frame against it:

| | |
|---|---|
| **next seq** | appended |
| **at or below the mark** | not appended; `send` returns the original offset, or `None` where the broker no longer remembers it (after a restart, below the mark) |
| **past the next** | `SequenceGap(expected, got)`, nothing appended, the connection open. Not a `Rejected`: a gap is never a row to drop |
| **another connection, same id** | the older one ends with `Superseded` (4409); the newer greeting's `last_seq` counts every row the older one queued |
| **stream not `exactly_once`** | `ExactlyOnceUnavailable(why="stream")` at `publish()` (4412) |
| **server before 0.23** | `ExactlyOnceUnavailable(why="broker")` at `publish()`: it refuses the parameter, nothing appended |

`seq=` is required on every send of an exactly-once publication and refused on a plain
one. After a reconnect, drop what you hold at or below `info.publisher.last_seq` (it
landed) and send from the next.

**Best effort across a lost buffer.** The marks survive a restart and `Stream.migrate`,
and come back with a WAL-replica restore (minus the replication lag, rows and marks
together). A restore from the published table alone, or a revive, starts with none. The broker caches `MAX_PUBLISHERS` (4,096)
publishers per stream and `DEDUPE_WINDOW` (64) committed ranges for each; the table holds
every mark.

## `outbox` and `inbox`: exactly-once through the caller's database

```python
store = await streamcast.SqliteStore.open(path, *, create=True)
store = await streamcast.PostgresStore.connect(dsn, *, create=True, **psycopg_kwargs)
async with store.transaction() as tx:           # BEGIN ... COMMIT; ROLLBACK on a raise
    await tx.execute(sql, params) -> int         # the driver's placeholders: ? or %s
    await tx.fetch(sql, params) -> list[tuple]
    tx.on_commit(callback)                      # after COMMIT, never on a rollback
streamcast.store_ddl(dialect) -> list[str]      # for a role without DDL rights

streamcast.outbox(uri, *, publisher, store, max_in_flight=64, send_batch=100,
                  poll_every=1.0, backoff=BACKOFF, notice=log.warning) -> Outbox
await out.send(row, *, tx) -> int               # its seq; shipped once tx commits
await out.send_many(rows, *, tx) -> int         # the first seq
await out.landed(seq) -> int | None             # the broker's offset
await out.flush(timeout=None) -> None           # until nothing is pending
await out.pending() -> int
await out.close() -> None                       # pending rows stay; flush first to drain

streamcast.inbox(uri, *, subscriber, store, offset=None,
                 inbox_max_bytes=INBOX_MAX_BYTES, inbox_max_rows=None,
                 inbox_resume_at=INBOX_RESUME_AT, catch_up=False,
                 on_pause_change=None) -> Inbox     # Callable[[bool], None] | None
async for offset, ts, row, tx in sub: ...       # tx holds the row's delete
await sub.recv() -> (offset, ts, row, tx)
await sub.recv_many(limit) -> (rows, tx)        # one tx for the batch
await sub.commit() -> None                      # end the open tx now
sub.caught_up() -> bool                         # not "the inbox is empty"
sub.paused -> bool                              # read-only: held at the window's bound
```

**One transaction holds the work, its input and its output.** An inbox row's
`tx` already holds the row's delete; an outbox `send` takes the same `tx`. A
crash before COMMIT redelivers the input and enqueues nothing; after it, the
accepted output is deduped by the broker ([SPEC §6b](SPEC.md#exactly-once-the-broker-dedupes-each-publisher-on-its-seq)),
subject to its buffer-loss limit. External side effects are outside this transaction.

| | |
|---|---|
| **the stream's id** | bound at the first connect in each state table, checked at every one after: a recreated stream is `StreamMismatch`, before anything is sent or received |
| **outbox seqs** | assigned at enqueue in the caller's transaction, gapless; a rollback returns them. Parallel producers take distinct publisher ids |
| **the sender** | connects only when it has rows; reconciles with the greeting (landed rows deleted unsent; `AcknowledgedRowsLost`, `StoreBehindBroker`); ordered pipelined batches; a refused row dropped and the rest renumbered; an outage said once |
| **inbox resume** | from `last_received_offset + 1`; unprocessed rows first. A new subscriber starts at the latest offset or `offset=`; one with state refuses `offset=` |
| **the receive window** | at `inbox_max_bytes` or `inbox_max_rows` the receiver stops and CLOSES its subscription; it reconnects below `inbox_resume_at` of each bound. An empty inbox always takes one row |
| **startup** | decided from the durable inbox and the same bounds; no pause state is stored or accepted. See [the receive window](#the-receive-window-paused-and-on_pause_change) |
| **one tx per store at a time** | a task asking for a second while it holds one gets `RuntimeError`, not a hang: `sub.commit()` first |

The next `recv` commits the previous processing transaction. A clean inbox context exit
also commits it; an exit by exception rolls it back. Closing an outbox stops its sender
without draining it. Schema-rejected rows are logged and deleted; their seqs are removed
from the pending sequence. `landed` returns an offset only while it is still known.

### The receive window: `paused` and `on_pause_change`

The inbox is a bounded window onto the stream, not a copy of its backlog, and
streamcast alone decides when it is full. Let R and B be this subscriber's
unprocessed rows and bytes in `streamcast_inbox`:

| | condition | what the receiver does |
|---|---|---|
| **high water** | `B >= inbox_max_bytes`, or `R >= inbox_max_rows` when it is set | stops inserting and **closes its subscription**; `paused` becomes True |
| **low water** | `B < inbox_max_bytes × inbox_resume_at` **and** (no row bound, or `R < inbox_max_rows × inbox_resume_at`) — both strict | reconnects from `last_received_offset + 1`; `paused` becomes False |

Inserting counts each row as it goes, so the row that crosses the byte bound
is the last one taken, and an empty inbox always takes one row, however large.
While paused the receiver recounts the store every `poll_every` and whenever
this inbox processes a row, so rows processed by other workers or processes
count too.

**Startup.** `inbox()` checks the saved stream identity and offset against the
first connection, then recounts R and B, so processing during that connect is
seen:

- **at or over either high bound** — equality included — it starts **paused**,
  and closes the checking subscription before `inbox()` returns;
- **under both** it starts **active**, in the band between low and high water
  too, and exactly at low water;
- `inbox_resume_at` decides only the way out of a pause, never the start.

Thresholds cannot reconstruct the previous process's hysteresis, and none is
stored: a receiver that stopped paused inside the band restarts active and
fills to the high bound, which the window allows anyway. Nothing the caller
passes sets the pause.

**`on_pause_change(paused)`** is how an application learns the state without
polling or recomputing it:

- called **once with the starting state before `inbox()` returns**, so an
  application that only listens never starts out wrong;
- then on **every transition**, paused to receiving and back, once each and in
  order, and never with an unchanged value. Closing the inbox reports nothing;
- **synchronously, in the receiver's own task**, after `paused` already holds
  the new value. The receiver waits for it, so keep it quick and
  non-blocking: set a flag, `put_nowait` on a queue, update a gauge. Awaiting,
  I/O, or taking a lock another task holds stalls receiving. What it raises
  fails the inbox: from `inbox()` itself at startup, its subscription closed,
  and afterwards from `recv` once the rows already received are processed;
- `None`, the default, calls nothing. `paused` is the same state, read-only.

Never persist it, feed it back, or derive it from counts: the window's rules
include the crossing row and the oversized first row, which a count of R and B
cannot replay.

A worked example, with `inbox_max_rows=100, inbox_resume_at=0.5`:

```python
async def billing(store):
    status = {"paused": None}

    def on_pause_change(paused: bool) -> None:
        status["paused"] = paused          # quick: no await, no I/O

    async with streamcast.inbox(
        uri, subscriber="billing", store=store,
        inbox_max_rows=100, inbox_resume_at=0.5,
        on_pause_change=on_pause_change,
    ) as sub:
        assert status["paused"] is sub.paused   # already reported, once
        async for offset, ts, row, tx in sub:
            await tx.execute("INSERT INTO invoices ...", (...))
```

| held at start, then | calls so far | why |
|---|---|---|
| 0, 40 or 50 rows | `False` | under 100: active, low water (50) included |
| 100 or 120 rows | `True` | at or over 100: paused, subscription closed |
| processing brings 100 down to 50 | `True` | 50 is not strictly below 50 |
| one more processed: 49 | `True, False` | below low water: reconnects, receives |
| the inbox reaches 100 again | `True, False, True` | the high bound, closed again |
| a restart with 70 held | `False` | the band starts active, whatever the last run's state |

Rows are JSON text in both tables, as on the wire: send a binary column in its
text encoding. The file `Cursor` (`connect(cursor=)`) stays for a consumer
with no database. [`SPEC.md`](SPEC.md) §6c has the reasoning.

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
hint naming the stream's current metadata, which is what
[`Stream.snapshot`](#reading-a-streams-history) takes, and `stream_id` is the id that
metadata records:

```python
table = await streamcast.Stream.scan(
    sub.info.metadata
).read_all()
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

**The filter holds for every row a subscription delivers:** live, replayed, and caught up.
The server filters what it sends; rows a `catch_up` reads from the published tables never
pass through it, so the client applies the same predicate to them, with a binary column's
value compared as bytes as the server compares it.

**Cost.** The predicate is compiled once at subscribe and specialised on its arity, then
runs per message per filtered subscriber against the dict `send` was called with. Measured
on a four-column row: 162 ns for one term, 260 ns for two, against 961 ns for the
`msgspec` encode already on the path. The encode stays shared — a filter decides whether
to enqueue the frame, not what to build — so filtering never costs a second serialisation.

**The replay is filtered identically**, through the same predicate. `_log.rows` already
yields dicts, so there is one implementation and nothing to diverge; pushing the predicate
into the scan's SQL would prune more but Python and SQL disagree about coercion and NULL,
and that disagreement would surface as a resume delivering what the live stream did not.

**`replay` in the greeting is an offset window, not a row count.** Filtering and
restore fences both make `end - start` differ from the number of rows delivered.

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
| `metadata="s3://bucket/prefix/trades/trades.metadata/version-hint.text"` | the metadata to read from; otherwise the greeting's |
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
last-writer-wins and is not supported. Use `inbox` to commit processing and receipt
state in the caller's database.

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
this library can own. `inbox` provides that transaction; a file cursor cannot.

An unreadable or empty file is treated as absent rather than as an error — a cursor is a
recovery hint, and refusing to start because a save was torn turns a crash into an outage.

## Refusals

```
StreamcastError
├── ProtocolError      the peer is not speaking streamcast, or sent a bad subscribe
├── StreamNotFound     nothing served at that path; `.serves` lists what is
├── NotReplayable      that offset cannot be served; `.why` says which of four
├── CatchUpUnavailable `catch_up=True` could not read the gap; says what to change
├── Rejected           a published row the schema refuses; nothing committed
├── StreamRetired      the stream is retired and takes no rows; `.at`, `.end_offset`
├── IngestFailed       `Stream.ingest` stopped partway; `.rows_loaded` is where to resume
├── Superseded         another connection claimed this publisher id
├── SequenceGap        publisher seq is above the next expected seq
├── ExactlyOnceUnavailable the stream or broker cannot dedupe publishers
├── StreamMismatch     the store is bound to another stream id
├── AcknowledgedRowsLost the broker mark is below the outbox's acknowledged position
├── StoreBehindBroker  the broker mark is ahead of the store's next seq
└── TooSlow            dropped for falling behind; `.offset` is where to resume
```

`NotReplayable.why` is the one worth branching on, because the next move differs:

| `.why` | what to do |
|---|---|
| `not_durable` | drop `offset=`, or give the server a log |
| `ahead` | your cursor is above the server's frontier — it was restored or rebuilt |
| `too_old` | `catch_up=True`, or read the log directly and subscribe from where you stopped |
| `evicted` | below what the scan's tier holds; `catch_up=True` if the published tables go back further, else accept the gap |

`EARLIEST` on a log that holds nothing yet is not refused: it starts at the log's first
row, and a row committed while the subscribe is being answered is replayed, not missed.
An older server refuses it with `why == "empty"`.

`EARLIEST` with `catch_up=True` takes everything the published tables still hold, from
wherever they begin: the stream's first row, or the floor retention has moved it to. Nothing
below that is reported lost, because `EARLIEST` asks for what is left.

`.fields` carries whatever numbers survived the close frame — `offset`, `earliest`,
`behind`, `max_replay`, `end_offset` — and `str(exc)` is a sentence built from them.

`Close` is the code enum: `BAD_REQUEST` 4400, `NO_SUCH_STREAM` 4404, `SUPERSEDED` 4409,
`RETIRED` 4410, `EXACTLY_ONCE_UNAVAILABLE` 4412, `NOT_REPLAYABLE` 4416, `TOO_SLOW` 4429.

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

stream = streamcast.Stream.open("trades", create=True, root="data", schema=SCHEMA, sort_by=("event_ts",))
```

`Stream` creates the log at `root/name` if it is not there and opens it if it is — the
create-or-open choice every server makes — and **closes it on `aclose`, because it opened
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
app = streamcast.create_app(streams=stream)
async with app.router.lifespan_context(app):
    ...
```

The current log is retired: sealed for good, published in full, and refusing writers
from then on. Then `trades-v2` is created with the new schema, starting at exactly the
offset the old log ended at. A new version of the metadata in
`root/trades/trades.metadata/` (and one beside the published tables, when they are on
S3) records the sequence. A migration that dies partway finishes on the rerun. Migration
introduces no gap at the seam. `Stream.open` and `Stream.restore` open whichever log the
metadata names as current.

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
Widening is refused too. A rename removes one column and adds another; historical rows
are not backfilled. The server replays only the current log. `catch_up=True` reads older
logs through `Stream.snapshot` when a consumer resumes below the migration seam.

### Loading rows in bulk: `Stream.ingest`

Rows sent one at a time are made durable one group commit at a time. A load you already
have as Parquet or Arrow doesn't need that, and `Stream.ingest` writes it straight to the
log's files with litelink's `ingest`: 5.1 million rows a second measured there, against
183 thousand through the buffer. With the server stopped:

```python
table = pq.read_table("trades-2025.parquet")                        # or a RecordBatchReader
start, end = streamcast.Stream.ingest("trades", table, root="data")  # offsets [start, end)
```

Measured end to end on a 2-vCPU VM: 50 million trade-like rows (five columns, about 18
bytes a row in Parquet) loaded in 24 s, 2.1 million rows a second, with peak memory around
1 GB however large the load, since a reader is read and written a row group at a time.
That is about 4 minutes per 500 million rows, before reading the source and uploading the
result, and the time scales with bytes, so wider rows take longer. The whole load is
staged on local disk until it is published.

The rows take the next offsets at the end of the stream's current log, as any append does;
a subscriber reads them by offset, like any other rows. Every declared column must be in
the source, in any order and castable to its type, and nothing reserves offsets until
litelink has checked that, so a refused source leaves no gap. `streamcast_ts` is stamped
with the time each batch is loaded, and a source carrying it is refused. The load is
published when it is durable (`publish=False` to skip). A push that fails leaves the rows
loaded and raises: publish again rather than ingesting again.

**Only with the server stopped.** Loaded rows are never sent to subscribers, so a
subscriber reading live would see the offsets jump past them, a gap it can't detect;
stopped, every subscriber comes back by offset and replays them. Rows the last server left
in the buffer are sealed first, which litelink needs. A retired stream raises
`StreamRetired`.

**A load that fails partway raises `IngestFailed`, which says where to resume.** litelink
commits a load 20 files at a time, so a failure leaves the files committed before it in the
log, and the offsets of files written but not yet committed become a gap. Offsets follow the
source's order, so what landed is exactly the source's first `rows_loaded` rows:

```python
try:
    streamcast.Stream.ingest("trades", table, root="data")
except streamcast.IngestFailed as failed:
    failed.loaded, failed.gap          # [start, end) of each, or None
    failed.batch, failed.row           # where the data was refused, or None
    # fix the data, then load the rest, before the server starts again
    streamcast.Stream.ingest("trades", fixed.slice(failed.rows_loaded), root="data")
```

When litelink refuses a value (a null in a required column, a NaN, an integer too wide
for its column), streamcast checks again the batches litelink had been reading, which is all
the extra work a failure costs, and names the batch and row. litelink's own exception is
`__cause__`. A failure that isn't the data's, such as a full disk, keeps litelink's message,
with `batch` and `row` None.

### Keeping a window: `Stream.retain`

**Nothing leaves a stream unless it is told to.** Given a window, the maintainer drops the
rows older than it, by `streamcast_ts`, from every log of the stream: the live one and
every retired one, including a retired log that now exists only as its published table.

```python
streamcast.Stream.retain("trades", root="data", max_age=timedelta(days=30))
streamcast.Stream.retain("trades", root="data", max_age=None)   # keep everything again
```

The setting is recorded in the stream's metadata, so it holds wherever the stream is
served and takes effect without a restart. It is an explicit call, like `migrate` and
`retire`, so two servers can't keep different windows. A build from before ignores it, and
one that rewrites the metadata drops it.

**Each pass happens in two halves, an hour apart.** The `clean-published` maintainer
process runs one pass an hour:

1. **The new floor is published.** It is the smallest offset whose `streamcast_ts` is newer
   than the cutoff, which also handles a clock that stepped backwards. A new version of the
   stream's metadata starts the stream there: retired logs wholly older than the cutoff are
   dropped from it, along with their statistics. From then on, a snapshot reads nothing
   below the floor, and a catch-up asking below it is refused with a message naming
   retention.
2. **After the grace** (`_retention.GRACE`, an hour), the dropped logs are deleted entirely,
   the retired log the floor cuts through is truncated, and the live log is truncated.
   Readers that resolved the stream before the floor moved get that hour to finish, because
   litelink deletes a retired log's files immediately.
   Each step writes the metadata's published copy on S3 before the local one, since
   readers elsewhere read that copy: if the write to S3 fails, the step is retried on the
   next pass, and the hour counts only from a floor S3 readers could see.

What still has to be done is recorded in the metadata, so a restart between the two halves
loses nothing. Logs are truncated in whole files, so a log can keep rows just below the
floor until the file holding them goes, though no reader sees them. The maintainer writes
the metadata with the stream's `S3Options` (see [Credentials](#credentials)).

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

**A retired stream is read-only, and still served.** `open` and `migrate` open its log for
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
streamcast.Stream.scan(metadata_uri, *, <the same>, columns=None, where=None, filters=(),
                       start_offset=None, end_offset=None, batch_size=1_000_000) -> Reader
streamcast.Stream.sql(metadata_uri, query, *, <the same>, filters=(), start_offset=None,
                      end_offset=None, batch_size=1_000_000) -> Reader

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
| **holds in memory** | a query's result, or one batch of it (and the broker's rows, if asked for them) | the rows not yet published |
| **addressed by** | the stream's metadata file | the broker's URI |

A stream is a sequence of logs — one per schema, after `Stream.migrate` — and its
metadata file says which, in order, where each is published, and the offsets and
`streamcast_ts` range each holds. `Stream.snapshot` reads that file and then the published
tables, on the reader's own machine with its own credentials. **Given the S3 metadata file,
it never touches the server**:

```python
s3 = streamcast.S3Options(region="us-east-1")    # or the environment / AWS profile

async with await streamcast.Stream.snapshot(
    "s3://market-data/prod/trades/trades.metadata/version-hint.text", s3_options=s3
) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()
```

The broker is consulted only for rows no table holds yet, and only when asked: a point
past the published end needs `broker=`, and is refused without it. Every other read is a
read of files.

```python
async with await streamcast.Stream.snapshot(sub.info.metadata) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()
    await snapshot.scan(columns=["price"], filters=[("price", ">", 500.0)]).read_all()
    async for offset, ts, row in snapshot.rows(1):
        ...
```

`Stream.scan` and `Stream.sql` are the one-shot forms: open, read once, close. They
return a `Reader`, as litelink's `scan` and `sql` return a reader, on a snapshot of its
own: opened at the reader's first read — which is where a refusal is raised — and closed
once the reader is done.

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
| `scan(columns=, where=, filters=, start_offset=, end_offset=, batch_size=)` | rows in `[start_offset, end_offset)`, oldest first, as a `Reader` |
| `sql(query, filters=, start_offset=, end_offset=, batch_size=)` | `query` over the snapshot, which it reads as the table `log`, holding only the rows in `[start_offset, end_offset)` that match `filters`, as a `Reader` |
| `rows(start, stop=None)` | one `(offset, ts, row)` at a time, built exactly as a server replays them |
| `close()` | or `async with` |

**`scan` and `sql` return a `Reader`: the result as it streams.** `await
reader.read_all()` is the whole result as a `pa.Table`; `async for batch in reader` is one
`pa.RecordBatch` at a time, `batch_size` rows (1,000,000 by default), so a result larger
than memory can still be read — a `GROUP BY` or a join has no offset order to page by.

```python
async with snapshot.sql("SELECT id, max(litelink_offset) AS last FROM log GROUP BY id") as result:
    result.schema                                  # once the query has run
    async for batch in result:
        ...
```

The query runs once, when the reader is first read, on a DuckDB cursor of its own and in
a worker thread, so another read of the same snapshot cannot end it short and the event
loop never waits on it. A reader is valid while its snapshot is open: a batch asked for
after the snapshot closed raises rather than ending the result early. Reading it to the
end, `read_all()`, `aclose()` or leaving `async with` releases its cursor.

`where` is SQL over the stream's columns. `filters` are `(column, operator, value)` terms,
ANDed with `where`. Across a migration the logs read as one table with `UNION ALL BY
NAME`, so a column a log lacked reads as null there, in a condition as in a result.

**`filters=` and `where=` both filter rows, but only `filters=` can skip a log.** The
result is the same either way; what differs is how much gets read.

- **`filters=`** is a list of `(column, operator, value)` terms — `==`, `<`, `<=`, `>`, `>=`,
  `in` — ANDed together. Because each term is that simple, it can be checked against every
  log's per-column min and max (the stream's manifest) *before the log is opened*: a
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
).read_all()
await snapshot.sql("SELECT max(price) FROM log WHERE side = 1",
                   filters=[("price", ">", 85_000.0)]).read_all()   # narrows `log`
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

**A newly opened snapshot resolves the latest published commit.** An existing snapshot
stays pinned. Opening resolves the table's current metadata with `litelink.current_metadata`, outside DuckDB's caches: through a disk-cached
connection, `version-hint.text` would pin a reader to an old snapshot (litelink#141).
Everything the hint names is written once, and caches safely. At table open, offset
bounds and row counts come from Iceberg manifest statistics without reading data files.
If a file lacks offset bounds or has delete files, opening falls back to a data scan.
Both paths use `s3_options`, resolved from the environment when omitted.

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
    await live.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()
    await live.scan(columns=["price"], filters=[("price", ">", 500.0)]).read_all()
    live.end_offset                              # one above the newest row a query sees
```

A `Live` is a `Snapshot` kept moving — **real-time analytics on a stream in one line**:
the published tables as a base, and the broker's rows appended in memory as they arrive,
so every `scan` and `sql` answers as of the newest row received. It is **online by
design**: it takes only the broker's address, because a view of the stream *now* has to be
listening to it. For an offline read, or a fixed point to come back to, use a snapshot. They take the same arguments as on a `Snapshot`, and the rows read the same
— the broker's carry their `streamcast_ts` like published ones. A reader from `live.scan`
or `live.sql` is the view as of the call: rows received while it is read are not in it,
and a rebase keeps the base it reads open until it is done.

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
 "metadata":"s3://market-data/prod/trades/trades.metadata.json",
 "metadata_hint":"s3://market-data/prod/trades/trades.metadata/version-hint.text",
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
