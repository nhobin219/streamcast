<p align="center">
  <img src="https://raw.githubusercontent.com/nhobin219/streamcast/main/docs/assets/streamcast-logo.svg" alt="streamcast" width="330">
</p>

[![CI](https://github.com/nhobin219/streamcast/actions/workflows/ci.yml/badge.svg)](https://github.com/nhobin219/streamcast/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/streamcast)](https://pypi.org/project/streamcast/)
[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](pyproject.toml)

# A JSON WebSocket pubsub framework for structured data

Publishers write, subscribers read, and every message is optionally appended to a
[litelink](https://github.com/nhobin219/litelink) log before any subscriber sees it. A
message is a **row** — with a log attached it is checked against a schema you declare,
which is what lets the log be an Iceberg table rather than a pile of frames. Each message
carries the offset it was written at, so a subscriber that stops can reconnect and ask for
the rest.

```
ws feed ─┐
publisher ├──► streamcast server ──► litelink log    durable BEFORE anyone sees it
publisher ┘         │  fan-out
                    ├──► strategy          offset 1861
                    ├──► dashboard         offset 1861
                    └──► recorder          offset 1861
```

Every subscriber receives the same bytes in the same order, from one `encode` call. The
API is a thin custom pubsub layer on top of standard `websockets`. For those familiar with
kdb+, a streamcast server is effectively a Python WebSocket
[tickerplant](https://code.kx.com/q/architecture/).

**A streamcast server is a log-backed pub/sub broker.** Publishers and subscribers are both
its clients — a feed handler calling `StreamHandle.send` in the same process or
`streamcast.publish` from another machine on one side; `connect`, `StreamHandle.snapshot` and
`StreamHandle.live` on the other — and the server owns the log, assigns the offsets, and is its
only writer.

## The log is the analytical table

The usual shape is a message log in one system and an analytical store in another, with a
pipeline extracting between them — two copies of every row and a job that keeps them in
step. There is no extraction step here and no second copy. A litelink log **is** an Iceberg
table, so the log your messages were appended to is the table an analytical engine
reads:

```python
import duckdb
import streamcast

# Published through streamcast, live.
async with streamcast.publish(uri) as producer:
    await producer.send({"event_ts": 1790038800123456, "price": 85565.0, "side": 1})

# The same rows as a table, from anywhere, from the stream's published
# tables: every log it has been through, as of a point, with no server
# involved.
async with await streamcast.StreamHandle.snapshot(
    "s3://bucket/prefix/trades/trades.metadata/version-hint.text"
) as snapshot:
    await snapshot.sql("SELECT count(*), max(price) FROM log WHERE side = 1").read_all()

# Or from any Iceberg engine, with neither streamcast nor litelink installed.
duckdb.sql("""
    SELECT count(*), max(price)
    FROM iceberg_scan('s3://bucket/prefix/trades/trades',
                      version_name_format = '%s%s.metadata.json')
""")
```

Rows land in a SQLite buffer first and seal into Parquet behind it, so the newest messages
are in the buffer and the rest are columnar — a snapshot as of `LATEST`, given `broker=`,
reads across both, and a reader anywhere else reads what the log has published. That is
one store with tiers, not a transactional copy and an analytical copy that have to be
reconciled.

The tiering, the published-table layout, consistency guarantees, and costs are
[litelink](https://github.com/nhobin219/litelink)'s, and its README and
[SPEC](https://github.com/nhobin219/litelink/blob/main/docs/SPEC.md) describe them in
depth — including why `version_name_format` is spelled out above, and how an engine
resolves the current metadata from `version-hint.text` with no catalog.

## Install

```bash
uv add streamcast
```

## API

**`connect` and `publish` are the `websockets` client API.** Same shapes, and every
keyword passed through — `ssl`, `ping_interval`, `max_queue` and the rest behave exactly
as they do there. **The broker is a FastAPI application**: `create_app` builds it, and
an ASGI server runs it — uvicorn, a dependency, with one worker and its lifespan on.
TLS, the bind address and keepalive are that server's settings.

`StreamHandle.open` takes a `Stream` definition and opens an existing local log by default.
Use `initialization="create_if_absent"` to create only when the stream exists nowhere,
or `initialization="restore_published"` to restore a published replica when the local
log is missing. Stop the old writer before restoring on another machine.
`StreamSpec` carries schema and policy; `Storage` carries absolute paths, log
configuration, replication and optional host-only `s3_options`.

**`publish` has no `websockets` counterpart**; it is streamcast's, shaped like `connect`
so it reads the same way. WebSocket itself has no verbs — the protocol is frames, and
both subscribing and publishing here are URL conventions on top of it.

```python
streamcast.Stream(spec=StreamSpec(...), storage=Storage(...))  # frozen definition
streamcast.StreamHandle(name="", *, log=None, owns_log=False, schema=None,
                       max_replay=100_000, max_backlog=8192, max_inbound=65_536,
                       max_in_flight=64, max_message_bytes=None, replicate=False, ...)
streamcast.StreamHandle.open(stream, *, initialization="open_existing")
    stream.definition  # the exact Stream passed to open; None for a direct handle
    await stream.send(row) -> int | None       # durable, then fan out
    await stream.send_many(rows) -> list       # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable · stream.schema
    stream.metadata_uri                         # where a reader finds its history

await streamcast.StreamHandle.snapshot(metadata_uri, *, as_of_offset=None, as_of_ts=None,
                                 broker=None, s3_options=None) -> Snapshot
    snapshot.scan(columns=, where=, filters=, start_offset=, end_offset=) -> Reader
    snapshot.sql(query, *, filters=, start_offset=, end_offset=) -> Reader  # table `log`
        await reader.read_all() -> pa.Table · async for batch in reader   # streamed
streamcast.StreamHandle.scan(metadata_uri, ...) · streamcast.StreamHandle.sql(uri, query) -> Reader
await streamcast.StreamHandle.live(broker, *, s3_options=None) -> Live   # kept current
    live.scan(...) · live.sql(query) -> Reader · await live.wait_for(offset | ts=)

streamcast.create_app(config=None, *, streams=()) -> fastapi.FastAPI
streamcast.BrokerConfig(maintain=True, transport_max_bytes=1<<20,
                       subscriber_yield_every=64, ...)  # positive frame interval
streamcast.connect(uri, *, offset=<unset>, cursor=None, cursor_uri=None,
                   catch_up=False, ...) -> Subscription
    await sub.recv() · async for offset, ts, row in sub
    async for batch in sub.batches(limit=500)   # what has arrived, never waiting for more
# snapshot, scan, sql and live also take litelink's cache settings:
#   memory_cache=True, disk_cache=False, cache_key=None, disk_cache_volume_limit=0.8
streamcast.publish(uri, ...) -> Publication          # any served stream
    await producer.send(row) · await producer.send_many(rows)
    await producer.submit(row) -> Future   # pipelined: up to max_in_flight=64 unanswered
streamcast.to_arrow · streamcast.from_arrow · streamcast.Cursor
streamcast.EARLIEST · streamcast.LATEST
```

Three deliberate exceptions:

- **Iterating yields `(offset, ts, msg)`**, not `message`. The offset is what makes a
  reconnect a resume rather than a restart, and a subscriber that has to ask for it
  separately will forget to. `ts` is when the server took the row.
- **A subscription is read-only.** It has no `send`, rather than a `send` that raises.
  Publishing uses `StreamHandle.send` locally or `publish` remotely.
- **`compression` defaults to `None`**, where `websockets` defaults to `"deflate"`; run
  the broker with it off too (uvicorn's `ws_per_message_deflate=False`, whose default is on).
  permessage-deflate is per connection while the encode is shared: `send` encodes a frame
  once and hands the same bytes to every subscriber, and deflate compresses those identical
  bytes once per subscriber. Measured on a six-column trade row — 0.564 µs to encode once,
  3.454 µs to deflate each — CPU per message is 4 µs at one subscriber and 691 µs at 200.
  Past that the server is CPU-bound and starts dropping subscribers at `max_backlog`.

  Turn it on where bandwidth costs more than CPU, which is few subscribers over a WAN:
  it is **5.8× smaller** here, 112 bytes to 19. There is no middle setting — without
  context takeover the same frames compress 1.1×, so compressing once and sharing the
  result is not available.

Routing is by `StreamHandle.name`: `trades` is served at `/trades`, an unnamed stream at `/`.
`create_app(streams=[trades, quotes])` serves both on one port.

Full reference in [`docs/API.md`](docs/API.md).

## The wire

Every frame is a text frame of JSON: a greeting, then `[offset, ts, msg]` per message.
A client in any language needs a JSON parser, plus, for binary columns, the decoding rules
in [SPEC §2](docs/SPEC.md#reading-a-row-in-another-language).

```
{"streamcast":4,"stream":"trades","end_offset":1861,"replay":[1200,1861],
 "metadata":"s3://market-data/prod/trades/trades.metadata.json",
 "metadata_hint":"s3://market-data/prod/trades/trades.metadata/version-hint.text",
 "stream_id":"6f1c…","durable":true}
[1861,1790038800124001,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

The offset and the stamp are positional, so `const [offset, ts, msg] = JSON.parse(frame)` is a client in
another language and `wscat ws://localhost:8765/trades?offset=0` is a working subscriber
with none at all.

`metadata_hint` names the stream's metadata — every log it has been through, where each
is published, and what each holds — through the hint naming its current version, and
`metadata` is a plain copy of it for clients from before the hint, so a
subscriber holding the greeting can read the
history itself rather than through the socket: `StreamHandle.snapshot(info.metadata)`, or any
Iceberg engine pointed at a log's published table. `stream_id` says which stream the
file is. Both `null` when the stream has no log. Credentials are never in it: they are
the reader's own. Key order comes from the log's schema, so a replayed message is
byte-identical to the live one it repeats.

Encoding is [msgspec](https://github.com/jcrist/msgspec): 0.285 µs for a six-column row
against 5.815 µs for stdlib `json`.

`offset` is `null` on a stream with no log — nothing assigned one, and a per-process
counter would look like a resume cursor until the server restarted.

## Server

The broker accepts publishers, fans messages out to subscribers, and with
a log attached replays what a subscriber missed — so neither end owns the process, and
either can restart without the other noticing.

Routing is exact-match on the stream's name: there is no topic hierarchy and no wildcard
subscription. A subscriber names one stream and resumes it by offset, which is the trade
the log buys.

The schema is yours, declared in JSON Schema. `streamcast` is the only import a durable
stream needs.

```python
from pathlib import Path

import asyncio, json, streamcast, uvicorn, websockets

SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
        "amount": {"type": "number"},
        "side": {"type": "integer", "format": "int32"},
    },
    "required": ["event_ts", "price", "amount", "side"],
}


async def main():
    # Creates the stream in data/trades/, or opens it if it is already there.
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(
                name="trades", schema=SCHEMA, sort_by=("event_ts",)
            ),
            storage=streamcast.Storage(root=Path("data").absolute()),
        ),
        initialization="create_if_absent",
    )

    # Fan-out, sealing and compaction, started and stopped by the app's lifespan;
    # Storage(replicate=True, published=...) also starts WAL shipping for WAL-enabled logs.
    app = streamcast.create_app(streams=stream)
    server = uvicorn.Server(
        uvicorn.Config(app, host="localhost", port=8765, ws_per_message_deflate=False)
    )
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)

    async with websockets.connect("wss://ws.bitstamp.net") as feed:
        await feed.send(SUBSCRIBE)
        async for message in feed:
            trade = json.loads(message)["data"]
            await stream.send(
                {  # a row, durable, then fanned out
                    "event_ts": int(trade["microtimestamp"]),
                    "price": float(trade["price"]),
                    "amount": float(trade["amount"]),
                    "side": int(trade["type"]),
                }
            )

    server.should_exit = True
    await serving


asyncio.run(main())
```

The app's lifespan starts everything the streams need: one maintainer covering every log it serves
— a set of five subprocesses, one per storage role (seal, compact, publish, clean, clean
published) — and, with `replicate=True`, one litestream for the logs with
`wal_replication` on. The maintainer is opt-out (`BrokerConfig(maintain=False)`);
litestream is opt-in. Streams with different `S3Options` get separate child processes;
`Maintain(dedicated=("trades",))` also gives a log its own maintainer. Without a
maintainer nothing ever seals — litelink is explicit that *"a maintainer is not
optional"*.

`StreamHandle.open` creates or opens the log; `StreamHandle(log=handle)` takes one you opened
yourself and reads its frontier. `streamcast.to_arrow(SCHEMA)` is the `pa.schema` if you
want it.

What it captures is a table, queried from its published tables:

```python
uri = stream.metadata_uri
await streamcast.StreamHandle.sql(uri, "SELECT count(*), max(price), sum(amount) FROM log").read_all()
await streamcast.StreamHandle.scan(uri, columns=["litelink_offset", "price"], where="side = 1").read_all()
await streamcast.StreamHandle.sql(uri, "SELECT max(streamcast_ts - event_ts) FROM log").read_all()   # feed latency, us
```

The table has two columns you did not declare. `litelink_offset` is the offset every frame
carries, and `streamcast_ts` is when the server took the row, in UTC microseconds — every
frame carries that too, as its `ts`, beside the row rather than in it.

### Changing the schema

A log's columns are fixed when it is created. To change them, stop the server and
migrate:

```python
stream = streamcast.StreamHandle.migrate("trades", root="data", schema=SCHEMA_V2)
```

That seals the current log, starts `trades-v2` at the next offset, and records both in a
metadata file. Columns can be added and removed. A column's type can never change. It is safe
to leave in your startup: a stream already of that shape is just opened.

### Backpressure

`StreamHandle.send` never awaits a consumer: it encodes the frame once and does one non-blocking
queue insert per subscriber. A consumer that stops reading fills its own queue, hits
`max_backlog`, and is **dropped**:

```
streamcast.TooSlow: the server dropped this subscriber for falling more than
8192 messages behind; resume at offset 20481
```

Dropping rather than buffering bounds the server's memory. Dropping rather than evicting
the oldest keeps what the subscriber received a contiguous prefix, so on a durable stream
the drop costs a reconnect and nothing else.

**`max_backlog` and `max_replay` are different limits**, and the names invite confusing
them:

| | `max_backlog` (8,192) | `max_replay` (100,000) |
|---|---|---|
| bounds | messages queued for **one** subscriber | how far back a subscribe may **ask** |
| checked | on every send, per subscriber | once, when the subscriber attaches |
| exceeded | that subscriber is **dropped** — `TooSlow`, 4429 | the subscribe is **refused** — `too_old`, 4416 |
| protects | the server's memory | the worker thread a replay scan holds |
| set on | `BrokerConfig`, per process | `StreamHandle`, per stream |

**They interact, which is why sizing one without the other goes wrong.** A replay is
served *before* the live queue, and live messages pile up behind it — so a subscriber
replaying `max_replay` messages has to finish within `max_backlog` new ones or it is
dropped at the moment it catches up, having done all the work. Raise one and check the
other; `just bench-replay` prints the arithmetic for your hardware.

### Recovering a server

A client moves boxes with a cursor. A **server** moves with `StreamHandle.restore`, which
rebuilds the log itself on a machine that never held it: from its replicated WAL when
there is one, and otherwise from its published table alone:

```python
stream = streamcast.StreamHandle.restore(
    "trades", root="data", published="s3://market-data/prod", replay_published=True,
)
```

Offsets are **fenced, not reissued**, so no offset a consumer holds is ever handed out
again carrying different data. From a replica, litelink skips 2²⁰ past what the replica
recorded (`replica_reserve=`). From the published table alone, it skips 2⁴⁰ past what the
old log last said it had issued (`published_reserve=`), since rows written after the last
publish are lost with the machine. The consumer resumes from the
cursor it already had and sees a gap, which `recv` allows.

Existing consumers resume with no intervention, because `max_replay` counts **rows**
rather than offset distance. The fence puts the new frontier a million offsets up, and a
consumer 150 rows behind is 150 rows behind — the distance check runs first and free, and
only a subscribe it would refuse pays to find out what the replay actually costs.

The local staging table comes back empty — its Parquet was on the dead machine — so a
restored stream that should replay history serves it from the published table, with
`replay_published=True`.

The log comes back with its exact shape, read from the stream's metadata: its schema,
binary encodings and system columns included, and its `sort_by`. Pass `schema=` and
`sort_by=` for a stream whose metadata predates recording them, and `config=` for the
restored log's policy, which is otherwise litelink's default when there was no replica.

A **planned** cutover loses nothing — stop the writer, let the sidecar ship its last
frames, then restore. Unplanned failover loses whatever never shipped.

> ⚠️ **Stop the old producer first.** The fence prevents offset reuse; nothing prevents two
> writers. litelink cannot detect a live writer on another host, and a restore against one
> succeeds — see [`SPEC.md`](docs/SPEC.md) §8b and
> [litelink#75](https://github.com/nhobin219/litelink/issues/75).

### Retiring a stream

A stream that just stops being written leaves its last rows on the box, unpublished.
`StreamHandle.retire` finishes it: every row published, the log refusing writes for good, and
the retirement recorded in its metadata. It is still served, read-only — subscribers
replay and catch up, publishers are refused with 4410. `StreamHandle.restore(..., revive=True)`
undoes it on any box, continuing at exactly the retired end, so a planned move is
`retire` on the old box and `revive` on the new one, losing nothing and skipping no
offsets:

```python
streamcast.StreamHandle.retire("trades", root="data")                     # old box, stopped
stream = streamcast.StreamHandle.restore(
    "trades", root="data", published="s3://market-data/prod", revive=True,
)                                                                    # new box
```

### Serving the whole history

`replay_published=True` with `max_replay=None` makes the server a complete gateway to the
log: no subscribe is refused for reaching too far back, and the server reads the published
table on the subscriber's behalf.

```python
from pathlib import Path

stream = streamcast.StreamHandle.open(
    streamcast.Stream(
        spec=streamcast.StreamSpec(
            name="trades", schema=SCHEMA, replay_published=True, max_replay=None
        ),
        storage=streamcast.Storage(
            root=Path("data").absolute(), published="s3://bucket/prefix"
        ),
    ),
    initialization="create_if_absent",
)
```

Every frame is still JSON over a plain WebSocket, so **a client in any language replays
the retained history from `offset=EARLIEST`** — no litelink, no Iceberg reader, no
object-storage credentials, nothing from this repo. `catch_up` exists because the
default is the opposite; this is the setting that makes it unnecessary.

It is not the default because of `max_backlog`. A replay is served before the live queue,
which fills behind it, so a subscriber reading ten million rows out of S3 accumulates live
messages for as long as that takes and is dropped the moment it catches up if it passed the
backlog on the way. Size the two together, or run it on a stream quiet enough that the
arithmetic does not bite. Each replay also holds a worker from the `to_thread` pool
(`min(32, cpu + 4)`) for its whole scan.

### Knowing a stream is alive

A subscriber cannot tell a quiet stream from a dead one — both are a socket with nothing
arriving. The broker answers `GET /stats` on the port it already has:

```json
{"served_at": 1758708123.9,
 "streams": [{"name": "trades", "durable": true, "end_offset": 123486128,
              "subscribers": 2, "started_ts": 1758701900.1, "uptime_s": 6223.8,
              "last_send_ts": 1758708122.7, "last_send_age_s": 1.2}]}
```

The same object is `stream.stats` in Python, so a mounted app writes whatever route it
wants over it and nothing needs a socket to read the numbers.

**There is no `status` field, and that is deliberate.** Freshness is domain knowledge: a
five-second socket is broken after thirty seconds of silence, while a stream that
publishes once a day at 00:20 UTC is healthy after twenty-three hours. A threshold chosen
in here would be wrong for one of them and would look authoritative to whoever read it.
So this reports numbers; your health check reads them and applies your rule.
[`examples/fastapi_app.py`](examples/fastapi_app.py) shows both halves — `/stats` funnels
the facts through, `/health` is the application deciding.

**`last_send_age_s` is null until this process sends something**, which is why `uptime_s`
is published beside it. After a restart the log still holds every row ever written, so
`end_offset` is large and nothing has been sent — indistinguishable, on its own, from a
stream that stopped. Nothing sent four seconds in is ordinary; nothing sent six hours in
is not.

On by default. It discloses less than the socket beside it — a wrong-path connect is
already answered with the names of every stream it serves, the greeting already carries
`end_offset`, and anyone who can reach the port can subscribe and read every row in
full. `BrokerConfig(stats=False)` turns it off, `stats="/_internal/streams"` moves it,
and the injected `authorization` decides who may read it (`stats.read`), stream by
stream ([`SECURITY.md`](SECURITY.md)).

### Mounting in an existing app

A service that is already an ASGI app — FastAPI, Starlette, anything — can serve a stream
on the port it already has, by mounting the broker app instead of running it on a
second one.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

streams = streamcast.create_app(streams=[trades, quotes])

@asynccontextmanager
async def lifespan(app):
    async with streams.router.lifespan_context(streams):  # starts the maintainers
        yield

app = FastAPI(lifespan=lifespan)
app.mount("/streams", streams)
```

Subscribers then connect to `ws://host/streams/trades`, and nothing about the client
changes — same URL shape, same frames, same refusals. It is the same application, with
the same runtime, whether it runs on its own or mounted: there is one broker, not a
second transport. [`examples/fastapi_app.py`](examples/fastapi_app.py) is the whole thing
as a running service: `just demo fastapi`, then point a consumer at
`ws://127.0.0.1:8770/streams/trades`.

**Entering the mounted app's lifespan is not optional.** Starlette does not run a mounted
sub-app's lifespan, so a host that left it out would start no maintainer — and a log with
nothing sealing it buffers every row it ever receives. Until the lifespan has started,
the app refuses every connection.

**One worker.** The lifespan owns each stream's writer, its maintainers and its storage
locks, so two workers would be two writers to one log: run the app under one ASGI worker,
lifespan on, with no reload. Compression is the server's setting, and uvicorn's default
is on: a host that enables permessage-deflate pays 4 µs of CPU per message at one
subscriber and 691 µs at 200, and the symptom is a CPU-bound server dropping subscribers
for falling behind.

The same application also manages streams at runtime — define, serve, stop, migrate and
retire over HTTP, behind an injected authorization policy — when it is given a registry;
[`docs/API.md`](docs/API.md#dynamic-management) is that contract.

## Client

Two ends, and a connection is one or the other. A subscriber has no `send`; a publisher
has no `recv`. Neither carries a method that raises.

### Producer

`StreamHandle.send` publishes from the server's own process. `streamcast.publish` does it from
anywhere else:

```python
async with streamcast.publish("ws://localhost:8765/trades") as producer:
    offset = await producer.send({"event_ts": 1790038800123456, "price": 85565.0})
```

Every served stream takes publishers; a retired one refuses them with 4410. `send` returns once the row is durable, exactly as the
local call does; `send_many` commits a group in one transaction and is the same throughput
lever it is locally. A row the schema refuses raises `Rejected`, naming the column, and the
connection stays open so the next row works.

**The server remains the only writer**, which is why this exists rather than opening the
log from another box. litelink allows one writer per log, and neither refuses a second nor
detects one — so two `WriteHandle`s on one log is a corruption path with no guard. Handing
rows to the process that already owns the handle resolves the concurrency where it can
actually be resolved: any number of publishers, one writer. Offsets stay contiguous and a
batch stays one commit even with publishers racing.

> ⚠️ **A plain publisher is at-least-once under retry.** A row is durable when `send`
> returns. If the connection drops before the reply arrives, the publisher cannot tell
> whether the append happened — retrying may duplicate the row, not retrying may lose it.
> A publisher that cannot tolerate a duplicate names itself, below.

#### Exactly-once publishing

On a stream served with `exactly_once=True`, a publisher that names itself has its rows
deduped by the broker:

```python
from pathlib import Path

stream = streamcast.StreamHandle.open(
    streamcast.Stream(
        spec=streamcast.StreamSpec(name="orders", schema=SCHEMA, exactly_once=True),
        storage=streamcast.Storage(root=Path("data").absolute()),
    ),
    initialization="create_if_absent",
)

async with streamcast.publish(uri, publisher="enricher") as out:
    seq = out.info.publisher.last_seq + 1  # where the broker has this publisher
    await out.send(row, seq=seq)  # rows 1, 2, 3, … with no holes
```

The publisher id names one ordered sequence. The broker keeps the last seq it appended
for it, committed in the same transaction as the rows, so a resent row is not appended
twice. Its reply carries the original offset if still known, otherwise `None`; a seq
past the next raises `SequenceGap` with nothing appended. After a reconnect the greeting
says where the broker has the publisher: drop what you hold at or below `last_seq` and
send from the next. A stream not served exactly-once, and a server from before 0.23,
refuse the publisher (`ExactlyOnceUnavailable`) rather than taking its rows at least
once.

**Best effort across a lost buffer**: the marks live in the log's buffer, so they survive a
restart and a migration, come back with a WAL-replica restore, and are lost by a restore
from the published table alone or a revive. [`SPEC.md`](docs/SPEC.md) §6b has the whole of it.

#### Exactly-once end to end: an outbox and an inbox

A process whose state lives in a database can commit its input, its work and
its output in one transaction of that database:

```python
store = await streamcast.SqliteStore.open("state.db")   # or PostgresStore.connect(dsn)

async with (
    streamcast.inbox(in_uri, subscriber="enricher", store=store) as sub,
    streamcast.outbox(out_uri, publisher="enricher", store=store) as out,
):
    async for offset, ts, row, tx in sub:
        await tx.execute("UPDATE totals SET n = n + 1")
        await out.send(transform(row), tx=tx)
```

The inbox row's delete, the caller's writes and the outbox row commit together; a sender
ships the outbox over an exactly-once publication, and a receiver keeps the inbox filled
up to a bounded window (`inbox_max_bytes`), leaving the backlog in the log. Only work
done in that transaction is atomic; external side effects need their own idempotency.
The buffer-loss limit above still applies, and the sender logs and drops schema-rejected
rows, renumbering the pending sequence. [`SPEC.md`](docs/SPEC.md) §6c, and
[`examples/outbox_inbox/demo.py`](examples/outbox_inbox/demo.py) runs it end to end
through two crashes.

### Consumer

```python
async with streamcast.connect("ws://localhost:8765/trades") as stream:
    async for offset, ts, msg in stream:
        print(offset, msg["price"], msg["amount"])
```

`msg` is exactly the row that was published — no offset key, nothing injected — so it can
be logged, forwarded, or appended to another stream whole. The parse happens once, at the
publisher.

### Filtering a subscription

A subscriber can name a predicate over the declared columns and be sent only matching rows:

```python
async with streamcast.connect(uri, where={"ticker": "AAPL"}) as sub: ...
async with streamcast.connect(uri, where={"ticker": ["AAPL", "NVDA"]}) as sub:  ...  # membership
async with streamcast.connect(uri, where={"ticker": "AAPL", "client_id": 1}) as sub: ...  # AND
```

Equality and membership over scalars, and nothing else — the predicate arrives from a
client over a socket, so there is no expression language to parse and no `eval` to reach.
A column the schema does not have is refused with a 4400 naming it, rather than served as
a subscription that silently never delivers.

**It costs the fan-out a dict lookup, not a second encode.** `send` encodes each message
once and every subscriber gets those same bytes; a filter decides whether to *enqueue* the
shared frame. Measured on a four-column row: 162 ns for a one-term predicate against
961 ns for the encode already on the path.

**The replay is filtered the same way**, through the same compiled predicate — a resume
that delivered something the live connection would not is the one failure worth preventing
here, and `offset + 1` with the same `where=` picks up exactly where it left off.

`replay` in the greeting is an offset window: filtering and restore fences mean `end -
start` need not equal the number of rows delivered.

### Resuming

The server records its frontier when a subscriber attaches, replays `[requested, frontier)`
from the log, then switches it to the live queue. Everything below the frontier is already
durable; everything above is already in the subscriber's queue. The two partition the
stream exactly — no gap, no duplicate.

```python
async with streamcast.connect(uri, cursor=".trades.offset") as stream:
    async for offset, ts, msg in stream:
        handle(msg)
```

| keyword | what it does |
|---|---|
| `offset=N` | resume from `N` inclusive; `streamcast.EARLIEST` for everything the log holds |
| `cursor=path` | keep the resume point on disk — loaded at connect, saved as the loop runs |
| `cursor_uri=s3://…` | ship that cursor to object storage — see [recovering a consumer](#recovering-a-consumer) |
| `catch_up=True` | read the gap from the published tables — see [recovering a consumer](#recovering-a-consumer) |

The cursor advances when you ask for the *next* message, and is not saved if the block
exits with an exception — so a crash re-delivers rather than skips. `sub.commit()` forces
it for a consumer that batches.

An offset the server cannot serve is refused, never silently rounded:

```
NotReplayable: offset 100 is below 5000, the earliest offset this stream's log
still serves. Reconnect with catch_up=True to read the rows between from the
published tables if they still hold them — it will say so if they do not — or
with offset=streamcast.EARLIEST to take what is left and accept the gap.
```

Four `why` values — `not_durable`, `ahead`, `too_old`, `evicted` — because the caller's
next move differs for each. `EARLIEST` on a log that holds nothing yet is not refused: it
starts at the log's first row. (An older server refuses it as `empty`.)

#### Recovering a consumer

A local cursor recovers a consumer that restarted. It does not recover one whose machine
is gone — which is what `cursor_uri` is for, the mirror of
[recovering a server](#recovering-a-server) at this end.

```python
async with streamcast.connect(
    uri,
    cursor=".trades.offset",
    cursor_uri="s3://streamcast/consumer1/stream.offset",
    catch_up=True,
) as stream:
    async for offset, ts, msg in stream:
        handle(msg)
```

**`cursor_uri` moves the box.** A daemon thread ships the cursor to object storage, and a
consumer starting elsewhere with no local file resumes from there. On connect the **local
cursor wins** — the remote is read only when there is no local one, which is the
disaster-recovery case and the only one where a copy that lags by up to `upload_every`
should decide.

**`catch_up` covers having been down too long.** A consumer past the server's `max_replay`
is refused; the rows are in the stream's published tables, not gone. It reads them with
`StreamHandle.snapshot`, from the metadata file the greeting names, with **nothing connected** —
holding a socket through a long catch-up gets the subscriber dropped for falling behind —
then opens the socket where the snapshot ended, looping if the server moved on meanwhile.

It reads the **published tables**, not the replicated WAL, so a catching-up consumer needs
S3 read access and nothing else: no litestream binary, no subprocess, no litelink handle.
The band the WAL would add is the one the server is about to send anyway.

Neither is automatic. Both are keywords on `connect`, because a consumer that would rather
fail loudly than resume from a copy that lags should be able to say so.

## Reading a stream

A subscription delivers rows one at a time. To ask a question of a stream — an aggregate,
a join, a scan of last Tuesday — read it as a table, `log`, that spans every log the
stream has been through.

**`StreamHandle.snapshot` reads a fixed point.** Given the stream's metadata file on S3, it reads
the published tables on your machine with your credentials, offline — or, given `broker=`,
it also takes the rows the server has not published yet, up to an offset or the latest:

```python
async with await streamcast.StreamHandle.snapshot(
    "s3://market-data/prod/trades/trades.metadata/version-hint.text"
) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()

# The same, completed from the server up to its newest row.
async with await streamcast.StreamHandle.snapshot(
    "s3://market-data/prod/trades/trades.metadata/version-hint.text",
    as_of_offset=streamcast.LATEST,
    broker="ws://localhost:8765/trades",
) as snapshot:
    ...
```

**`StreamHandle.live` is real-time analytics on a stream in one line.** It is a snapshot kept
current with the broker's rows as they arrive, so every query answers as of the row that
arrived a moment ago. It is online by design — it takes only the broker's address, because
a view of the stream *now* has to be listening to it:

```python
async with await streamcast.StreamHandle.live("ws://localhost:8765/trades") as live:
    await live.sql("SELECT side, sum(amount) FROM log GROUP BY side").read_all()
```

Choosing a point, pruning with `filters=` against `where=`, memory, reconnects and
`wait_for` are in [`docs/API.md`](docs/API.md#reading-a-streams-history).

## Chaining

Each stage is a server, so a pipeline is servers end to end and every hop is independently
resumable:

```
market feed ─► streamcast ─► live runner ─► streamcast ─► dashboard
                  │                             │
                litelink                     litelink
```

Offsets are per server and are not translated between hops.

## What it is not

- **No broker-side consumer acknowledgements or consumer groups.** Routing is exact-match
  on a stream's name, with no topic hierarchy or wildcards. The broker does not track
  what a subscriber has consumed.
- **Not tuned for high fan-out across a WAN.** `compression` costs CPU per subscriber
  while the encode is shared, so it defaults off — see the API section. Turn it on for
  few subscribers over a WAN.
- **Not a place for frames that are not rows.** A typed log has nowhere to put a
  subscription ack or a heartbeat; the feed handler drops them.

## Documentation

- [`docs/API.md`](docs/API.md) — every public call, on one page
- [`docs/SPEC.md`](docs/SPEC.md) — the design, the protocol, and the invariants
- [`examples/`](examples/) — a live public feed through a server, and a resuming consumer
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — setup, the gates, and what a good PR looks like

## Development

```bash
just bootstrap          # uv sync + git hooks
just check              # lint + format-check + typecheck + tests
just --list             # the rest
```

Most of the suite needs no network, container or credentials. The replication and
catch-up tiers do: `just rustfs` starts a local S3 endpoint and `just check-all` runs
every gate against it. Without one those tests skip, and a skip is not a pass.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
