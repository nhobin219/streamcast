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
its clients — a feed handler calling `Stream.send` in the same process or
`streamcast.publish` from another machine on one side; `connect`, `Stream.snapshot` and
`Stream.live` on the other — and the server owns the log, assigns the offsets, and is its
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
async with await streamcast.Stream.snapshot(
    "s3://bucket/prefix/trades.metadata.json"
) as snapshot:
    await snapshot.sql("SELECT count(*), max(price) FROM log WHERE side = 1")

# Or from any Iceberg engine, with neither streamcast nor litelink installed.
duckdb.sql("""
    SELECT count(*), max(price)
    FROM iceberg_scan('s3://bucket/prefix/trades',
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

**`serve` and `connect` are the `websockets` API.** Same names, same shapes, and every
keyword passed through — `ssl`, `ping_interval`, `process_request`, `max_queue` and the
rest behave exactly as they do there, and `serve` returns an object that proxies
`websockets.Server` (`sockets`, `serve_forever`, `connections`, `is_serving`).

**`publish` has no `websockets` counterpart**; it is streamcast's, shaped like `connect`
so it reads the same way. WebSocket itself has no verbs — the protocol is frames, and
both subscribing and publishing here are URL conventions on top of it.

```python
streamcast.Stream(name="", *, log=None, owns_log=False, max_replay=100_000)
streamcast.Stream.new(name="", *, root, schema, sort_by=None, config=None,
                      published=None, s3_options=None, replay_published=False,
                      max_replay=100_000)                     # None = no bound
    await stream.send(row) -> int | None       # durable, then fan out
    await stream.send_many(rows) -> list       # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable · stream.schema
    stream.metadata_uri                         # where a reader finds its history

await streamcast.Stream.snapshot(metadata_uri, *, as_of_offset=None, as_of_ts=None,
                                 broker=None, s3_options=None) -> Snapshot
    await snapshot.scan(columns=, where=, filters=, start_offset=, end_offset=)
    await snapshot.sql(query, *, filters=, start_offset=, end_offset=)  # table `log`
await streamcast.Stream.scan(metadata_uri, ...) · await streamcast.Stream.sql(uri, query)
await streamcast.Stream.live(broker, *, s3_options=None) -> Live   # kept current
    await live.scan(...) · await live.sql(query) · await live.wait_for(offset | ts=)

streamcast.serve(streams, host, port, *, maintain=True, replicate=False,
                 max_backlog=8192,                       # frames per subscriber
                 max_inbound=65_536,                     # rows queued to commit
                 max_in_flight=64, ...) -> Server        # an int, or {stream: int}
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
  Publishing is `Stream.send`, in the server's own process.
- **`compression` defaults to `None`**, where `websockets` defaults to `"deflate"`.
  permessage-deflate is per connection while the encode is shared: `send` encodes a frame
  once and hands the same bytes to every subscriber, and deflate compresses those identical
  bytes once per subscriber. Measured on a six-column trade row — 0.564 µs to encode once,
  3.454 µs to deflate each — CPU per message is 4 µs at one subscriber and 691 µs at 200.
  Past that the server is CPU-bound and starts dropping subscribers at `max_backlog`.

  Turn it on where bandwidth costs more than CPU, which is few subscribers over a WAN:
  it is **5.8× smaller** here, 112 bytes to 19. There is no middle setting — without
  context takeover the same frames compress 1.1×, so compressing once and sharing the
  result is not available.

Routing is by `Stream.name`: `trades` is served at `/trades`, an unnamed stream at `/`.
`serve([trades, quotes])` serves both on one port.

Full reference in [`docs/API.md`](docs/API.md).

## The wire

Every frame is a text frame of JSON: a greeting, then `[offset, ts, msg]` per message.
A client in any language needs a JSON parser, plus, for binary columns, the decoding rules
in [SPEC §2](docs/SPEC.md#reading-a-row-in-another-language).

```
{"streamcast":4,"stream":"trades","end_offset":1861,"replay":[1200,1861],
 "metadata":"s3://market-data/prod/trades.metadata.json","stream_id":"6f1c…","durable":true}
[1861,1790038800124001,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

The offset and the stamp are positional, so `const [offset, ts, msg] = JSON.parse(frame)` is a client in
another language and `wscat ws://localhost:8765/trades?offset=0` is a working subscriber
with none at all.

`metadata` is the stream's metadata file — every log it has been through, where each
is published, and what each holds — so a subscriber holding the greeting can read the
history itself rather than through the socket: `Stream.snapshot(info.metadata)`, or any
Iceberg engine pointed at a log's published table. `stream_id` says which stream the
file is. Both `null` when the stream has no log. Credentials are never in it: they are
the reader's own. Key order comes from the log's schema, so a replayed message is
byte-identical to the live one it repeats.

Encoding is [msgspec](https://github.com/jcrist/msgspec): 0.285 µs for a six-column row
against 5.815 µs for stdlib `json`.

`offset` is `null` on a stream with no log — nothing assigned one, and a per-process
counter would look like a resume cursor until the server restarted.

## Server

`serve` runs the broker. It accepts publishers, fans messages out to subscribers, and with
a log attached replays what a subscriber missed — so neither end owns the process, and
either can restart without the other noticing.

Routing is exact-match on the stream's name: there is no topic hierarchy and no wildcard
subscription. A subscriber names one stream and resumes it by offset, which is the trade
the log buys.

The schema is yours, declared in JSON Schema. `streamcast` is the only import a durable
stream needs.

```python
import asyncio, json, streamcast, websockets

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
    # Creates the log at data/trades, or opens it if it is already there.
    stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA,
                                   sort_by=("event_ts",))

    # Fan-out, sealing, compaction and WAL shipping: one call.
    async with streamcast.serve(stream, "localhost", 8765):
        async with websockets.connect("wss://ws.bitstamp.net") as feed:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                trade = json.loads(message)["data"]
                await stream.send({                  # a row, durable, then fanned out
                    "event_ts": int(trade["microtimestamp"]),
                    "price": float(trade["price"]),
                    "amount": float(trade["amount"]),
                    "side": int(trade["type"]),
                })

asyncio.run(main())
```

`serve` starts everything the streams need: one maintainer covering every log it serves —
a set of five subprocesses, one per storage role (seal, compact, publish, clean, clean
published) — and, with `replicate=True`, one litestream for the logs with
`wal_replication` on. The maintainer is opt-out (`maintain=False`); litestream is opt-in. Without a maintainer nothing ever seals — litelink
is explicit that *"a maintainer is not optional"*.

**One of each per server, not per log.** A maintainer process is a full interpreter with
litelink, pyarrow, pyiceberg and duckdb loaded — 149 MB RSS measured here — so the set of
five is shared by every log rather than started again for each, and litestream adds
40–170 MB per process on top.
The marginal cost mattered more than the total: a stream taking a row a minute cost the
same as the busiest one, which made "should this be its own stream" a resource question it
should not be. `Maintain(dedicated=("trades",))` gives a named log a set of its own.

`Stream.new` creates or opens the log; `Stream(log=handle)` takes one you opened yourself
and does no I/O. `streamcast.to_arrow(SCHEMA)` is the `pa.schema` if you want it.

What it captures is a table, queried from its published tables:

```python
uri = stream.metadata_uri
await streamcast.Stream.sql(uri, "SELECT count(*), max(price), sum(amount) FROM log")
await streamcast.Stream.scan(uri, columns=["litelink_offset", "price"], where="side = 1")
await streamcast.Stream.sql(uri, "SELECT max(streamcast_ts - event_ts) FROM log")   # feed latency, us
```

The table has two columns you did not declare. `litelink_offset` is the offset every frame
carries, and `streamcast_ts` is when the server took the row, in UTC microseconds — every
frame carries that too, as its `ts`, beside the row rather than in it.

### Changing the schema

A log's columns are fixed when it is created. To change them, stop the server and
migrate:

```python
stream = streamcast.Stream.migrate("trades", root="data", schema=SCHEMA_V2)
```

That seals the current log, starts `trades-v2` at the next offset, and records both in a
metadata file. Columns can be added and removed. A column's type can never change. It is safe
to leave in your startup: a stream already of that shape is just opened.

### Surviving a feed that changes

`send` validates the row against the schema, so a feed that changes shape breaks capture —
a missing field, an unexpected type or a new key all raise, and that message is lost:

```
ValueError: row leaves non-nullable columns NULL: ['price']
ValueError: row names columns this log does not have: ['surprise']
```

If keeping every message matters more than strictness, declare the columns nullable, add
one for the raw message, and parse best-effort:

```python
SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": ["integer", "null"]},
        "price": {"type": ["number", "null"]},
        "raw": {"type": ["string", "null"]},
    },
    "required": ["event_ts", "price", "raw"],
}

def number(value):                      # whatever the feed sent, or nothing
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def row(message: str) -> dict:
    """Best effort: take what parses, keep the whole message either way."""
    try:
        data = json.loads(message)["data"]
    except (ValueError, KeyError, TypeError):
        data = {}

    return {
        "event_ts": number(data.get("microtimestamp")),
        "price": number(data.get("price")),
        "raw": message,
    }

await stream.send(row(message))
```

The row and its source land in **one append**, so a message is never captured without the
bytes it came from, and whatever the parse missed can be backfilled from the log later.
Run against a feed that drops a field, sends a non-trade event, and then sends invalid
JSON, all four rows are captured with the typed columns null and `raw` intact.

`required` still names every column, because in JSON Schema `required` is about the key
being present and `["number", "null"]` is what makes the value nullable — see
[`docs/API.md`](docs/API.md). Every column is nullable here precisely because best-effort
extraction means any of them can be missing.

Two costs, both real. A raw string column roughly doubles the log and compresses worse than
typed columns, which is [`SPEC.md`](docs/SPEC.md) §5's argument running the other way — this
is a deliberate trade, not a default. And subscribers receive the column too, since the wire
carries every declared column.

streamcast does not do the extraction for you. Feeds nest their payloads differently — the
example above reaches through `["data"]` — so a general extractor needs per-field paths, at
which point it is a feed-handler layer rather than a flag. It belongs in your feed handler,
where it already knows the feed.

### Handling multiple publishers

Several publishers writing to one stream interleave in one log, so a row has to say who
wrote it. Declare a publisher key and a per-publisher sequence alongside your own columns:

```python
SCHEMA = {
    "type": "object",
    "properties": {
        "publisher": {"type": "string"},        # who wrote it
        "seq": {"type": "integer"},             # monotonic, per publisher
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
    },
    "required": ["publisher", "seq", "event_ts", "price"],
}
```

The server needs no configuration for this — it already serialises publishers, so offsets
stay contiguous and a `send_many` stays one commit whoever else is writing. The columns are
for the **publishers**, so each can find its own rows again after a restart. See
[recovering a producer](#recovering-a-producer).

**Declare them before anyone publishes.** A column added later means migrating the stream
to a new log (`Stream.migrate`), and is nullable for ever — older logs read null — so a
publisher that forgets to set it writes NULL silently, and a recovery scan cannot tell that
apart from another publisher's row. Declared up front they are `required` and non-null, and a publisher that
forgets fails loudly at `send`.

A row that already carries a natural unique key needs none of this — match on that instead.
And a single publisher needs no key at all: its own cursor is enough.

### Backpressure

`Stream.send` never awaits a consumer: it encodes the frame once and does one non-blocking
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
| set on | `serve`, per process | `Stream`, per stream |

**They interact, which is why sizing one without the other goes wrong.** A replay is
served *before* the live queue, and live messages pile up behind it — so a subscriber
replaying `max_replay` messages has to finish within `max_backlog` new ones or it is
dropped at the moment it catches up, having done all the work. Raise one and check the
other; `just bench-replay` prints the arithmetic for your hardware.

### Recovering a server

A client moves boxes with a cursor. A **server** moves with `Stream.restore`, which
rebuilds the log itself on a machine that never held it: from its replicated WAL when
there is one, and otherwise from its published table alone:

```python
stream = streamcast.Stream.restore(
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
`Stream.retire` finishes it: every row published, the log refusing writes for good, and
the retirement recorded in its metadata. It is still served, read-only — subscribers
replay and catch up, publishers are refused with 4410. `Stream.restore(..., revive=True)`
undoes it on any box, continuing at exactly the retired end, so a planned move is
`retire` on the old box and `revive` on the new one, losing nothing and skipping no
offsets:

```python
streamcast.Stream.retire("trades", root="data")                     # old box, stopped
stream = streamcast.Stream.restore(
    "trades", root="data", published="s3://market-data/prod", revive=True,
)                                                                    # new box
```

### Serving the whole history

`replay_published=True` with `max_replay=None` makes the server a complete gateway to the
log: no subscribe is refused for reaching too far back, and the server reads the published
table on the subscriber's behalf.

```python
stream = streamcast.Stream.new("trades", root="data", schema=SCHEMA,
                               published="s3://bucket/prefix",
                               replay_published=True, max_replay=None)
```

Every frame is still JSON over a plain WebSocket, so **a client in any language replays the
entire stream from offset 1** — no litelink, no Iceberg reader, no object-storage
credentials, nothing from this repo. `catch_up` exists because the default is the opposite;
this is the setting that makes it unnecessary.

It is not the default because of `max_backlog`. A replay is served before the live queue,
which fills behind it, so a subscriber reading ten million rows out of S3 accumulates live
messages for as long as that takes and is dropped the moment it catches up if it passed the
backlog on the way. Size the two together, or run it on a stream quiet enough that the
arithmetic does not bite. Each replay also holds a worker from the `to_thread` pool
(`min(32, cpu + 4)`) for its whole scan.

### Knowing a stream is alive

A subscriber cannot tell a quiet stream from a dead one — both are a socket with nothing
arriving. `serve(stats=True)` answers `GET /stats` on the port it already has:

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

On by default, unlike `publish=`. That grants writes; this discloses strictly less than
the socket beside it — a wrong-path connect is already answered with the names of every
stream it serves, the greeting already carries `end_offset`, and anyone who can reach the
port can subscribe and read every row in full. A server that needs this private needs the
port private. `stats=False` turns it off, `stats="/_internal/streams"` moves it, and a
`process_request` of your own still runs for every other path.

### Mounting in an existing app

A service that is already an ASGI app — FastAPI, Starlette, anything — can serve a stream
on the port it already has, instead of running `serve()` on a second one.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from streamcast.asgi import asgi

streams = asgi([trades, quotes])

@asynccontextmanager
async def lifespan(app):
    async with streams:            # starts the maintainers, stops them on exit
        yield

app = FastAPI(lifespan=lifespan)
app.mount("/streams", streams)
```

Subscribers then connect to `ws://host/streams/trades`, and nothing about the client
changes — same URL shape, same frames, same refusals. `pip install 'streamcast[asgi]'`
adds Starlette and nothing else.

`serve` and `asgi` are two transports for one `Stream` and you call **one** of them — the
`Stream` holds the offsets, the log and the fan-out either way.
[`examples/fastapi_app.py`](examples/fastapi_app.py) is the whole thing as a running
service: `just demo fastapi`, then point a consumer at
`ws://127.0.0.1:8770/streams/trades`.

**`async with streams` is not optional.** Starlette does not run a mounted sub-app's
lifespan, so an app that left the maintainers to `lifespan` events would start none of them
once mounted — and a log with nothing sealing it buffers every row it ever receives.

Two settings stop being this library's and become the ASGI server's, and their defaults
differ from `serve`'s:

| | `serve` | mounted |
|---|---|---|
| keepalive | `ping_interval=20`, so a dead peer surfaces in ~40s | uvicorn's `--ws-ping-interval` |
| compression | off, because the encode is shared and deflate is per connection | the host app's |

Compression is the one to watch: a host app that enables permessage-deflate globally pays
4 µs of CPU per message at one subscriber and 691 µs at 200, and the symptom is a
CPU-bound server dropping subscribers for falling behind.

## Client

Two ends, and a connection is one or the other. A subscriber has no `send`; a publisher
has no `recv`. Neither carries a method that raises.

### Producer

`Stream.send` publishes from the server's own process. `streamcast.publish` does it from
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

> ⚠️ **Publishing is at-least-once under retry.** A row is durable when `send` returns. If
> the connection drops before the reply arrives, the publisher cannot tell whether the
> append happened — retrying may duplicate the row, not retrying may lose it. streamcast
> does not resolve that ambiguity; a publisher that cannot tolerate a duplicate carries its
> own key in the row and deduplicates downstream, which is the only place it is decidable.
> The fix is small: carry a publisher key and a per-publisher sequence as columns, and on
> reconnect replay from the offset you were last acked for, filtering in memory. The offset
> bounds the read; the key identifies your rows in it. [`SPEC.md`](docs/SPEC.md) §6b has it.

#### Recovering a producer

`cursor=` records the offset this publisher was last acknowledged for, and `cursor_uri=`
ships it to object storage so a producer can resume on another box. The same two keywords
a consumer takes, doing the same job at the other end of the stream — and distinct from
[recovering a server](#recovering-a-server), which moves the log itself.

```python
async with streamcast.publish(
    uri, cursor=".trades-producer.offset",
    cursor_uri="s3://streamcast/producer1/cursor.offset",
) as producer:
    start = producer.resumed_from          # where this publisher got to, or None
```

**It does not resume by itself, and that is the difference from a consumer.** A consumer
cursor is enough on its own: the server replays from it. A producer cursor says where this
publisher got to, not what it should send next — that is its own outbox, or a position in
whatever it reads from, and the library cannot know either. So it is reported and you act
on it.

Acting on it is the replay in [`SPEC.md`](docs/SPEC.md) §6b: subscribe from
`resumed_from` **inclusive**, and the first row is this publisher's own last acknowledged
one, so the sequence it carried comes back out of the log. That is why one integer on disk
is enough.

Saves are throttled to once a second and settled on a clean exit; `producer.commit()`
forces one, or `commit(offset)` states what you consider settled. A cursor that lags only
widens the replay — a cursor that leads would skip rows and duplicate them.

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

One consequence worth knowing: `replay` in the greeting stops being a count. Unfiltered,
`end - start` is exactly how many messages arrive before live begins; filtered, it is the
offset window and the number delivered is at most that. A loop that must terminate on a
count — the publisher recovery in [`docs/SPEC.md`](docs/SPEC.md) §6b — wants an unfiltered
subscription.

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
[recovering a producer](#recovering-a-producer) at this end.

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
`Stream.snapshot`, from the metadata file the greeting names, with **nothing connected** —
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

**`Stream.snapshot` reads a fixed point.** Given the stream's metadata file on S3, it reads
the published tables on your machine with your credentials, offline — or, given `broker=`,
it also takes the rows the server has not published yet, up to an offset or the latest:

```python
async with await streamcast.Stream.snapshot(
    "s3://market-data/prod/trades.metadata.json"
) as snapshot:
    await snapshot.sql("SELECT side, sum(amount) FROM log GROUP BY side")

# The same, completed from the server up to its newest row.
async with await streamcast.Stream.snapshot(
    "s3://market-data/prod/trades.metadata.json",
    as_of_offset=streamcast.LATEST,
    broker="ws://localhost:8765/trades",
) as snapshot:
    ...
```

**`Stream.live` is real-time analytics on a stream in one line.** It is a snapshot kept
current with the broker's rows as they arrive, so every query answers as of the row that
arrived a moment ago. It is online by design — it takes only the broker's address, because
a view of the stream *now* has to be listening to it:

```python
async with await streamcast.Stream.live("ws://localhost:8765/trades") as live:
    await live.sql("SELECT side, sum(amount) FROM log GROUP BY side")
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

- **Not a message queue.** No topics beyond a name, no consumer groups, and no consumer
  acknowledgements — where a subscriber has got to is its own cursor, not state the server
  keeps. A publisher does get an ack, the offset once the row is durable; nothing tracks
  what a subscriber has consumed. A subscriber needing at-least-once with server-side acks
  wants one.
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
