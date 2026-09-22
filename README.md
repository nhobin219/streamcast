# A WebSocket multicaster with replay

**One connection in, one stream out, resumable.**

[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)

## Introduction

streamcast is a Python library for the thing that goes wrong when six processes on one box
all want the same market data feed: six connections to the exchange, six times the
bandwidth, a subscription limit you were not told about until you hit it, and six streams
that can quietly differ from each other. One process holds the upstream subscription;
everything else on the box reads from it, and gets **the same bytes in the same order**.

Market data is the case it was built against and the one the examples use, but nothing
here is specific to it: a feed is a websocket that sends messages, and a message is a row.

```
upstream ws feed
      │  one connection
      ▼
streamcast server ──► litelink log      durable BEFORE any subscriber sees it
      │  fan-out
      ├──► strategy          offset 1861
      ├──► dashboard         offset 1861
      └──► recorder          offset 1861
```

A streamcast server is effectively a Python WebSocket
[tickerplant](https://code.kx.com/q/architecture/) — a term used in kdb+/q systems for a
process that captures a feed, optionally writes it to a log file, and publishes it to
registered subscribers, which is this one almost exactly.

**The log is what makes an offset a resume cursor.** With a
[litelink](https://github.com/nhobin219/litelink) log attached, every message is durable
before any subscriber sees it, so a consumer that crashes, restarts, or falls behind
reconnects with the last offset it processed and the server replays the gap before
switching it to live — with no window in which a message is in neither place. Without one
the fan-out is identical, offsets are `null`, and `?offset=` is refused: there is simply
nothing to resume from.

```python
async with streamcast.connect(uri, offset=1862) as stream:
    async for offset, msg in stream:
        ...
```

**Status: early.** The fan-out, the replay and the backpressure isolation work and are
tested. Read [what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet)
before you rely on it.

## Quick start

```bash
uv add git+https://github.com/nhobin219/streamcast     # not on PyPI yet
```

**The schema is yours**, declared in JSON Schema — the wire is JSON, so the columns are
too. `streamcast` is the only import a durable stream needs:

```python
import asyncio, json, streamcast, websockets

SCHEMA = {                                 # every field worth a column
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
    stream = streamcast.Stream("trades", root="data", schema=SCHEMA,
                               sort_by=("event_ts",))

    # Fan-out, sealing, compaction and WAL shipping — all of it, one call.
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

Underneath it is an ordinary [litelink](https://github.com/nhobin219/litelink) table —
`streamcast.to_arrow(SCHEMA)` is the `pa.schema`, and `log=` takes one you opened
yourself. The greeting publishes the schema, so a subscriber in another language reads
the columns without this repo.

Any number of consumers, on that box or another — and they receive the **row**, not a blob
to parse:

```python
async with streamcast.connect("ws://localhost:8765/trades") as stream:
    async for offset, msg in stream:
        print(offset, msg["price"], msg["amount"])
```

`msg` is exactly what was published — no offset key, nothing injected — so it can be
logged, forwarded, or appended to another stream whole.

**The parse happens once, at the publisher.** Six consumers used to mean six JSON parses of
the same frame; now it means none.

**`serve` also starts a maintainer**, one subprocess per stream that has a log, and stops
it when the server closes. Without one nothing ever seals: litelink is explicit that *"a
maintainer is not optional"*, and measured on 100,000 rows past the 8 MiB seal target, a
server without one held every row in its SQLite buffer and wrote zero Parquet files.
**And litestream**, if the log has `wal_replication` on — `serve` is the one thing you
start. Both are opt-out (`maintain=False`, `replicate=False`) for a deployment running its
own; see [`docs/API.md`](docs/API.md#maintain) for the cadences, why the maintainer is
never a thread, how the sidecar guarantees only one litestream ever touches a database,
and where the subprocesses get their credentials.

And the log is a real table:

```python
log.sql("SELECT count(*), max(price), sum(amount) FROM log").read_all()
log.scan(columns=["litelink_offset", "price"], where="side = 1")   # prunes on statistics
```

That is the whole API for live fan-out. Replay, resume and the durable tier are the same
two calls with an `offset=`.

## The API is `websockets`, with one modification

`serve` and `connect` have the same shapes and pass every keyword through, so ssl,
`ping_interval`, `process_request` and the rest work exactly as they do there. Two things
differ, both deliberately:

**Iterating a subscription yields `(offset, msg)`**, not `message`. The offset is the only
thing that makes a reconnect a resume rather than a restart, and a subscriber that has to
ask for it separately will forget to. `msg` is a `dict` over your columns and nothing
else — the offset is framing, not data, and never appears inside it.

**A subscription is read-only.** It has no `send` — rather than a `send` that raises —
because publishing is `Stream.send` in the server's own process. Nothing inherits a method
it has to refuse.

```python
import streamcast

streamcast.Stream(name="", *, log=None, root=None, schema=None, sort_by=None,
                  config=None, archive=None, s3=None,
                  max_backlog=8192, max_replay=100_000)
    await stream.send(row) -> int | None       # durable, then fan out
    await stream.send_many(rows) -> list       # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable · stream.schema

streamcast.serve(streams, host, port, *, maintain=True, replicate=True, ...) -> Server
streamcast.connect(uri, *, offset=<unset>, cursor=None, ...) -> Subscription
streamcast.to_arrow · streamcast.from_arrow · streamcast.Cursor · streamcast.EARLIEST
```

Routing is by `Stream.name`: a stream named `trades` is served at `/trades`, an unnamed one
at `/`. `serve([trades, quotes])` serves both on one port, which is the multiplexing this
library is named for.

Details in [`docs/API.md`](docs/API.md).

## Resuming

A consumer's whole recovery story is one integer.

```python
async with streamcast.connect(uri, cursor=".trades.offset") as stream:
    async for offset, msg in stream:
        handle(msg)
```

Pass `cursor=` a path and the resume point lives on disk: loaded at connect, resumed one
above, saved as the loop runs. Stop the consumer and start it again and it picks up where
it stopped. `offset=` still wins if you give it.

**The cursor lags on purpose and never leads.** It advances when you ask for the *next*
message — coming back for another is the only evidence that the last was handled — and it
is not saved at all if the block exits with an exception, so a crash re-delivers rather
than skips. `sub.commit()` forces it for a consumer that batches.

**Add `cursor_uri="s3://bucket/consumer1/"` and it survives the box too.** A daemon thread
ships the cursor to object storage, and a consumer starting on a different machine with no
local file resumes from there — the same idea as the server's WAL replication, one layer
out.

The server records its frontier at the instant the subscriber attaches, replays
`[requested, frontier)` out of the log, and only then switches it to the live queue.
Everything below the frontier is already durable; everything from it up is already in the
subscriber's queue. **The two partition the stream exactly** — no gap, no duplicate — and
that is what makes this exactly-once rather than best-effort. `docs/SPEC.md` §3 is the
argument; `tests/test_resume.py` is the argument as a test.

An offset the server cannot serve is **refused, never silently rounded**:

```
NotReplayable: offset 100 is below 5000, the earliest offset this stream's log
still holds. The rows between are gone from it — read the archive for them, or
subscribe with offset=0 and accept the gap.
```

Five distinct reasons arrive as five distinct messages, because the caller's next move
differs for each.

## A slow consumer is only its own problem

The obvious broadcast — `for s in subscribers: await s.send(frame)` — makes the slowest
consumer the rate of the whole stream, because every other subscriber's frame is queued
behind an `await` on a TCP window that is not opening. streamcast gives each subscriber its
own queue and its own task, and `Stream.send` **never awaits a consumer**: it encodes the
frame once and does one non-blocking insert per subscriber.

A consumer that stops reading fills its own queue, hits `max_backlog`, and is **dropped**
— not buffered without bound, which is the OOM this design exists to avoid, and not
drop-oldest, which would leave it a stream with a hole in the middle and nothing marking
it. What it received is a contiguous prefix, so on a durable stream the drop costs a
reconnect and nothing else.

```
streamcast.TooSlow: the server dropped this subscriber for falling more than
8192 messages behind; resume at offset 20481
```

## Chaining

Each stage is a server, so a pipeline is servers end to end and every hop is independently
resumable:

```
market feed ─► streamcast ─► live runner ─► streamcast ─► dashboard
                  │                             │
                litelink                     litelink
```

The dashboard box runs a litelink capture with S3 publishing off, so it keeps a local
window and drops what ages out. When it restarts it reads the maximum offset it persisted,
hands that back on its subscription, and the runner's server replays the difference. That
is the whole of the recovery path, and it is the same two calls at every hop.

## What it is not

**Not a message broker.** No fan-in — nothing
publishes into a stream over the wire, and `Stream.send` runs in the server's own process
([`SPEC.md`](docs/SPEC.md) §6 and §9). No topics beyond a name, no consumer groups, no
acknowledgements, and no delivery guarantee beyond "what you received is a contiguous
prefix, and the log holds the rest". A subscriber that needs at-least-once with acks wants
a queue, not a multicast.

**Not a wide-area transport.** `compression` defaults to off because permessage-deflate
keeps a 32 KB compressor per connection, which turns a frame encoded once into a frame
compressed once per subscriber — the wrong trade on the LAN this is built for, and the
right one across a WAN, where you pass `compression="deflate"` and get it back.

**Not a replacement for reading the log.** `max_replay` bounds how far back a subscribe may
ask; past that the answer is litelink directly, which needs nothing from streamcast.

**Not a place for frames that are not rows.** A typed log has nowhere to put a subscription
ack or a heartbeat, so the feed handler drops them — the same division of labour a kdb
tickerplant has, where the feed handler parses and the plant stores typed rows.

## What goes over the wire

Every frame is JSON text — the greeting, then an `[offset, msg]` pair per message:

```
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":[1200,1861],"durable":true}
[1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

A frame is a **positional pair**: the offset is the server's framing, `msg` is the
publisher's row. `const [offset, msg] = JSON.parse(frame)` is the whole client in another
language, and `wscat ws://localhost:8765/trades?offset=0` is a working subscriber with none
at all.

Encoding is [msgspec](https://github.com/jcrist/msgspec) — measured at 0.285 µs for a
six-column row against 5.815 µs for stdlib `json`, which is what earns it a place on a path
every publish and every replayed row crosses.

Key order comes from the log's schema, not from the dict you passed, so **a replayed
message is byte-identical to the live one it repeats** — two subscribers holding the same
offset hold the same bytes.

**`offset` is `null` on a stream with no log.** Nothing assigned one, and a per-process
counter would look exactly like a resume cursor until the server restarted.

## Not implemented yet

**Remote publishers.** `Stream.send` is in the server's process; a client cannot publish
into a stream. **Registered intent** — one designated publisher and many read-only nodes,
coordinated through the server — is designed and unbuilt. **Arrow IPC as a negotiated wire
format** would make a bulk replay 492x cheaper to encode and 2.4x smaller, at the cost of
the `wscat` affordance; it is measured and unbuilt. See [`docs/SPEC.md`](docs/SPEC.md) §9.

## Documentation

- [`docs/API.md`](docs/API.md) — every public call, on one page
- [`docs/SPEC.md`](docs/SPEC.md) — the design, the protocol, and the invariants
- [`examples/`](examples/) — a live public feed through a server, and a resuming consumer
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — setup, the gates, and what a good PR here looks like

## Development

```bash
just bootstrap          # uv sync + git hooks
just check              # lint + format-check + typecheck + tests, same as CI
just --list             # the rest
```

Tooling is uv + ruff + [ty](https://github.com/astral-sh/ty) + pytest; commits follow
[Conventional Commits](https://www.conventionalcommits.org), enforced by a hook. The test
suite needs no network, no container and no credentials.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
