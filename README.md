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

```
exchange ws feed
      │  one connection
      ▼
streamcast server ──► litelink log      durable BEFORE any subscriber sees it
      │  fan-out
      ├──► strategy          offset 1861
      ├──► dashboard         offset 1861
      └──► recorder          offset 1861
```

With a [litelink](https://github.com/nhobin219/litelink) log attached it stops being a
fan-out and becomes a Python [tickerplant](https://code.kx.com/q/architecture/) — a term
used in kdb+/q systems for a process that captures a feed, writes it to a log file, and
publishes it to registered subscribers, which is this one almost exactly. Every message is
durable before any subscriber sees it,
so an offset is a **resume cursor**: a consumer that crashes, restarts, or falls behind
reconnects with the last offset it processed and the server replays the gap out of the log
before switching it to live — with no window in which a message is in neither place.

```python
async with streamcast.connect(uri, offset=1862) as stream:
    async for offset, message in stream:
        ...
```

### Multicaster or tickerplant?

The argument that decides it is `log=`.

**Without a log it is a multicaster.** One `encode` per message, one frame object shared
by every queue, identical bytes to every subscriber — a guarantee
([`SPEC.md`](docs/SPEC.md) I6), not an implementation detail. No per-consumer filtering,
no partitioning, no per-message routing. Offsets are `null`, because nothing assigned any.

**With one it is a tickerplant.** The feed is captured, logged, and published to
subscribers that can resume — named streams on one port, a subscribe negotiation, an
offset namespace it owns, and a delivery contract written in close codes.

```python
streamcast.Stream("trades")            # a multicaster. no offsets, nothing to resume from
streamcast.Stream("trades", log=log)   # a tickerplant. offsets are resume cursors
```

**It is not a message broker**, and the machinery that would make it one is deliberately
absent: no fan-in — nothing publishes into a stream over the wire — no acknowledgements,
no consumer groups, no per-message routing. See [what it is not](#what-it-is-not).

**Status: early.** The fan-out, the replay and the backpressure isolation work and are
tested. Read [what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet)
before you rely on it.

## Quick start

```bash
uv add git+https://github.com/nhobin219/streamcast     # not on PyPI yet
```

**The schema is yours.** streamcast declares no columns — the log is an ordinary
[litelink](https://github.com/nhobin219/litelink) table with whatever shape you gave it,
which is what makes it queryable rather than a pile of frames.

```python
import asyncio, json, litelink, pyarrow as pa, streamcast, websockets

SCHEMA = pa.schema([                       # every field worth a column
    pa.field("event_ts", pa.int64(), nullable=False),
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("side", pa.int64()),
])

async def main():
    log = litelink.new("data", "trades", schema=SCHEMA, sort_by=("event_ts",))
    stream = streamcast.Stream("trades", log=log)

    with log, await streamcast.serve(stream, "localhost", 8765):
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
the same frame; now it means none. And the log is a real table:

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

streamcast.Stream(name="", *, log=None, max_backlog=8192, max_replay=100_000)
    await stream.send(row) -> int              # durable, then fan out
    await stream.send_many(rows) -> list[int]  # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable

streamcast.serve(streams, host, port, **websockets_kwargs) -> Server
streamcast.connect(uri, *, offset=None, **websockets_kwargs) -> Subscription
streamcast.EARLIEST
```

Routing is by `Stream.name`: a stream named `trades` is served at `/trades`, an unnamed one
at `/`. `serve([trades, quotes])` serves both on one port, which is the multiplexing this
library is named for.

Details in [`docs/API.md`](docs/API.md).

## Resuming

A consumer's whole recovery story is one integer.

```python
offset = None                       # live from now; or streamcast.EARLIEST for all of it
while True:
    try:
        async with streamcast.connect(uri, offset=offset) as stream:
            async for offset, msg in stream:
                handle(msg)

    except (ConnectionClosed, OSError, streamcast.TooSlow):
        offset = None if offset is None else offset + 1
```

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

Every frame is JSON text — the greeting, then one object per row:

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
