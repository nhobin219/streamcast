# A WebSocket multicaster, and a tickerplant when you give it a log

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
streamcast broker ──► litelink log      durable BEFORE any subscriber sees it
      │  fan-out
      ├──► strategy          offset 1861
      ├──► dashboard         offset 1861
      └──► recorder          offset 1861
```

With a [litelink](https://github.com/nhobin219/litelink) log attached it stops being a
fan-out and becomes a tickerplant. Every message is durable before any subscriber sees it,
so an offset is a **resume cursor**: a consumer that crashes, restarts, or falls behind
reconnects with the last offset it processed and the broker replays the gap out of the log
before switching it to live — with no window in which a message is in neither place.

```python
async with streamcast.connect(uri, offset=1862) as stream:
    async for offset, message in stream:
        ...
```

### Multicaster or broker?

Both, on different planes, and the argument that decides it is `log=`.

**The data plane is multicast.** One `encode` per message, one frame object shared by
every queue, identical bytes to every subscriber — a guarantee
([`SPEC.md`](docs/SPEC.md) I6), not an implementation detail. No per-consumer filtering,
no partitioning, no per-message routing.

**The control plane is a broker's.** Named streams on one port, a subscribe negotiation,
an offset namespace the server owns, replay out of durable storage, and a delivery
contract written in close codes.

The two together are a tickerplant, and `Stream` is either one depending on how it is
built:

```python
streamcast.Stream("trades")            # a multicaster. offsets die with the process
streamcast.Stream("trades", log=log)   # a tickerplant. offsets are resume cursors
```

**Throughout these docs, "the broker" means the process** — the thing at the other end of
a subscriber's socket — and never a claim about category. What would make that word an
overclaim is all deliberately absent: no fan-in (nothing publishes over the wire), no
acknowledgements, no consumer groups, no per-message routing. See
[what it is not](#what-it-is-not).

**Status: early.** The fan-out, the replay and the backpressure isolation work and are
tested. Read [what it is not](#what-it-is-not) and [not implemented yet](#not-implemented-yet)
before you rely on it.

## Quick start

```bash
uv add git+https://github.com/nhobin219/streamcast     # not on PyPI yet
```

```python
import asyncio
import litelink
import streamcast
import websockets

async def main():
    # `SCHEMA` is streamcast's, and a log must be created with it and nothing
    # else — three columns: when it arrived, whether it is text, and the bytes.
    log = litelink.new("data", "trades", schema=streamcast.SCHEMA)
    stream = streamcast.Stream("trades", log=log)

    with log, await streamcast.serve(stream, "localhost", 8765):
        async with websockets.connect("wss://ws.bitstamp.net") as feed:
            await feed.send(SUBSCRIBE)
            async for message in feed:
                await stream.send(message)      # durable, then fanned out

asyncio.run(main())
```

Any number of consumers, on that box or another:

```python
async with streamcast.connect("ws://localhost:8765/trades") as stream:
    async for offset, message in stream:
        print(offset, message)
```

That is the whole API for live fan-out. Replay, resume and the durable tier are the same
two calls with an `offset=`.

## The API is `websockets`, with one modification

`serve` and `connect` have the same shapes and pass every keyword through, so ssl,
`ping_interval`, `process_request` and the rest work exactly as they do there. Two things
differ, both deliberately:

**Iterating a subscription yields `(offset, message)`**, not `message`. The offset is the
only thing that makes a reconnect a resume rather than a restart, and a subscriber that has
to ask for it separately will forget to.

**A subscription is read-only.** It has no `send` — rather than a `send` that raises —
because publishing is `Stream.send` in the broker's own process. Nothing inherits a method
it has to refuse.

```python
import streamcast

streamcast.Stream(name="", *, log=None, max_backlog=8192, max_replay=100_000)
    await stream.send(message) -> int          # durable, then fan out
    await stream.send_many(messages) -> list[int]   # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable

streamcast.serve(streams, host, port, **websockets_kwargs) -> Server
streamcast.connect(uri, *, offset=None, **websockets_kwargs) -> Subscription
streamcast.SCHEMA · streamcast.EARLIEST
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
            async for offset, message in stream:
                handle(message)

    except (ConnectionClosed, OSError, streamcast.TooSlow):
        offset = None if offset is None else offset + 1
```

The broker records its frontier at the instant the subscriber attaches, replays
`[requested, frontier)` out of the log, and only then switches it to the live queue.
Everything below the frontier is already durable; everything from it up is already in the
subscriber's queue. **The two partition the stream exactly** — no gap, no duplicate — and
that is what makes this exactly-once rather than best-effort. `docs/SPEC.md` §3 is the
argument; `tests/test_resume.py` is the argument as a test.

An offset the broker cannot serve is **refused, never silently rounded**:

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
streamcast.TooSlow: the broker dropped this subscriber for falling more than
8192 messages behind; resume at offset 20481
```

## Chaining

Each stage is a broker, so a pipeline is brokers end to end and every hop is independently
resumable:

```
market feed ─► streamcast ─► live runner ─► streamcast ─► dashboard
                  │                             │
                litelink                     litelink
```

The dashboard box runs a litelink capture with S3 publishing off, so it keeps a local
window and drops what ages out. When it restarts it reads the maximum offset it persisted,
hands that back on its subscription, and the runner's broker replays the difference. That
is the whole of the recovery path, and it is the same two calls at every hop.

## What it is not

**Not a message broker**, whatever the prose calls the process. No fan-in — nothing
publishes into a stream over the wire, and `Stream.send` runs in the broker's own process
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

## Not implemented yet

**Remote publishers.** `Stream.send` is in the broker's process; a client cannot publish
into a stream. **Registered intent** — one designated publisher and many read-only nodes,
coordinated through the broker — is designed and unbuilt. **Binary payloads cost 4/3 their
size on disk**, because litelink refuses `binary` columns today and says to encode as text;
`kind` is already on the row, so that becomes a storage change with no wire change. See
[`docs/SPEC.md`](docs/SPEC.md) §9.

## Documentation

- [`docs/API.md`](docs/API.md) — every public call, on one page
- [`docs/SPEC.md`](docs/SPEC.md) — the design, the protocol, and the invariants
- [`examples/`](examples/) — a live public feed through a broker, and a resuming consumer
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
