# streamcast

A replayable WebSocket multicaster. One upstream stream in, appended to a
[litelink](https://github.com/nhobin219/litelink) log — an Iceberg table on disk — and
broadcast to any number of downstream subscribers. Each message carries the offset it was
written at, so a subscriber that stops can reconnect and ask for the rest.

[![license](https://img.shields.io/badge/license-Apache%20v2-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)

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

Every subscriber receives the same bytes in the same order, from one `encode` call.
The API is `websockets` with two deliberate differences, listed below.

A streamcast server is a Python WebSocket
[tickerplant](https://code.kx.com/q/architecture/): a process that captures a feed,
optionally writes it to a log, and publishes it to registered subscribers.

**Status: early.** Nothing is released. Read [what it is not](#what-it-is-not) and
[not implemented yet](#not-implemented-yet) first.

## Install

```bash
uv add git+https://github.com/nhobin219/streamcast     # not on PyPI yet
```

## Server

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

`serve` starts everything the stream needs: a maintainer subprocess per stream with a log,
and litestream if the log has `wal_replication` on. Both are opt-out (`maintain=False`,
`replicate=False`). Without a maintainer nothing ever seals — litelink is explicit that
*"a maintainer is not optional"*.

`Stream.new` creates or opens the log; `Stream(log=handle)` takes one you opened yourself
and does no I/O. `streamcast.to_arrow(SCHEMA)` is the `pa.schema` if you want it.

## Consumer

```python
async with streamcast.connect("ws://localhost:8765/trades") as stream:
    async for offset, msg in stream:
        print(offset, msg["price"], msg["amount"])
```

`msg` is exactly the row that was published — no offset key, nothing injected — so it can
be logged, forwarded, or appended to another stream whole. The parse happens once, at the
publisher.

The log is a table, queryable without streamcast:

```python
log.sql("SELECT count(*), max(price), sum(amount) FROM log").read_all()
log.scan(columns=["litelink_offset", "price"], where="side = 1")   # prunes on statistics
```

## API

```python
streamcast.Stream(name="", *, log=None, owns_log=False,
                  max_backlog=8192, max_replay=100_000)
streamcast.Stream.new(name="", *, root, schema, sort_by=None, config=None,
                      archive=None, s3=None,
                      max_backlog=8192, max_replay=100_000)
    await stream.send(row) -> int | None       # durable, then fan out
    await stream.send_many(rows) -> list       # ONE fsync for the group
    stream.end_offset · stream.subscribers · stream.durable · stream.schema

streamcast.serve(streams, host, port, *, maintain=True, replicate=True, ...) -> Server
streamcast.connect(uri, *, offset=<unset>, cursor=None, cursor_uri=None,
                   catch_up=False, ...) -> Subscription
streamcast.to_arrow · streamcast.from_arrow · streamcast.Cursor · streamcast.EARLIEST
```

Routing is by `Stream.name`: `trades` is served at `/trades`, an unnamed stream at `/`.
`serve([trades, quotes])` serves both on one port.

Two differences from `websockets`, which otherwise passes every keyword through:

- **Iterating yields `(offset, msg)`**, not `message`. The offset is what makes a
  reconnect a resume rather than a restart.
- **A subscription is read-only.** It has no `send`, rather than a `send` that raises.
  Publishing is `Stream.send`, in the server's own process.

Full reference in [`docs/API.md`](docs/API.md).

## Resuming

The server records its frontier when a subscriber attaches, replays `[requested, frontier)`
from the log, then switches it to the live queue. Everything below the frontier is already
durable; everything above is already in the subscriber's queue. The two partition the
stream exactly — no gap, no duplicate.

```python
async with streamcast.connect(uri, cursor=".trades.offset") as stream:
    async for offset, msg in stream:
        handle(msg)
```

| keyword | what it does |
|---|---|
| `offset=N` | resume from `N` inclusive; `streamcast.EARLIEST` for everything the log holds |
| `cursor=path` | keep the resume point on disk — loaded at connect, saved as the loop runs |
| `cursor_uri=s3://…` | ship that cursor to object storage, so another box can resume |
| `catch_up=True` | read the gap from the archive when the server will not replay that far |

The cursor advances when you ask for the *next* message, and is not saved if the block
exits with an exception — so a crash re-delivers rather than skips. `sub.commit()` forces
it for a consumer that batches.

`catch_up` reads the archive with **nothing connected**, then opens the socket where the
archive ended, looping if the server moved on. Holding a socket through a long catch-up
would get the subscriber dropped for falling behind.

An offset the server cannot serve is refused, never silently rounded:

```
NotReplayable: offset 100 is below 5000, the earliest offset this stream's log
still holds. The rows between are gone from it — reconnect with catch_up=True
to read them from the archive, or with offset=0 to accept the gap.
```

Five `why` values — `not_durable`, `empty`, `ahead`, `too_old`, `evicted` — because the
caller's next move differs for each.

## Backpressure

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

## The wire

Every frame is JSON text: a greeting, then an `[offset, msg]` pair per message.

```
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":[1200,1861],"durable":true}
[1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

The offset is positional, so `const [offset, msg] = JSON.parse(frame)` is a client in
another language and `wscat ws://localhost:8765/trades?offset=0` is a working subscriber
with none at all. Key order comes from the log's schema, so a replayed message is
byte-identical to the live one it repeats.

Encoding is [msgspec](https://github.com/jcrist/msgspec): 0.285 µs for a six-column row
against 5.815 µs for stdlib `json`.

`offset` is `null` on a stream with no log — nothing assigned one, and a per-process
counter would look like a resume cursor until the server restarted.

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

- **Not a message broker.** No fan-in: nothing publishes into a stream over the wire. No
  topics beyond a name, no consumer groups, no acknowledgements. A subscriber needing
  at-least-once with acks wants a queue.
- **Not a wide-area transport.** `compression` defaults to off, because permessage-deflate
  keeps a 32 KB compressor per connection and compresses once per subscriber a frame that
  was encoded once. Pass `compression="deflate"` across a WAN.
- **Not a query interface.** `catch_up` covers resuming from further back than
  `max_replay`; querying history is litelink directly, or any Iceberg engine.
- **Not a place for frames that are not rows.** A typed log has nowhere to put a
  subscription ack or a heartbeat; the feed handler drops them.

## Not implemented yet

**Remote publishers.** `Stream.send` runs in the server's process; a client cannot publish
into a stream. **Registered intent** — one designated publisher and many read-only nodes —
is designed and unbuilt. **Arrow IPC as a negotiated wire format** would make a bulk
replay 492x cheaper to encode and 2.4x smaller, at the cost of the `wscat` affordance.
See [`docs/SPEC.md`](docs/SPEC.md) §9.

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
