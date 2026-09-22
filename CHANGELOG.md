# Changelog

All notable changes are recorded here. Versions follow
[Semantic Versioning](https://semver.org); the format is loosely
[Keep a Changelog](https://keepachangelog.com).

## 0.2.0

**Breaking, and the whole of it is one correction: the schema is the caller's.**

0.1.0 owned a fixed three-column schema — `recv_ts`, `kind`, `payload` — and
stored each upstream frame whole in the string column. litelink's own websocket
example says the opposite in as many words: *"Every field the feed sends that is
worth a column ... which is the reason to declare a schema rather than store the
frame whole."* Storing it whole threw away pruning, compression, a queryable
archive and a cheap replay — every property the table was for — and left a
library that needed none of litelink to do what it did.

The argument that defended it was circular: that a caller's extra column "would
have to be filled by `send`, which has nothing to fill it with". True only
because `send` took bytes.

### What changed

- **`send` takes a row**, litelink's `Row`, over your declared columns.
  `streamcast.SCHEMA` is gone; create the log with your own schema.
- **Subscribers receive the row**, not a blob — `async for offset, row in sub`.
  The parse happens once, at the publisher, instead of once per consumer.
- **Every frame is JSON text.** The binary header, the payload `kind` and the
  base64 path for binary messages are all gone; the offset is a column in the
  row. `wscat ws://broker:8765/trades?offset=0` is now a working subscriber.
- **msgspec** replaces stdlib `json`, which serialisation moving onto the hot
  path in both directions is what earns. Measured on a six-column trade row:
  0.285 us against 5.815 to encode (20.4x), 0.386 against 4.989 to decode.
- **A replayed frame is byte-identical to the live one it repeats** — both
  project through the log's declared column order. `_log.replay` checks the
  scan's column order against what it projected, once per batch, because the
  optimisation below is only sound while that holds.
- **`benchmarks/replay.py`**, because `SPEC.md` §4 was sizing `max_replay`
  against ~1M rows/s and an 80,000 msg/s threshold that were guesses stated as
  measurements. Real figures: ~390,000 rows/s warm, ~0.6 s cold for the first
  scan in a process, and a ~30,000 msg/s threshold.

### Also

- A replay lets Arrow build the row dicts. `scan` already projects the batch
  into wire order, so `to_pylist()` does it in C: 1.55 us a row against 2.38,
  for identical bytes.
- A frame that is not a row now has nowhere to go, so the feed handler drops
  subscription acks and heartbeats — the same division of labour a kdb
  tickerplant has.

## 0.1.0

First release. A WebSocket multicaster with a durable log behind it.

### The broadcast

- `Stream` — offsets, subscribers and the subscribe partition, with no socket in
  it. `send` makes a message durable before any subscriber sees it and never
  awaits a consumer; `send_many` commits a group in one transaction.
- `serve` and `connect`, shaped after `websockets` and passing every keyword
  through. Routing is by `Stream.name`: `/trades`, or `/` for an unnamed stream.
  One port serves any number of streams.
- Iterating a subscription yields `(offset, message)` rather than `message`, and
  a subscription is read-only — it has no `send`, rather than a `send` that
  raises.

### Resuming

- `?offset=` replays out of a litelink log and switches to live with no gap and
  no duplicate at the join. The broker records its frontier at the instant a
  subscriber attaches; everything below it comes from the log, everything from
  it up is already in that subscriber's queue.
- `streamcast.EARLIEST` asks for everything the log still holds.
- Five distinct refusals — `not_durable`, `empty`, `ahead`, `too_old`,
  `evicted` — because the caller's next move differs for each. A refusal
  travels as a code and some numbers and is turned back into a sentence at the
  subscriber, since a close frame has 123 bytes and that is not a sentence.

### Backpressure

- One queue and one task per subscriber. A consumer that stops reading fills its
  own queue and nobody else's, and is dropped at `max_backlog` rather than
  buffered without bound or served a stream with a hole in it. What it received
  is a contiguous prefix, so on a durable stream a drop costs a reconnect.

### Storage

- `streamcast.SCHEMA` is the three columns a streamcast log carries, and a log
  with any other shape is refused at `Stream` construction rather than at the
  first message.
- Binary payloads are stored base64, at 4/3 their size, because litelink refuses
  `binary` columns today. `kind` is already on the row, so this becomes a
  storage change with no wire change when litelink's blob fields land.
