# Changelog

All notable changes are recorded here. Versions follow
[Semantic Versioning](https://semver.org); the format is loosely
[Keep a Changelog](https://keepachangelog.com).

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
