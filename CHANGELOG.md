# Changelog

All notable changes are recorded here. Versions follow
[Semantic Versioning](https://semver.org); the format is loosely
[Keep a Changelog](https://keepachangelog.com).

**Nothing has been released yet.** There is no tag, no PyPI package and no
release workflow, so `0.1.0` is a number in `pyproject.toml` rather than
something anyone can install. The entry below describes what the library is,
not what changed for users — there are none — and the design decisions it
records are kept because they were arrived at expensively, not because anybody
has to migrate across them.

## 0.1.0 — unreleased

A WebSocket multicaster with replay. One process holds the upstream
subscription and fans it out; with a litelink log attached, an offset is a
resume cursor and a consumer that stops can catch up.

### Recovery

- **`connect(catch_up=True)`** reads the gap from the log's archive when a
  consumer has fallen past the server's `max_replay`, then picks the socket up
  where the archive ended. Nothing is connected while the archive is read: a
  subscriber holding a socket through a long catch-up is dropped for falling
  behind, which would fail exactly the consumers that need it. It loops —
  read, connect, and if the server moved on, read the newly archived rows —
  bounded by `catch_up_retries` (3).
- **`connect(cursor=path)`** keeps the resume point on disk, loaded at connect
  and saved as the loop runs; **`cursor_uri=`** ships it to object storage on a
  daemon thread so a consumer can resume on a different box.
- Failures land at `connect`, not at the first `recv`, and say what to fix: an
  unreadable archive names what was tried, which credential source, and four
  ways out.

### The broadcast

- `Stream` — offsets, subscribers and the subscribe partition, with no socket
  in it. `send` takes a **row** and makes it durable before any subscriber sees
  it, and never awaits a consumer; `send_many` commits a group in one
  transaction.
- `serve` and `connect`, shaped after `websockets` and passing every keyword
  through. Routing is by `Stream.name`: `/trades`, or `/` for an unnamed
  stream. One port serves any number of streams.
- A subscription is read-only — it has no `send`, rather than a `send` that
  raises.

### The schema is the caller's

streamcast declares no columns. The log is an ordinary litelink table with
whatever shape the application gave it, which is what makes it queryable,
prunable and readable by any Iceberg engine.

An earlier design owned a fixed `(recv_ts, kind, payload)` schema and stored
each upstream frame whole. litelink's own websocket example says the opposite
in as many words — *"the reason to declare a schema rather than store the frame
whole"* — and storing it whole threw away pruning, compression, a queryable
archive and a cheap replay. `docs/SPEC.md` §5 records why, because the mistake
defended itself in a docstring and could be made again.

### The wire

- Every frame is JSON text: the greeting, then an **`[offset, msg]` pair** per
  message. `msg` is the publisher's row and nothing else — no offset key, no
  injected metadata.
- `wscat ws://localhost:8765/trades?offset=0` is a working subscriber, and
  `const [offset, msg] = JSON.parse(frame)` is the whole client in another
  language.
- msgspec, not stdlib `json`: serialisation sits on the hot path in both
  directions, and is measured at 0.285 us against 5.815 to encode a six-column
  row (20.4x), 0.386 against 4.989 to decode.
- A replayed frame is byte-identical to the live one it repeats, because both
  resolve to the log's declared column order.
- `offset` is `null` on a stream with no log. Nothing assigned one, and a
  per-process counter would look exactly like a resume cursor until the server
  restarted.

### Resuming

- `?offset=` replays out of the log and switches to live with no gap and no
  duplicate at the join. The server records its frontier at the instant a
  subscriber attaches; everything below it comes from the log, everything from
  it up is already in that subscriber's queue.
- `streamcast.EARLIEST` asks for everything the log still holds.
- Five distinct refusals — `not_durable`, `empty`, `ahead`, `too_old`,
  `evicted` — because the caller's next move differs for each. A refusal
  travels as a code and some numbers and becomes a sentence at the subscriber,
  since a close frame has 123 bytes and that is not a sentence.

### Backpressure

- One queue and one task per subscriber. A consumer that stops reading fills
  its own queue and nobody else's, and is dropped at `max_backlog` rather than
  buffered without bound or served a stream with a hole in it. What it received
  is a contiguous prefix, so on a durable stream a drop costs a reconnect.

### Measured, not asserted

`benchmarks/replay.py` exists because `docs/SPEC.md` §4 was sizing `max_replay`
against ~1M rows/s and an 80,000 msg/s threshold that were guesses stated as
measurements, and both were about 2x optimistic. Real figures: ~390,000 rows/s
warm, ~0.6 s cold for the first scan in a process, ~30,000 msg/s threshold.
