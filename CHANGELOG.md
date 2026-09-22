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

A replayable WebSocket multicaster. One upstream stream in, appended to a
litelink log — an Iceberg table on disk — and broadcast to any number of
downstream subscribers. Each message carries the offset it was written at, so
a subscriber that stops can reconnect and ask for the rest.

### Serving the whole history

- **`max_replay=None` removes the bound**, so no subscribe is refused as
  `too_old`. With **`Stream.new(replay_archive=True)`** the server reads the
  archive on the subscriber's behalf, which together make it a complete
  gateway to the log: a client in any language replays the entire stream over
  a plain WebSocket, with no litelink, no Iceberg reader and no object-storage
  credentials of its own. `catch_up` exists because the default is the
  opposite; this is the setting that makes it unnecessary.

  Not the default, because a replay is served ahead of the live queue and
  `max_backlog` is what drops a subscriber that fell behind while reading it.
  Size the two together.

  `replay_archive` is a handle property rather than a `LogConfig` field —
  litelink persists a config in the log's `meta` table, so a field there would
  be durable policy shared by every process, and one caller's `set_config`
  would change another's read tier. It is litelink's `include_archive` on the
  way in; named for the replay here because `archive=` beside it already means
  "where".

### Construction

- **`Stream.new(root=…, schema=…)`** creates or opens the log; `Stream(log=…)`
  takes one already open and does no I/O. litelink's own rule — *"the
  initialiser takes already built collaborators and does no I/O, so a test can
  substitute any of them"* — applied here, with the same split it uses for
  `litelink.new`. `Sidecar.new` and `connect`'s deferred cursor read are the
  same change: constructing a `connect(...)` no longer reaches S3 before
  anything has awaited it. The split also deletes two hand-written errors —
  the bad argument combinations are now refused by the signature.

### Fixed

- **A log evicted dry is refused as `evicted`, not an error.** litelink 0.4.0
  fixes which tiers a handle reads at assembly, and a server opens its log
  local-only on purpose — serving a replay out of object storage means a long
  network read on a worker thread while the subscriber's socket sits attached,
  which is what `catch_up` exists to avoid. litelink now refuses a local-only
  read of a log whose local table has been evicted dry rather than serving the
  buffer alone, and that refusal arrives as the `evicted` it is.
- **`EARLIEST` reports what the server can actually serve.** It asked
  `coverage()`, which spans the archive, while the scan reads local files —
  so on a partially evicted log it could resolve below what the very next scan
  would return and be refused `evicted` for an offset just called the
  earliest. It asks the tiers the handle reads.

- **A catch-up whose archive does not go back far enough is refused, not
  half-served.** `Catcher.prepare` ruled out an archive that *ended* below the
  request; nothing ruled out one that *started* above it. A consumer asking
  for offset 100 against an archive floored at 500 was handed 500 first and
  told nothing — 400 rows lost, and a cursor advanced past them. It now fails
  at `connect`, naming both ends of the missing range. This is the same hole
  at the join `_replay_from` prevents on the server side.
- **The `evicted` refusal no longer promises what it cannot deliver.** It said
  the rows were "gone from the log" and to read them from the archive, which
  is contradictory: `earliest` there is the first offset the SCAN returned,
  and litelink picks the tier per scan, so the archive may or may not go back
  further. The message now says "if it still holds them", and `catch_up`
  reports the difference.

- **Closing a subscription mid-stream no longer waits out `close_timeout`.**
  `websockets` pauses its reader at `max_queue` (16), so a consumer with more
  than that buffered had already stopped reading the socket — and the server's
  Close echo is just another frame on it. `close()` waited the full 10s and
  then dropped the connection. Measured: 10.01s before, 0.00s after, and the
  close code goes from 1006 to 1000. It hit any consumer killed mid-replay,
  any `break` out of the loop, and any `async with` exited early.

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
