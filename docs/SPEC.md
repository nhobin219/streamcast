# Stream multicasting

What the system is, why each piece is the shape it is, and what must stay true.
[`API.md`](API.md) says what you can call.

Numbers marked *measured* were taken on the machine this was developed on with
`just bench`; they move with hardware and are there for their ratios.

---

## 1. Architecture

**A log-backed pub/sub broker.** One process owns the log and the fan-out;
publishers and subscribers are both its clients. A publisher is the server's
own feed handler calling `Stream.send`, or `streamcast.publish` from another
machine — either way the server is litelink's single writer, which is what
makes the offsets one sequence.

The common shape is one process holding the upstream subscription. Everything
else on the box reads from it.

**Any websocket feed.** Nothing in this section is specific to market data —
that is only the case it was built against, and the one the examples use. A
feed is a websocket that sends messages; a message is a row.

```
upstream ws feed
      │  ONE connection
      ▼
┌─────────────────────────────────────────┐
│ server process                          │
│                                         │
│   Stream.send(message)                  │
│       │                                 │
│       ├─► litelink.append   durable     │   ← before anyone sees it
│       │                                 │
│       └─► queue per subscriber          │   ← one insert each, never awaited
│             │                           │
│             ├─► pump ──► subscriber A   │
│             ├─► pump ──► subscriber B   │
│             └─► pump ──► subscriber C   │
│                                         │
│   serve() also starts, per stream:      │
│     maintainer  (5 subprocesses)  §5    │   ← nothing seals without it
│     litestream  (sidecar)               │   ← only with replicate=True, if the log ships its WAL
└─────────────────────────────────────────┘
```

Three problems, one shape:

**Subscription limits.** Websocket APIs cap connections per account, per IP,
or both — exchanges are the case this was built against, but it is not special
to them. Six consumers on a VM is six connections; one server is one.

**Divergence.** Six connections can be served six subtly different streams —
different reconnect points, different dropped frames, a rebalance that reaches
one and not another. One connection cannot, and every subscriber here receives
the *same bytes* from the *same* `encode` call.

**Recovery.** A consumer that stops has, without a log, no way back to what it
missed. With one, it has an offset.

### Scope

In: fan-out, ordering, offsets, replay, per-subscriber backpressure isolation,
and the recovery that rests on them — a consumer's cursor, and reading the
published tables when a consumer has fallen past what the server will replay.
Reading a stream's whole history from another machine (`Stream.snapshot`) is
in too, because catch-up is built on it.

Out: acknowledgements, consumer groups, topic hierarchies and wildcard
subscriptions, delivery guarantees beyond "a contiguous prefix",
authentication, and transport security. Wildcards are out for a reason worth
stating, since "broker" invites them: a subscriber resumes by offset and an
offset belongs to one log, so a subscription spanning streams would need a
cursor per stream rather than the single integer §3 rests on. The last two
belong to whatever owns the socket — `websockets` under `serve`, the host
application and its ASGI server under `streamcast.asgi` — and are passed
through rather than reimplemented ([`SECURITY.md`](../SECURITY.md)).

---

## 2. The wire

**Every frame is a WebSocket text frame of JSON.** The greeting, then
`[offset, ts, msg]` per message:

```
{"streamcast":4,"stream":"trades","end_offset":1861,"replay":[1200,1861],
 "metadata":"s3://market-data/prod/trades.metadata.json","stream_id":"5f0c…",
 "durable":true}
[1861,1790038800124001,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

**The frame's parts are different kinds of thing.** The offset and the stamp
are the server's framing; `msg` is the publisher's row, untouched — no offset
key, no timestamp key, no injected metadata, so a subscriber can log it,
forward it or append it to another stream whole (§6, where a row carrying
`streamcast_ts` as a key would be refused by the next server).

- **`offset`** is the log's `litelink_offset`, or `null` on a stream with no
  log.
- **`ts`** is `streamcast_ts`: when the server took the row, in UTC
  microseconds — the value the log stored, so a replay sends exactly what the
  live frame did (invariant 10). A stream with no log sends its send time.
  It is `null` only for a row of a log created before the column existed,
  live and replayed alike. A microsecond epoch (about 1.8e15) is well inside
  the integers a JavaScript number holds exactly.

Positional, not keys in the object. A subscriber consumes them positionally
either way — `offset, ts, msg = await sub.recv()` in Python,
`const [offset, ts, msg] = JSON.parse(frame)` in JS — so a key would be a name
in the contract that nothing reads, and injecting one would mean `msg` is not
quite the row that was published. The server's fields come first, the row
last.

There is no binary header, no length prefix and no payload kind. A message is
a row of a typed table (§5), and a row is a JSON object — there is nothing for
a header to describe. What text buys:

```
wscat ws://localhost:8765/trades?offset=0
```

A working subscriber with no client library at all, printing rows a human can
read. A consumer in another language needs a JSON parser, and, for a stream
with binary columns, the few decoding rules in "Reading a row in another
language" below. Nothing else.

**msgspec, not `json`.** Serialisation is on the hot path in both directions —
every publish encodes a row, every replayed row re-encodes one — which is what
earns a compiled dependency. Measured on a six-column trade row: **0.285 us
against 5.815 us** to encode (20.4x) and 0.386 against 4.989 to decode (12.9x).
At stdlib speed the encode would cost more than the Parquet read it rides on.
The ratio narrows as one large string column comes to dominate a frame.

**Key order comes from the log's schema, not from the caller's dict.** A live
row arrives in whatever order the publisher built it; a replayed row arrives
from Arrow in the order the scan projected. Both resolve to the declared
columns, so a replayed frame is byte-identical to the live one it repeats —
which is I6 across the one boundary where it could break, and what stops two
subscribers holding the same offset from holding different bytes.

A nullable column the caller omits becomes JSON `null`, because that is exactly
what the table stores for it and therefore exactly what a replay will send.

**`offset` is `null` on a stream with no log.** Such a stream assigns nothing,
and a per-process counter would hand a subscriber an integer that looks exactly
like a resume cursor and is not one — right until the server restarts and the
same integers mean different messages. `null` cannot be mistaken for a cursor;
arithmetic on it fails where `7 + 1` quietly succeeds.

### A subscribe is a URL

```
ws://localhost:8765/trades?offset=1200
      └── stream ──┘ └── resume ──┘
```

There is no application handshake in front of the data. The stream is the path
and the resume point is the query, so subscribing is the WebSocket open. The
price is that a refusal has to be a close code rather than a reply; the return
is the `wscat` line above.

### The greeting

It exists so that "the connection opened" means something on a stream that is
silent, which for market data outside a session is most of them. A subscriber
that receives it knows the server understood its offset, knows whether the
offsets it is about to see survive a restart, and knows what is about to be
replayed before any of it arrives.

`connect` awaits it before returning, so entering the `async with` block MEANS
the server accepted the subscribe. The alternative surfaces a refused offset as
a failure of whatever `recv` the application happened to reach first, which on a
quiet stream is minutes later and somewhere else.

**It names where the history is read.** `metadata` is the URI of the stream's
metadata file (§5) and `stream_id` the id that file records; both are null on a
stream with no log. They are all a reader on another machine needs, with its
own credentials, to read every log the stream has had — which is what
`catch_up` and `Stream.snapshot` start from. The id is checked against the
file, because a `file://` path can exist on two machines for two different
streams. Credentials are never sent.

### Reading a row in another language

**Everything a client needs to read a row is in the greeting's `schema`**: the
stream's columns as JSON Schema, in the spellings of §5 ("Binary and nested
columns"). JSON carries every value directly except binary, which has no JSON
form and travels as text. So a client in any language reads a row like this:

1. **Keep the greeting's `schema`.** It is the first frame. `null` means the
   stream has no log and declares no columns: its rows are plain JSON, with
   nothing to decode.
2. **For each data frame**, parse it as JSON into `[offset, ts, msg]`. Then **walk
   `msg` against `schema`**, one property at a time, recursing into nested
   values:

   | property schema | the value in `msg` | read it as |
   |---|---|---|
   | `type` is a list with `"null"`, e.g. `["string", "null"]` | `null`, or a value of the other type | null, or the rest of this table for the other type |
   | `"string"` with `"contentEncoding": "base16"` | hex text | bytes. RFC 4648 base16: two characters per byte. The server writes lowercase; accept either case |
   | `"string"` with `"contentEncoding": "base64"` | base64 text | bytes. RFC 4648 base64: the standard alphabet, padded |
   | … plus `"format": "bytesN"` | as above | exactly N bytes |
   | `"object"` with `properties` (a struct) | a JSON object | each named field, by its own schema. `additionalProperties: false`: no other keys |
   | `"object"` with `additionalProperties: {…}` (a map) | a JSON object | every value by that one schema. Keys are strings |
   | `"array"` with `items` | a JSON array | every element by `items` |
   | `"integer"`, `"format": "int32"` or `"int64"` | a JSON number | an integer. **An int64 can exceed 2⁵³**, e.g. a nanosecond timestamp, and a parser that reads every number as a double (JavaScript's `JSON.parse`) rounds it. Use one that keeps 64-bit integers if the column needs them |
   | `"number"`, `"format": "float"` or `"double"` | a JSON number | a float. Always finite: a stream with a log refuses NaN and ±inf, so it never sends one |
   | `"boolean"`, `"string"` | as is | as is |
   | anything this table doesn't list | as is | leave it as it arrived: a newer server may spell something this client doesn't know, and one unfamiliar column is no reason to drop a row |

3. **The offset is the frame's first element**, an integer, or `null` on a
   stream with no log (above), and **`ts` the second**, an integer, or `null`
   for a row of a log that predates it. Keys arrive in `schema`'s property order.
   Nothing depends on that, but it makes frames diffable.

**Writing is the same rules in reverse.** A remote publisher sends a binary
value as text in its column's encoding, and a map as a JSON object, never a
list of pairs. A `where=` value for a binary column is text in its encoding
too: `where={"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"}`.

A JavaScript reader, as a sketch:

```js
const hex = (s) => Uint8Array.from(s.match(/../g) ?? [], (b) => parseInt(b, 16));
const b64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

function read(schema, value) {
  if (value === null || schema == null) return value;
  const type = Array.isArray(schema.type) ? schema.type.find((t) => t !== "null") : schema.type;
  if (type === "string" && schema.contentEncoding === "base16") return hex(value);
  if (type === "string" && schema.contentEncoding === "base64") return b64(value);
  if (type === "array") return value.map((v) => read(schema.items, v));
  if (type === "object" && schema.properties)
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, read(schema.properties[k], v)]));
  if (type === "object" && schema.additionalProperties)
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, read(schema.additionalProperties, v)]));
  return value;
}

// the greeting, the first message: const schema = JSON.parse(event.data).schema;
// each message after it:            const [offset, ts, msg] = JSON.parse(event.data);
//                                   const row = read(schema, msg);
```

The Python client does exactly this: `Subscription.recv` compiles `_codec`
from the greeting's `schema` once, and decodes each frame, so a consumer gets
`bytes` from the socket just as it does from catch-up. `tests/test_node.py`
runs this reader, verbatim, in Node's built-in `WebSocket` — the API every
browser exposes — against a real server, in CI.

**Why text frames.** The payload is JSON, and a text frame is what a
browser's `WebSocket` hands straight to `JSON.parse`, what devtools display,
and what JSON feeds of this kind send; binary frames are for binary formats.
It costs nothing on the server: `Stream.send` encodes a frame ONCE as UTF-8
bytes, every subscriber shares them, and `websockets` sends them as a text
frame without re-encoding (`send(frame, text=True)`). The ASGI transport has
to hand ASGI a `str`, so it decodes once per subscriber, ~50 ns. A receiver
validates UTF-8 on a text frame — ~50 ns on a typical 87-byte frame, against
~590 ns for msgspec to parse it.

### Refusals

A close frame gives the reason 123 bytes, which is not a sentence. So a refusal
travels as compact JSON and the sentence is built at the subscriber:

```
4416  {"error":"not_replayable","why":"evicted","offset":100,"earliest":5000}
      ↓
      offset 100 is below 5000, the earliest offset this stream's log still
      serves. Reconnect with catch_up=True to read the rows between from
      the published tables if they still hold them — it will say so if not —
      or with offset=streamcast.EARLIEST to take what is left.
```

The English has exactly one home (`_errors._WHY`) and can be reworded without a
protocol change. `refusal()` trims by dropping whole fields from the end, so a
server serving three hundred streams still sends a valid 4404 — the list goes,
the code stays.

**The close CODE is what the client dispatches on; the reason only fills in the
numbers.** A reason can be trimmed or rewritten by an intermediary, and a
refusal that lost its detail is still a refusal — reporting it as a clean end
would turn a rejected subscribe into an empty stream.

| code | meaning | becomes |
|---|---|---|
| 4400 | the path or query will not parse | `ProtocolError` |
| 4404 | nothing served there | `StreamNotFound` |
| 4416 | that offset cannot be served | `NotReplayable` |
| 4429 | you fell too far behind | `TooSlow` |
| 1000/1001 | the server finished on purpose | ends the iteration |

---

## 3. Ordering, and the subscribe partition

Two atomicity claims, both load-bearing, both invisible when broken.

### I1 — commits are queued, made and delivered in one order

A durable row is committed off the event loop, on a writer thread of the
stream's own, and delivered back on the loop in commit order. Two concurrent
senders cannot produce a subscriber that sees offset 8 before offset 7:

```python
# on the loop — _commit: no await
check(stored_row)                        # what append checks, before queueing
writer.submit(Job(rows, stored, now, done))   # FIFO: queue order is call order

# on the stream's writer thread
offsets = log.extend(rows)               # one transaction; durable on return
loop.call_soon_threadsafe(deliver, jobs, offsets)   # FIFO: commit order

# on the loop — _deliver: no await
self._end_offset = offsets[-1] + 1       # the frontier, then …
self._fan_out(encode(offset, ts, row))   # … put_nowait per subscriber
```

Both loop-side steps contain no `await`, and `tests/test_invariants.py` reads
the AST to keep it so: checking and queueing are one step, so rows are queued
in the order `send` was called; one thread commits them first in, first out;
and `call_soon_threadsafe` runs one thread's callbacks in the order it
scheduled them, so `_deliver` sees commits in commit order and advances the
frontier and fans out in one step. A stream with no log has nothing to commit
and does the delivery inside `send`.

**Why the commit is off the loop.** Every stream a broker serves shares one
event loop. A commit on it — a SQLite transaction and fsync at
`synchronous=FULL`, ~2 ms — stalled every other stream for its length, and a
publisher sending in a loop never yielded at all: measured, a broker's event
loop did not run for the whole 3 s of a flat-out publisher. On the writer
thread SQLite releases the GIL while it waits on the disk, so streams commit in
parallel and the loop stays responsive.

**Sends that queue behind a commit share the next one, by default.** Under
concurrent publishers, sends that queued while a commit was in flight share the
next transaction, each one's rows still adjacent and in queue order, every row
still durable before its send returns and before any subscriber sees it.
Measured 4,232 rows/s from 8 concurrent publishers against ~1,300 one commit
at a time. A lone publisher never waits for a group to form: groups are only
what queued anyway. What it gives up is that a commit's failure — the disk,
SQLite itself — fails every send in the group, which would fail the next send
regardless. A stream created with `group_commit=False` commits each `send` and
each `send_many` in a transaction of its own. The greeting's `group_commit`
says which guarantee a stream makes. Rows are checked on the
loop before they are queued, so one bad row is refused alone rather than
failing a group.

**On a stream with no log, a publish loop that never awaits starves every
subscriber.** `for … : await stream.send(…)` over a list in memory runs to
completion before any pump gets the loop back, so every subscriber sees the run
arrive at once — and a run longer than `max_backlog` drops all of them. A real
publisher awaits its upstream between messages and never meets this; a backfill
from memory uses `send_many`, or yields.

### I2 — attaching reads the frontier with nothing in between

```python
self._subscribers.add(subscriber)     # from here on, nothing is missed
frontier = self._end_offset           # below here, everything is durable
```

Every offset below `frontier` is already committed to the log — because `send`
appends *before* it bumps the counter, and I1 says nothing can observe the
between-state. Every offset from `frontier` up is already in this subscriber's
queue, because it joined the set first.

**The two sets partition the stream exactly.** Nothing is in both; nothing is in
neither. That is what makes a resume exactly-once rather than best-effort, and
it holds while a publisher is running, which is the only case that matters.

```
       replayed from the log          live, from this subscriber's queue
   ├───────────────────────────────┤├──────────────────────────────────►
requested                      frontier
```

Both invariants are checked out of the AST by `tests/test_invariants.py`,
because both compile fine when broken and fail only under a race nobody will
reproduce on purpose.

### I3 — durable before delivered

A message a subscriber has seen is always a message the log holds; never the
other way round. A server that dies between the two has published nothing it
cannot replay, which is what makes recovery a replay rather than a
reconciliation.

This is `send`'s statement order and nothing else. An implementation that
broadcast first and appended after would be faster and would make every
`TooSlow` recovery a guess.

---

## 4. Backpressure

**The obvious broadcast is the bug.** `for s in subscribers: await s.send(frame)`
makes the slowest consumer the rate of the whole stream, because every other
subscriber's frame is queued behind an `await` on a TCP window that is not
opening. No amount of tuning fixes it; the broadcast must not await a consumer
at all.

So: one `asyncio.Queue` and one task per subscriber, and `offer` is synchronous,
never blocks, never raises.

*Measured*: 500 sends of 64 KB with a subscriber that had stopped reading, under
2 s total — the stalled socket is not on the publish path at all.

### What a full queue means

There is no free answer, and the one taken is to **drop the subscriber**.

| | leaves | |
|---|---|---|
| drop oldest | a hole in the middle, unmarked | offsets still increase across it, so nothing can see it |
| grow unbounded | the server's memory set by its worst consumer | the OOM this design exists to avoid |
| **drop the subscriber** | **a contiguous prefix** | reconnect at `last + 1` and the log fills the gap |

Only the third is compatible with "every client receives the same data", which
is §1's second problem. With a log it is not even data loss; without one it is,
said out loud with a 4429 rather than discovered later.

The sentinel travels **in** the queue rather than beside it, in a slot reserved
by making the queue `max_backlog + 1` deep. A flag would only be read between
`queue.get()` and `connection.send()`, where the pump is not; and evicting a
message to make room for the sentinel punches exactly the hole the whole policy
is avoiding.

### Sizing

`max_backlog` is counted in **messages**, not bytes, because that is what the
queue holds — a pointer to a frame every subscriber shares. The frame is encoded
once (*measured*: 0.285 us for a six-column row with msgspec, independent of
subscriber count) and the per subscriber cost is the insert.

**`max_backlog` and `max_replay` are sized against each other**, not
independently. A replay streams while live messages queue behind it, so a
subscriber that takes longer to catch up than `max_backlog` messages of live
traffic is dropped the moment it arrives, having done all the work.

*Measured* (§5): a replay runs at ~390,000 rows/s warm, so `max_replay` of
100,000 is **~0.27 s**, and a feed above **~30,000 messages/s** would put 8,192
messages in the queue while it ran. The first replay in a process also pays
~0.6 s of cold cost — extension loading and Iceberg metadata — which drops that
threshold to ~13,000 messages/s for that one subscriber. `just bench-replay`
prints both, and the arithmetic, for your own shape and hardware.

Every number here comes from `benchmarks/replay.py`. Raise one of the two
settings and check the other.

### The inbound side: `max_inbound`

A slow consumer is dropped; a slow DISK cannot be. Every publisher on a stream
feeds one writer thread, and if commits fall behind what arrives, rows queue
in memory — with nothing bounding them, until the broker is out of it.
`serve(max_inbound=)` (65,536 rows by default) bounds that queue per stream, across
all its publishers, and at the bound a sender **waits** rather than being
refused: a local `send` waits for room, and a publisher connection's reader
stops reading its socket, so TCP holds the remote publisher back and its
`submit` waits on its window. The check and the queueing are adjacent
statements with no `await` between them — `tests/test_invariants.py` holds
them so — or two senders could take the same room. A batch larger than the
bound is let in when the queue is empty, so it waits rather than deadlocks.

Per connection the bound is `serve(max_in_flight=)`, 64 frames: the replies
one publisher may be owed. Each frame is at most `websockets`' `max_size`, so
a connection's share of memory is bounded too, and `max_inbound` caps what
any number of connections add up to.

The broker's three bounds are `serve`'s keywords, not the `Stream`'s: they
are the serving process's settings, and change with a restart rather than
with the stream. Each is one int for every stream or a map naming every
stream served, checked in full before any stream is set.

### What bounds memory, at each end

Neither end can be run out of memory by the other, or by a stall:

| where | what | bound | at the bound |
|---|---|---|---|
| broker | a subscriber's outbound queue | `serve(max_backlog=)`, 8,192 frames | the subscriber is dropped (`TooSlow`) |
| broker | a stream's rows queued for commit | `serve(max_inbound=)`, 65,536 rows | sends wait; publishers are held at the socket |
| broker | replies owed one publisher connection | `serve(max_in_flight=)`, 64 frames | the connection stops being read |
| both | a connection's received frames | `websockets`' `max_queue` (16) × `max_size` (1 MiB) | the connection stops being read |
| broker | a replay | one batch at a time, into the bounded subscriber queue | — |
| client | a publisher's unanswered sends | `publish(max_in_flight=)`, 64 frames | `submit` waits |
| client | a catch-up | one batch at a time | — |
| client | a snapshot's rows from the broker | `max_tail`, 1,000,000 rows | the snapshot is refused |
| client | a live view's unpublished rows | `max_tail`, 1,000,000 rows | the view stops; its next query raises |

### A subscriber that walks away

A disconnect is noticed by `send` raising — but only if there is something to
send. On a quiet stream the pump is parked in `queue.get()` and nothing wakes
it: the task lives for ever and the `Subscriber` stays in the fan-out set.

So the pump races the connection's own closed future. Two tasks per
*subscriber*, created once at attach — not two per message, which is what racing
inside the pump's loop would have cost.

This was a real defect, found by the first end-to-end test. It leaks one task
and one set entry per disconnect and is invisible until the box runs out of
something.

---

## 5. The durable tier

**The schema is the caller's, per stream.** The log is an ordinary litelink
table with whatever shape the application gave it, which is litelink's own model — *"the library owns exactly one column,
`litelink_offset`; everything else is the caller's schema"* — and the whole
reason to put litelink underneath this rather than an append-only file.

```python
SCHEMA = pa.schema([
    pa.field("event_ts", pa.int64(), nullable=False),
    pa.field("price", pa.float64()),
    pa.field("amount", pa.float64()),
    pa.field("side", pa.int64()),
])
log = litelink.new("data", "trades", schema=SCHEMA, sort_by=("event_ts",))
await stream.send({"event_ts": …, "price": …, "amount": …, "side": …})
```

### Why a typed table, not the frame whole

The alternative shape is a fixed three-column schema — a receive timestamp, a
kind, and the upstream frame verbatim in a string column. litelink's own
websocket example rules it out in as many words: *"Every field the feed sends
that is worth a column. §7 prunes on Iceberg statistics, so a query for one
minute of trades never reads the rest — **which is the reason to declare a
schema rather than store the frame whole**."*

A blob column gives up every property the table exists for:

| | with a blob column | with real columns |
|---|---|---|
| **pruning** | nothing to prune on; a one-minute query reads every byte in range | statistics per column (§7) |
| **compression** | JSON text, poorly | float64 against its neighbours |
| **the published table** | one string per row; parse JSON in SQL to ask anything | a table any Iceberg engine reads |
| **the replay** | strings out of Arrow, re-encoded per row | columns, already typed |
| **the subscriber** | a blob to parse, once per consumer | the row, parsed once at the publisher |

`send` takes a row and the caller fills it, so there is no column the library
would have to invent a value for.

**The consequence is that a frame which is not a row has nowhere to go.**
Subscription acks, heartbeats and reconnect notices are dropped by the feed
handler. That is the same division of labour a kdb tickerplant has — the feed
handler parses, the plant stores typed rows — and it forces the decision to be
made once, by the publisher, instead of independently by every consumer.

### The system columns

**`_log.SYSTEM` defines them, as JSON Schema, in one place.** It is what a new
log is created with, which names a declaration may not use, which columns the
wire and the greeting leave out, and whether a log is current enough that
`Stream.migrate` leaves it alone. Each log's entry in `metadata.json` records
its `system_schema` beside its user `schema`, because the two differ between
logs: one from before a system column existed lacks it.

**A system column's type never changes**, for the same reason a user column's
does not: the logs are read together with `UNION ALL BY NAME`. One that needs
a different type is a new name beside the old, `streamcast_ts_v2`. A test pins
every existing entry.

Today there is one, `streamcast_ts`: int64 microseconds since the epoch,
stamped by the server at append, beside the application's columns and
litelink's `litelink_offset`. It answers "when did this server have it", which no
application column carries — a row's own timestamps are the publisher's — and
`streamcast_ts - event_ts` is feed latency per row, over the whole history.

It is owned on exactly the terms litelink owns its offset:

* **On the wire as framing, never as a key.** It is element 1 of every frame
  (§2), beside the row as the offset is. `_log.columns` leaves it out of the
  row, and that tuple fixes the key order of every frame, live and replayed —
  so `msg` stays exactly the publisher's row, and invariant 10 holds with the
  stamp read back from the log on replay.
* **Never in the greeting's `schema`**, which is filtered by the same rule.
  Each log's `system_schema` in the metadata file names it instead.
* **The server's to fill.** A row that carries it is refused rather than
  overwritten, and a declaration that names it is refused at `Stream.new`.

**This is not owning the shape, and the line is worth drawing precisely.** The
design this section argues against owned the ROW — a fixed schema with the
frame stored whole — and so gave up pruning, compression and a readable
published table. One scalar beside the application's own columns gives up none of
those: every declared column is still a real column.

The rest is decided, not incidental:

* **One value per commit.** `send_many` is one transaction; its rows share a
  stamp, because distinct values would claim an order in time the commit does
  not have.
* **Wall clock**, because a stored time has to mean something on another
  machine. A clock step on the server shows in it; it is monotonic in offset
  only while the server's clock is.
* **An int64 epoch, in microseconds.** That is how litelink stores every
  timestamp — the same value in the table, in a JSON frame and in a
  subtraction, with no conversion between them — and microseconds is the unit
  `event_ts` already uses.
* **Per log, not per server.** A log created before the column existed opens
  unchanged and is never stamped — adding a column is not a side effect an
  `open` should have (litelink#29). A handle passed as `Stream(log=)` is
  stamped only if its own schema has the column. The metadata file's
  `system_schema` says which.

### The counter

`Stream.end_offset` is read from the log **once**, at construction, and
maintained by `send` thereafter — `append` returns the offset it assigned, so
asking the log per message would be a round trip for a number the previous call
already returned. It cannot drift, because litelink allows exactly one writer.

A server restarted against an existing log continues its offsets. It must: a
restart that reset them would hand the same integers to different data, and
every consumer cursor in the system would silently point somewhere else.

### The replay

```python
reader = await asyncio.to_thread(log.scan, columns=…, start_offset=…, end_offset=…)
while (batch := await asyncio.to_thread(_next_batch, reader)) is not None:
    ...
```

**Every blocking call is in a thread, and that is not an optimisation.** On the
event loop a replay is the whole server stopped — no live message fanned out, no
other subscriber served, no keepalive answered. litelink is built for this: its
buffer and reader each hold their own lock and its SQLite connections are opened
`check_same_thread=False`.

Batches rather than rows, because that is the unit litelink hands back and the
unit a thread hop should cost.

**Measured, and the shape matters more than the number.** A replay is dominated
by DuckDB reading Parquet, not by anything this library does:

| | |
|---|---|
| fixed, per scan | ~11 ms warm; **~0.6 s cold** for the first scan in a process |
| marginal | **~2.5 us/row** (≈390,000 rows/s) |
| where it goes | ~48% DuckDB, ~11% Arrow→Python, ~41% encode |

The cold figure is extension loading and Iceberg metadata resolution, and it is
paid by the first subscriber to resume after a server starts.

**The split moved, and the reason is worth recording.** Under the old blob
schema the same profile read 91% DuckDB and 1% encode — the scan was dragging a
long string column off disk, and the "encode" was `struct.pack` over bytes that
were already bytes. Typed columns make the read far cheaper and give the
encoder real work, so the two came into balance.

The dicts the encoder receives are built by Arrow, not by Python. `scan`
projects each batch into `(litelink_offset, *columns)` order, so
`batch.to_pylist()` builds them in C — 1.55 us a row against 2.38 for
rebuilding them in Python, for identical bytes. `_log.rows` checks the batch's
column order against what it projected before relying on it, once per batch:
the saving holds only while that does, and a silent reordering would break I6.

**Which tiers a replay reads was decided when the log was opened.** litelink
passes `published=` per read, and a server reads staging and the buffer only
unless `Stream.new(replay_published=True)` says otherwise — serving a replay
out of object storage is a long network read held on a worker thread while
the subscriber's socket sits attached, which is the failure `catch_up` avoids
by doing the same read client-side with nothing connected. A replay that
reaches below what staging still holds is refused `evicted` rather than
served from wherever staging starts.

### The schema, in JSON

**A stream's columns are declared in JSON Schema and converted here**, not in
litelink. That is a division of labour rather than a convenience: litelink
speaks Arrow and is deliberately general about what it stores, while
streamcast is specifically about JSON websockets — so the layer mapping one
onto the other sits on the side that knows about JSON. Putting it in litelink
was considered and rejected; it would make a JSON codec part of the public
surface of a library whose value is being general.

What it buys is an import list of one. `Stream.new(name, root=…, schema=…)` creates
the log, `serve` maintains it (and replicates it, with `replicate=True`), and a caller reaches for neither
litelink nor pyarrow.

**`format` carries the width, because JSON Schema does not.** `integer` does
not choose between int32 and int64 and `number` does not choose between
float32 and float64, so the format names JSON Schema already reserves —
`int32`, `int64`, `float`, `double` — do it. Omitted, the WIDER of each pair
wins: a feed that overflows an int32 is a silent wrong answer, while one that
would have fitted costs four bytes a row.

| JSON | format | Arrow |
|---|---|---|
| `boolean` | — | `bool` |
| `integer` | — / `int64` / `int32` | `int64` / `int64` / `int32` |
| `number` | — / `double` / `float` | `float64` / `float64` / `float32` |
| `string` | — | `string` |

**`required` is about presence; `"null"` in a type is about the value.**
Different rules, verified against a real validator rather than a reading of
the spec — a null is rejected by `type`, an absence by `required`. Arrow has
two states and no "absent", since a row omitting a column stores NULL, so:

    nullable = (not in `required`) or ("null" in its type)

Three of the four combinations map exactly. The fourth — optional with a
non-null type — means "may be absent, but never null when present", which a
stream cannot express, and is **refused rather than widened**: accepting it
would make streamcast take rows its own declared schema rejects. The rule that
leaves is that every property is either required with a plain type, or nullable
through its type, and every schema published is one that would be accepted.

**It refuses up front what litelink would refuse at the first append**:
`date-time`, the narrow and unsigned integer widths Iceberg widens or can't
represent, and unions. The message can then name JSON Schema's vocabulary
rather than Arrow's.

### Binary and nested columns

| JSON Schema | Arrow |
|---|---|
| `object` + `properties` | `struct`, whose fields follow every rule above |
| `object` + `additionalProperties: {…}` | `map<string, …>` (JSON has only string keys) |
| `array` + `items` | `list` |
| `string` + `contentEncoding: "base16"/"base64"` | `binary` |
| … + `format: "bytesN"` | `fixed_size_binary(N)` |

**JSON has no bytes, so a binary column says how it is written.** `base16`
(hex) is what OTLP/JSON uses for trace and span ids and what every trace tool
shows. `base64` is JSON Schema's convention, and a third smaller. The choice
is per column, and it is kept in the Arrow field's metadata
(`streamcast.encoding`), which litelink preserves at every depth, so a
reopened log still knows it. `_codec` does the converting, compiled once per
stream, and a stream of scalars pays nothing:

- **Out:** msgspec writes bytes as base64 by itself. A `base16` column is
  converted to hex before the frame is encoded, on the live path and on replay
  alike.
- **In:** a remote publisher can only send text, and litelink refuses a `str`
  for a binary column. So every binary value, at any depth, is decoded with its
  column's encoding before it is validated or stored. The client decodes what
  it receives the same way, so a consumer gets `bytes` whether a row came off
  the socket or out of the published tables by catch-up.

**A map has to arrive as a dict, and the reason is invariant 10.** Arrow hands
a replayed map back as a list of pairs, which would encode as `[["k","v"]]`
against the live frame's `{"k":"v"}`. So `_log.rows` reads maps as dicts
(`maps_as_pydicts="strict"`, which raises on a duplicate key rather than
keeping the last one). And `send` refuses a map value that is not a dict,
**before** the append: litelink would store a list of pairs, and it would then
replay as a different frame than was sent live.

**`where=` compares scalar and binary columns.** A binary filter value is text
in the column's encoding, decoded once at subscribe, so
`{"trace_id": "4bf92f35…"}` works. A struct, list or map column is refused:
equality on whole nested values is not a filter anyone means.

**The greeting publishes it**, with the widths stated explicitly, so a
subscriber in another language reads the columns without this repo and gets
back exactly the types the columns are.

One caveat the mapping cannot fix, only document: **JSON integers beyond 2^53
do not survive every parser.** Python and msgspec carry int64 exactly; a
JavaScript subscriber silently rounds. A nanosecond `event_ts` is past it,
and a microsecond one — which the examples use — is not.

### An existing log is checked, not adopted

`Stream.new(root=…, schema=…)` opens a log that is already there, and litelink's
`open` takes none of the shape: it reads it from disk. So a declaration that
disagreed would be silently ignored and every send validated against columns
the caller never wrote down. It is compared and refused instead, which is the
one failure this convenience would otherwise introduce.

To change the schema, a stream is **migrated** instead.

### Migration: a stream becomes a sequence of logs

litelink fixes a log's shape at creation, so a new schema means a new log.
`Stream.migrate` makes the stream a sequence of them, with the server
stopped:

1. **The current log is retired** (litelink's `retire()`): every buffered row
   is sealed, the whole log is published — including the trailing run a plain
   `publish()` holds back for compaction that will now never come — staging is
   evicted, and the log refuses every writer from then on. A log with
   `wal_replication` needs its sidecar running to retire, so `migrate` starts
   one for the call, under the same `flock` the server's takes.
2. **The next log is created**, `trades-v2` and so on, starting at exactly the
   old log's `end_offset`.
3. **The metadata records both**, the new one live, at
   `root/<stream>.metadata.json` and, when the logs publish to S3,
   `<published>/<stream>.metadata.json`.

**Offline, so dense.** A live rotation would have to create the next log
(100–300 ms, measured) while `send` kept writing the old one, and so could not
know where the next should start without fencing a gap. Nothing is migrated
live. A migration is a deploy, because publishers have to change shape at the
same time. With nothing sending, the seam is exact and the offsets are one
sequence with no gap.

**The metadata is written last, by atomic rename**, so a crash never leaves it
naming a log that does not exist. A crash before it leaves an orphan log that
the metadata doesn't name. The next `migrate` adopts the orphan if it is empty,
starts at the seam and has the requested shape. Otherwise it refuses, because
adopting a log holding rows the metadata cannot account for is not a decision to
make silently. A crash after the retire is resumable too: the retired log is
opened read-only on the rerun, and the migration carries on from there.

### The metadata file

```json
{"streamcast_metadata": 2, "stream": "trades", "stream_id": "6f1c…",
 "sealed_logs": [{"name": "trades", "published": "s3://market-data/prod",
                  "start_offset": 1, "end_offset": 1001,
                  "start_ts": 1790038800123456, "end_ts": 1790042400654321,
                  "schema": {…}, "system_schema": {…}}],
 "live_log": {"name": "trades-v2", "published": "s3://market-data/prod",
              "start_offset": 1001, "start_ts": 1790042411000000,
              "schema": {…}, "system_schema": {…}},
 "manifest": "trades.manifest.parquet"}
```

**Every durable stream has one, written by `serve`.** Before it listens,
`serve` writes the file for any stream that has none, and when the logs
publish to S3 compares it with the copy there and uploads it if they differ. **A failure
is a failure to start.** The file is what a reader on another machine starts
from, so a stream without one can only be read through its own server, and
that is better found out at deploy than at the first remote read. It is
`serve`'s job rather than `Stream.new`'s so that a `Stream(log=…)` gets one
too: an initialiser does no I/O. A pre-0.9 log gains its file at its first
`serve` and nothing about the log changes.

- **`sealed_logs` and `live_log` are separate keys.** Only sealed logs have
  statistics (#27), so only they can be pruned, and the structure says so.
- **`stream_id` is minted once**, when the file is first written, as
  Iceberg's `table-uuid` is. A `file://` path can exist on two machines for
  two different streams, and the id is how a reader tells them apart.
- **Each log says where it is read from.** `published` is the prefix its
  table sits under, at `<published>/<name>` — every log publishes, to
  `file://<root>/<name>/published` when nothing else is configured. A reader
  opens each table at the snapshot its `version-hint.text` names.
- **Each log carries its `streamcast_ts` range**, `start_ts` and, once
  sealed, `end_ts`, so a read as of a time skips logs that begin after it
  without opening them. Null on a log without the column, and on a live log
  that has no rows yet.
- **`manifest` points to the sealed logs' statistics**, and is null until
  there is a sealed log to describe.
- **Version 1 is still read.** A version-1 file has no `published` or
  timestamps; `serve` fills them in from the logs on this disk and rewrites
  it as version 2.
- **The live log it names is the only one `serve` will serve.** A
  `Stream(log=…)` handed a sealed log is refused at start, because serving
  it would write to a log the file says is finished. Handed the live one, it
  reads its seam and its retired logs from the file at `serve`.

**A missing bucket is not a missing file.** pyarrow reports both as "not
found", and a restore that read a mistyped bucket as "no copy" would rebuild
the stream's first log as though it were live. So the bucket is checked.

**Idempotent**, so `Stream.migrate(...)` can sit in a server's startup: a
current log that already has the requested schema and every system column is
opened, not migrated. Migrating with an UNCHANGED schema is therefore the
upgrade onto today's system columns — how a log from before `streamcast_ts`
gains it, and how one will gain any column a later release adds.

**The type rule.** A stream's logs are read together with `UNION ALL BY NAME`.
There a changed type fails nothing, which is the problem. Measured in DuckDB:

| one log | the next | union |
|---|---|---|
| `int32` | `int64` | `BIGINT`, exact |
| `int64` | `float64` | `DOUBLE`, loses integers past 2^53 |
| `int64` | `string` | `VARCHAR`, a different column under the old name |

So **a column's type is fixed for the life of the stream**, with no exception
for widening: one rule a reader never has to look up. It covers removed
columns too. The metadata records every log's schema, and a re-added name must
take the type it had. Adding columns, removing them and changing nullability
are free.

**There is no rename at the storage layer.** A rename is an application
concept. Here it is a removal and an addition, which is exactly what is
stored: nothing is backfilled and no two columns are merged. A read across the
seam returns both, each null in the logs that did not have it, and treating
them as one is the application's job on the table it reads back. The type
rule is what keeps that honest: a name can only ever mean one type, so a
column that comes back is the same column.

**This server replays only the current log.** A subscribe below the seam is
refused `evicted`, naming where the current log starts. It is refused because
the scan cannot see the problem: the current log holds nothing below its
start, so a replay of the range would come back empty and read as "nothing
outstanding". That is the hole at the join invariant 4 forbids. A consumer
that was caught up when the server stopped resumes exactly at the seam and
loses nothing. Reading across the seam belongs to `Stream.snapshot`, not to
the server's replay. `EARLIEST` on a freshly migrated stream is where the
current log begins.

Retired logs stay on disk and in their published tables. `serve`'s maintainer leaves
them alone: `retire()` published, evicted and swept them completely, and litelink
refuses them a writer, so there is nothing left to maintain.
litestream replicates only the current log, since nothing writes to a retired
one.

**Not built: rollback.** Undoing a migration would be an update to the metadata file that
makes the previous log current again. That log would also have to be fenced
above everything the abandoned one issued, the way `restore` fences, so that no
offset is reused.

### The manifest: skipping whole logs

Iceberg's manifests hold per-file bounds and prune files within one table.
Nothing in Iceberg says which *tables* a query can skip, and a migrated stream
is several. `<stream>.manifest.parquet` is that summary, one level up: one row
per **sealed** log, with a struct per column (`min`, `max`, `null_count`,
`value_count`, and `nan_count` for floats), rolled up from the log's own
Iceberg statistics (litelink#85). The live log has no row and is never pruned.

**`Stream.migrate` writes it.** After the old log is retired,
and before the next log exists, `migrate` reads the retired log's
`column_statistics()` across every tier. A failure at that point leaves the
stream exactly as it was. The retired log's row is added to the manifest
(replacing any earlier row of the same name, so a retried migration does
not duplicate it), and the manifest is saved and published **before**
`metadata.json`, which is the commit and then points to it by its name,
relative to itself. If writing the manifest fails, nothing is committed: the
next `migrate` adopts the empty log the failed one created, and writes both.

**Pruning fails towards include, in every rule.** Including a log with no
match is a wasted scan. Excluding one that holds a match is a wrong answer
with no symptom. So:

- **A predicate is rewritten against the bounds**, never evaluated against a
  statistics row. `x > v` becomes "`max > v`", and `x == v` becomes
  "`min <= v <= max`". Terms are ANDed. Anything else cannot decide: an
  operator outside `==, <, <=, >, >=, in`, a column not in the manifest, a
  `None` or NaN literal, or a value the column's type does not compare with.
- **Missing statistics never prune.** That covers a sealed log with no row, a
  column the log lacks, an all-null column (no bounds), and a count the log
  did not record.
- **Floats are finite, and a NaN count that says otherwise stops pruning.**
  litelink refuses NaN and ±inf on every write path (litelink#87), so its
  `column_statistics()` (litelink#85) reports a NaN count of 0 and every
  float column is prunable. The rule for a count that is non-zero or
  *unknown* (which is what pyiceberg records by itself) stays as a defence.
  It costs nothing, and without it the bounds would be unsafe: they exclude
  NaN, and DuckDB sorts NaN above every float. Measured on 1.5.5, a file
  holding `[10.0, NaN]` returns the NaN for `x > 5` and not for `x > 50`,
  through `iceberg_scan`, `read_parquet` and litelink's `sql` alike. That
  layout-dependent answer is why litelink bans NaN rather than stores it.
- **Only numeric and boolean columns.** Iceberg truncates string bounds, and
  binary and nested columns have no useful order.

**It is evaluated in Python, not as a PyArrow expression.** A vectorised
filter drops rows whose expression is NULL, and NULL here means "no
statistics". The natural default of the filter would be to exclude exactly
the logs it knows nothing about. The manifest has one row per sealed log, so
there is nothing to vectorise.

**The reader's `sealed_logs` are the authority.** A manifest row for a log the
reader's `metadata.json` does not list is ignored, so a migration landing
between the two reads adds nothing.

`tests/test_manifest.py` checks every exclusion against DuckDB, using a
native table as the most inclusive answer, over generated logs: NULLs,
all-null columns, logs missing a column, NaN and infinity (on purpose, since
no real log holds one and a defence nothing exercises can break unnoticed),
and predicates over
several terms.

### The five refusals

| `why` | when | the caller's next move |
|---|---|---|
| `not_durable` | no log attached | drop `offset=`, or give the server a log |
| `empty` | the log holds nothing yet | subscribe live |
| `ahead` | above the frontier | the server was restored or rebuilt; investigate |
| `too_old` | further back than `max_replay` | `catch_up=True`, or read the log directly |
| `evicted` | below what the scan's tier holds | `catch_up=True` if the published tables go back further, else accept the gap |

Five rather than one, because the move differs for each and collapsing them made
every one of them a guess.

`evicted` is the only one that cannot be decided before the scan opens, so
`_replay_from` pulls the first row and compares it to the request. Serving from
wherever the log happens to start would give a stream that silently begins above
where it asked — a hole at the join, which is the one wrong answer a resume must
never give.

**An empty scan below the frontier is not always a hole.** A restore fences
2**20 offsets that were never issued, so a replay inside the fence reads
nothing and there is nothing to refuse. Rows evicted from staging read nothing
too. The published tier tells them apart: if it holds rows from the requested
offset on, the rows exist and staging has dropped them, so the replay is
refused `evicted`; otherwise the range was never written and the replay is
empty.

**`earliest` reads the tiers the replay reads.** `coverage()` reports three —
published, staging and buffer — and a server that replays from staging and the
buffer reports the lowest of those two, so `EARLIEST` is never a promise the
scan cannot keep.

### Reading a stream's history: `Stream.snapshot`

A stream is a sequence of logs, each publishing an ordinary Iceberg table, and
its metadata file says which, in order, where each is published, and the
offsets and `streamcast_ts` range each holds. `Stream.snapshot(metadata_uri)`
reads that file and then the tables, on the reader's machine with the reader's
credentials. The tables are read through DuckDB's `iceberg_scan`, each pinned
to the snapshot its `version-hint.text` named when it was opened, and the logs
are read as one table with `UNION ALL BY NAME`.

**A fixed point, named at most one way.** Nothing: everything published.
`as_of_offset=N`: every row up to and including `N`, with the rows above the
published end read from `broker=` and refused without one (`LATEST` is the
broker's frontier). `as_of_ts=T`: every row stamped at or before `T`, from
published rows only, since the broker is consulted as of an offset and not a
time; a `T` past what the live log has published is refused rather than
answered short. A
server clock step can put two rows out of order by stamp while their offsets
stay monotonic, and that is the one way `as_of_ts` is not exact.

**Correct or it raises.** A missing metadata file, a file whose `stream_id`
is not the one the greeting named, and a range neither the tables nor the
broker holds all raise `SnapshotUnavailable`, the last with both numbers. So
does **a retired log whose table holds fewer rows than the manifest's
`record_count`**, the whole log's count read at retirement. A short table
reads as a smaller log, and offsets are not dense across a restore fence, so
nothing downstream could tell missing rows from fenced ones; the count can.

**The heavy work is the reader's.** Pruning, the table reads and the query
run on the reader; the broker's cost is one greeting and, when asked, a
bounded tail. `filters=` terms and offset bounds, on `scan` and `sql` alike,
are pruned against the manifest first, so a retired log they exclude is never
opened, and are applied to the rows as well, so the answer does not depend on
what was pruned. SQL is not mined for terms: a misread predicate would drop a
log that held matches and answer short, so that waits on a sound extractor
(#57).

**The caller's conditions apply over the union, not inside each log.** A log
from before a migration added a column has no such column, so a condition on
it inside that log's read would not bind. Over the union the column is NULL
there, as in any other read. The snapshot's own limits (the offsets, and the
`as_of_ts` bound on logs that carry the stamp) stay inside, where they always
bind and push down.

### Keeping it current: `Stream.live`

A `Live` is a snapshot kept moving: the published tables as a base, the
broker's rows appended in memory as they arrive, and every query answered as
of the newest row received. It is built entirely from what exists, and takes
only the broker's address: the greeting names the metadata file and the
stream's id, read again at every reconnect, because a live view wants the
stream as the broker serves it now. Opening it is one connection for the
greeting, a published snapshot of that file, and a subscription at the
snapshot's `end_offset` with `catch_up=True` — the join catch-up already makes without a gap or a
duplicate. A query freezes the tail at the newest row and runs the snapshot's
own read, with the tail as one more piece, so `scan` and `sql` mean the same
thing on both.

**Memory is the publish lag, not the stream's age.** Every `rebase_every`
seconds, and after every reconnect, a fresh published snapshot replaces the
base and the tail rows it now covers are dropped. A row that arrives below the
base's end, or below the newest row received, is dropped on arrival, so
nothing is counted twice. Rebasing after a reconnect is what picks up a
migration: a restart is when one happens, and its new log is in the fresh
metadata.

**Reading never waits on a query.** The receiving task only appends dicts;
they become Arrow when a query asks. Queries run in threads, so a slow
aggregate stalls neither the loop nor the socket, and the server never drops
the view for falling behind because of one (invariant 2).

**Narrowing applies on both sides of the join.** `where=` goes to the
subscription, so the server sends only matching rows, and the same terms go to
the published base as `filters=`, which also skips retired logs that can't
match. One view has to see one stream, so only terms both sides read the same
way are taken: equality and membership over non-null scalars on scalar
columns. `None` is "is null" to the subscription's Python comparison and
matches nothing as SQL `= NULL`; a binary column is text in its encoding on
the wire and bytes in the table. Both are refused at open.

**A start offset bounds what a view sees, not what it holds.** `start_offset=`
is the floor of every query, and `LATEST` is the broker's frontier at open. A
view from now still drops rows as they are published and reads them from the
tables above its start, so its memory is the publish lag like any other
view's: there is no need for a row-count or time-window bound.

**Waiting for a time is proven by a later row.** `wait_for(ts=T)` means every
row stamped at or before T is visible. A view knows that only once it holds a
row stamped after T: rows arrive in stamp order (while the server's clock is
monotonic), so a later one proves nothing earlier is still coming. On a quiet
stream that row may be long in coming, and the wait lasts until it does; the
caller bounds it. Published rows count, so a time the tables have already
passed needs no new row. A server that sent its clock periodically would let
an idle stream prove time passing without a row (#63).

**A broken view raises.** A closed connection, `TooSlow` or a network error
reconnects from the last row received, with catch-up, under a capped backoff.
Anything else is kept and raised by the next query, so the view never answers
from data it has stopped receiving.

### Catching up from the published tables

`too_old` and `evicted` are the two refusals that mean *the rows exist, just not
here*. `connect(catch_up=True)` is the client reading them out of the stream's
published tables itself, so recovering a consumer that has been down a long
time is a flag rather than an orchestration problem.

The rule that shapes it: **nothing is connected while the tables are read.**
The obvious design opens the socket at the published end first, which closes
the gap by construction — and makes the server queue for a subscriber that will
not read a message until it has pulled millions of rows out of object storage.
`max_backlog` is 8,192, so it is dropped with `TooSlow` before the catch-up
finishes: a recovery that guarantees its own failure on exactly the consumers
that need it. *Measured* with `max_backlog=16`, where the first shape died
immediately and this one caught up 20,000 rows.

**It is built on `Stream.snapshot`.** Each round takes a snapshot of everything
published, streams its rows from the consumer's offset, and connects at the
snapshot's `end_offset` — or at the last row plus one, if that is higher. The
snapshot reads every log the stream has had, so a catch-up crosses a
migration's seam as it crosses anything else.

| round ends | because |
|---|---|
| connected | the published tables got inside the server's replay window |
| refused again | the server moved on while the gap was read; more was published too, so go again |
| `catch_up_retries` exhausted | the stream is written faster than it is published — raise `max_replay`, publish more often, or allow more rounds |

Rows already yielded are not re-read: a round that fails starts the next above
where it stopped. Memory is one `RecordBatch`, and every blocking call crosses
into a thread, exactly as the server's replay does.

**The gaps that nothing holds.** If the published tables end below the
server's window, a range exists that the server has forgotten and was never
published. If they start above the consumer's offset, the rows it asked for
are in neither. Both are reported with the numbers rather than half-served: a
consumer that silently resumed above a gap would have lost data and been told
it recovered.

**Where the metadata file comes from**: an explicit `connect(metadata=)`, else
the greeting. The refusal does not carry it — a close frame is 123 bytes and
a bucket URI does not fit beside the numbers — so a refused client spends one
throwaway connection asking. The greeting's `stream_id` is checked against the
file; an explicit `metadata=` is trusted to be the file the caller meant.

**The published tables, not the WAL replica.** A WAL replica carries the
buffer — the unsealed tail and the range between `published_through` and the
frontier — and that band is exactly what the SERVER still holds and streams
once the catch-up hands back to the socket. Restoring it here would fetch a
second copy of the next few seconds of the subscription. It would also fail
on a log with no replica, and `wal_replication` is opt-in, so most have none.
And it needs the litestream binary on the CONSUMER, where a catch-up needs
read access to the tables and nothing else — no subprocess, no scratch
directory, nothing to provision on every box that might fall behind.

**Credentials are the client's.** The server never sends any, and the client
resolves them the way litelink does — the ordinary AWS chain, overridable with
`S3Options`. Tables that cannot be read raise `CatchUpUnavailable` at
`connect` rather than at the first `recv`, because a consumer told its
subscription was open and handed a credentials error minutes later from
whatever line read next is the failure the eager greeting exists to prevent.
A stream that publishes only to local `file://` tables can be caught up on the
server's own machine; anywhere else the read says the history is local.

---

## 6. Chaining

Each stage is a server, so a pipeline is servers end to end and every hop is
independently resumable:

```
market feed ─► streamcast A ─► live runner ─► streamcast B ─► dashboard
                   │                              │
                litelink                       litelink
```

The live runner is a *subscriber* of A and *embeds* B in its own process — a
`Stream` plus a `serve`, exactly as §1 shows. Nothing publishes into a server
over the wire in this topology — each stage publishes into a `Stream` it
holds. A stage that cannot hold one publishes remotely instead (§6b).

The dashboard box runs a litelink capture with S3 publishing off, so it keeps a
local window and drops what ages out. On restart it reads the maximum offset it
persisted and hands that back on its subscription; B replays the difference.
That is the whole recovery path, and it is the same two calls at every hop.

**Offsets are per server.** A message that travels A → runner → B has one offset
in A's log and a different one in B's. They are not translated and must not be
compared: a consumer's cursor is only meaningful against the server that issued
it. A pipeline that needs end-to-end correlation puts its own id in the payload.

---

## 6b. Remote publishing

`streamcast.publish(uri)` hands rows to a server, which appends them with the
same `Stream.send` / `send_many` a local publisher calls. Opt-in on the server
(`serve(..., publish=True)`), because a server that became writable on an
upgrade would be a change nobody asked for.

**It adds no authority, and that is the whole argument for it.** litelink
allows one writer per log, refuses neither a second nor detects one, and has
no lease that spans machines — so two `WriteHandle`s on one log is a
corruption path with no guard (§8b). Publishing to the process that already
owns the handle resolves the concurrency where it can actually be resolved:
any number of publishers, one writer.

**Concurrency is free because of I1.** Each send is queued for the stream's
writer in one step and delivered in commit order, so two handlers calling it
cannot interleave. `send_many` stays one transaction, so a batch's offsets
are adjacent even with another publisher racing it. Nothing coordinates the
publishers and nothing needs to.

**Granularity is the publisher's, exactly as it is locally.** A frame is a row
or a list of rows, and the shape is the request: an object means `send`, a
list means `send_many`. The consequences are the local ones too — §3's note
about a publish loop that never yields, on a stream with no log, applies to a
remote publisher in the same way and for the same reason.

A row the schema refuses is answered and the connection stays open, because
that is what the local call does: `send` raises, the caller catches it, the
next call works. Closing would make one bad row cost every good one behind it.

### Publishing is at-least-once under retry

A row is durable when `send` returns. If the connection drops before the reply
arrives, the publisher cannot tell whether the append happened — and with
`submit`, which keeps up to `max_in_flight` rows unanswered on one connection,
that is true of every row still in flight. Retrying may
duplicate the row; not retrying may lose it. **streamcast does not resolve
this**, and the reason is that it cannot: the ambiguity is in the publisher's
knowledge, not in the log.

It is worse than the consumer-side ambiguity §8 documents, and worth saying
so. A re-delivered message is handled by an idempotent consumer; a duplicated
row is in the log for ever, every consumer sees it, and no cursor undoes it.

**The recommended shape is a publisher key and a per-publisher sequence.**
Carry both as ordinary columns:

```python
SCHEMA = {
    "type": "object",
    "properties": {
        "publisher": {"type": "string"},       # who wrote it
        "seq":       {"type": "integer"},      # monotonic, per publisher
        "price":     {"type": "number"},
    },
    "required": ["publisher", "seq", "price"],
}
```

On reconnect, replay the window you could not account for and filter it in
memory. The offset `send` already returned is what bounds the read:

```python
landed = last_acked_seq
async with streamcast.connect(uri, offset=last_acked_offset + 1) as sub:
    # The greeting says exactly what is about to be replayed, so this is a
    # `for` over a known count rather than a loop working out when to stop.
    lo, hi = sub.info.replay or (0, 0)
    for _ in range(hi - lo):
        _offset, _ts, row = await sub.recv()
        if row["publisher"] == me:
            landed = max(landed, int(row["seq"]))

resume_at = landed + 1
```

That is the whole of it: an ordinary subscribe, and a comparison. Nothing
queries the published tables — they lag, so a row published a second ago is not in it —
and nothing needs new server state.

**A publisher recovering this way needs no litelink**, which is the point of
doing it over the socket rather than against the log. It needs streamcast and
the ability to reach the server, exactly like a consumer; no Iceberg reader,
no object-storage credentials, no second dependency on a box whose only job
is to publish. The same argument as `catch_up` reading the published tables rather
than the WAL replica (§5), one layer out.

**The offset and the key do different jobs, and both are needed.** The offset
bounds *where to look*: `send` already returned it, so the replay covers only
the rows written while the replies still owed were in flight — at most
`max_in_flight` of this publisher's — however large the log is. The
key identifies *what to look for*: the window holds other publishers' rows
too, and `(publisher, seq)` is what picks yours out of it. A row that already
carries a natural unique key needs no extra columns — match on that instead.

Per-publisher keys also mean several publishers on one stream do not
interfere: each asks only about its own, so recovery is independent of what
anyone else wrote.

**Two ways to get the loop wrong, both silent.** Reading until some condition
on the messages blocks for ever when the window is empty, which is the common
case — a publisher that was acked for everything has nothing to replay, and
`info.replay` is what says so without a message arriving. And `landed` starts
at the last acknowledged seq rather than at zero: the replay covers only the
post-acknowledgement window, so finding nothing in it means the last
acknowledged row is still the last one that landed. Reading that as "nothing
landed" resends everything.

A ULID in place of the integer works and removes the need to persist a
counter, since a restart naturally produces higher values; `max()` is still
well defined over them. What no key removes is the need for the publisher to
know what it was trying to send, which is its own durable outbox and outside
this library either way.

The cost is two columns. A publisher that can tolerate a duplicate — most
market data, where the same trade twice is caught downstream or does not
matter — pays nothing and skips all of it.

---

## 7. Invariants

| | |
|---|---|
| **I1** | `Stream._commit` and `_deliver` contain no `await`. Rows are queued in call order, committed in queue order on the stream's writer thread, and delivered — frontier and fan-out — in commit order. |
| **I2** | Joining the fan-out set and reading the frontier are adjacent statements. The replay range and the live queue partition the stream exactly. |
| **I3** | A message is durable before it is delivered, never after. |
| **I4** | What a subscriber receives is a contiguous prefix of the stream from where it subscribed, in increasing offset order. A drop ends it; nothing punches a hole in it. The order is CHECKED at the subscriber, not assumed — see below. |
| **I5** | Offsets are assigned once and never reused for the life of a log. Inherited from litelink, which owns the column. |
| **I6** | Every subscriber receives the identical frame bytes for a given offset — one `encode` call, shared — and a **replayed** frame is byte-identical to the live one it repeats, because both project through the log's declared column order. |

**I4's ordering on ONE connection is TCP's, and is not what `recv` checks.**
A subscription is one connection and TCP delivers a byte stream in order, so
frames on it cannot overtake each other. That part needs no guard, and storing
a consumer's offset and resuming from it is safe on that guarantee alone.

`Subscription.recv` compares each offset against the last anyway, for the two
places an offset arrives from somewhere TCP does not cover:

| what it catches | why TCP does not |
|---|---|
| **the catch-up join** | `_offset` is set by rows read from OBJECT STORAGE, and the first frame off the socket is compared against it. Two sources spliced into one stream, and the splice depends on `Catcher.start` and the server's replay window agreeing about an inclusive/exclusive boundary |
| **our own replay/live partition (I2)** | the server writes the replay and then the live queue onto one connection; TCP preserves the order they were *written*, not whether `_attach` picked the frontier correctly. An overlap repeats an offset |

The first is the only one reachable today. What both would otherwise be is
silent: processing a stream whose offsets went backwards means skipping data
once a cursor is involved. `<=` rather than `!= previous + 1`, because
litelink's offset space has legitimate gaps — a `restore` fences 2**20 of them
— so a jump forward is ordinary and only a step backwards is wrong.

It does **not** span a reconnect: a new `Subscription` starts with no previous
offset, so nothing is compared across the gap. The log is what makes resuming
safe there, not this check.

I1, I2 and the mechanisms behind I4 are checked by `tests/test_invariants.py`
against the source. I3 and I4 are checked end to end. I5 is litelink's.

---

## 8. Failure modes

| what happens | what the system does |
|---|---|
| a subscriber stops reading | its queue fills, it is dropped with 4429, everyone else is unaffected |
| a subscriber disconnects | the pump's race with `wait_closed` unwinds the handler; the set entry goes |
| a subscriber closes mid-replay | `close` drains what is in flight so the handshake completes. Without that, `websockets` pauses its reader at `max_queue` and the peer's Close is never read — measured at a full 10s `close_timeout` and a 1006 |
| the upstream feed drops | the producer's business — `examples/trades/producer.py` reconnects and the offsets simply continue |
| the server dies | subscribers see a reset; on restart they resume from their cursors and the log fills the gap |
| the log is full / the disk is full | `append` raises, `send` raises, **nothing is broadcast** — the failure is at the publisher, where it can be handled |
| a replay outruns `max_backlog` | the subscriber is dropped right after catching up. Size the two together (§4) |
| two publishers on one log | litelink refuses: one writer per log. A second server on the same directory fails to open |
| the server is restored from a replica | offsets are fenced by litelink and jump; a consumer resuming into the fence gets `ahead` rather than silence |
| a consumer was down past `max_replay` | refused with `too_old`; `catch_up=True` reads the gap from the published tables and then connects (§5) |
| a catching-up consumer has no credentials | `CatchUpUnavailable` at `connect`, naming the endpoint, the credential source, and four ways out |

---

## 8b. Recovering a server

`Stream.restore(name, root=…, published=…)` stands a stream up on a box that
never held its log: litelink rebuilds it from the published table and the
replicated WAL, and the result serves and appends like any other.

**Offsets are fenced, not reissued**, and that is what makes the move safe for
consumers. litelink burns 2**20 offsets, so the restored stream resumes above
anything the dead machine may have served. No offset a consumer holds is ever
handed out again carrying different data — the one thing a resume cannot
survive. `recv` permits a forward jump for exactly this reason (I4).

**The fence is a million offsets wide, and it does not strand anyone,
because `max_replay` counts ROWS rather than offset distance.** A consumer
150 rows behind a failed-over producer is 150 rows behind; measuring it as
1,048,746 was a property of the proxy, not of the work.

`max_replay` exists to bound what a replay costs, and that cost is rows.
Offset distance is a proxy for it and an exact one only while the offset space
is dense — which litelink's is not, by design: a `restore` fences 2**20
offsets that were never issued, and I4 already says a forward jump is
ordinary. So the distance check runs first, free, and only a subscribe it
would REFUSE pays to find out what the replay actually costs:

```
behind = frontier - requested            # free
if behind > max_replay:
    behind = rows the log holds from `requested` on    # ~30 ms, in a thread
    if behind > max_replay:  refuse
```

*Measured* at 30.8 ms over 1,000,000 rows in 59 files, and it scales with FILE
COUNT — roughly 0.4 ms each, because the manifests are read per file rather
than pruned. Affordable once per subscribe against a replay that costs 0.27 s
for 100,000 rows, and never paid by a consumer near the frontier.

An existing consumer therefore resumes from the cursor it already had, with a
default server and a plain `connect` — no raised bound, no `catch_up`.

| what is recovered | what is not |
|---|---|
| the published table in full, adopted via `version-hint.text` | the staging table — rebuilt EMPTY; its Parquet was on the dead machine |
| the unsealed tail and the band between `published_through` and `end_offset`, from the replicated `buffer.db` | rows appended inside the replication lag — served to callers, never shipped |

Nothing copies published files back into staging, so staging stays empty and
a replay without `replay_published=True` sees nothing below the buffer. With
it, the server reads history from the published table.

**A planned cutover loses nothing**: stop the writer, let the sidecar ship its
last frames, then restore. Only unplanned failover loses rows, and it loses
the ones the old box never managed to replicate.

### ⚠️ Two writers on one log corrupts it

The fence stops offsets being REUSED. Nothing stops the machine you are
failing over from, if it is still alive. litelink cannot detect a live writer
on another host — there is no lock that spans machines — and `restore`
succeeds against one.

*Measured*: a restore against a live primary returned a handle fenced
1,048,575 offsets above it. Both handles then appended (offset 202 on the
primary, 1048777 on the revived one — no collision, because the fence works),
and both published to the same table. Nothing refused, nothing warned.

**Stop the old producer before restoring.** This is an operational
requirement, not something either library enforces, and it is
[tracked upstream](https://github.com/nhobin219/litelink/issues/75).

---

## 9. Open

**A catch-up that does not re-read what it already has.** `catch_up` reads
the published tables from the consumer's offset each round, and a round that fails
after yielding rows starts the next above them — so nothing is re-delivered
WITHIN one `connect`. Across two, a consumer that died mid-catch-up starts
from its cursor again, which may be well below where it got to, because the
cursor only advances as rows are handled. That is the safe direction and it
costs a re-read; a consumer that cannot afford it should commit more often.

**Registered intent.** One designated publisher and many read-only nodes,
coordinated through the server, so that exactly one process pushes to S3 and
the rest are local-only. The registration would have to propagate to the
litelink tier to be worth anything, which is where the design stops.

**Bytes, not messages, for `max_backlog`.** The queue is bounded in messages
because that is what it holds; an operator thinks in memory. A byte bound needs
the frame length per entry, which is one `len()` — the open question is whether
two bounds or one replaced bound.

**Arrow IPC as a negotiated wire format.** Measured: one IPC batch of 10,000
rows encodes 492x faster than 10,000 JSON frames and is 2.4x smaller (0.48 MB
against 1.13). That lands almost entirely on replay, which is the bulk path — a
live single row in a 1-row batch is mostly framing overhead. The cost is the
`wscat` affordance and a subscriber that needs pyarrow, so it would be
`?format=arrow` alongside the default rather than instead of it, with the
client unpacking batches transparently. Two encoders and two client paths is
the price; nobody has needed it yet.

**Large binary payloads.** A `binary` column suits identifiers, hashes and
small values: it goes through litelink's SQLite buffer like any other value. A
payload of a megabyte or more belongs in litelink's blob fields (its SPEC
§15), which are not built. OTel's `bytes_value` is the first real use for them
(#40).

**A consumer cursor that is not last-writer-wins.** `cursor_uri` ships one
integer to object storage on an interval so a consumer can resume on another
box, and two consumers sharing a key overwrite each other. That is documented
rather than solved: the fix is a compare-and-set, which S3 gained only
recently and which litelink does not use either — and the failure it prevents
is re-delivery, which is the safe direction. A consumer that needs more wants
its cursor committed in the same transaction as its work, in its own database.

**A batching consumer's cursor.** The automatic save advances as the consumer
reads, which for a batch is ahead of what it has flushed — so such a consumer
must drive `streamcast.Cursor` itself. A `connect(..., autosave=False)` that
kept the load-at-connect and dropped the automatic write would close the gap;
it is one keyword and has not been needed yet.

**Compression per stream rather than per connection.** permessage-deflate keeps
a compressor per connection, so a frame encoded once is compressed N times; the
default here is therefore off. A shared dictionary applied at `encode` would
restore the once-per-message property, at the cost of speaking something no
off-the-shelf client understands.
