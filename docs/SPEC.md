# Stream multicasting

What the system is, why each piece is the shape it is, and what must stay true.
[`API.md`](API.md) says what you can call.

Numbers marked *measured* were taken on the machine this was developed on with
`just bench`; they move with hardware and are there for their ratios.

---

## 1. Architecture

One process holds the upstream subscription. Everything else on the box reads
from it.

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
│     maintainer  (subprocess)  §5        │   ← nothing seals without it
│     litestream  (sidecar)               │   ← only if the log ships its WAL
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
archive when a consumer has fallen past what the server will replay.

Out: acknowledgements, consumer groups, delivery guarantees beyond "a
contiguous prefix", authentication, and transport security. The last two are
`websockets`' and are passed through rather than reimplemented
([`SECURITY.md`](../SECURITY.md)).

---

## 2. The wire

**Every frame is JSON text.** The greeting, then an `[offset, msg]` pair per
message:

```
{"streamcast":1,"stream":"trades","end_offset":1861,"replay":[1200,1861],"durable":true}
[1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015,"side":0}]
```

**The frame is a pair, and the halves are different kinds of thing.** The
offset is the server's framing; `msg` is the publisher's row, untouched — no
offset key, no injected metadata, so a subscriber can log it, forward it or
append it to another stream whole.

Two earlier versions put the offset INSIDE the object, first as
`litelink_offset` and then as `offset`, and both were wrong the same way. A
subscriber consumes the offset positionally — `offset, msg = await sub.recv()`
in Python, `const [offset, msg] = JSON.parse(frame)` in JS — so the key name
was a contract nobody wanted, argued about twice; and injecting it meant `msg`
was never quite the row that was published. A pair has no key to name, which
is the point.

There is no binary header, no length prefix and no payload kind either, and an
earlier protocol had all three — a `>QB` header in front of an opaque blob.
They existed to carry a whole upstream frame stored verbatim, which §5 explains
was the wrong shape for the log. Nothing read the offset out of that header
except a field in `Subscriber` that nothing read either, so removing it cost
nothing and bought this:

```
wscat ws://localhost:8765/trades?offset=0
```

A working subscriber with no client library at all, printing rows a human can
read. A consumer in another language needs a JSON parser rather than this
document.

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

### Refusals

A close frame gives the reason 123 bytes, which is not a sentence. So a refusal
travels as compact JSON and the sentence is built at the subscriber:

```
4416  {"error":"not_replayable","why":"evicted","offset":100,"earliest":5000}
      ↓
      offset 100 is below 5000, the earliest offset this stream's log still
      holds. The rows between are gone from it — reconnect with
      catch_up=True to read them from the archive, or with offset=0 to
      accept the gap.
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

### I1 — `send` contains no `await`

The offset is assigned, the row is made durable, and the frame is offered to
every subscriber with nothing able to interleave. Two concurrent senders cannot
produce a subscriber that sees offset 8 before offset 7.

```python
kind = kind_of(message)
if self._log is not None:
    offset = self._log.append(_log.row(kind, message))   # sync; durable on return
    self._end_offset = offset + 1
else:
    offset = self._end_offset
    self._end_offset = offset + 1

self._fan_out(encode(offset, kind, message))             # put_nowait per subscriber
```

`async def` with no `await` in it is deliberate: it is the signature
`websockets` has, and it leaves room to move the append off the loop later
without breaking callers. Any such move has to restore this property some other
way — an explicit ordering lock — because nothing else provides it.

**The other side of I1: a publish loop that never awaits starves every
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

An earlier version of this section claimed ~1M rows/s and an 80,000/s threshold.
Both were guesses stated as measurements, and both were roughly 2x optimistic;
`benchmarks/replay.py` is where the real numbers come from now. Raise one of the
two settings and check the other.

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

**The schema is the caller's, per stream.** streamcast declares no columns. The
log is an ordinary litelink table with whatever shape the application gave it,
which is litelink's own model — *"the library owns exactly one column,
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

### Why not store the frame whole

An earlier design owned a fixed three-column schema — `recv_ts`, `kind`,
`payload` — and stored each upstream frame verbatim in the string column. It is
worth recording why that was wrong, because it looked reasonable and it
defended itself in a docstring.

litelink's own websocket example says it in as many words: *"Every field the
feed sends that is worth a column. §7 prunes on Iceberg statistics, so a query
for one minute of trades never reads the rest — **which is the reason to declare
a schema rather than store the frame whole**."*

Storing it whole threw away every property the table was for:

| | with a blob column | with real columns |
|---|---|---|
| **pruning** | nothing to prune on; a one-minute query reads every byte in range | statistics per column (§7) |
| **compression** | JSON text, poorly | float64 against its neighbours |
| **the archive** | one string per row; parse JSON in SQL to ask anything | a table any Iceberg engine reads |
| **the replay** | strings out of Arrow, re-encoded per row | columns, already typed |
| **the subscriber** | a blob to parse, once per consumer | the row, parsed once at the publisher |

The argument that defended it was circular: that a caller's extra column *"would
have to be filled by `send`, which has nothing to fill it with"*. True only
because `send` took bytes. `send` takes a row, the caller fills it, and the
premise disappears.

**The consequence is that a frame which is not a row has nowhere to go.**
Subscription acks, heartbeats and reconnect notices are dropped by the feed
handler. That is the same division of labour a kdb tickerplant has — the feed
handler parses, the plant stores typed rows — and it forces the decision to be
made once, by the publisher, instead of independently by every consumer.

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

Encode was briefly 86% of it, because the first version built a dict per row in
Python before handing it to msgspec. `scan` already projects the batch into
`(litelink_offset, *columns)` order, so `batch.to_pylist()` builds those dicts
in Arrow's own C — 1.55 us a row against 2.38, for identical bytes. `_log.replay`
checks the batch's column order against what it projected before relying on it,
once per batch, because the saving is only sound while that holds and a silent
reordering would break I6.

**`include_archive` is not passed.** litelink's default decides from the tiers:
local disk while the local table holds files — every ordinary server, and it
keeps a replay off the network — and the archive when the log has been fully
evicted and it is the only place the rows are, where refusing to look would be a
silent short serve.

### The schema, in JSON

**A stream's columns are declared in JSON Schema and converted here**, not in
litelink. That is a division of labour rather than a convenience: litelink
speaks Arrow and is deliberately general about what it stores, while
streamcast is specifically about JSON websockets — so the layer mapping one
onto the other sits on the side that knows about JSON. Putting it in litelink
was considered and rejected; it would make a JSON codec part of the public
surface of a library whose value is being general.

What it buys is an import list of one. `Stream.new(name, root=…, schema=…)` creates
the log, `serve` maintains and replicates it, and a caller reaches for neither
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

**It refuses up front what litelink would refuse at the first append** —
nested objects, arrays, `date-time`, `byte`, and the narrow integer widths
Iceberg widens silently — where the message can name JSON Schema's vocabulary
rather than Arrow's.

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

### The five refusals

| `why` | when | the caller's next move |
|---|---|---|
| `not_durable` | no log attached | drop `offset=`, or give the server a log |
| `empty` | the log holds nothing yet | subscribe live |
| `ahead` | above the frontier | the server was restored or rebuilt; investigate |
| `too_old` | further back than `max_replay` | `catch_up=True`, or read the log directly |
| `evicted` | below what the log still holds | `catch_up=True`, or accept the gap |

Five rather than one, because the move differs for each and collapsing them made
every one of them a guess.

`evicted` is the only one that cannot be decided before the scan opens, so
`_replay_from` pulls the first row and compares it to the request. Serving from
wherever the log happens to start would give a stream that silently begins above
where it asked — a hole at the join, which is the one wrong answer a resume must
never give.

**`earliest` asks three tiers, not two.** `coverage()` reports the archive and
the buffer, because it answers "what can this reader serve" for a reader
assembled from an archive and a replica, where the local Iceberg table is empty
by construction. A server reads its *own* log, where that table holds almost
everything. *Measured*: 60 rows sealed into 4 Parquet files, `coverage()`
reporting `archive=None, buffered=None`, and every `offset=EARLIEST` subscribe
refused as "holds no rows yet". `table_extent()` is the third tier.

### Catching up from the archive

`too_old` and `evicted` are the two refusals that mean *the rows exist, just not
here*. `connect(catch_up=True)` is the client reading them out of the log's
archive itself, so recovering a consumer that has been down a long time is a
flag rather than an orchestration problem.

The rule that shapes it: **nothing is connected while the archive is read.**
The obvious design opens the socket at the archive's frontier first, which
closes the gap by construction — and makes the server queue for a subscriber
that will not read a message until it has pulled millions of rows out of object
storage. `max_backlog` is 8,192, so it is dropped with `TooSlow` before the
catch-up finishes: a recovery that guarantees its own failure on exactly the
consumers that need it. *Measured* with `max_backlog=16`, where the first shape
died immediately and this one caught up 20,000 rows.

So it is a loop, and each round is: read the archive from the consumer's offset
to whatever the archive now reaches, then try to connect there.

| round ends | because |
|---|---|
| connected | the archive got inside the server's replay window |
| refused again | the server moved on while the gap was read; the archive moved too, so go again |
| `catch_up_retries` exhausted | the stream is published faster than it is archived — raise `max_replay`, sync more often, or allow more rounds |

Rows already yielded are not re-read: a round that fails starts the next above
where it stopped. Memory is one `RecordBatch`, and every blocking call crosses
into a thread, exactly as the server's replay does.

**The gap that nothing holds.** If the archive's frontier is itself below the
server's window, a range exists that the server has forgotten and the archive
never received. That is reported with both numbers rather than half-served: a
consumer that silently resumed above it would have lost data and been told it
recovered.

**Where the archive location comes from**, in order: an explicit `archive=`, the
refusal, then the greeting. The refusal carries it last, so the numbers survive
the 123-byte trim and a long bucket URI is what drops; the greeting has no such
limit, and a client that did not get it from the refusal spends one throwaway
connection asking.

**Credentials are the client's.** The server never sends any, and the client
resolves them the way litelink does — the ordinary AWS chain, overridable with
`S3Options`. An archive that cannot be read raises `CatchUpUnavailable` at
`connect` rather than at the first `recv`, because a consumer told its
subscription was open and handed a credentials error minutes later from
whatever line read next is the failure the eager greeting exists to prevent.

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
over the wire, which is why there is no remote publisher (§9).

The dashboard box runs a litelink capture with S3 publishing off, so it keeps a
local window and drops what ages out. On restart it reads the maximum offset it
persisted and hands that back on its subscription; B replays the difference.
That is the whole recovery path, and it is the same two calls at every hop.

**Offsets are per server.** A message that travels A → runner → B has one offset
in A's log and a different one in B's. They are not translated and must not be
compared: a consumer's cursor is only meaningful against the server that issued
it. A pipeline that needs end-to-end correlation puts its own id in the payload.

---

## 7. Invariants

| | |
|---|---|
| **I1** | `Stream.send` and `send_many` contain no `await`. Offset assignment, durability and fan-out are one step. |
| **I2** | Joining the fan-out set and reading the frontier are adjacent statements. The replay range and the live queue partition the stream exactly. |
| **I3** | A message is durable before it is delivered, never after. |
| **I4** | What a subscriber receives is a contiguous prefix of the stream from where it subscribed, in increasing offset order. A drop ends it; nothing punches a hole in it. The order is CHECKED at the subscriber, not assumed — see below. |
| **I5** | Offsets are assigned once and never reused for the life of a log. Inherited from litelink, which owns the column. |
| **I6** | Every subscriber receives the identical frame bytes for a given offset — one `encode` call, shared — and a **replayed** frame is byte-identical to the live one it repeats, because both project through the log's declared column order. |

**I4's ordering is TCP's guarantee, and this library cannot test it.** A
subscription is one connection and TCP delivers a byte stream in order, so
frames on it cannot overtake each other — but that is a claim about the
network between two hosts, and a loopback test establishes only what the pump,
the queue and the replay/live join do. Those are the parts this library
controls, and the parts that could have been wrong.

So `Subscription.recv` compares each offset against the last and raises rather
than trusting the reasoning. One comparison per message, and what it prevents
is the expensive failure: processing a stream whose offsets went backwards
means silently skipping data once a cursor is involved. `<=` rather than
`!= previous + 1`, because litelink's offset space has legitimate gaps — a
`restore` fences 2**20 of them — so a jump forward is ordinary and only a step
backwards is wrong.

I1, I2 and the mechanisms behind I4 are checked by `tests/test_invariants.py`
against the source. I3 and I4 are checked end to end. I5 is litelink's.

---

## 8. Failure modes

| what happens | what the system does |
|---|---|
| a subscriber stops reading | its queue fills, it is dropped with 4429, everyone else is unaffected |
| a subscriber disconnects | the pump's race with `wait_closed` unwinds the handler; the set entry goes |
| a subscriber closes mid-replay | `close` drains what is in flight so the handshake completes. Without that, `websockets` pauses its reader at `max_queue` and the peer's Close is never read — measured at a full 10s `close_timeout` and a 1006 |
| the upstream feed drops | the server's business — `examples/server.py` reconnects and the offsets simply continue |
| the server dies | subscribers see a reset; on restart they resume from their cursors and the log fills the gap |
| the log is full / the disk is full | `append` raises, `send` raises, **nothing is broadcast** — the failure is at the publisher, where it can be handled |
| a replay outruns `max_backlog` | the subscriber is dropped right after catching up. Size the two together (§4) |
| two publishers on one log | litelink refuses: one writer per log. A second server on the same directory fails to open |
| the server is restored from a replica | offsets are fenced by litelink and jump; a consumer resuming into the fence gets `ahead` rather than silence |
| a consumer was down past `max_replay` | refused with `too_old`; `catch_up=True` reads the gap from the archive and then connects (§5) |
| a catching-up consumer has no credentials | `CatchUpUnavailable` at `connect`, naming the endpoint, the credential source, and four ways out |

---

## 9. Open

**A catch-up that does not re-read what it already has.** `catch_up` reads
the archive from the consumer's offset each round, and a round that fails
after yielding rows starts the next above them — so nothing is re-delivered
WITHIN one `connect`. Across two, a consumer that died mid-catch-up starts
from its cursor again, which may be well below where it got to, because the
cursor only advances as rows are handled. That is the safe direction and it
costs a re-read; a consumer that cannot afford it should commit more often.

**Remote publishers.** `Stream.send` runs in the server's process, so a client
cannot publish into a stream. It was designed and deliberately not built: the
chained topology (§6) is served by embedding a server in the publishing
process, which is simpler and needs no new authority model. A
`streamcast.publish(uri)` returning a write-only handle — a sibling of
`Subscription`, not a method on it — is the shape if it is ever wanted.

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

**Binary columns.** litelink refuses them today, so a stream whose rows carry
real bytes has no column for them. There is no base64 workaround here any more
— that belonged to the blob schema §5 removed — and the answer is litelink's
§15.

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
