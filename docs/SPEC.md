# Stream multicasting

What the system is, why each piece is the shape it is, and what must stay true.
[`API.md`](API.md) says what you can call.

Numbers marked *measured* were taken on the machine this was developed on with
`just bench`; they move with hardware and are there for their ratios.

---

## 1. Architecture

One process holds the upstream subscription. Everything else on the box reads
from it.

```
exchange ws feed
      │  ONE connection
      ▼
┌─────────────────────────────────────────┐
│ broker process                          │
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
└─────────────────────────────────────────┘
```

Three problems, one shape:

**Subscription limits.** Exchanges cap connections per account and per IP. Six
consumers on a VM is six connections; one broker is one.

**Divergence.** Six connections can be served six subtly different streams —
different reconnect points, different dropped frames, a rebalance that reaches
one and not another. One connection cannot, and every subscriber here receives
the *same bytes* from the *same* `encode` call.

**Recovery.** A consumer that stops has, without a log, no way back to what it
missed. With one, it has an offset.

### Scope

In: fan-out, ordering, offsets, replay, and per-subscriber backpressure
isolation.

Out: acknowledgements, consumer groups, delivery guarantees beyond "a
contiguous prefix", authentication, and transport security. The last two are
`websockets`' and are passed through rather than reimplemented
([`SECURITY.md`](../SECURITY.md)).

---

## 2. The wire

Two frame kinds, told apart by WebSocket's own text/binary bit rather than by a
discriminator this library defines.

| | | |
|---|---|---|
| **greeting** | text, exactly one, first | JSON |
| **message** | binary, one per message | `>QB` header, then payload |

```
 0        8   9                                   n
 ┌────────┬───┬───────────────────────────────────┐
 │ offset │ k │ payload                           │
 └────────┴───┴───────────────────────────────────┘
   uint64   u8   UTF-8 if k=0, raw bytes if k=1
```

The header is fixed-width rather than a varint: the saving would be ~6 bytes on
a payload rarely under a hundred, and a fixed header can be sliced without being
parsed — which is what `frame_offset` does on the send path.

**`kind` is per message, not per stream.** WebSocket lets a feed mix text and
binary, and a subscriber handed `str` where the publisher sent `bytes` has been
handed different data, not a different encoding.

### A subscribe is a URL

```
ws://broker:8765/trades?offset=1200
      └── stream ──┘ └── resume ──┘
```

There is no application handshake in front of the data. The stream is the path
and the resume point is the query, so subscribing is the WebSocket open.

The price is that a refusal has to be a close code rather than a reply. The
return is that `wscat ws://broker:8765/trades?offset=0` is a working subscriber
— which is worth more than symmetry on a path nobody debugs when it works.

### The greeting

```json
{"streamcast": 1, "stream": "trades", "end_offset": 1861,
 "replay": [1200, 1861], "durable": true}
```

It exists so that "the connection opened" means something on a stream that is
silent, which for market data outside a session is most of them. A subscriber
that receives it knows the broker understood its offset, knows whether the
offsets it is about to see survive a restart, and knows what is about to be
replayed before any of it arrives.

`connect` awaits it before returning, so entering the `async with` block MEANS
the broker accepted the subscribe. The alternative surfaces a refused offset as
a failure of whatever `recv` the application happened to reach first, which on a
quiet stream is minutes later and somewhere else.

### Refusals

A close frame gives the reason 123 bytes, which is not a sentence. So a refusal
travels as compact JSON and the sentence is built at the subscriber:

```
4416  {"error":"not_replayable","why":"evicted","offset":100,"earliest":5000}
      ↓
      offset 100 is below 5000, the earliest offset this stream's log still
      holds. The rows between are gone from it — read the archive for them,
      or subscribe with offset=0 and accept the gap.
```

The English has exactly one home (`_errors._WHY`) and can be reworded without a
protocol change. `refusal()` trims by dropping whole fields from the end, so a
broker serving three hundred streams still sends a valid 4404 — the list goes,
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
| 1000/1001 | the broker finished on purpose | ends the iteration |

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
other way round. A broker that dies between the two has published nothing it
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
| grow unbounded | the broker's memory set by its worst consumer | the OOM this design exists to avoid |
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
once (*measured*: 0.20 us, independent of subscriber count) and the per
subscriber cost is the insert (*measured*: 0.95 us per subscriber at 100
subscribers).

**`max_backlog` and `max_replay` are sized against each other**, not
independently. A replay streams while live messages queue behind it, so a
subscriber that takes longer to catch up than `max_backlog` messages of live
traffic is dropped the moment it arrives, having done all the work. The defaults
hold with room: a replay reads at roughly 1M rows/s from local Parquet, so
100,000 messages is ~0.1 s, and a feed would have to exceed 80,000 messages/s to
put 8,192 messages in the queue in that time. Raise one and check the other.

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

streamcast owns three columns, and a log with any other shape is refused at
`Stream` construction rather than at the first message:

```python
SCHEMA = pa.schema([
    pa.field("recv_ts", pa.int64(),  nullable=False),   # broker clock, microseconds
    pa.field("kind",    pa.int32(),  nullable=False),   # 0 text, 1 binary
    pa.field("payload", pa.string(), nullable=False),   # text as-is; binary base64
])
```

**Strict on purpose.** A log with an extra `venue` column would have to be
filled by `send`, which has nothing to fill it with — so the column would be
NULL on every row and the schema would be a lie told at creation. A stream that
needs more columns than this wants litelink directly.

**No `sort_by`.** litelink's default is offset order and every read here is an
offset range, so the default is the correct answer rather than a fallback.

**Binary is base64, at 4/3 the size.** litelink refuses `binary` columns today —
its buffer leg pushes the read's boundary predicate into SQLite, which decodes
blobs as UTF-8 and fails — and says to encode as text for now. `kind` is already
on the row, so when litelink's blob fields land this becomes a storage change
with no wire change. A `str` message, which is what every JSON feed sends, pays
nothing.

### The counter

`Stream.end_offset` is read from the log **once**, at construction, and
maintained by `send` thereafter — `append` returns the offset it assigned, so
asking the log per message would be a round trip for a number the previous call
already returned. It cannot drift, because litelink allows exactly one writer.

A broker restarted against an existing log continues its offsets. It must: a
restart that reset them would hand the same integers to different data, and
every consumer cursor in the system would silently point somewhere else.

### The replay

```python
reader = await asyncio.to_thread(log.scan, columns=…, start_offset=…, end_offset=…)
while (batch := await asyncio.to_thread(_next_batch, reader)) is not None:
    ...
```

**Every blocking call is in a thread, and that is not an optimisation.** A
replay is DuckDB reading Parquet: milliseconds to seconds depending on how far
behind the subscriber is, and on the event loop that is the whole broker stopped
— no live message fanned out, no other subscriber served, no keepalive answered.
litelink is built for this: its buffer and reader each hold their own lock and
its SQLite connections are opened `check_same_thread=False`.

Batches rather than rows, because that is the unit litelink hands back and the
unit a thread hop should cost.

**`include_archive` is not passed.** litelink's default decides from the tiers:
local disk while the local table holds files — every ordinary broker, and it
keeps a replay off the network — and the archive when the log has been fully
evicted and it is the only place the rows are, where refusing to look would be a
silent short serve.

### The five refusals

| `why` | when | the caller's next move |
|---|---|---|
| `not_durable` | no log attached | drop `offset=`, or give the broker a log |
| `empty` | the log holds nothing yet | subscribe live |
| `ahead` | above the frontier | the broker was restored or rebuilt; investigate |
| `too_old` | further back than `max_replay` | read the log directly |
| `evicted` | below what the log still holds | read the archive, or accept the gap |

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
by construction. A broker reads its *own* log, where that table holds almost
everything. *Measured*: 60 rows sealed into 4 Parquet files, `coverage()`
reporting `archive=None, buffered=None`, and every `offset=EARLIEST` subscribe
refused as "holds no rows yet". `table_extent()` is the third tier.

---

## 6. Chaining

Each stage is a broker, so a pipeline is brokers end to end and every hop is
independently resumable:

```
market feed ─► streamcast A ─► live runner ─► streamcast B ─► dashboard
                   │                              │
                litelink                       litelink
```

The live runner is a *subscriber* of A and *embeds* B in its own process — a
`Stream` plus a `serve`, exactly as §1 shows. Nothing publishes into a broker
over the wire, which is why there is no remote publisher (§9).

The dashboard box runs a litelink capture with S3 publishing off, so it keeps a
local window and drops what ages out. On restart it reads the maximum offset it
persisted and hands that back on its subscription; B replays the difference.
That is the whole recovery path, and it is the same two calls at every hop.

**Offsets are per broker.** A message that travels A → runner → B has one offset
in A's log and a different one in B's. They are not translated and must not be
compared: a consumer's cursor is only meaningful against the broker that issued
it. A pipeline that needs end-to-end correlation puts its own id in the payload.

---

## 7. Invariants

| | |
|---|---|
| **I1** | `Stream.send` and `send_many` contain no `await`. Offset assignment, durability and fan-out are one step. |
| **I2** | Joining the fan-out set and reading the frontier are adjacent statements. The replay range and the live queue partition the stream exactly. |
| **I3** | A message is durable before it is delivered, never after. |
| **I4** | What a subscriber receives is a contiguous prefix of the stream from where it subscribed. A drop ends it; nothing punches a hole in it. |
| **I5** | Offsets are assigned once and never reused for the life of a log. Inherited from litelink, which owns the column. |
| **I6** | Every subscriber receives the identical frame bytes for a given offset — one `encode` call, shared. |

I1, I2 and the mechanisms behind I4 are checked by `tests/test_invariants.py`
against the source. I3 and I4 are checked end to end. I5 is litelink's.

---

## 8. Failure modes

| what happens | what the system does |
|---|---|
| a subscriber stops reading | its queue fills, it is dropped with 4429, everyone else is unaffected |
| a subscriber disconnects | the pump's race with `wait_closed` unwinds the handler; the set entry goes |
| the upstream feed drops | the broker's business — `examples/broker.py` reconnects and the offsets simply continue |
| the broker dies | subscribers see a reset; on restart they resume from their cursors and the log fills the gap |
| the log is full / the disk is full | `append` raises, `send` raises, **nothing is broadcast** — the failure is at the publisher, where it can be handled |
| a replay outruns `max_backlog` | the subscriber is dropped right after catching up. Size the two together (§4) |
| two publishers on one log | litelink refuses: one writer per log. A second broker on the same directory fails to open |
| the broker is restored from a replica | offsets are fenced by litelink and jump; a consumer resuming into the fence gets `ahead` rather than silence |

---

## 9. Open

**Remote publishers.** `Stream.send` runs in the broker's process, so a client
cannot publish into a stream. It was designed and deliberately not built: the
chained topology (§6) is served by embedding a broker in the publishing process,
which is simpler and needs no new authority model. A `streamcast.publish(uri)`
returning a write-only handle — a sibling of `Subscription`, not a method on it —
is the shape if it is ever wanted.

**Registered intent.** One designated publisher and many read-only nodes,
coordinated through the broker, so that exactly one process pushes to S3 and the
rest are local-only. The registration would have to propagate to the litelink
tier to be worth anything, which is where the design stops.

**Bytes, not messages, for `max_backlog`.** The queue is bounded in messages
because that is what it holds; an operator thinks in memory. A byte bound needs
the frame length per entry, which is one `len()` — the open question is whether
two bounds or one replaced bound.

**Binary without base64** — see §5. Waiting on litelink's §15.

**Compression per stream rather than per connection.** permessage-deflate keeps
a compressor per connection, so a frame encoded once is compressed N times; the
default here is therefore off. A shared dictionary applied at `encode` would
restore the once-per-message property, at the cost of speaking something no
off-the-shelf client understands.
