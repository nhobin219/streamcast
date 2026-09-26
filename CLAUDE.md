# streamcast

A JSON WebSocket pubsub framework for structured data. Publishers write, subscribers
read, and every message is appended to a litelink log — an Iceberg table — before any
subscriber sees it. Each message carries the offset it was written at, so a subscriber
that stops can reconnect and ask for the rest.

**The log is the analytical table**, which is the reason litelink is underneath rather
than an append-only file: no export step and no second copy, so the Parquet a message was
appended to is the Parquet an analytical engine reads.

Read [`docs/SPEC.md`](docs/SPEC.md) before changing behaviour. It is short and it carries
the reasoning — particularly §3 (the two atomicity invariants) and §4 (why a slow
consumer is dropped rather than buffered).

## Commands

```bash
just check              # lint + format-check + typecheck + tests. What CI runs.
just test-fast          # skips the backpressure tier, for an inner loop
just test tests/test_resume.py -k partition
just bench              # fan-out and publish throughput
just bench-replay       # replay cost, and which layer it is spent in
just demo               # a live public feed through a server
just rustfs             # an S3 endpoint, so the replication tier runs
just check-all          # every gate with replication REQUIRED, as CI runs it
```

The replication tier SKIPS without an endpoint, and a skip is not a pass — it is
where litestream is actually run and where "never two instances on one database"
is checked. `just rustfs` then `just check-all` is the honest local gate.

## The schema is the caller's, in JSON Schema

streamcast declares no columns. The log is an ordinary litelink table with
whatever shape the application gave it, and `send` takes a **row**.

The schema is declared in **JSON Schema** and converted here (`_schema.py`),
not in litelink: litelink speaks Arrow and is deliberately general about what
it stores, while streamcast is specifically about JSON websockets, so the
mapping sits on the side that knows about JSON. `format` carries the width
JSON Schema will not (`int32` vs `int64`, `float` vs `double`), and anything
litelink would refuse is refused here where the message can name JSON
Schema's vocabulary.

An earlier design owned a fixed `(recv_ts, kind, payload)` schema and stored
each upstream frame whole. That threw away pruning, compression, a queryable
archive and a cheap replay — everything the table was for — and litelink's own
example says so in as many words: *"the reason to declare a schema rather than
store the frame whole."* `docs/SPEC.md` §5 records why, because the mistake
defended itself in a docstring and could be made again.

## What this library must never do

These are the failures the design exists to prevent. A change that makes one of them
possible is wrong even if every test passes.

1. **Deliver a message a subscriber cannot account for.** What a subscriber receives is a
   contiguous prefix from where it subscribed, in increasing offset order. A drop ends it;
   nothing punches a hole in the middle of it, because a hole is invisible — the offsets
   on either side still increase. Ordering on one connection is TCP's guarantee and needs
   no help; `recv`'s offset check is there for the catch-up join, where the rows come from
   object storage rather than the socket, and as an assertion on I2's partition.
2. **Let one consumer's speed affect another's.** `Stream.send` must never await a
   consumer. The broadcast is a synchronous `put_nowait` per subscriber and nothing else.
3. **Broadcast before the log has the message.** `append` then fan out, never the
   reverse. A server that died between the two has published nothing it cannot replay.
4. **Serve a replay that silently starts above where it was asked.** A resume that begins
   at the wrong place is a hole at the join. Refuse with 4416 instead.
5. **Reorder.** Two concurrent senders must not produce a subscriber that sees offset 8
   before offset 7.
6. **Let a log grow without a maintainer.** Nothing here calls `seal_due()` except
   `_maintain`. A server started with `maintain=False` and no external maintainer buffers
   every row it ever receives — measured at 15.7 MB and climbing past the 8 MiB seal
   target, with zero Parquet files.
7. **Hold a socket open while reading the archive.** `catch_up` reads the gap with
   NOTHING connected. Opening the connection first closes the window by construction and
   makes the server queue for a subscriber that is not reading — `max_backlog` then drops
   it, so the recovery fails on exactly the consumers that needed it. The loop closes the
   window instead.
8. **Run two litestream instances against one database.** It is the one thing litestream
   forbids. The sidecar takes an `flock` beside the log — not beside `log.root`, which is
   the shared parent — holds it for the server's life, sets `PR_SET_PDEATHSIG` so a
   `SIGKILL` cannot orphan the child, and stands by rather than starting a second.
9. **Save a consumer's cursor ahead of its work.** A cursor behind the work re-delivers,
   which is safe; a cursor ahead of it skips messages for ever. `_cursor` advances only
   when the loop asks for the next message, and never when the handler raised.
10. **Let a replayed frame differ from the live one it repeats.** Both project through
    the log's declared column order, so the bytes match. `_log.rows` checks each batch's
    column order against what it projected for exactly this reason — and a catch-up reads
    through the same function, so an archived row is built the same way too.

## The two invariants that fail silently

`docs/SPEC.md` §3. Both compile fine when broken and produce a defect only under a race:

- **`Stream.send` and `send_many` contain no `await`.** Offset assignment, durability and
  fan-out are one step against the event loop.
- **Joining the fan-out set and reading the frontier are adjacent statements.** That is
  what makes the replay range and the live queue partition the stream exactly.

`tests/test_invariants.py` reads the AST and asserts both. **If a change makes one fail,
the change is probably wrong.** If it is genuinely right, the spec section changes with
it in the same PR.

## Working here

- **Falsify every test.** Break the code, confirm the test fails, restore, confirm it
  passes. Two real defects in this repo were found that way — a leaked handler task per
  disconnect, and a replay that refused every `EARLIEST` once the buffer had sealed.
- **Comments carry the reasoning, not the mechanics.** Why the obvious alternative was
  rejected, with the measurement if there was one. Match the density of the file.
- **`__init__` takes built collaborators and does no I/O.** litelink's rule, and
  `tests/test_invariants.py` enforces it. Construction that opens a log, reads a cursor,
  writes a config or probes PATH belongs in a factory — `Stream.new`, `Sidecar.new`,
  `connect._resolve`. The one recorded exception is `Stream.__init__` reading
  `end_offset()` off the log it was handed: a read on an injected collaborator rather
  than the construction of one.
- **A durable fact has one home.** The stream's name is on the `Stream` and routing reads
  it; the offset counter is maintained by `send` and nothing re-reads it from the log;
  refusal sentences live in `_errors._WHY` and both ends build from there.
- **Conventional Commits**, closed scope list in `scripts/check_commit_msg.py`.
- Blank line after every compound-statement block — `scripts/check_blank_lines.py --fix`.

## Review

When a change is complete and needs adversarial review, use the `critical-reviewer`
subagent — see [`.claude/skills/critical-review/SKILL.md`](.claude/skills/critical-review/SKILL.md)
for how to drive it and when the loop is finished. It is expensive; use it for a specific
doubt, not as a routine second pass.

## Layout

```
src/streamcast/
    _protocol.py    the wire: frames, greeting, refusal codec, subscribe URLs
    _errors.py      the refusal vocabulary and the close codes that carry it
    _stream.py      Stream — offsets, fan-out, the subscribe partition
    _subscriber.py  one subscriber: bounded queue, pump, the overflow sentinel
    _log.py         the litelink tier: columns, replay, earliest
    _catchup.py     reading the gap from the archive when the server will not
    _cursor.py      where a consumer keeps the offset it finished with
    _remote.py      shipping a consumer's cursor to S3, for recovery on another box
    _schema.py      JSON Schema <-> Arrow, the layer that keeps pyarrow out of sight
    _maintain.py    the maintainer subprocess, and the supervisor that owns it
    _replicate.py   the litestream sidecar: flock-guarded, never two on one db
    _transport.py   Peer — the four methods a transport must offer
    _server.py      serve — routing, close codes, maintainer lifetime
    asgi.py         asgi — the same streams, mounted in an ASGI app
    _client.py      connect, Subscription, and close code → exception
```

`_stream.py` is where the correctness lives and `_server.py`/`asgi.py`/`_client.py` are
transport.
That split is deliberate: every guarantee in the spec is testable without a socket.
