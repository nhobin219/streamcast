# streamcast

A JSON WebSocket pubsub framework for structured data. Publishers write, subscribers
read, and every message is optionally appended to a litelink log — an Iceberg table —
before any subscriber sees it. Each message carries the offset it was written at, so a subscriber
that stops can reconnect and ask for the rest.

A server is a log-backed pub/sub broker: publishers and subscribers are both its clients,
and it owns the log, assigns the offsets, and is its only writer. Routing is exact-match
on the stream's name — no topic hierarchy, no wildcards — because a subscriber resumes by
offset, and an offset belongs to one log.

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
just demo               # every runnable demo; `just demo NAME` runs its processes
just demo trades        # a live public feed: producer, broker, consumer
just rustfs             # an S3 endpoint, so the replication tier runs
just check-all          # every gate with replication and Node REQUIRED, as CI runs it
```

The replication tier SKIPS without an endpoint, and a skip is not a pass — it is
where litestream is actually run and where "never two instances on one database"
is checked. `just rustfs` then `just check-all` is the honest local gate; the suite with
`STREAMCAST_REQUIRE_S3=1` turns the skip into a failure the same way.

The JavaScript client test (`tests/test_node.py`) skips without Node 22+ in the same
way, and `check-all` requires it: put `node` on PATH or set `STREAMCAST_NODE`. It runs
the SPEC's reader in the browser's `WebSocket` API, so it is what says a front end can
read a stream.

## The schema is the caller's, in JSON Schema

The log is an ordinary litelink table with whatever shape the application gave
it, and `send` takes a **row**. streamcast adds exactly one column of its own,
`streamcast_ts` — the time the server took the row, stamped at append — on the
terms litelink owns `litelink_offset`: sent beside the row as framing
(`[offset, ts, msg]`) and never as a key in it, never in the greeting's
`schema`, and listed in each log's `system_schema` in the metadata file
instead. One owned scalar beside the application's columns is not owning
the shape; see `docs/SPEC.md` §5 for where that line is.

The schema is declared in **JSON Schema** and converted here (`_schema.py`),
not in litelink: litelink speaks Arrow and is deliberately general about what
it stores, while streamcast is specifically about JSON websockets, so the
mapping sits on the side that knows about JSON. `format` carries the width
JSON Schema will not (`int32` vs `int64`, `float` vs `double`), and anything
litelink would refuse is refused here where the message can name JSON
Schema's vocabulary.

An earlier design owned a fixed `(recv_ts, kind, payload)` schema and stored
each upstream frame whole. That threw away pruning, compression, a queryable
published table and a cheap replay — everything the table was for — and
litelink's own example says so in as many words: *"the reason to declare a schema rather than
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
6. **Let a log grow without a maintainer.** Nothing here calls `seal()` except
   `_maintain`. A server started with `maintain=False` and no external maintainer buffers
   every row it ever receives — measured at 15.7 MB and climbing past the 8 MiB seal
   target, with zero Parquet files.
7. **Hold a socket open while reading the published tables.** `catch_up` reads the gap
   with NOTHING connected. Opening the connection first closes the window by construction and
   makes the server queue for a subscriber that is not reading — `max_backlog` then drops
   it, so the recovery fails on exactly the consumers that needed it. The loop closes the
   window instead.
8. **Run two litestream instances against one database.** It is the one thing litestream
   forbids. The sidecar takes an `flock` beside the log — not beside `log.root`, which is
   the shared parent — holds it for the server's life, and stands by rather than starting
   a second. Both it and the maintainer's processes start through `_process.popen`, which sets
   `PR_SET_PDEATHSIG` so a `SIGKILL` of the server cannot orphan either.
9. **Save a consumer's cursor ahead of its work.** A cursor behind the work re-delivers,
   which is safe; a cursor ahead of it skips messages for ever. `_cursor` advances only
   when the loop asks for the next message, and never when the handler raised.
10. **Let a replayed frame differ from the live one it repeats.** Both project through
    the log's declared column order, so the bytes match. `_log.rows` checks each batch's
    column order against what it projected for exactly this reason — and a catch-up reads
    through the same function, so a published row is built the same way too.

## The two invariants that fail silently

`docs/SPEC.md` §3. Both compile fine when broken and produce a defect only under a race:

- **`Stream._submit`, `_commit` and `_deliver` contain no `await`.** A durable row is
  checked and queued for the stream's writer thread in one step, committed in queue order,
  and handed back in commit order; `_deliver` advances the frontier and fans out in one
  step against the event loop. The `max_inbound` check (`_room`) is followed directly by
  `_submit`, with no `await` between, or two senders could take the same room.
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
  writes a config or probes PATH belongs in a factory — `Stream.open`, `Sidecar.new`,
  `connect._resolve`. The one recorded exception is `Stream.__init__` reading
  `end_offset()` off the log it was handed: a read on an injected collaborator rather
  than the construction of one.
- **A durable fact has one home.** The stream's name is on the `Stream` and routing reads
  it; the offset counter is maintained by `send` and nothing re-reads it from the log;
  refusal sentences live in `_errors._WHY` and both ends build from there.
- **A function that needs a capability takes that capability, not its owner.**
  Protocols, not base classes (`_transport.Peer` is four methods). A module has one
  reason to change; a new concern gets a module of its own rather than growing an old
  one.
- **Write no safety argument you have not verified.** If the weaker design cannot be
  proved safe, keep the conservative one and say in a comment why it is needed, not that
  the alternative "would also be safe". A dependency feature you use must exist at the
  lowest version `pyproject.toml` allows: litelink once shipped a pyarrow option to a
  floor three releases older than the option.
- **Reproduce a reported defect here first**, as a failing test, before any fix. If it
  cannot be made to fail here, the defect is probably in the caller; say so with the
  evidence instead of changing the library. A change one caller needs that a rule above
  forbids is answered with why, not built.
- **A need that is litelink's goes to litelink.** The log is a litelink table; when a fix
  belongs in the storage engine, file it there with a reproduction against litelink
  alone, not through streamcast (litelink's issue 187, a claim orphaned by a failed commit, went
  over that way). litelink is released first and this repository raises its floor
  after; a defect spanning both ships as a release of each.
- **Conventional Commits**, closed scope list in `scripts/check_commit_msg.py`. The
  `commit-msg` hook can reject a commit silently when its output is piped, so check
  `git log -1` after each; after an interrupted or rejected edit, check `git diff` before
  the next commit, because the write may have landed.
- Blank line after every compound-statement block — `scripts/check_blank_lines.py --fix`.

### Tests

- **Check that a falsifying break applied**: a patch that matched nothing, or one that
  hangs instead of failing, proves nothing. Say in the pull request what you broke.
- **A test can pass without testing anything.** Two published-copy tests had gone
  vacuous: one compared two `None`s under the nested layout, and one patched a function
  `serve` no longer calls. The S3 fixture exports the rustfs credentials, which hid that
  `serve`'s children never got a stream's `S3Options`. When you touch an old test, check
  that it still asserts something. A test about what a child process gets clears the
  environment first.
- **A new test takes seconds** — measure with `pytest --durations`. One that needs more
  is narrowed to the claim it proves, or goes in the slow tier. Checking that one child
  got its credentials by serving every maintainer role took 28 s under load; starting
  only the role in question took 2 s.
- **No network** outside the marked tiers: brokers on loopback port 0, logs and
  databases in temp directories, a real tool (git, uv) offline from its cache.
- **Make concurrency deterministic.** Force an interleaving by running the other
  operation from a hook at the point where the first has read something and not yet
  committed it. Never sleeps or timing.
- **Measure what you claim.** A cost or performance claim carries before-and-after
  numbers from one session on one machine, and they go in the pull request and the
  CHANGELOG.

## Design rules the code already follows

- **Read the source of truth, never a copy derived from it.** A stream's metadata is the
  version its `version-hint.text` names (`_versions.load`), never the plain
  `<stream>.metadata.json` beside it. A copy written after the commit point is stale
  after a crash between the two, and a write built on it undoes the one it missed:
  `serve` once republished a stale copy to S3 and took a retention floor off it.
- **A commit built on a read is a compare-and-swap.** `_versions.commit(expected=)`,
  under the `flock` beside the hint, raises `Conflict` when the version changed since it
  was read, rather than undoing what replaced it.
- **Destruction is two-phase, and the grace runs from when every reader could see the
  change.** Retention publishes the new floor first and deletes after an hour. Its
  commits write the copy remote readers resolve before the local one
  (`published_first`): written the other way, a failed S3 write started a grace those
  readers never had.
- **A statistic an operation makes false is retracted before it, and recounted after.**
  A log's manifest `record_count` is nulled before the log is truncated, or every read
  in between refuses it as published short.
- **Read a moving bound before the query it bounds.** `end_offset()` before the
  `min(...)` it caps, so a row appended in between lands above the bound rather than
  below it.
- **Every crash point is a state to reason about.** For anything that deletes, truncates
  or rewrites durable state, list the writes in order and say, for each gap, what a
  reader and the next pass see. Make each step idempotent, so the next pass finishes it.
- **`serve`'s children get what they need through their environment, never their
  argv.** The maintainer roles and litestream open the logs themselves, so a stream's
  `S3Options` reach them as `AWS_*` and `LITESTREAM_*` variables
  (`_process.environment`): anyone on the box can read an argv in `ps`. One environment
  holds one set of credentials, so streams with different options get processes of
  their own. Key anything per log by `(root, name)`: a name is not unique across roots,
  and keyed by name, one stream's credentials reached a same-named log in another root.

## Compatibility between versions: expand, then contract

Anything another version reads — the greeting, the frames, the files beside the logs —
changes in two releases. 0.20.0 changed the greeting in one: a 0.19 process and a 0.20.0
process refused each other, publishers included, so every process had to upgrade at
once, and 0.20.0 was yanked (#135).

1. **Expand.** Readers accept the old and the new; writers keep writing the old. 0.20.1
   reads protocol 4 or 5 and still serves 4.
2. **Contract**, once every reader has upgraded: writers switch, and
   `_protocol.VERSION` bumps so a stray old reader is refused rather than misreading.

**Measure interoperability before you claim it.** The interop tier
(`tests/test_interop.py`, CONTRIBUTING) installs the last releases from PyPI into their
own virtualenvs beside the branch, runs each server against each client, and publishes,
subscribes and opens `Stream.snapshot(info.metadata)`; add a release to its `RELEASES`
when a change must still talk to it. #136 has the matrix that started it: it found that
a 0.20.0 client refuses a 0.20.1 server, which the pull request and the CHANGELOG had
both said worked. The release notes give the upgrade order the matrix supports.

**Before 1.0, prefer the consistent API.** A breaking change is fine when it leaves one
standard shape instead of two: say it loudly in the CHANGELOG, ship it in a minor
release, and add no compatibility shim. Fixes ship as a patch. Under 0.x a
minor is a breaking release, so callers cap the minor, and a pull request says whether
it breaks and which version it should ship as.

## Review

**What this library must never do is the review standard.** The adversarial review in
`CLAUDE.md` is expensive and kept for a specific doubt, and these surfaces are always
one: anything that deletes, truncates or rewrites durable
state (retention, migration, metadata commits); the catch-up join; the greeting and the
frames; and anything `serve`'s subprocesses do with credentials. Each of these had a
reachable critical defect that its first passing suite did not show. Retention took four
rounds before one came back clean.

**Widen the reviewer's bar to the change's own failure.** I1-I6 (`docs/SPEC.md` §7) do
not name every way a change can fail; say in the brief what counts as critical for this change,
so it is not graded as operability. Bars used so far:

- deleting rows a reader can still resolve;
- a reader silently given a stale view of the logs;
- a stream that no longer serves or restores after an upgrade;
- two litestreams on one database;
- a client/server pair the pull request says works and does not.

Reproduce each finding before acting on it; the skill says how to send the fix back and
when the loop is finished.

## Docs and comments

Describe the code as it is: the decision and its reasoning, in the present tense, and
why the obvious alternative was rejected. History ("this used to", "before #N") goes in
the commit message and the CHANGELOG. Add beside existing prose; never shorten or reword
a sentence the change does not need.

## Releasing

- **A new public feature is released only once a reviewed change that uses it exists in
  a caller.** If that caller is dropped, the feature is not released.
- **A release is a pull request, `release-X.Y.Z`, with two commits.**
  - `build(deps): require litelink A.B.C`, when litelink moved. Raise the floor, keep the
    cap below litelink's next minor, extend the pin comment with what the new version
    brings, and relock with `uv lock --refresh-package litelink` (uv's index cache lags
    a fresh upload).
  - `build: X.Y.Z`, which bumps `version` and renames `## Unreleased` to
    `## X.Y.Z — <date>`.

  It is a minor release for new or breaking API, and a patch for fixes.
- **A tag is the release, and a PyPI version can never be corrected.** Push a `v*` tag
  only once a maintainer has approved that version at that commit; moving or deleting a
  pushed tag needs the same approval. The tag runs `release.yml`, whose publish job then
  waits a second time, for a maintainer to approve the `pypi` environment.
- **A tag goes on `main`, on a tree CI passed.** Confirm `main` carries the version and
  the dated CHANGELOG before tagging; never chain a merge and a tag in one command — a
  blocked merge once put a tag on the previous version. A pull request merged while up
  to date with `main` after a full CI pass has exactly the tree CI ran
  (`git rev-parse origin/main^{tree}` equals `git rev-parse <pr-head>^{tree}`), and
  needs no second run on `main`; one that was behind, or merged after another landed,
  waits for `main`'s run. A run still pending is not a pass.
- **Watch the release workflow through, and confirm the upload** at
  `https://pypi.org/pypi/streamcast/X.Y.Z/json`; the project's own JSON is cached and
  lags behind.
- **A release that splits a deployment is yanked on PyPI**, with a reason that names the
  release to use instead and the issue.

## Layout

```
src/streamcast/
    _protocol.py    the wire: frames, greeting, refusal codec, subscribe URLs
    _errors.py      the refusal vocabulary and the close codes that carry it
    _stream.py      Stream — offsets, fan-out, the subscribe partition
    _writer.py      the durable append on a writer thread per stream, and group commit
    _dedupe.py      exactly-once publishers: each one's mark, committed with its rows
    _store.py       Store, Transaction: the caller's database, SQLite and Postgres
    _outbox.py      outbox: rows enqueued in the caller's tx, and the sender
    _inbox.py       inbox: the receiver, its bounded window, and processing
    _limits.py      the queue bounds' defaults, and serve's per-stream form of each
    _subscriber.py  one subscriber: bounded queue, pump, the overflow sentinel
    _log.py         the litelink tier: columns, replay, earliest
    _metadata.py    a migrated stream's logs, in order, and which one is current
    _manifest.py    per-log statistics, and pruning whole logs on them — soundly
    _published.py   one published table, read with DuckDB as another machine reads it
    _snapshot.py    Stream.snapshot — a stream's history as of one point, anywhere
    _live.py        Stream.live — a snapshot kept current with the broker's rows
    _catchup.py     reading the gap from the published tables when the server will not
    _cursor.py      where a consumer keeps the offset it finished with
    _remote.py      shipping a consumer's cursor to S3, for recovery on another box
    _publish.py     publish, Publication — the producer end, over a socket
    _schema.py      JSON Schema <-> Arrow, the layer that keeps pyarrow out of sight
    _codec.py       what JSON needs done to binary and map values, in and out
    _maintain.py    the maintainer: five role subprocesses, and the supervisor of each
    _replicate.py   the litestream sidecar: flock-guarded, never two on one db
    _process.py     starting a child the kernel kills with the server
    _stats.py       Stats — what a stream reports about itself, and /stats
    _transport.py   Peer — the four methods a transport must offer
    _server.py      serve — routing, close codes, maintainer lifetime
    asgi.py         asgi — the same streams, mounted in an ASGI app
    _client.py      connect, Subscription, and close code → exception
```

`_stream.py` is where the correctness lives and `_server.py`/`asgi.py`/`_client.py` are
transport.
That split is deliberate: every guarantee in the spec is testable without a socket.
