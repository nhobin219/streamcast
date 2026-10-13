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
is checked. `just rustfs` then `just check-all` is the honest local gate.

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
  writes a config or probes PATH belongs in a factory — `Stream.new`, `Sidecar.new`,
  `connect._resolve`. The one recorded exception is `Stream.__init__` reading
  `end_offset()` off the log it was handed: a read on an injected collaborator rather
  than the construction of one.
- **A durable fact has one home.** The stream's name is on the `Stream` and routing reads
  it; the offset counter is maintained by `send` and nothing re-reads it from the log;
  refusal sentences live in `_errors._WHY` and both ends build from there.
- **One job per unit.** A module, class or function does one thing, and its docstring can
  say what in a sentence; when it can't, split it. `_versions` writes a commit, `_retention`
  decides what a pass drops, `_process` starts a child: none reaches into another's job.
- **Read the source of truth, never a copy derived from it.** A copy written after the
  commit point is stale after a crash between the two, and a write built on it undoes the
  one it missed. `_metadata.load` reads the version the hint names, not the plain
  `<stream>.metadata.json` beside it.
- **Key anything per log by `(root, name)`.** A log's name is not unique across roots;
  keyed by name, one stream's credentials reached a same-named log in another root.
- **Timestamps are int64 epoch microseconds.** Never a datetime or timestamp column type.
- **Conventional Commits**, closed scope list in `scripts/check_commit_msg.py`: a lowercase
  subject of at most 72 characters.
- Blank line after every compound-statement block — `scripts/check_blank_lines.py --fix`.
- **Kill processes by PID.** `pkill -f pattern`, or `kill $(pgrep -f …)`, in one shell
  command matches that command's own shell and kills it. List the PIDs, then kill by
  number in a separate command.
- **Delegated work is bounded to about five minutes.** A subagent asked for a review or an
  update gets a scope it can finish in that time; split anything larger, or do it inline.
- **Handoffs and scratch notes live in the scratchpad**, never in a commit.

## Tests

- **Seconds, not minutes.** No test runs over a few seconds. Measure with `--durations`;
  when one is slow, time its phases and narrow it to the claim it proves. An end-to-end
  test that served every maintainer role to check that one child got its credentials took
  28 s under load; starting the one role it was about took 2 s.
- **No flaky tests.** A test that waits polls with a deadline and fails with a message
  naming what never happened.
- **A test can pass without testing anything.** `None == None`, a patch on a function
  nothing calls any more, a fixture that quietly supplies what the code under test should
  have: two published-copy tests had become vacuous this way, and the S3 fixture's exported
  credentials hid that `serve`'s children never got the stream's `S3Options`. Falsify
  every test you write or touch; a test about what a child process gets clears the
  environment first.
- **Push to the PR before running the full suite**, so the owner can run it locally while
  it runs here. The full gate is `just check-all`, or
  `STREAMCAST_REQUIRE_S3=1 uv run pytest` with `just rustfs` up.

## Durable state and anything destructive

From time-based retention (#127), where four review rounds each found a reachable way to
delete rows a reader could still see:

- **Every reachable crash point is a state to reason about.** List the writes in order and,
  for each gap between two, say what a reader and the next pass see. Make each step
  idempotent, so the next pass finishes it.
- **Commits built on a read are a compare-and-swap.** `_versions.commit(expected=)` under
  the `flock` beside the hint: a commit over a version that changed since it was read
  raises `Conflict` rather than undoing what replaced it.
- **Destruction is two-phase.** Publish the change first, so readers stop needing what will
  go; delete after a grace. **The grace runs from when every reader could see the change**:
  write the copy remote readers resolve before the local one (`published_first`), or a
  failed remote write starts a grace those readers never had.
- **A statistic the operation makes false is retracted before it, and recounted after.**
  A log's manifest `record_count` is nulled before it is truncated; otherwise every read
  between the truncate and the recount refuses the log as published short.
- **Read a moving bound before the query it bounds.** `end_offset()` before the
  `min(...)` it caps, so a row appended in between is above the bound, not dropped below it.
- **Anything hard to undo waits for the owner**: deleting or moving a published tag, a
  force push, a yank. Look at the target first.

## Processes and credentials

- **A child gets what it needs through its environment, never its argv.** `serve`'s
  maintainer roles and litestream open the logs themselves, so the stream's `S3Options`
  reach them as `AWS_*` and `LITESTREAM_*` variables (`_process.environment`). Anyone on the
  box can read an argv in `ps`; only the process's user can read its environment.
- **One environment holds one set of credentials**, so streams with different
  `S3Options` get processes of their own: one per role per set.

## Wire and on-disk compatibility: expand, then contract

0.20.0 moved the greeting to protocol 5 in one step, and a 0.19 client and a 0.20.0 server
refused each other, publishers included: every process had to upgrade at once. It was
yanked, and 0.20.1 split the change (#135). Any change to what another version reads —
the greeting, the frames, the files beside the logs — ships in two releases:

1. **Expand.** Readers accept the old and the new; writers keep writing the old. Readers
   upgrade, one process at a time.
2. **Contract**, once readers are upgraded: writers switch, and the protocol `VERSION`
   bumps so a stray old reader is refused cleanly rather than misreading.

**Before claiming any interoperability, measure it.** Install the previous releases from
PyPI into their own virtualenvs beside the branch, and run each server against each client:
publish, subscribe and `Stream.snapshot(info.metadata)`. The matrix on #136 is the template.
It is what found that a 0.20.0 client still refuses a 0.20.1 server, which the PR and the
changelog had claimed worked. **State the upgrade order** in the changelog.

## Docs

- **Docs describe the current state.** No "an earlier version did X", no pitch openings.
  History belongs in the CHANGELOG, and a design's rejected alternative in the comment
  beside the code.
- **Add beside existing prose; don't reword what the change doesn't need.** Rewrite a
  sentence only when it has become wrong.
- **Every user-visible change gets an entry under `## Unreleased`** in `CHANGELOG.md` in
  the same PR, with breaking changes marked "Breaking:".

## Working with the owner

- **Every change goes through a PR, and the owner merges it.** Merge only when told to,
  for that PR; permission given once doesn't carry to the next. `main` requires an
  approving review and allows squash merges only, so `gh pr merge --squash`, and `--admin`
  only when the owner says so.
- **Update a PR's body with**
  `gh api -X PATCH repos/nhobin219/streamcast/pulls/N -F body=@file`; `gh pr edit` fails
  here. Commits and PR bodies end with the attribution lines the session gives.
- **"Run the reviewer" means the critical-reviewer loop below**, not a generic review, and
  it runs until a round is clean.

## Releasing

When the owner says to cut one:

1. **Branch `release-X.Y.Z`.** If litelink moved, raise its floor and keep the cap below
   its next minor (`litelink>=0.12.4,<0.13`), extend the pin comment with what the new
   version brings, and `uv lock --refresh-package litelink` (uv's index cache lags a fresh
   upload). Commit that as `build(deps): require litelink X.Y.Z`. Then bump `version`,
   rename `## Unreleased` to `## X.Y.Z — <date>`, and commit `build: X.Y.Z`. A minor for
   new or breaking API, a patch for fixes.
2. **Gate it:** the full suite with S3 required against the new lock, and CI green.
3. **Merge, then check, then tag**, as separate steps. Confirm `main` carries the version
   and the dated changelog, then `git tag -a vX.Y.Z -m "streamcast X.Y.Z"` on that commit
   and push it. Chained in one command, a blocked merge once pushed a tag on the previous
   version; the workflow's tag check refused it, and the tag had to be deleted.
4. **The publish waits for the owner.** The release workflow pauses at "Publish to PyPI"
   for the `pypi` environment. Tell the owner. GitHub's mobile app can't approve it; a
   browser can, or with the owner's OK,
   `gh api -X POST repos/nhobin219/streamcast/actions/runs/<run>/pending_deployments -F "environment_ids[]=<id>" -f state=approved`.
5. **Confirm on** `https://pypi.org/pypi/streamcast/X.Y.Z/json`. The project's own JSON is
   cached and lags.

## litelink

- **litelink is the storage layer, in its own repo** (`nhobin219/litelink`), and changes
  there go through the same PR-and-owner process. **Releasing litelink to PyPI needs the
  owner's confirmation first.**
- **A litelink bug found from here is filed there**, with a minimal reproduction against
  litelink alone, rather than worked around here without a word. #187, a claim orphaned by
  a failed commit, was filed that way and fixed in 0.12.4. Read the comments the litelink
  engineer leaves on shared issues before designing against them.
- **Every streamcast release caps litelink below its next minor**, because under 0.x a
  minor is litelink's breaking release.

## Review

When a change is complete and needs adversarial review, use the `critical-reviewer`
subagent — see [`.claude/skills/critical-review/SKILL.md`](.claude/skills/critical-review/SKILL.md)
for how to drive it and when the loop is finished. It is expensive; use it for a specific
doubt, not as a routine second pass.

**What always gets it**: anything that deletes, truncates or rewrites durable state
(retention, migration, metadata commits), the catch-up join, the greeting and the frames,
and anything a subprocess does with credentials. Each of those had a reachable critical
defect that its first passing suite did not show.

## Layout

```
src/streamcast/
    _protocol.py    the wire: frames, greeting, refusal codec, subscribe URLs
    _errors.py      the refusal vocabulary and the close codes that carry it
    _stream.py      Stream — offsets, fan-out, the subscribe partition
    _writer.py      the durable append on a writer thread per stream, and group commit
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
