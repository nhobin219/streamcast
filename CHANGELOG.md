# Changelog

All notable changes are recorded here. Versions follow
[Semantic Versioning](https://semver.org); the format is loosely
[Keep a Changelog](https://keepachangelog.com).

The 0.1.0 entry describes what the library is rather than what changed, since
there was nothing to have changed from. Everything above it is ordinary.

## Unreleased

### Fixed

- **`EARLIEST` on a log that holds nothing yet no longer loses rows.** The
  server refused it as `NotReplayable("empty")`, and a client that then
  subscribed "from now", as the message advised, missed any row committed
  between the refusal and the second subscribe. `EARLIEST` now starts at the
  log's first row: a replay from the frontier read before the subscriber joins,
  which carries any row committed in between. A client still understands
  `empty` from an older server. Found as an intermittent CI failure in the
  keyed-table example, and the same race was in `Stream.live` at open.

## 0.14.0 — 2026-10-04

### Added

- **`Stream.retire`** (#96): finish a stream for good.
  - **What it does:** publishes every row (including the trailing run a plain
    `publish` holds back), retires the log in litelink, and records the
    retirement in `metadata.json`, locally and beside the tables.
  - **Read-only from then on:** a retired stream is still served. Subscribers
    replay and catch up from the published table. A local `send` raises
    `StreamRetired`, a publisher is refused with the new close code 4410, and
    it gets no maintainer or sidecar.
  - **Undoing it:** `Stream.restore(..., revive=True)`, on any box, continues
    it on a new log at exactly the retired end, losing nothing.
  - **Older builds refuse it:** a retired stream's `metadata.json` is version
    3, so an older streamcast refuses it instead of quietly starting a new log.
- **`Stream.restore(replica_reserve=, published_reserve=)`**, litelink's offset
  fences, passed through.
- **`Stream.restore(schema=, sort_by=, config=)`.** The restored log's shape
  comes from the stream's metadata by default: its exact schema (binary
  encodings and system columns included, which an Iceberg schema doesn't keep)
  and its `sort_by`. litelink 0.10.1 checks them against the replica or table,
  and needs them for a table no 0.10 publish stamped. `schema=` and `sort_by=`
  override, or cover a stream whose metadata predates recording them.
  `config=` sets the restored log's policy, and the new log's on `revive=True`.
- **`metadata.json` records each log's `sort_by`.** `serve` fills it in for an
  existing stream from its open live log; a log not on the machine stays
  unknown (`null`).

- **litelink's read-cache settings on every reader of the published tables**
  (#77): `memory_cache`, `disk_cache`, `cache_key` and
  `disk_cache_volume_limit` on `Stream.snapshot`, `scan`, `sql` and `live`:
  the reads that repeat. Not on `connect`, whose catch-up reads its gap once.
  - **litelink's defaults:** memory on, disk off.
  - **The key is the caller's:** relative to litelink's cache root, absolute,
    or its `default`. Nothing is keyed automatically.
  - **Separate databases:** readers with different settings read through
    different DuckDB databases.

- **`Subscription.recv_many(limit=500)` and `Subscription.batches(limit=500)`**
  (#86): a subscriber's version of group commit.
  - **What a batch holds:** at least one row, then every row already on the
    connection, up to `limit`. It never waits for more. A consumer that keeps
    up gets small batches at `recv`'s latency; one that falls behind gets
    everything that queued, and catch-up batches come full.
  - **The same guarantees as `recv`:** the same rows in the same order, with
    the same offset check.
  - **The cursor:** it counts a batch as handled when the next is asked for,
    so the automatic save is now right for batching consumers.
  - **A connection that ends mid-batch:** the rows before the end are
    returned, and the refusal is raised next, naming the last row delivered.
  - **No new buffer:** one row at most is read ahead, outside `websockets`'
    own bounded queue.

- **Metrics in the OTel example** (`examples/otel/metrics.py`):
  - **`StreamMetricExporter`**, an OTel metric exporter that publishes each
    collection to a stream, one row per data point. It covers sums, gauges
    and both kinds of histogram, with exemplars kept, so a metric leads to
    the trace it measured.
  - **Delta temporality** for counters and histograms, so a window's total
    is a `sum()` over the stored table.
  - **The services record metrics:** an order count by outcome and
    `http.server.request.duration`.
  - **`otel/export.py` re-exports metrics as OTLP**, and **`just demo otel`
    shows them in otel-gui's Metrics tab.** The dashboard moves to otel-gui
    3.0.0, the first release that accepts metrics.

### Changed

- **BREAKING: `serve` and `asgi` no longer take `publish=`**, and every served
  stream takes publishers. Whether a stream can be written is the stream's to
  say: a retired one refuses publishers with 4410. A call passing `publish=`
  raises `TypeError`. There's no authentication yet, for readers or writers,
  so a reachable port is a readable and writable one, as it was for readers
  already.

- **`Stream.restore` works on a stream that never had WAL replication.**
  litelink rebuilds the log from its published table when there's no replica,
  fenced 2^40 above what the old log last recorded issuing (litelink#144).

- **litelink 0.9** (`>=0.9.0,<0.10`), for `litelink.current_metadata`.
- **A published table's current metadata is resolved outside DuckDB**, with
  `litelink.current_metadata`. Through a disk-cached connection,
  `version-hint.text` pinned a reader to the first snapshot it saw
  (litelink#141, fixed in litelink 0.9 by that function). Every file the hint
  names is written once and caches safely.

- **The one-shot OTel demo runs without a maintainer**, as the migration demo
  does. Its maintainer processes were still opening the logs when the run
  ended, and cost it several seconds: `demo.main` took 3–11 s with them and
  about 1 s without.
- **The OTel exporters reconnect after a dropped connection.** They used to
  hold one publication for the life of the process, so a broker restart
  ended a service's telemetry for good, silently. Each exporter now takes a
  `common.Publisher` with the stream's URI: it connects on the first batch,
  drops a connection that fails, and the next batch connects again. A
  failed batch is still dropped rather than raised, so telemetry never fails
  the application. One line is printed when telemetry starts being dropped
  and one when it is published again.

## 0.13.1 — 2026-10-03

### Changed

- **Python 3.14 support.** `requires-python` is now `>=3.11,<3.15`, and
  litelink `>=0.8.1,<0.9`, the release that opened 3.14. CI tests the floor
  and the top of the range, 3.11 and 3.14.
- **litelink 0.8.1** (`>=0.8.1,<0.9`). Two of its fixes reach streamcast:
  - **One S3 client per process.** A reader that reloads the published
    tables (`Stream.live`, `Stream.snapshot`, catch-up) no longer builds a new
    S3 client per load. On 3.14 those piled up to over a thousand idle
    connections.
  - **Offline credential chains.** S3 reads with credentials from a profile,
    instance metadata or SSO no longer download DuckDB's `aws` extension on
    first use, or fail offline as "no S3 credentials".

## 0.13.0 — 2026-10-03

### Changed

- **litelink 0.8** (`>=0.8.0,<0.9`). Its one change, `ingest()` no longer
  pushing a short tail on a log without `wal_replication`, does not touch
  streamcast, which does not call `ingest()`.
- **The server pipelines a publisher's frames.** Each is checked and queued
  for the writer as it is read, without waiting for the one before it to
  commit, and answered in the order it arrived; a refused or failed frame is
  answered in its place and the rest carry on. `serve(max_in_flight=64)`
  (and `asgi`) bounds the replies one connection may be owed, past which the
  server stops reading it.
- **BREAKING: `max_backlog` moves from `Stream` to `serve` and `asgi`.**
  `Stream(max_backlog=)` and `Stream.new(max_backlog=)` are gone; pass
  `serve(..., max_backlog=)`. A queue bound is the serving process's
  setting, not the stream's, so it now changes with a restart without
  touching the code that builds the streams. `serve` and `asgi` take each of
  their bounds — `max_backlog`, `max_inbound`, `max_in_flight` — as one int
  for every stream, or a map from stream name to int that must name exactly
  the streams served; a missing or unknown name, or a bound below 1, raises
  `ValueError` at the call.

### Added

- **`max_inbound`** on `serve` and `asgi` (65,536 rows by default): rows a durable stream may have
  queued for commit, across all its publishers, before a send waits. The
  broker's memory bound when its disk falls behind — at the bound a local
  `send` waits and a remote publisher's connection stops being read, so TCP
  holds it back; nothing is refused.
- **`max_tail`** on `Stream.snapshot`, `scan`, `sql` and `live`
  (1,000,000 rows): what a reader holds from the broker that the published
  tables do not. A snapshot past it is refused and a live view stops, each
  saying publishing is behind, rather than running out of memory waiting on
  a stalled publisher.
- **`Publication.submit(row)` and `submit_many(rows)`**: publish without
  waiting for the acknowledgement. Each returns once the frame is written,
  with a future of the offsets that resolves once the rows are durable. Up
  to `max_in_flight` (`publish(max_in_flight=64)`) are unanswered at once,
  and the server groups what arrives while a commit is in flight — measured
  8,198 rows/s from one publisher against 998 with `send`, which still waits
  for each. `close()` waits for the acknowledgements still owed.

## 0.12.0 — 2026-10-03

### Changed

- **A durable `send` commits on a writer thread of the stream's own**, not
  on the event loop every stream shares. A SQLite commit and fsync ran on the
  loop, so one busy stream stalled the whole broker — measured, a publisher
  sending in a loop kept the loop from running at all for the 3 s it ran,
  and a burst stalled it for up to 71 ms. Streams now commit in parallel,
  and the loop is free while they do. Offsets, order, durability before
  broadcast and the subscribe partition are unchanged. A single publisher
  sending as fast as it can pays a thread hop each way: about 800 rows/s
  where it was about 1,500, at the cost of everything else on the broker.

### Added

- **Group commit, on by default** (`group_commit` on `Stream`,
  `Stream.new`, `Stream.restore` and `Stream.migrate`): sends from several
  publishers that queue behind a commit share the next transaction —
  measured 4,232 rows/s from 8 publishers against ~1,300. Each send's rows
  stay adjacent and every row is durable before its send returns; a lone
  publisher never waits for a group. `group_commit=False` makes each `send`
  or `send_many` its own commit. The greeting's new `group_commit` field
  says which a stream makes; a greeting without it means each send commits
  alone.

## 0.11.0 — 2026-10-03

### Changed — breaking

- **litelink 0.7** (`>=0.7.0,<0.8`).
- **`s3=` is now `s3_options=`** everywhere streamcast takes credentials,
  matching litelink: `Stream.new`, `Stream.restore`, `Stream.migrate`,
  `Stream(...)`, `Stream.snapshot`, `Stream.scan`, `Stream.sql`, `Stream.live`,
  `connect` and `publish`.
- **`Stream.restore(hydrate=)` is gone**, with litelink's `hydrate()`. A
  restored stream's staging table comes back empty; serve its history from
  the published table with `replay_published=True`.
- **The maintainer is litelink's five-process split.** `serve` (and `asgi`)
  start one subprocess per role — `seal`, `compact`, `publish`, `clean`
  (`evict()`, `reclaim("buffer")`, `reclaim("staging")`, `sweep("staging")`)
  and `clean-published` (`reclaim("published")`, `sweep("published")`) —
  each covering every log, where one process ran everything. A minute-long
  push never delays a seal; the cost is five interpreters (~150 MB each)
  where there was one. `Maintain`'s `maintain_every` is replaced by one
  cadence per role: `seal_every` 0.25 s, `compact_every`, `publish_every`
  and `clean_every` 10 s, `clean_published_every` 60 s. `clean` is what
  deletes buffer rows already in staging, which `seal()` and `publish()` no
  longer do. A pass that fails waits its full interval before the next.
- **litestream is opt-in: `serve(replicate=True)`**, where it ran by default
  for logs with `wal_replication`. Such a log served without it gets a
  `UserWarning` naming it, at every start.

### Changed

- **A migrated stream's retired logs are no longer handed to the
  maintainer.** `retire()` published, evicted and swept them completely, and
  litelink refuses them a writer, so each role only printed "cannot open"
  for them. One exception: the `publish` role retires, through litelink,
  any old log a migration before streamcast 0.10 only sealed. With no
  archive such a log was never published, and snapshots, live views and
  catch-up could not read across its seam.

### Fixed

- **A snapshot or live view whose live log has published nothing has that
  log's columns.** A column a migration added was missing from the table
  until the new log first published, so a query naming it failed.

## 0.10.1 — 2026-10-01

### Added

- **`examples/otel/analytics.py`**, in `just demo otel`: real-time analytics
  with `Stream.live`. Every few seconds one SQL query over the live view of
  `spans` prints each service's recent error rate against its long-term
  rate, and p95 latency, flagging a service running hot.

### Fixed

- **A snapshot or live view with no rows yet answers queries.** Its `log`
  table had only `litelink_offset`, so any query naming one of the stream's
  columns failed to bind. It now has the stream's columns, as the live log
  declares them, and the answer is empty.

## 0.10.0 — 2026-10-01

### Added

- **`Stream.snapshot(metadata_uri)`**: a stream's history as of one point,
  read on any machine from its published tables, with the reader's own
  credentials (#32). Every log a migration left behind reads as one table.
  The point is everything published, `as_of_offset=` (with `broker=` for rows
  not yet published, and `LATEST` for the broker's frontier), or `as_of_ts=`
  on `streamcast_ts`. A `Snapshot` has `scan`, `sql` (over the table `log`)
  and `rows`; `Stream.scan` and `Stream.sql` are the one-shot forms.
  `filters=` terms and `start_offset`/`end_offset`, on `scan` and `sql`,
  prune whole retired logs on the manifest before any is opened. SQL is not
  mined for terms yet (#57). Anything it cannot answer exactly raises `SnapshotUnavailable`.
- **`Stream.live(broker)`**: a stream's history kept current in memory
  (#59), found from the broker's greeting. The published tables plus the
  broker's rows as they arrive, so `scan` and `sql` answer as of the newest
  row received, and
  `wait_for(offset)` or `wait_for(ts=)` waits until that point is visible —
  a time once a row stamped after it arrives, so on a quiet stream it waits
  for the next row. Memory holds only what is
  not yet published: the base is re-pinned every `rebase_every` seconds and
  after every reconnect. Dropped connections reconnect with catch-up; a
  failure that can't be fixed is raised by the next query. `where=` narrows
  the view on the server and the published tables alike, and
  `start_offset=` (or `LATEST`, from now) is the lowest offset it sees.
- **`Stream.metadata_uri`**: where a reader finds the stream's metadata file.
- **`connect(metadata=)`**: the metadata file `catch_up` reads from, in place
  of the greeting's.

- **`examples/keyed_table/`**: orders written as a keyed table log (each row a
  whole record, keyed by id, with a `deleted` flag), and a subscriber that
  keeps the table in SQLite: the last row by id, where not deleted. It writes
  the offset it applied in the same transaction, so it resumes on its own.
  Also branches built on it: each client forks main's view into a private
  database, follows its own `branch_id`, and commits with one `send_many`
  onto main.
- **`just demo book`**: a producer publishing Bitstamp's live BTC/USD order
  book as a keyed table log, and a static browser page that subscribes and
  keeps the table in AG Grid as rows arrive.
- **`examples/migration/`**: a pipeline of streams, A -> B -> C, and a
  schema migration of B tested in a shadow, D -> E, against production's live
  and historical data without production waiting on it. Old and new are
  diffed with one DuckDB join on the source offset each row carries; `--bug`
  shows a wrong migration named order by order.

### Changed — breaking

- **litelink 0.6** (`>=0.6.1,<0.7`), and its vocabulary with it (#42). The
  archive is the published table: `Stream.new` and `Stream.restore` take
  `published=` for `archive=`, and `replay_published=` for
  `replay_archive=`. Every log publishes; without a location, to a table
  under its own directory.
- **The protocol is version 4.**
  - **Every frame is `[offset, ts, msg]`**, where it was `[offset, msg]`.
    `ts` is `streamcast_ts`, when the server took the row, in UTC
    microseconds: the value the log stores, so a replayed or caught-up frame
    carries the same `ts` it did live. A stream with no log sends its send
    time; a log created before the column existed sends `null`.
  - **Subscriptions yield `(offset, ts, msg)`**: `offset, ts, msg = await
    sub.recv()` and `async for offset, ts, msg in sub`. In JavaScript,
    `const [offset, ts, msg] = JSON.parse(frame)`. `Snapshot.rows` yields
    the same triple.
  - **The greeting's `metadata`** (the metadata file's URI) and `stream_id`
    replace `log`, whose `name`, `archive` and `owned` are gone; a log's
    system columns are in its metadata entry's `system_schema`. Refusals no
    longer carry a location.
- **`connect(archive=)` is gone**, replaced by `connect(metadata=)`.
- **The metadata file is version 2**: each log records `published`, and its
  `streamcast_ts` range as `start_ts` and `end_ts`. Version 1 is still read,
  and `serve` rewrites it as version 2.

### Changed

- **`catch_up=True` is built on `Stream.snapshot`**, so it reads every log of
  a migrated stream and crosses the seam a server refuses as `evicted`.
- **`Stream.migrate` retires the old log** with litelink's `retire()`: sealed,
  published in full, and refusing writers from then on. A migration that
  died after the retire finishes on the rerun. A log with
  `wal_replication` gets its sidecar started for the call.
- **The maintainer publishes every log** on each sweep.

- **The examples are rewritten as producer, broker and subscriber**, each its
  own process, because that is the shape to copy. `examples/broker.py` is one
  generic broker (`--stream NAME=SCHEMA`); `examples/trades/` holds the
  Bitstamp producer and the resuming consumer that replace `server.py` and
  `consumer.py`; `fastapi_app.py` is a broker only, with the producer
  publishing to its mount; and the OTel services are a producer of their
  own (`otel/services.py`).
- **`just demo NAME`** replaces the `demo-*` recipes and starts every process
  a demo has, in one terminal with each line labelled by its role, stopping
  them all on Ctrl-C. `just demo` alone (or `--help`) lists every runnable
  demo; `just demo trades` is the one to start with, and `just demo consumer`
  adds one more consumer from a second terminal.

### Fixed

- **Ctrl-C on a server in a terminal could cut the maintainer's last seal
  short.** The terminal signals its whole foreground process group, and the
  maintainer was in it: interrupted at once, then sent SIGTERM by the stopping
  server while already in its final seal pass, it abandoned that pass
  mid-transaction and printed a traceback. The maintainer and the litestream
  sidecar now run in sessions of their own, so only the server hears the
  terminal, and it stops them.
- **`just demo otel`** no longer loses its dashboard on the first export.
  otel-gui loads its trace and logs protobuf definitions lazily into one
  shared root, and the exporter's first traces and logs requests arrive
  together; the interleaved loads crash otel-gui. The demo now sends one
  empty request to each, in turn, before the exporter starts.

## 0.9.0 — 2026-09-30

### Added

- **`examples/otel/`** and **`just demo-otel`**: OpenTelemetry logs and
  traces from two simulated services, published to two streams, and
  re-exported as OTLP by `examples/otel/export.py` (OpenTelemetry's own
  exporters) to otel-gui's local dashboard. The OTel schemas and conversions
  live in the example; OTel is a dev dependency only.

- **`Stream(name, schema=...)`** — a stream without a log can declare a
  schema, and then accepts exactly the rows a stream with a log would: every
  row goes through litelink's `validate_row`, so a wrong type, an unknown or
  missing column, or a non-finite float is refused with the same message and
  sent to no one. The greeting publishes the schema, frames follow its column
  order, and `where=` is checked against it. Without a schema, nothing is
  checked, as before, and a non-finite float reaches subscribers as `null`.

- **Binary and nested columns.** A stream can declare `binary` and
  `fixed_size_binary(N)` columns (`{"type": "string", "contentEncoding":
  "base16" | "base64"}`, with `format: "bytesN"`), and structs, lists and
  maps (`object` + `properties`, `array` + `items`, `object` +
  `additionalProperties`), nested at any depth. Binary is `bytes` in Python
  and text on the wire in the column's encoding, both ways: a remote publisher
  sends text, a `Subscription` hands back `bytes`. Replayed frames stay
  byte-identical to live ones, maps included. `where=` filters binary columns
  by their text and refuses nested ones. Requires litelink 0.5.

- **`<stream>.manifest.parquet`**, the sealed logs' per-column statistics. Each
  `Stream.migrate` adds the retired log's bounds and counts, read from its
  Iceberg manifests through litelink's `column_statistics()`, and writes the
  file before `metadata.json`, which points to it. A reader of the whole
  stream prunes on it (#32). Pruning fails towards including a log in every
  case it cannot decide. Requires a litelink release that provides
  `column_statistics`.

- **`Stream.migrate`** — change a stream's schema. With the server stopped, it
  seals the current log for good and creates the next (`trades-v2`, …) with
  the new schema. The new log starts at exactly the old one's end offset, so
  offsets stay one dense sequence. A metadata file records the logs, locally and
  in the archive, each with its user `schema` and its `system_schema`. It is
  idempotent, so it can sit in a server's startup, and migrating with an
  unchanged schema is the upgrade onto today's system columns — how a log
  from before `streamcast_ts` gains it.
- **`_log.SYSTEM`** — the one definition of the columns streamcast owns, as
  JSON Schema. A system column's type never changes; one that needs a new
  type gets a new name.

  Columns may be added and removed. **A column's type is fixed for the life of
  the stream**, including after removal, because the logs are read together
  with `UNION ALL BY NAME`, where a changed type coerces silently.

### Changed

- `Stream.new` and `Stream.restore` open the log a stream's metadata names as
  live.
- **`serve` writes every durable stream's `metadata.json`** before it listens
  (the ASGI app at lifespan start), uploads it when there is an archive, and
  **refuses to start if it cannot**. It holds `stream_id`, `sealed_logs`,
  `live_log` and a `manifest` pointer for #27. A stream from an earlier
  release gains its file at its first `serve`. A `Stream(log=…)` handed a
  sealed log is refused.
- A migrated stream's server replays only its current log. A subscribe below
  the seam is refused `evicted` with `earliest` at the seam, rather than being
  served an empty replay. Reading across the seam is #32.
- **litelink is capped at `>=0.5.1,<0.6`.** Under 0.x a minor is litelink's
  breaking release, so a streamcast release now resolves only the litelink
  line it was tested against, and each release moves the cap.

### Fixed

- **A SIGKILLed server left its maintainer running** (#45), reparented to
  PID 1: ~207 MB of interpreter still contending for the logs' leases, and
  holding the server's inherited file descriptors. On Linux it now starts
  with `PR_SET_PDEATHSIG`, as the litestream sidecar already did, through one
  shared helper; a test SIGKILLs a real server and checks both children go.

- **`streamcast.publish` sent `bytes` as base64 to every binary column**,
  whatever its declared encoding, so a `base16` column rejected every row
  published from Python. The publisher now applies the column's encoding, as
  the subscriber's `recv` already did. JSON publishers sending hex text were
  unaffected.

## 0.8.0 — 2026-09-29

### Added

- **`streamcast_ts`** — every row a server stores carries the time the server
  took it, in UTC microseconds, so `streamcast_ts - event_ts` is feed latency
  per row over the whole archive. `Stream.new` creates every log with the
  column. It is stored and never sent: no frame carries it, the greeting's
  `schema` leaves it out, and invariant 10 holds. `send_many` stamps its group
  with one value, because the group commits as one transaction.
- **`Greeting.log.owned`** — the table's columns that `schema` leaves out:
  `litelink_offset`, and `streamcast_ts` on a log that has it. Additive, so
  the protocol version is unchanged.

### Changed

- The name `streamcast_ts` is reserved. `Stream.new` refuses a declaration
  that uses it, and `send` refuses a row that supplies it. A log created
  before this release opens unchanged and is not stamped; a handle passed as
  `Stream(log=)` is stamped only if its schema has the column.

## 0.7.0 — 2026-09-26

### Added

- **`connect(uri, where={...})`** — server-side subscription filtering. A
  subscriber names equality or membership over the declared columns and is
  sent only matching rows, carried as `?where=` so `wscat` can use it too.

  The predicate runs against the dict `send` was called with, so the shared
  encode is untouched: a filter decides whether to enqueue the one frame every
  subscriber gets rather than building a second. 162 ns for one term against
  961 ns for the encode already on the path, compiled once at subscribe and
  specialised on arity.

  **The replay is filtered through the same predicate**, because a resume that
  delivered what the live connection would not is the failure worth preventing.
  One consequence: `replay` in the greeting becomes an upper bound rather than
  a count when a filter is set, so §6b's recovery loop wants an unfiltered
  subscription. `Greeting.where` echoes what the server applied.

  A column the schema does not have is a 4400 naming it, not a subscription
  that silently never delivers.

## 0.6.0 — 2026-09-26

### Changed

- **One maintainer and one litestream per serving process**, not one of each
  per served log. A maintainer is a full interpreter with litelink, pyarrow,
  pyiceberg and duckdb loaded — 149 MB RSS measured — so four streams cost
  596 MB one-per-log against 149 MB shared, and litestream added 40-170 MB per
  process on top. The marginal cost was the worse half: a stream taking a row
  a minute cost the same as the busiest one, which made "should this be its
  own stream" a resource question it should not be.

  The maintainer sweeps its logs in one loop with the `try` INSIDE it, so a
  log whose recovery fails costs that log a pass and the others nothing, and
  `maintain()` is staggered across logs so N of them do not come due together.
  `Maintain(dedicated=("trades",))` gives a named log its own process.

  litestream takes a merged `dbs` config. **The flock stays per log**, because
  it protects the database rather than the replicator: the sidecar holds one
  lock per log and replicates exactly the logs it holds, so two servers under
  one root divide the databases between them instead of one replicating
  nothing. A lock that frees up mid-run is taken and the process restarted,
  since litestream reads its `dbs` once at startup.

  **Breaking for the hand-run maintainer.** `python -m streamcast maintain`
  took `--root PATH --name NAME` and now takes `--log PATH NAME`, repeatable.
  Two values rather than one `root:name` string because a root is a path and
  a path may contain a colon. `serve` is unaffected.

### Added

- **`Stream.stats`** and **`serve(stats=True)`** — the numbers needed to tell a
  quiet stream from a dead one, which a subscriber cannot do. `stats` is a
  property over counters already held; `serve` publishes every stream's on
  the port it already has, at `/stats` or a path you name, and a
  `process_request` of your own composes with it rather than being replaced.

  **On by default, unlike `publish=`.** That grants writes; this discloses
  strictly less than the socket beside it — a wrong-path connect already names
  every stream served, the greeting already carries `end_offset`, and anyone
  who can reach the port can subscribe and read every row. `stats=False` turns
  it off.

  It carries **no verdict** — no `status`, no threshold — because freshness is
  domain knowledge: a five-second socket is broken after thirty seconds while
  a daily publisher is healthy after twenty-three hours. Classification is the
  application's, and `examples/fastapi_app.py` shows both halves.

  `last_send_*` is None until the process sends something, which means *not in
  this process* rather than *never*. `uptime_s` is published beside it because
  a restart otherwise reads exactly like a stall: a large `end_offset` and
  nothing sent.

- **`streamcast.asgi`** — the same streams as an ASGI app, mountable in an
  existing FastAPI or Starlette service instead of running `serve()` on a
  second port. `pip install 'streamcast[asgi]'` adds Starlette and nothing
  else. `async with` the returned object from the host app's lifespan, which
  is what owns the maintainers: Starlette does not run a mounted sub-app's
  lifespan, so nothing else would start them.

  Keepalive and compression become the ASGI server's settings, and their
  defaults are not this library's — `docs/API.md` has the table and why
  compression is the one that bites.

  `examples/fastapi_app.py` is a complete service that runs: `just
  demo-fastapi`, then point `just demo-consumer` at
  `ws://127.0.0.1:8000/streams/trades`.

- **`streamcast._transport.Peer`** — the connection surface the stream layer
  uses, named so a second transport can satisfy it. `send`, `close`,
  `wait_closed`, `async for`, and nothing else; `_stream` and `_subscriber`
  are written against it and neither transport knows the other exists.

## 0.5.0 — 2026-09-24

### Added

- **`streamcast.publish(uri)`** — a producer that is not the server's process.
  The server appends with the same `Stream.send` / `send_many` a local
  publisher calls, so `send` returns once the row is durable and `send_many`
  is the same one-transaction lever it is locally. A sibling of
  `Subscription`, not a method on it: a connection is one end or the other.

  **It adds no authority**, which is the argument for it. litelink allows one
  writer per log and neither refuses a second nor detects one, so two
  `WriteHandle`s on one log is a corruption path with no guard. Publishing to
  the process that already holds the handle resolves the concurrency where it
  can be resolved — any number of publishers, one writer. Offsets stay
  contiguous and a batch stays one commit under racing publishers, which I1
  gives for free.

- **`serve(..., publish=True)`**, off by default, so an upgrade cannot make a
  server writable on its own. A publisher meeting a server that does not allow
  it is told which setting to change.

- **`publish(uri, cursor=…, cursor_uri=…)`** records the offset this
  publisher was last acknowledged for and ships it to object storage, the same
  two keywords `connect` takes. One integer is enough: the recovery replay
  scans INCLUSIVE of it, so the first row delivered is this publisher's own
  last one and the sequence it carried comes back out of the log. It does not
  resume by itself — `resumed_from` reports it, `commit()` forces a save, and
  the publisher acts on it, because a producer cursor says where it got to
  and not what to send next.

- **`Rejected`** for a row the schema refuses, carrying litelink's message and
  the column it names. Nothing is committed and the connection stays open, so
  the next row works — what `Stream.send` raising does locally.

  Publishing is **at-least-once under retry**: a dropped connection before the
  reply leaves the publisher unable to say whether the append happened.
  `docs/SPEC.md` §6b has the publisher-key pattern that turns recovery into a
  query against the log.

## 0.4.0 — 2026-09-23

### Changed — breaking

- **`cursor_uri` names the object, not the prefix holding it.** A trailing `/`
  used to mean "append the local file's name", so the remote key depended on
  what the local one happened to be called: renaming a local file moved the
  remote object, and one `cursor_uri` shared by two consumers with different
  local names wrote to two places while reading as one setting. A URI
  identifies one object. A prefix is now refused, naming the key it would have
  built, and the local path and the remote key are independent —
  `cursor=".trades.offset"` with
  `cursor_uri="s3://bucket/consumer1/stream.offset"` is an ordinary pairing.

## 0.3.0 — 2026-09-23

### Added

- **`Stream.restore(name, root=…, archive=…)`** — server-side failover, the
  counterpart to `connect(cursor=)` on the consumer side. Rebuilds the log
  from the archive and the replicated WAL on a box that never held it, and
  returns a stream ready to `serve` and `send` to. `hydrate=` re-registers
  archived files into the local tier; it has no default because it costs
  egress and the window is the caller's.

  Offsets are **fenced, not reissued** — litelink burns 2**20 — so no offset a
  consumer holds is ever handed out again carrying different data. The fence
  is also a million wide, so existing cursors look a million behind: a
  failover meant to be transparent restores with `max_replay=None` and
  `replay_archive=True`. `catch_up` still recovers their data from the archive
  but cannot rejoin the live stream across a range that was never issued.
  Measured, and documented in SPEC §8b.
- `CatchUpUnavailable` names both causes of a gap that will not close — an
  archive falling behind, and a restore fence — because the fixes differ and
  the message asserted the first.

  ⚠️ Stop the old producer first. The fence prevents offset reuse; nothing
  prevents two writers, and litelink cannot detect a live one on another host
  — [litelink#75](https://github.com/nhobin219/litelink/issues/75).

### Changed

- Requires litelink **0.4.1**, where `restore(include_archive=)` reaches the
  handle it builds. In 0.4.0 it was accepted and dropped.

## 0.2.0 — 2026-09-22

### Changed — breaking

- **The greeting publishes the log as an object**: `"log": {"name", "archive"}`,
  replacing the flat `"archive"` field, and the protocol version goes to 2 —
  a 0.1.0 client gets a clear `ProtocolError` rather than a silent
  misreading. `info.log` is what `litelink.snapshot` takes, so a subscriber
  holding a greeting can read the whole history straight from object storage
  instead of through the socket.

### Fixed

- **Catch-up worked only when the log was named after the stream.** They need
  not be equal: `Stream.new` feeds one name through, `Stream(log=handle)`
  takes a log the caller named. `catch_up` asked the archive for a table named
  after the STREAM, found nothing, and reported it as a credentials failure —
  naming an endpoint and a credential chain for a problem that was neither.
  The server knows the log's name, so the greeting carries it.

## 0.1.0 — 2026-09-22

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
