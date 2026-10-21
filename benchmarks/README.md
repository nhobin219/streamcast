# benchmarks

```
just bench                       # fan-out and publish
just bench --subscribers 500 --messages 100000
just bench-replay                # replay, and which layer it is spent in
just bench-replay --rows 200000
just bench-snapshot              # reading a stream's history: 100k rows in 1, 4, 16 logs
just bench-snapshot --rows 1000000 --logs 1 8
just bench-identity build --rows 1000000    # rerun to finish four cached tables
```

Three numbers, because three different things could be the bottleneck and only one
of them is streamcast's.

**encode** — building one frame. Done once per message however many subscribers
there are. If this scaled with subscriber count, the library's central claim would
be wrong.

**send** — the queue insert per subscriber, with no sockets. Linear in subscribers
by construction; the question is the constant.

**send / send_many, durable** — the same call with a litelink log attached. One
SQLite transaction at `synchronous=FULL`, dominating everything above it by two
orders of magnitude. That gap is the entire argument for `send_many`.

`bench-replay` is separate because a replay has a different shape: real fixed
setup per scan and then a cheap per-row walk, so a single us/row figure taken at
one size is meaningless. It reports cold against warm, fixed against marginal,
and the split across DuckDB, Arrow and the encoder — and then the arithmetic
that sizes `max_replay` against `max_backlog` for your hardware. The numbers in
[`docs/SPEC.md`](../docs/SPEC.md) §4 come from it, after an earlier version of
that section stated two guesses as measurements and had both about 2x wrong.

`bench-snapshot` measures `Stream.snapshot` over a stream written as `--logs` logs, each
a migration adding a column, so the read is the real `UNION ALL BY NAME`. It reports the
cold first query, the floor (opening a snapshot, reading one row), a full count, scan and
aggregate, and the same selective read written as `where=` and as `filters=`, which is
what pruning saves, beside what deciding the prune costs. Each config runs in its own
process, so every cold figure is cold.

Then an end-to-end pass over loopback with real subscribers, reporting p50 and p99
delivery latency. p99 rather than a mean, because the interesting failure is a
tail: a fan-out that is fast on average and occasionally parks is a fan-out that
is awaiting something it should not be.

Numbers move with hardware. Measure before and after **in the same session on the
same machine** — a comparison across two runs on two boxes says nothing. If a
change costs throughput, say so in the commit with the figures rather than leaving
it to be discovered.

## `bench-identity`: what `_streamcast_id` costs, in object-store bytes

Measured on 20 million rows in [`results/identity/`](results/identity/README.md).
Fresh bounded 1M/5M measurements, cache conditions and limitations are in
[`results/identity-bounded/`](results/identity-bounded/README.md).

```
just rustfs
just bench-identity build --rows 1000000 --out bench-identity     # run four times; one table per invocation
just bench-identity measure --out bench-identity --core --repeat 3 --heavy-repeat 3
just bench-identity measure --out bench-identity --only lookup --repeat 3
just bench-identity clean --out bench-identity                    # delete the tables
```

A stream that declares an identity carries a 16-byte hash of it on every row,
and a lookup by the whole key filters on it. Whether that is worth having is a
question about what a reader fetches from object storage, so this measures
that as well as latency: a forwarding proxy (`s3proxy.py`) sits between the
reader and the store and records every request and the bytes it returned.

`build` generates one sessions-like table (`--concurrent` sessions open at
once, each a dense run of `line_number`, interleaved in time order as a live
feed writes them) and writes it twice through `Stream.ingest`, as two streams
published to S3: `natural`, without an identity, and `keyed`, with
`identity=("session_id", "line_number")`. The same seed gives the same rows.
A second stream, `labels` (every tenth row's key and a label), is written the
same two ways, for a join of two streams on the key. `build.json` under
`--out` records the metadata URIs, sample keys for the lookups, ingest times,
the Parquet layout read back from the footers (files, row groups, compressed
bytes and encodings per column) and the versions it ran on. The local logs
are deleted once published; readers use only S3.

Completed tables are checkpointed atomically after each build and verified against
their remote Parquet layout before reuse. `--max-tables` defaults to one; repeat
the same command until all four are present. Use separate `--out` directories for
1M, 5M and 10M rows: a changed shape is refused rather than mixed with cached data.
An interrupted table is retried under a new prefix. Its orphaned objects remain
under the build's prefix until `clean`. Do not run two builders against one output
directory. `--core` limits measurement to the SQL lookup, dedupe and join arms;
each arm has a 180-second timeout (`--arm-timeout`) and `--resume` reuses completed
arms. Keep the same measurement settings when resuming.

For dedupe and join, "hash beside natural" means grouping or joining on both
representations. The manifest alternative plans on the required natural columns'
non-null bounds; these global operations have no selective natural predicate, so
every file survives. This measures planning overhead and hash-only execution,
without claiming file pruning where the workload offers none. The report checks
agreement of the recorded answers across arms, ignoring the ingestion timestamp.

`measure` runs every arm in `identity.py`'s `ARMS`: a question (a lookup by
the full key, as `SELECT *` and as an existence count; 64 keys at once; a
dedupe; a join of two streams) asked one way of one stream:

- `streamcast`, through `Stream.snapshot`, including the key term `filters=`
  adds on a keyed stream;
- `duckdb`, the same SQL over a bare `iceberg_scan`: what DuckDB does alone;
- `pruned`, files chosen from the Iceberg manifests on the natural columns'
  bounds (pyiceberg `plan_files`, metadata only), then only those files read,
  filtered on `_streamcast_id` alone. This is the one design in which the hash
  could save reads, so it is measured rather than argued.

Each arm runs `--repeat` times cold (a fresh connection or snapshot, DuckDB's
caches off) and `--repeat` + 1 times warm (one cached connection; the first,
`fill`, is kept apart), both through the proxy and straight to the store.
Both direct and proxy latency are reported; the latter includes the Python
forwarding hop. `--proxy-only` omits the direct phases for a bounded traffic run.
For every
proxied run it records the requests, the bytes, and **those bytes by Parquet
column and the number of row groups each was read from**, from mapping each
ranged GET onto the column chunks in the file's footer: this is what says
whether DuckDB read `session_id` for a lookup that also names
`_streamcast_id`. Each arm also gets one cold `EXPLAIN ANALYZE`, saved under
`explain/` with the bytes it read. It writes `measure-<time>.json` (every
run) and `measure-<time>.md` (the tables).

"Cold" is DuckDB-cold, not disk-cold: the store serves the files from the page
cache of the box that wrote them, over loopback, so latencies are a floor
with no network round trip in them. Bytes and requests carry over to a remote
store; latencies do not. The build needs about 1.4x one stream's size free on
the local disk while it writes it, and the store keeps all four until `clean`.
