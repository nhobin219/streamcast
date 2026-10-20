# benchmarks

```
just bench                       # fan-out and publish
just bench --subscribers 500 --messages 100000
just bench-replay                # replay, and which layer it is spent in
just bench-replay --rows 200000
just bench-snapshot              # reading a stream's history: 100k rows in 1, 4, 16 logs
just bench-snapshot --rows 1000000 --logs 1 8
just bench-identity build --rows 20000000   # then `measure`, then `clean`
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

```
just rustfs
just bench-identity build --rows 20000000 --out bench-identity    # ~8 min, ~2.5 GB in the store
just bench-identity measure --out bench-identity                  # every query, 3 times
just bench-identity measure --out bench-identity --only lookup --modes raw
just bench-identity clean --out bench-identity                    # delete the tables
```

A stream that declares an identity carries a 16-byte hash of it on every row,
and a lookup by the whole key filters on it. Whether that is worth having is a
question about what a reader fetches from object storage, so this measures
that and not wall time: a forwarding proxy (`s3proxy.py`) sits between DuckDB
and the store and records every request and the bytes it returned.

`build` generates one sessions-like table (`--concurrent` sessions open at
once, each a dense run of `line_number`, interleaved in time order as a live
feed writes them) and writes it twice through `Stream.ingest`, as two streams
published to S3: `natural`, without an identity, and `keyed`, with
`identity=("session_id", "line_number")`. The same seed gives the same rows.
It writes `build.json` under `--out`: the metadata URIs, sample keys for the
lookups, the ingest time of each, the Parquet layout read back from the files'
footers (files, row groups, compressed bytes per column), and the versions it
ran on. The local logs are deleted once published; readers use only S3.

`measure` runs each query in `identity.py`'s `QUERIES` against each stream,
two ways: through `Stream.snapshot` (what streamcast does, including the key
term `filters=` adds on a keyed stream) and as the same SQL over a bare
`iceberg_scan` (what DuckDB does on its own). Reads are uncached. For each it
prints, and writes to `measure-<time>.json`:

- the requests and bytes the store served, median of `--repeat`;
- **those bytes by Parquet column**, from mapping each ranged GET onto the
  column chunks in the file's footer. This is what says whether DuckDB read
  `session_id` to answer a lookup on `_streamcast_id` alone, or read
  `text` for row groups no row matched;
- the rows returned and the first of them, so two arms that should agree can
  be checked to.

Wall times go through a Python proxy and are not the store's; compare bytes
and requests. The build needs about 1.4x the table's size free on the local
disk while it writes one stream, and the store keeps both until `clean`.
