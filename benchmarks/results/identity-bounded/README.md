# Bounded identity benchmark, 2026-10-21

**Recommendation: change PR #180 before merging. Remove the automatic addition of
the hash predicate to natural-key lookups.** It increases cold object-store reads
on both measured layouts, and DuckDB fetches both natural filter columns as well
as the hash. The manifest-first alternative also reads more here. These results
do not justify a general lookup optimization or closing #159 as a demonstrated
read-saving feature. An opt-in stored identity for a consumer's particular dedupe
or join remains a separate decision that needs that consumer's measurements.

This is a benchmark and documentation change only. It does not remove the rewrite,
merge the feature, close the issue, or release anything.

## What ran

The library code is PR #180 head `cff995559009e7812b3a631b88941bfd648f06d5`.
All fresh work uses the same two-vCPU machine and running
`rustfs/rustfs:1.0.0-rc.4` container over loopback. Python 3.11.17, DuckDB 1.5.5
(iceberg `45163a28`, httpfs `827222f`), litelink 0.14.0, PyArrow 25.0.1,
xxhash 4.0.1, NumPy 2.4.6; DuckDB memory limit 1 GB and disk spilling enabled.
The clock source is `kvm-clock`.

- **1,000,000 main rows plus 100,000 label rows**, each written with and without
  identity: 15 SQL arms, 210 records. Three cold and three warm repetitions plus
  one separate cache-fill run, both through the proxy and directly to rustfs.
  These arms cover payload lookup, full-table dedupe and a two-stream join.
- **5,000,000 main rows plus 500,000 label rows**, likewise written both ways:
  20 SQL arms, 140 records. Three cold and three warm repetitions plus a separate
  fill run, through the proxy only. This adds existence lookups to isolate filtering
  from payload projection. Direct-store latency at this size was not measured.
- Every new build invocation completes at most one table, saves `build.json`
  atomically, and verifies and reuses the earlier tables on the next invocation.
  All eight build invocations completed within their external 120-second (1M) or
  180-second (5M) limits. The slowest recorded ingest was 53.4 seconds; that timer
  excludes setup and final metadata work, so it is not the full invocation time.
  The earlier 20M build and results were preserved, not rerun or pooled with these
  measurements. No new 10M or 20M build was attempted.

Each seed-180 generator uses 1,000 concurrent UUID sessions with mean length 2,000,
interleaved in time order, batches of 250,000; labels contain every tenth key.
The keys are unique: dedupe returns zero duplicated keys. Joins return 100,000 or
500,000 matches with the same label sum across arms. Lookup repetitions use the
same three dispersed existing keys in every arm. The report checks equality of
recorded result summaries across representations (the text field and ingestion
timestamp are excluded); it is not a full-table byte-for-byte equivalence check.

The primary comparison uses the **same keyed files** for all three designs:
natural predicates/group/join; hash beside natural predicates/group/join; and
natural-column manifest selection followed by hash-only execution. Separate
unkeyed streams and plain hash-only SQL are controls in the raw records.
Payload lookup projects the same columns, excluding `_streamcast_id`.

"Cold" means a fresh DuckDB connection with its caches disabled, **not cold server
disk**: rustfs retains the operating-system page cache. "Warm" reuses a connection
after a separate fill, but changes lookup keys between repetitions; payload reads
can therefore remain. Connection setup, initial metadata-URI resolution and footer
attribution setup are excluded. Manifest planning is included on every pruned query.
There is no randomized arm order or statistical significance claim.

The proxy counts response-body bytes and requests, not HTTP headers or TLS traffic.
Latency below is wall time through that proxy, including its Python forwarding hop.
The 1M direct-store measurements are separate in `m1.json`; they are not mixed into
proxy latency. No abandoned response bodies occurred in either completed run.

## Measured layout

| main rows / stream | rows | files / row groups | total MB | natural key MB | hash MB | hash / natural |
|---|---:|---:|---:|---:|---:|---:|
| m1 / natural | 1,000,000 | 1 / 3 | 45.032 | 2.471 | 0.000 | — |
| m1 / keyed | 1,000,000 | 1 / 3 | 61.435 | 2.491 | 16.381 | 6.57x |
| m1 / labels_natural | 100,000 | 1 / 1 | 0.621 | 0.284 | 0.000 | — |
| m1 / labels_keyed | 100,000 | 1 / 1 | 2.348 | 0.284 | 1.726 | 6.07x |
| m5 / natural | 5,000,000 | 1 / 14 | 226.030 | 14.111 | 0.000 | — |
| m5 / keyed | 5,000,000 | 1 / 15 | 308.707 | 14.161 | 81.904 | 5.78x |
| m5 / labels_natural | 500,000 | 1 / 1 | 2.473 | 1.510 | 0.000 | — |
| m5 / labels_keyed | 500,000 | 1 / 1 | 10.601 | 1.510 | 8.128 | 5.38x |

These are compressed Parquet column-chunk sizes, read from the stored footers;
total file sizes include all columns and footers. MB means 1,000,000 bytes.

## Same-file query comparisons

### 1M main rows

| operation / design | proxy cold ms | proxy warm ms | cold / warm MB | cold / warm requests |
|---|---:|---:|---:|---:|
| lookup by full key / natural | 807 (709–816) | 116 (95–384) | 17.419 / 0.000 | 13 / 1 |
| lookup by full key / beside | 903 (694–993) | 283 (107–405) | 33.800 / 0.000 | 16 / 1 |
| lookup by full key / manifest | 1097 (1011–1304) | 709 (495–791) | 32.098 / 0.011 | 15 / 10 |
| dedupe on the key / natural | 995 (971–1007) | 791 (706–796) | 2.568 / 0.000 | 8 / 1 |
| dedupe on the key / beside | 1601 (1501–1797) | 1200 (1099–1202) | 18.949 / 0.000 | 11 / 1 |
| dedupe on the key / manifest | 1403 (1393–1700) | 1113 (1104–1290) | 16.457 / 0.011 | 14 / 10 |
| join two streams on the key / natural | 601 (512–606) | 198 (195–205) | 2.917 / 0.000 | 17 / 2 |
| join two streams on the key / beside | 908 (678–1105) | 201 (200–398) | 21.023 / 0.000 | 21 / 2 |
| join two streams on the key / manifest | 1482 (1205–1684) | 1004 (914–1093) | 18.248 / 0.021 | 26 / 20 |

### 5M main rows

| operation / design | proxy cold ms | proxy warm ms | cold / warm MB | cold / warm requests |
|---|---:|---:|---:|---:|
| lookup by full key / natural | 897 (896–1089) | 401 (191–501) | 28.485 / 14.830 | 35 / 3 |
| lookup by full key / beside | 1392 (1303–1496) | 312 (207–393) | 104.028 / 14.830 | 47 / 3 |
| lookup by full key / manifest | 1286 (1218–1405) | 696 (493–900) | 97.983 / 15.816 | 27 / 11 |
| lookup exists / natural | 586 (518–697) | 118 (100–186) | 13.644 / 0.000 | 33 / 1 |
| lookup exists / beside | 1489 (1219–2108) | 204 (195–297) | 89.675 / 0.000 | 46 / 1 |
| lookup exists / manifest | 1194 (1092–1289) | 526 (499–679) | 82.178 / 0.011 | 26 / 10 |
| dedupe on the key / natural | 4588 (4107–6785) | 5299 (5086–5806) | 14.434 / 0.000 | 20 / 1 |
| dedupe on the key / beside | 5601 (5011–5998) | 5626 (5478–6299) | 96.338 / 95.946 | 35 / 32 |
| dedupe on the key / manifest | 3593 (3505–3990) | 2794 (2787–3204) | 82.178 / 0.011 | 26 / 10 |
| join two streams on the key / natural | 1210 (1187–1230) | 703 (702–802) | 16.161 / 0.000 | 41 / 2 |
| join two streams on the key / beside | 2016 (1696–2193) | 776 (619–1013) | 106.193 / 0.000 | 57 / 2 |
| join two streams on the key / manifest | 2599 (2378–2706) | 1607 (1513–1778) | 90.524 / 0.021 | 38 / 20 |


Latency is median (minimum–maximum), in milliseconds, over three repetitions.
Bytes and request counts are medians; per-run values and byte ranges attributed
to columns are in `m1.json` and `m5.json`.

## What this establishes and what it leaves open

On one cold 5M payload lookup with the added hash predicate, the proxy observed
76.43 MB from `_streamcast_id` in 14/15 row groups, 7.12 MB from `line_number`
in 15/15, and 5.89 MB from `session_id` in 12/15. The natural columns are still
fetched as filter inputs; the payload is fetched from one row group. This directly
answers whether this DuckDB version avoids those columns when the hash is added:
it does not on the measured query. Footer read-ahead can incidentally overlap a
column even when that column is not part of the query; small attributed reads
alone should not be interpreted as semantic filtering.

Every main table has one file (1M natural/keyed: 3/3 row groups; 5M: 14/15).
Manifest selection retains **1 of 1** for every lookup. Global dedupe and join
have no selective predicate; their natural non-null bounds retain every main and
label file. The manifest-first dedupe/join therefore measure planning plus a
hash-only full scan, not successful file pruning. The existence arm shows the
hash-only filter without projecting natural columns.

The stored hash is materially larger than the repeated UUID and small-integer
natural columns here. The 5M manifest/hash dedupe is a qualified positive:
3,593 ms cold versus 4,588 ms for natural columns, and 2,794 versus 5,299 ms warm,
at 82.178 MB versus 14.434 MB of cold reads. Plain hash-only `iceberg_scan` dedupe
is 4,583 ms cold / 3,303 ms warm: essentially tied cold and faster warm. This
advantage does not hold at 1M. Manifest arms also change the scanner from
`iceberg_scan` to `read_parquet`; no natural-only `read_parquet` control was run,
so that dedupe gain cannot be attributed solely to hashing or planning. No result
here establishes a remote-store latency benefit or a general dedupe/join win.
The raw latency spreads, cache states and I/O costs must travel with any timing
comparison. Warm-cache performance cannot be presented as avoiding cold reads.

Unverified: a clustered or partitioned multi-file layout where natural bounds
actually prune files; 10M fresh runs; high-cardinality or wide natural keys that
compress less well; duplicate-heavy data; remote-network latency; ingestion CPU
overhead isolated from generation and storage; fresh 20M replication of the old
results; and these new arms through `Stream.snapshot` rather than SQL over its
published tables. The older `results/identity/` run is historical evidence only.

## Reproduce

```sh
just rustfs
# Execute each build command four times; each call verifies and reuses completed tables.
just bench-identity build --rows 1000000 --out bench-identity-1m
just bench-identity build --rows 5000000 --out bench-identity-5m
just bench-identity measure --out bench-identity-1m --core --repeat 3 --heavy-repeat 3 --memory-limit 1GB --no-explain
just bench-identity measure --out bench-identity-5m --core --repeat 3 --heavy-repeat 3 --memory-limit 1GB --no-explain --proxy-only
```

The current `--core` also includes existence arms, so reproducing the 1M command
adds those five arms to the original 15. Completed measurement arms can be reused
with `--resume` and identical settings. Each arm has a 180-second timeout. Preserve
the output directory on interruption; a changed build shape is refused. Neither
fresh run used `EXPLAIN ANALYZE`; column-read claims come from observed ranges.

Validation: all 350 recorded summaries agree across arms on their respective
workloads. A deliberately corrupted summary was rejected by the report's check;
a changed build shape was refused; all four 1M cached tables were revalidated and
reused without changing the checkpoint. Repository lint, format and blank-line
checks pass, as does the standalone type check of `benchmarks/identity.py`.
Repository `just typecheck` fails on two existing `possibly-missing-submodule`
warnings in untouched `tests/test_limits.py:34–35`, also recorded in PR #180's
earlier gate results. The commit skips only that pre-commit hook after reproducing
its failure; it does not count the skip as a pass. This is not a passing full gate.
The full application test
suite was not rerun for this benchmark-only change; this report is not a deployment
or merge gate. No new critical review was required for benchmark/docs changes.
