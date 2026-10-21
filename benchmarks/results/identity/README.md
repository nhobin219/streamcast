# `_streamcast_id`: what it costs and what it saves, measured

`just bench-identity` (see `benchmarks/README.md`) on 20,000,000 sessions-like rows,
2026-10-21. The raw records are `measure-20261021T024146.json` (every run, with its
bytes by column); the tables `measure-20261021T024146.md`; one cold
`EXPLAIN ANALYZE` per arm, with the bytes it read, under `explain/`.

**Setup.** rustfs `rustfs/rustfs:1.0.0-rc.4` on loopback, DuckDB 1.5.5 (iceberg
`45163a28`, httpfs `827222f`, parquet `v1.5.5`), litelink 0.14.0, pyarrow 25.0.1,
xxhash 4.0.1, Python 3.11.17, a 2-vCPU box, DuckDB `memory_limit` 1.5 GB. One table,
written twice through `Stream.ingest` with the same seed: `natural` (no identity) and
`keyed` (`identity=("session_id", "line_number")`). 1,000 sessions open at once, mean
2,000 lines each, interleaved in time order; `session_id` a UUID string. A second
stream, `labels` (every tenth row's key), is written the same two ways for the join.

Latency is the wall time straight to the store, median (min-max) of 5 runs (3 for the
dedupe and the join), cold (fresh connection, DuckDB's caches off) and warm (one cached
connection, after a run that fills the cache). Bytes and requests are counted by a
forwarding proxy and are exact; DuckDB's own HTTP counter in `EXPLAIN ANALYZE` agrees.
The store serves from page cache over loopback, so the latencies have no network round
trip in them: against a remote store the gap in bytes costs more, not less. No request
was abandoned mid-body in this run (`aborted` 0 everywhere).

## The layout

| | natural | keyed |
|---|---:|---:|
| files / row groups | 2 / 53 | 3 / 58 |
| table | 909.0 MB | 1,238.6 MB (+36%) |
| `session_id` (dictionary, zstd) | 28.07 MB, 1.40 B/row | 28.21 MB |
| `line_number` (zstd) | 33.37 MB, 1.67 B/row | 33.40 MB |
| `_streamcast_id` | -- | **327.36 MB, 16.37 B/row** |
| ingest of 20M rows | 119.4 s | 192.8 s (+61%) |
| `labels`, 2M rows | 9.9 MB | 42.5 MB (`_streamcast_id` 32.38 MB) |

The hash is **5.3x the two natural key columns together** (327.36 MB against 61.61 MB).
It is random 16 bytes, so neither the dictionary nor zstd shrinks it, while a
session's id repeats on every line and its line numbers are small integers.

## 1. Where the bytes go with the hash BESIDE the natural predicates

`SELECT * ... WHERE _streamcast_id = k AND session_id = s AND line_number = n`, the
predicate the pull request's rewrite produces, over `iceberg_scan` (cold run of
`explain/a-lookup-by-full-key-select-hash-beside-natural-the-pr-s-rewrite-keyed-duckdb.txt`):

| column | bytes fetched | row groups fetched from (of 58) |
|---|---:|---:|
| `_streamcast_id` | 309,554,850 | 56 |
| `line_number` | 32,685,549 | 56 |
| `session_id` | 22,708,402 | 46 |
| `text` and the other payload columns | 14,829,555 | 1 |
| footers and Iceberg metadata | 91,712 | |
| total (165 requests) | 379,870,068 | |

The plan pushes all three terms into the one `ICEBERG_SCAN` as filters and reads all
3 files. **DuckDB does not avoid the natural columns, and it does not avoid the hash:
it fetches every filter column for nearly every row group**, and fetches the payload
only for the row group that holds the match. File and row-group pruning is on
statistics alone; the natural columns' row-group bounds rule out a few row groups for
some keys (a large `line_number` is above some groups' maximum), the hash's rule out
none. Over the 5 cold samples the PR's rewrite read 281.6-389.4 MB, against
60.7-78.1 MB for the natural predicate alone.

Through `Stream.snapshot` (`filters=`, where the PR adds the term), the rewrite read
219.2-269.5 MB against 78.8-105.7 MB for the same `filters=` on the natural stream;
here DuckDB fetched the hash from 26 of 58 row groups and both natural columns from
52-57, plus `litelink_offset` for the snapshot's offset bound.

## 2. Can the hash win?

| question | natural columns | hash | |
|---|---|---|---|
| **a. lookup, `SELECT *`**, duckdb | 1,616 ms cold / 687 warm, 73.2 MB | hash only: 3,792 / 800 ms, 344.1 MB; beside natural: 4,092 / 990 ms, 369.0 MB | hash loses: 4.7-5.0x the bytes, 2.3-2.5x cold latency |
| a. lookup, `SELECT *`, streamcast `filters=` | 2,898 / 1,595 ms, 99.5 MB | the PR's rewrite: 3,897 / 1,280 ms, 259.6 MB | hash loses cold (+34%) and in bytes (2.6x); warm its median is 20% lower, with the spreads overlapping |
| **a. lookup, exists (`count(*)`)**, duckdb | 1,398 / 389 ms, 57.0 MB | hash only: 2,993 / 510 ms, 328.2 MB; beside natural: 4,394 / 514 ms, 354.5 MB | hash loses: 5.8-6.2x the bytes, 2.1-3.1x cold latency |
| a. 64 keys (semi-join), duckdb | 2,204 / 1,295 ms (natural stream), 2,998 / 696 ms (keyed), 62.0-62.4 MB | 2,704 / 572 ms, 328.2 MB | cold between the two natural arms, warm the lowest; 5.3x the bytes |
| **b. dedupe** (`GROUP BY key HAVING count(*) > 1`), duckdb | 20,697 / 17,606 ms (natural stream), 19,321 / 20,123 ms (keyed), 62.0-62.4 MB | 15,010 / 15,588 ms, 328.2 MB | **hash wins latency, 22-28% lower cold, 11-23% lower warm**, at 5.3x the bytes |
| b. dedupe, streamcast `sql` | 23,106 / 21,805 ms (natural), 27,987 / 20,513 ms (keyed), 96.1-97.7 MB | 18,137 / 29,316 ms, 363.5 MB | hash wins cold (21-35% lower), loses warm (34-43% higher) |
| **c. join two streams**, duckdb | 4,099 / 3,396 ms (natural), 4,710 / 3,610 ms (keyed), 69.4-69.8 MB | 6,209 / 3,790 ms, 361.4 MB | hash loses: 32-52% higher cold, 5-12% higher warm, 5.2x the bytes |

Every arm returned the same answer as its natural counterpart (the `result` column).
The one place the hash wins is CPU, not I/O: grouping on one fixed 16-byte value is
cheaper than grouping on a string and an integer, which on this box outweighs reading
5.3x the bytes from a store on loopback. Over a network, 266 more MB per dedupe is
about 2 s more at 1 Gbit/s, which would close most of the 4-6 s gap. The warm dedupe
does not stay warm: the aggregation needs the buffer pool the file cache sits in, so
warm runs refetch the key columns (warm MB equals cold MB in that table).

## 3. The one design that could save reads: prune on the natural bounds, filter on the hash

`pruned`: pyiceberg's `plan_files` picks files from the manifests with the natural
predicate (metadata only), and only those files are read with `_streamcast_id = k`
alone.

| | latency cold / warm | bytes | files planned |
|---|---:|---:|---:|
| lookup `SELECT *` | 3,793 / 1,091 ms | 344.1 MB | 3 of 3 |
| lookup exists | 2,998 / 598 ms | 328.2 MB | 3 of 3 |
| 64 keys | 2,411 / 797 ms | 328.2 MB | 3 of 3 |

**The manifests rule out no file**, so this reads the whole hash column, exactly as the
hash-only lookup does, and loses to the natural predicate by the same margin. A stream
written in time order interleaves its sessions: every file's `session_id` bounds span
nearly the whole UUID space and every file's `line_number` starts at 0. File bounds
on the natural columns would prune only if the table were clustered by `session_id`
(`sort_by`, or a partition); then DuckDB already prunes the natural predicate on the
same bounds, and the remaining reads are the surviving files' key columns, which the
hash is 5.3x the size of. On this layout no variant measured here makes a lookup by
`_streamcast_id` read less than the lookup by the natural columns.

## How this run was made, and what was discarded

- An earlier full run kept every arm in one process. Its bytes are identical to this
  run's, arm for arm, where the sample does not change them. Its warm runs of several
  arms slowed to 14-57 s with nothing read from the store, while the same queries on
  their own ran warm in about 1 s, so the harness now runs each arm in its own process
  and that run's records are not used. Its cold latencies differ from this run's by up
  to about 70% for the same arm: **on this box a cold latency gap under about 40% is
  not a robust finding.** Every direction stated above (hash loses the lookups, the
  join, and the snapshot lookup; hash wins the duckdb dedupe) holds in both runs.
- A run before that was killed by the OOM killer in the dedupe (DuckDB defaults to 80%
  of the host's memory per database); `measure` now sets `--memory-limit` (1.5 GB) and
  a spill directory. That run also showed 5 requests DuckDB abandoned mid-body, which
  the proxy then dropped from its count; it now records them as `aborted`, with the
  bytes that reached the client and the length that was asked for.
