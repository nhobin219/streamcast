"""What `_streamcast_id` costs and saves, measured in object-store bytes and requests.

    just rustfs
    just bench-identity build --rows 20000000 --out bench-identity
    just bench-identity measure --out bench-identity
    just bench-identity clean --out bench-identity

`build` writes ONE generated table twice, as two streams published to S3: the
same rows, one declared with `identity=("session_id", "line_number")`, so its
writer fills `_streamcast_id`, and one without. Both go through `Stream.new` and
`Stream.ingest`, the bulk path a backfill takes, and are published by litelink
exactly as a stream's maintainer would publish them: Iceberg tables, Parquet
with footer statistics. The rows are sessions-like: `--concurrent` sessions
open at once, each a run of dense `line_number`s, interleaved in time order as
a live feed writes them, so a file holds slices of many sessions and its
`session_id` bounds are wide. A second, smaller table, `labels` (every tenth
row's key and a label), is written the same two ways, for a join of two
streams on the key.

`measure` runs every arm in `ARMS` -- one question (a lookup by the full key,
a dedupe on it, a join on it), asked one way of one stream -- and records,
per run, the wall time, the requests and bytes the store served, and those
bytes attributed to the Parquet column chunks they fell in, from the files'
own footers: so "did DuckDB read `session_id` to answer a lookup that also
names `_streamcast_id`" is answered by the byte ranges it fetched, not by
its plan. An arm is asked one of three ways:

* `streamcast`: through `Stream.snapshot`, which is what the pull request
  changes. `filters=` that fix the whole identity get the key term added
  beside the natural ones (`_snapshot._keyed`).
* `duckdb`: the same SQL over `iceberg_scan` of the published table on a bare
  DuckDB connection: what DuckDB does on its own, without the snapshot's
  offset bounds and union around it.
* `pruned`: the design the hash would need to save reads. The files are
  chosen from the Iceberg manifests on the NATURAL columns' bounds (metadata
  only, pyiceberg's `plan_files`), and only those files are read, filtered on
  `_streamcast_id` alone.

Each arm runs `--repeat` times COLD (a fresh connection or snapshot with
DuckDB's caches off, so every run fetches what it reads) and `--repeat` + 1
times WARM (one cached connection; the first run fills the cache and is kept
apart as `fill`). Every phase runs twice: through `s3proxy.CountingProxy`, a
forwarding proxy that records requests and bytes, and straight to the store,
whose wall times are the latency figures (the proxy's are inflated by a
Python hop). Arms that can be explained also get one `EXPLAIN ANALYZE`
through the proxy, saved beside the results with the bytes it read.

"Cold" is DuckDB-cold, not disk-cold: the store serves from the page cache
of a box that just wrote the files. The store is the `just rustfs` container
unless `AWS_ENDPOINT_URL` and the AWS credentials say otherwise; `--bucket`
names the bucket.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import litelink
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyiceberg.expressions import And, BooleanExpression, EqualTo, Or
from pyiceberg.table import StaticTable

import streamcast
from streamcast import _identity, _schema

try:
    # Not a dependency of streamcast or its dev group, for one benchmark's
    # generator: `uv run --with numpy` brings it for the run.
    import numpy as np  # ty: ignore[unresolved-import]
except ImportError:
    sys.exit(
        "this benchmark needs numpy: uv run --with numpy python benchmarks/identity.py"
    )

sys.path.insert(0, str(Path(__file__).parent))
from s3proxy import CountingProxy, Seen  # noqa: E402  # ty: ignore[unresolved-import]

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

IDENTITY = ("session_id", "line_number")
SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "session_id": {"type": "string"},
        "line_number": {"type": "integer"},
        "ts": {"type": "integer"},
        "kind": {"type": "integer", "format": "int32"},
        "tokens": {"type": "integer", "format": "int32"},
        "text": {"type": "string"},
    },
    "required": ["session_id", "line_number", "ts", "kind", "tokens", "text"],
}
SCHEMA_ARROW = _schema.to_arrow(SCHEMA)
LABELS: dict[str, Any] = {
    "type": "object",
    "properties": {
        "session_id": {"type": "string"},
        "line_number": {"type": "integer"},
        "label": {"type": "integer", "format": "int32"},
    },
    "required": ["session_id", "line_number", "label"],
}
LABELS_ARROW = _schema.to_arrow(LABELS)
LABEL_EVERY = 10
TS0 = 1_790_000_000_000_000
WORDS = (
    "the a of to and in is it you that he was for on are with as I his they be "
    "at one have this from or had by hot word but what some we can out other "
    "were all there when up use your how said an each she which do their time "
    "if will way about many then them write would like so these her long make "
    "thing see him two has look more day could go come did number sound no most "
    "people my over know water than call first who may down side been now find "
    "error retry tool_call result assistant user system token stream session "
    "line offset parquet iceberg query filter column bytes request"
).split()


def rustfs() -> litelink.S3Options:
    """`just rustfs`'s endpoint, unless the environment names another store."""
    if os.environ.get("AWS_ENDPOINT_URL"):
        return litelink.S3Options().resolved()

    return litelink.S3Options(
        endpoint="http://127.0.0.1:9002",
        access_key="streamcast",
        secret_key="streamcast-secret",
        region="us-east-1",
    ).resolved()


# -- the tables ----------------------------------------------------------------


@dataclass(frozen=True)
class Shape:
    rows: int
    concurrent: int
    mean_lines: int
    batch: int
    seed: int


def sessions(shape: Shape, samples: list[dict[str, Any]]) -> Iterator[pa.RecordBatch]:
    """The table, a batch at a time, the same for every call with one `shape`.

    `concurrent` slots each hold an open session; every row goes to a slot at
    random, takes that session's next `line_number`, and a session whose
    length (geometric, mean `mean_lines`) is reached is replaced by a new one.
    So line numbers are dense from 0 within a session and sessions interleave.
    `samples` collects rows spread over the table, for lookups that hit.
    """
    rng = np.random.default_rng(shape.seed)
    ids = np.random.default_rng(shape.seed + 1)
    names: list[str] = []

    def opened(n: int) -> np.ndarray:
        first = len(names)
        names.extend(str(uuid.UUID(bytes=ids.bytes(16), version=4)) for _ in range(n))
        return np.arange(first, first + n, dtype=np.int64)

    k = shape.concurrent
    slot_session = opened(k)
    slot_line = np.zeros(k, dtype=np.int64)
    slot_target = rng.geometric(1 / shape.mean_lines, k)
    texts = pa.array(
        [" ".join(rng.choice(WORDS, int(rng.integers(3, 40)))) for _ in range(50_000)]
    )
    marks = set(np.linspace(0, shape.rows - 1, 64, dtype=np.int64).tolist())

    for start in range(0, shape.rows, shape.batch):
        n = min(shape.batch, shape.rows - start)
        slots = rng.integers(0, k, n)
        # Each row's rank among this batch's rows in its slot, vectorised: a
        # stable sort groups the slots, and a row's rank is its position less
        # where its slot's group starts.
        order = np.argsort(slots, kind="stable")
        counts = np.bincount(slots, minlength=k)
        group = np.cumsum(counts) - counts
        rank = np.empty(n, dtype=np.int64)
        rank[order] = np.arange(n) - group[slots[order]]
        line = slot_line[slots] + rank
        session = slot_session[slots]

        slot_line += counts
        done = np.flatnonzero(slot_line >= slot_target)
        slot_session[done] = opened(len(done))
        slot_line[done] = 0
        slot_target[done] = rng.geometric(1 / shape.mean_lines, len(done))

        used, index = np.unique(session, return_inverse=True)
        session_id = pa.DictionaryArray.from_arrays(
            pa.array(index.astype(np.int32)), pa.array([names[i] for i in used])
        ).cast(pa.string())
        batch = pa.record_batch(
            {
                "session_id": session_id,
                "line_number": pa.array(line),
                "ts": pa.array(TS0 + 1_000 * (start + np.arange(n, dtype=np.int64))),
                "kind": pa.array(rng.integers(0, 4, n, dtype=np.int32)),
                "tokens": pa.array(rng.integers(1, 4_000, n, dtype=np.int32)),
                "text": texts.take(pa.array(rng.integers(0, len(texts), n))),
            },
            schema=SCHEMA_ARROW,
        )
        for mark in sorted(m for m in marks if start <= m < start + n):
            samples.append(
                {
                    "row": mark,
                    "session_id": batch["session_id"][mark - start].as_py(),
                    "line_number": batch["line_number"][mark - start].as_py(),
                }
            )

        yield batch


def labels(shape: Shape) -> Iterator[pa.RecordBatch]:
    """Every `LABEL_EVERY`th row of `sessions`, its key and a label: a second
    stream that shares the key, for a join of two streams on it."""
    start = 0
    for batch in sessions(shape, []):
        index = np.arange(start, start + batch.num_rows)
        keep = pa.array(index % LABEL_EVERY == 0)
        start += batch.num_rows
        kept = batch.filter(keep)
        yield pa.record_batch(
            {
                "session_id": kept["session_id"],
                "line_number": kept["line_number"],
                "label": pa.array(
                    (kept["line_number"].to_numpy() & 7).astype(np.int32)
                ),
            },
            schema=LABELS_ARROW,
        )


@dataclass(frozen=True)
class Table:
    """One stream `build` writes: its name, schema, identity and rows."""

    name: str
    schema: dict[str, Any]
    arrow: pa.Schema
    identity: tuple[str, ...] | None
    rows: Callable[[Shape, list[dict[str, Any]]], Iterator[pa.RecordBatch]]


TABLES = (
    Table("natural", SCHEMA, SCHEMA_ARROW, None, sessions),
    Table("keyed", SCHEMA, SCHEMA_ARROW, IDENTITY, sessions),
    Table("labels_natural", LABELS, LABELS_ARROW, None, lambda shape, _: labels(shape)),
    Table(
        "labels_keyed", LABELS, LABELS_ARROW, IDENTITY, lambda shape, _: labels(shape)
    ),
)


async def publish(
    table: Table,
    shape: Shape,
    prefix: str,
    options: litelink.S3Options,
    work: Path,
) -> dict[str, Any]:
    """One stream, published under `prefix`; what a reader needs."""
    name = table.name
    root = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=work))
    try:

        def opened() -> streamcast.Stream:
            return streamcast.Stream.new(
                name,
                root=root,
                schema=table.schema,
                published=prefix,
                s3_options=options,
                identity=table.identity,
            )

        await opened().aclose()
        samples: list[dict[str, Any]] = []
        reader = pa.RecordBatchReader.from_batches(
            table.arrow, table.rows(shape, samples)
        )
        started = time.perf_counter()
        streamcast.Stream.ingest(
            name, reader, root=root, s3_options=options, publish=True, flush=True
        )
        loaded = time.perf_counter() - started

        stream = opened()
        log = stream.log
        assert log is not None
        log.publish(flush=True)
        stream.ensure_metadata()
        uri = stream.metadata_uri
        assert uri is not None
        await stream.aclose()
        return {
            "metadata_uri": uri,
            "table": f"{prefix}/{name}/{name}",
            "identity": table.identity,
            "ingest_seconds": loaded,
            "samples": samples,
        }
    finally:
        # The reader resolves everything from S3. The local log is a second
        # copy of the table, and two of them do not fit a small disk.
        shutil.rmtree(root, ignore_errors=True)


def filesystem(options: litelink.S3Options) -> pafs.S3FileSystem:
    endpoint = options.endpoint or ""
    scheme, _, host = endpoint.partition("://")
    return pafs.S3FileSystem(
        access_key=options.access_key,
        secret_key=options.secret_key,
        region=options.region or "us-east-1",
        endpoint_override=host or None,
        scheme=scheme or "https",
    )


def data_files(table: str, options: litelink.S3Options) -> list[str]:
    """The table's Parquet files, as `bucket/key`."""
    fs = filesystem(options)
    found = fs.get_file_info(
        pafs.FileSelector(table.removeprefix("s3://"), recursive=True)
    )
    return sorted(f.path for f in found if f.path.endswith(".parquet"))


def layout(paths: list[str], options: litelink.S3Options) -> dict[str, Any]:
    """Files, row groups, and each column's compressed bytes and encodings,
    from the footers."""
    fs = filesystem(options)
    columns: dict[str, int] = {}
    encodings: dict[str, list[str]] = {}
    rows = groups = size = 0
    stats: dict[str, bool] = {}
    for path in paths:
        info = fs.get_file_info(path)
        size += info.size
        with fs.open_input_file(path) as f:
            meta = pq.ParquetFile(f).metadata

        rows += meta.num_rows
        groups += meta.num_row_groups
        for g in range(meta.num_row_groups):
            group = meta.row_group(g)
            for c in range(group.num_columns):
                chunk = group.column(c)
                name = chunk.path_in_schema
                columns[name] = columns.get(name, 0) + chunk.total_compressed_size
                stats[name] = chunk.is_stats_set
                seen = encodings.setdefault(name, [])
                for encoding in (*chunk.encodings, chunk.compression):
                    if encoding not in seen:
                        seen.append(encoding)

    return {
        "files": len(paths),
        "rows": rows,
        "row_groups": groups,
        "bytes": size,
        "column_bytes": columns,
        "column_encodings": encodings,
        "column_has_stats": stats,
    }


async def build(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    options = rustfs()
    prefix = f"s3://{args.bucket}/identity-bench/{uuid.uuid4().hex[:12]}"
    shape = Shape(args.rows, args.concurrent, args.mean_lines, args.batch, args.seed)
    streams = {}
    for table in TABLES:
        print(
            f"building {table.name} from {shape.rows:,} rows under {prefix}",
            file=sys.stderr,
        )
        built = await publish(table, shape, prefix, options, out)
        built["layout"] = layout(data_files(built["table"], options), options)
        print(
            f"  {built['ingest_seconds']:.1f} s to ingest {built['layout']['rows']:,} rows; "
            f"{built['layout']['files']} files, {built['layout']['row_groups']} row groups, "
            f"{built['layout']['bytes'] / 1e6:.1f} MB",
            file=sys.stderr,
        )
        streams[table.name] = built

    manifest = {
        "prefix": prefix,
        "shape": shape.__dict__,
        "streams": streams,
        "versions": versions(),
    }
    (out / "build.json").write_text(json.dumps(manifest, indent=2, default=list))
    print(f"wrote {out / 'build.json'}", file=sys.stderr)


# -- the arms ------------------------------------------------------------------


def blob(key: bytes) -> str:
    return "'" + "".join(f"\\x{b:02x}" for b in key) + "'::BLOB"


def quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def natural(sample: dict[str, Any], alias: str = "") -> str:
    return (
        f"{alias}session_id = {quoted(sample['session_id'])} "
        f"AND {alias}line_number = {sample['line_number']}"
    )


def hashed(sample: dict[str, Any]) -> str:
    return f'"_streamcast_id" = {blob(sample["key"])}'


def natural_filter(sample: dict[str, Any]) -> BooleanExpression:
    return And(
        EqualTo(term="session_id", value=sample["session_id"]),
        EqualTo(term="line_number", value=sample["line_number"]),
    )


def any_of(sample: dict[str, Any]) -> BooleanExpression:
    terms = [natural_filter(s) for s in sample["batch"]]
    return Or(*terms) if len(terms) > 1 else terms[0]


def values(sample: dict[str, Any], *, keys: bool) -> str:
    if keys:
        return ", ".join(f"({blob(s['key'])})" for s in sample["batch"])

    return ", ".join(
        f"({quoted(s['session_id'])}, {s['line_number']})" for s in sample["batch"]
    )


def constant(sql: str) -> Callable[[dict[str, Any]], str]:
    """An arm's SQL that takes no sample: a dedupe or a join reads it all."""
    return lambda _: sql


NO_KEY = 'EXCLUDE ("_streamcast_id")'


@dataclass(frozen=True)
class Arm:
    """One question asked one way of one stream.

    `sql` is the query, over `{main}` (and `{labels}`) for `duckdb`, over
    `{files}` for `pruned`, and over `log` for `streamcast`. A `streamcast`
    arm with `scan` reads through `Snapshot.scan` instead, and `explain` is
    the SQL equivalent of that scan, for its plan. `prune` is the natural
    filter `pruned` plans files with.
    """

    question: str
    name: str
    stream: str
    how: str
    sql: Callable[[dict[str, Any]], str] | None = None
    scan: Callable[[Any, dict[str, Any]], Any] | None = None
    prune: Callable[[dict[str, Any]], BooleanExpression] | None = None
    explain: Callable[[dict[str, Any]], str] | None = None
    labels: str | None = None

    @property
    def id(self) -> str:
        return f"{self.question} | {self.name} | {self.stream} | {self.how}"


LOOKUP, EXISTS, DEDUPE, JOIN, BATCH = (
    "a. lookup by full key, SELECT *",
    "a. lookup by full key, exists (count)",
    "b. dedupe on the key",
    "c. join two streams on the key",
    "a. lookup of 64 keys (semi-join)",
)


def lookups(question: str, projection: str) -> list[Arm]:
    keyed_projection = projection if projection != "*" else f"* {NO_KEY}"
    return [
        Arm(
            question,
            "natural predicate",
            "natural",
            "duckdb",
            lambda s: f"SELECT {projection} FROM {{main}} WHERE {natural(s)}",
        ),
        Arm(
            question,
            "natural predicate",
            "keyed",
            "duckdb",
            lambda s: f"SELECT {keyed_projection} FROM {{main}} WHERE {natural(s)}",
        ),
        Arm(
            question,
            "hash only",
            "keyed",
            "duckdb",
            lambda s: f"SELECT {keyed_projection} FROM {{main}} WHERE {hashed(s)}",
        ),
        Arm(
            question,
            "hash beside natural (the PR's rewrite)",
            "keyed",
            "duckdb",
            lambda s: (
                f"SELECT {keyed_projection} FROM {{main}} "
                f"WHERE {hashed(s)} AND {natural(s)}"
            ),
        ),
        Arm(
            question,
            "manifest-pruned on natural, then hash only",
            "keyed",
            "pruned",
            lambda s: f"SELECT {keyed_projection} FROM {{files}} WHERE {hashed(s)}",
            prune=natural_filter,
        ),
    ]


ARMS: list[Arm] = [
    *lookups(LOOKUP, "*"),
    Arm(
        LOOKUP,
        "filters= (natural terms)",
        "natural",
        "streamcast",
        scan=lambda snap, s: snap.scan(
            filters=[
                ("session_id", "==", s["session_id"]),
                ("line_number", "==", s["line_number"]),
            ]
        ),
        explain=lambda s: f"SELECT * FROM log WHERE {natural(s)}",
    ),
    Arm(
        LOOKUP,
        "filters= (the PR adds the hash term)",
        "keyed",
        "streamcast",
        scan=lambda snap, s: snap.scan(
            filters=[
                ("session_id", "==", s["session_id"]),
                ("line_number", "==", s["line_number"]),
            ]
        ),
        explain=lambda s: (
            f"SELECT * {NO_KEY} FROM log WHERE {hashed(s)} AND {natural(s)}"
        ),
    ),
    Arm(
        LOOKUP,
        "hash only, sql",
        "keyed",
        "streamcast",
        lambda s: f"SELECT * {NO_KEY} FROM log WHERE {hashed(s)}",
    ),
    *lookups(EXISTS, "count(*)"),
    Arm(
        BATCH,
        "natural columns",
        "natural",
        "duckdb",
        lambda s: (
            f"SELECT count(*) FROM {{main}} t JOIN (VALUES {values(s, keys=False)}) "
            f"k(s, n) ON t.session_id = k.s AND t.line_number = k.n"
        ),
    ),
    Arm(
        BATCH,
        "natural columns",
        "keyed",
        "duckdb",
        lambda s: (
            f"SELECT count(*) FROM {{main}} t JOIN (VALUES {values(s, keys=False)}) "
            f"k(s, n) ON t.session_id = k.s AND t.line_number = k.n"
        ),
    ),
    Arm(
        BATCH,
        "hash",
        "keyed",
        "duckdb",
        lambda s: (
            f"SELECT count(*) FROM {{main}} t JOIN (VALUES {values(s, keys=True)}) "
            f'k(id) ON t."_streamcast_id" = k.id'
        ),
    ),
    Arm(
        BATCH,
        "manifest-pruned on natural, then hash",
        "keyed",
        "pruned",
        lambda s: (
            f"SELECT count(*) FROM {{files}} t JOIN (VALUES {values(s, keys=True)}) "
            f'k(id) ON t."_streamcast_id" = k.id'
        ),
        prune=any_of,
    ),
    *[
        Arm(
            DEDUPE,
            name,
            stream,
            how,
            constant(
                f"SELECT count(*) AS duplicated FROM (SELECT {key} FROM "
                f"{'{main}' if how == 'duckdb' else 'log'} "
                f"GROUP BY ALL HAVING count(*) > 1)"
            ),
        )
        for name, stream, key in (
            ("natural columns", "natural", "session_id, line_number"),
            ("natural columns", "keyed", "session_id, line_number"),
            ("hash", "keyed", '"_streamcast_id"'),
        )
        for how in ("duckdb", "streamcast")
    ],
    *[
        Arm(
            JOIN,
            name,
            stream,
            "duckdb",
            constant(
                f"SELECT count(*) AS matched, sum(l.label) AS labels "
                f"FROM {{main}} t JOIN {{labels}} l ON {on}"
            ),
            labels=f"labels_{stream}",
        )
        for name, stream, on in (
            (
                "natural columns",
                "natural",
                "t.session_id = l.session_id AND t.line_number = l.line_number",
            ),
            (
                "natural columns",
                "keyed",
                "t.session_id = l.session_id AND t.line_number = l.line_number",
            ),
            ("hash", "keyed", 't."_streamcast_id" = l."_streamcast_id"'),
        )
    ],
]


class Attribution:
    """Byte ranges of tables' files, mapped to the column chunks they fall in."""

    def __init__(self, chunks: dict[str, list[tuple[int, int, int, str]]]) -> None:
        self._chunks = chunks

    @classmethod
    def open(cls, tables: dict[str, str], options: litelink.S3Options) -> Attribution:
        """`tables` maps a label (empty for the main stream) to a table."""
        fs = filesystem(options)
        chunks: dict[str, list[tuple[int, int, int, str]]] = {}
        for label, table in tables.items():
            for path in data_files(table, options):
                with fs.open_input_file(path) as f:
                    meta = pq.ParquetFile(f).metadata

                spans = []
                for g in range(meta.num_row_groups):
                    group = meta.row_group(g)
                    for c in range(group.num_columns):
                        chunk = group.column(c)
                        first = chunk.data_page_offset
                        if chunk.has_dictionary_page and chunk.dictionary_page_offset:
                            first = min(first, chunk.dictionary_page_offset)

                        name = f"{label}{chunk.path_in_schema}"
                        spans.append(
                            (first, first + chunk.total_compressed_size, g, name)
                        )

                chunks["/" + path] = spans

        return cls(chunks)

    def columns(self, seen: Seen) -> tuple[dict[str, int], dict[str, int]]:
        """Bytes per column, and how many row groups each was read from.
        What fell in no column chunk is `(footer/other)`, and what was read
        from no data file is `(metadata)`: Iceberg's JSON, manifest lists and
        manifests."""
        totals: dict[str, int] = {}
        groups: dict[str, set[tuple[str, int]]] = {}
        for request in seen.log:
            path = request.path.split("?", 1)[0]
            spans = self._chunks.get(path)
            if spans is None:
                totals["(metadata)"] = totals.get("(metadata)", 0) + request.bytes_out
                continue

            if request.method != "GET" or not request.bytes_out:
                continue

            low = 0
            if request.range and request.range.startswith("bytes="):
                low = int(request.range[6:].split("-", 1)[0] or 0)

            high = low + request.bytes_out
            covered = 0
            for first, last, group, name in spans:
                overlap = min(high, last) - max(low, first)
                if overlap > 0:
                    totals[name] = totals.get(name, 0) + overlap
                    groups.setdefault(name, set()).add((path, group))
                    covered += overlap

            rest = request.bytes_out - covered
            if rest:
                totals["(footer/other)"] = totals.get("(footer/other)", 0) + rest

        ordered = dict(sorted(totals.items(), key=lambda kv: -kv[1]))
        return ordered, {name: len(found) for name, found in groups.items()}


def samples_of(built: dict[str, Any]) -> list[dict[str, Any]]:
    """The build's sample rows with their keys; each carries the whole set as
    `batch`, for the 64-key lookup."""
    identity = _identity.Identity(IDENTITY, SCHEMA_ARROW)
    samples = [dict(s, key=identity.key(s)) for s in built["samples"]]
    return [dict(s, batch=samples) for s in samples]


def with_endpoint(options: litelink.S3Options, endpoint: str) -> litelink.S3Options:
    return litelink.S3Options(
        endpoint=endpoint,
        access_key=options.access_key,
        secret_key=options.secret_key,
        region=options.region,
    )


@dataclass(frozen=True)
class Budget:
    """DuckDB's memory limit for every database a measurement opens, and where
    a query over it spills. DuckDB's default is 80% of the host's memory per
    database, and the cold phases open one per run: a dedupe of 20 million
    keys under a capped scope is then killed rather than spilled."""

    memory_limit: str
    spill: Path

    def apply(self, connection: Any) -> None:
        connection.execute(f"SET memory_limit = {quoted(self.memory_limit)}")
        # streamcast shares one database across snapshots, and DuckDB refuses
        # to move a temporary directory it has already spilled to.
        (current,) = connection.execute(
            "SELECT current_setting('temp_directory')"
        ).fetchone()
        if current != str(self.spill):
            connection.execute(f"SET temp_directory = {quoted(str(self.spill))}")


BUDGET = Budget("1.5GB", Path(tempfile.gettempdir()))


@dataclass
class Context:
    """What a phase reads with: one DuckDB connection or one snapshot, and the
    options (the proxy's endpoint or the store's) it reads through."""

    options: litelink.S3Options
    cached: bool
    connection: duckdb.DuckDBPyConnection | None = None
    snapshot: Any = None

    @classmethod
    async def open(
        cls,
        arm: Arm,
        built: dict[str, Any],
        options: litelink.S3Options,
        *,
        cached: bool,
    ) -> Context:
        context = cls(options, cached)
        if arm.how == "streamcast":
            context.snapshot = await streamcast.Stream.snapshot(
                built["metadata_uri"],
                s3_options=options,
                memory_cache=cached,
                memory_limit=BUDGET.memory_limit,
            )
            BUDGET.apply(context.snapshot._connection)  # noqa: SLF001
        else:
            context.connection = litelink.duckdb_connection(
                s3_options=options, memory_cache=cached
            )
            context.connection.execute(
                f"SET enable_external_file_cache = {str(cached).lower()}"
            )
            BUDGET.apply(context.connection)

        return context

    async def close(self) -> None:
        if self.snapshot is not None:
            await self.snapshot.close()

        if self.connection is not None:
            self.connection.close()


def files_for(
    arm: Arm, built: dict[str, Any], sample: dict[str, Any], options: litelink.S3Options
) -> list[str]:
    """`pruned`'s plan: the files whose manifest bounds admit the natural
    filter, read from the manifests alone (through `options`, so the bytes
    are counted)."""
    assert arm.prune is not None
    metadata = litelink.current_metadata(built["table"], s3_options=options)
    table = StaticTable.from_metadata(
        metadata,
        {
            "s3.endpoint": options.endpoint or "",
            "s3.access-key-id": options.access_key or "",
            "s3.secret-access-key": options.secret_key or "",
            "s3.region": options.region or "us-east-1",
        },
    )
    return [
        task.file.file_path
        for task in table.scan(row_filter=arm.prune(sample)).plan_files()
    ]


async def execute(
    arm: Arm,
    context: Context,
    streams: dict[str, Any],
    sample: dict[str, Any],
    metadata: dict[str, str],
    *,
    explain: bool = False,
) -> tuple[pa.Table, dict[str, Any]]:
    """Run `arm` once on `context`; the result and what the run decided."""
    notes: dict[str, Any] = {}
    prefix = "EXPLAIN ANALYZE " if explain else ""
    if arm.how == "streamcast":
        snap = context.snapshot
        if explain:
            sql = (arm.explain or arm.sql)(sample)  # ty: ignore[call-non-callable]
            return await snap.sql(prefix + sql).read_all(), notes

        if arm.scan is not None:
            return await arm.scan(snap, sample).read_all(), notes

        assert arm.sql is not None
        return await snap.sql(arm.sql(sample)).read_all(), notes

    assert arm.sql is not None and context.connection is not None
    sql = arm.sql(sample)
    if arm.how == "pruned":
        # pyarrow's S3 client gives up on a response slower than 1 byte/s
        # for 3 s, which a request queued behind the proxy's other threads
        # can hit; the plan is retried, and the retries are recorded.
        for attempt in range(3):
            try:
                files = await asyncio.to_thread(
                    files_for, arm, streams[arm.stream], sample, context.options
                )
                break
            except OSError:
                if attempt == 2:
                    raise

                notes["plan_retries"] = attempt + 1

        notes["files_planned"] = len(files)
        if not files:
            return pa.table({}), notes

        listed = ", ".join(quoted(f) for f in files)
        sql = sql.replace("{files}", f"read_parquet([{listed}])")
    else:
        sql = sql.replace("{main}", f"iceberg_scan({quoted(metadata[arm.stream])})")
        if arm.labels is not None:
            sql = sql.replace(
                "{labels}", f"iceberg_scan({quoted(metadata[arm.labels])})"
            )

    connection = context.connection
    table = await asyncio.to_thread(
        lambda: connection.execute(prefix + sql).to_arrow_table()
    )
    return table, notes


def summarise(table: pa.Table) -> object:
    """A result small enough to compare across arms."""
    if table.num_rows <= 2:
        rows = table.drop_columns(
            [c for c in table.column_names if c in ("text",)]
        ).to_pylist()
        return json.loads(json.dumps(rows, default=str))

    return {"rows": table.num_rows}


async def measure(args: argparse.Namespace) -> None:
    out = Path(args.out)
    manifest = json.loads((out / "build.json").read_text())
    streams = manifest["streams"]
    direct = rustfs()
    explains = out / "explain"
    explains.mkdir(exist_ok=True)
    samples = samples_of(streams["keyed"])
    global BUDGET  # noqa: PLW0603 -- one setting for every database the run opens
    spill = out / "spill"
    spill.mkdir(exist_ok=True)
    BUDGET = Budget(args.memory_limit, spill.resolve())
    # Each arm's runs are appended as it finishes, so a run that dies keeps
    # what it measured, and `--resume` carries on from the next arm.
    progress = out / "records.jsonl"
    if args.arm is None:
        orchestrate(args, progress)
        records = [
            json.loads(line) for line in progress.read_text().splitlines() if line
        ]
        report(args, manifest, records)
        return

    (arm,) = [a for a in ARMS if a.id == args.arm]
    with CountingProxy(direct.endpoint or "") as proxy:
        through = with_endpoint(direct, proxy.endpoint)
        attribution = {
            name: Attribution.open(
                {"": streams[name]["table"]}
                | (
                    {"labels.": streams[f"labels_{name}"]["table"]}
                    if f"labels_{name}" in streams
                    else {}
                ),
                direct,
            )
            for name in ("natural", "keyed")
        }
        # Resolved once, outside every measured block, through the store: a
        # run's bytes are its own query's, not this lookup's.
        metadata = {
            name: litelink.current_metadata(built["table"], s3_options=direct)
            for name, built in streams.items()
        }
        records: list[dict[str, Any]] = []
        # A dedupe or a join reads the whole key column of 20 million
        # rows; three runs of each show its spread at a fraction of the time.
        repeat = (
            args.repeat if arm.question not in (DEDUPE, JOIN) else args.heavy_repeat
        )
        for path, options in (("proxy", through), ("direct", direct)):
            runs = [("cold", i, False) for i in range(repeat)]
            runs += [("fill", 0, True)] + [("warm", i, True) for i in range(repeat)]
            warm: Context | None = None
            try:
                for phase, i, cached in runs:
                    sample = samples[(i * 17 + 5) % len(samples)]
                    if cached:
                        if warm is None:
                            warm = await Context.open(
                                arm, streams[arm.stream], options, cached=True
                            )

                        context = warm
                    else:
                        context = await Context.open(
                            arm, streams[arm.stream], options, cached=False
                        )

                    try:
                        with proxy.measure() as seen:
                            started = time.perf_counter()
                            table, notes = await execute(
                                arm, context, streams, sample, metadata
                            )
                            seconds = time.perf_counter() - started
                    finally:
                        if not cached:
                            await context.close()

                    columns, groups = attribution[arm.stream].columns(seen)
                    records.append(
                        {
                            "arm": arm.id,
                            "question": arm.question,
                            "name": arm.name,
                            "stream": arm.stream,
                            "how": arm.how,
                            "path": path,
                            "phase": phase,
                            "run": i,
                            "sample_row": sample["row"],
                            "seconds": seconds,
                            "result": summarise(table),
                            "requests": seen.requests if path == "proxy" else None,
                            "bytes": seen.bytes_out if path == "proxy" else None,
                            "aborted": seen.aborted if path == "proxy" else None,
                            "bytes_declared": (
                                seen.bytes_declared if path == "proxy" else None
                            ),
                            "methods": seen.methods() if path == "proxy" else None,
                            "columns": columns if path == "proxy" else None,
                            "row_groups_read": groups if path == "proxy" else None,
                            **notes,
                        }
                    )
            finally:
                if warm is not None:
                    await warm.close()

        if args.explain and (arm.how != "streamcast" or arm.explain or arm.sql):
            await explain(
                arm,
                streams,
                samples,
                metadata,
                through,
                proxy,
                attribution,
                explains,
            )

        with progress.open("a") as f:
            f.writelines(json.dumps(r, default=str) + "\n" for r in records)


def orchestrate(args: argparse.Namespace, progress: Path) -> None:
    """Each arm in a process of its own, appending to `progress`.

    **A fresh process per arm**, as `snapshot.py` runs each config: the cold
    phases open a DuckDB database per run, and in one long process under a
    capped scope the later arms' warm runs slowed to tens of seconds with
    nothing read from the store, while the same query on its own ran warm
    in about one second. One process per arm keeps one arm's memory out of
    the next one's timings.
    """
    if not args.resume:
        progress.write_text("")

    done = {
        json.loads(line)["arm"] for line in progress.read_text().splitlines() if line
    }
    arms = [a for a in ARMS if not args.only or any(o in a.id for o in args.only)]
    for number, arm in enumerate(arms, 1):
        if arm.id in done:
            continue

        print(f"[{number}/{len(arms)}] {arm.id}", file=sys.stderr)
        command = [
            sys.executable,
            __file__,
            "measure",
            "--out",
            args.out,
            "--repeat",
            str(args.repeat),
            "--heavy-repeat",
            str(args.heavy_repeat),
            "--memory-limit",
            args.memory_limit,
            "--arm",
            arm.id,
        ]
        if not args.explain:
            command.append("--no-explain")

        subprocess.run(command, check=True)


def report(
    args: argparse.Namespace, manifest: dict[str, Any], records: list[dict[str, Any]]
) -> None:
    out = Path(args.out)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    summary = {
        "build": manifest,
        "versions": versions(),
        "repeat": args.repeat,
        "heavy_repeat": args.heavy_repeat,
        "memory_limit": args.memory_limit,
        "records": records,
    }
    (out / f"measure-{stamp}.json").write_text(
        json.dumps(summary, indent=2, default=str)
    )
    (out / f"measure-{stamp}.md").write_text(render(manifest, records))
    print(f"wrote {out / f'measure-{stamp}.json'} and .md", file=sys.stderr)


async def explain(
    arm: Arm,
    streams: dict[str, Any],
    samples: list[dict[str, Any]],
    metadata: dict[str, str],
    options: litelink.S3Options,
    proxy: CountingProxy,
    attribution: dict[str, Attribution],
    into: Path,
) -> None:
    """One cold `EXPLAIN ANALYZE` of `arm`, with the bytes it read, to a file."""
    sample = samples[5 % len(samples)]
    context = await Context.open(arm, streams[arm.stream], options, cached=False)
    try:
        with proxy.measure() as seen:
            table, notes = await execute(
                arm, context, streams, sample, metadata, explain=True
            )
    finally:
        await context.close()

    columns, groups = attribution[arm.stream].columns(seen)
    plan = "\n".join(str(v) for v in table.column(table.num_columns - 1).to_pylist())
    slug = "".join(c if c.isalnum() else "-" for c in arm.id.lower()).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")

    lines = [
        f"# {arm.id}",
        f"# requests {seen.requests}, bytes {seen.bytes_out:,}, aborted {seen.aborted} "
        f"(asked for {seen.bytes_declared:,}) {json.dumps(notes)}",
        "# bytes by column (row groups read):",
        *(f"#   {c}: {b:,} ({groups.get(c, '-')})" for c, b in columns.items()),
        "",
        plan,
    ]
    (into / f"{slug}.txt").write_text("\n".join(lines) + "\n")


def spread(values: list[float], digits: int = 0) -> str:
    """Median (min-max), or the one value when every run agrees."""
    if not values:
        return "-"

    low, high, mid = min(values), max(values), statistics.median(values)
    if f"{low:.{digits}f}" == f"{high:.{digits}f}":
        return f"{mid:.{digits}f}"

    return f"{mid:.{digits}f} ({low:.{digits}f}-{high:.{digits}f})"


def render(manifest: dict[str, Any], records: list[dict[str, Any]]) -> str:
    """The results as markdown: layout, then one table per question."""
    lines = ["# identity-column measurement", ""]
    lines += [
        "## Layout",
        "",
        "| stream | rows | files | row groups | MB |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, built in manifest["streams"].items():
        lay = built["layout"]
        lines.append(
            f"| {name} | {lay['rows']:,} | {lay['files']} | {lay['row_groups']} | "
            f"{lay['bytes'] / 1e6:.1f} |"
        )

    lines += [
        "",
        "| stream | column | MB | bytes/row | encodings |",
        "|---|---|---:|---:|---|",
    ]
    for name, built in manifest["streams"].items():
        lay = built["layout"]
        for column, size in lay["column_bytes"].items():
            lines.append(
                f"| {name} | {column} | {size / 1e6:.2f} | {size / lay['rows']:.2f} | "
                f"{', '.join(lay['column_encodings'].get(column, []))} |"
            )

    arms = list(dict.fromkeys(r["arm"] for r in records))
    questions = list(dict.fromkeys(r["question"] for r in records))
    for question in questions:
        lines += [
            "",
            f"## {question}",
            "",
            "Latency in ms, straight to the store: median (min-max). Bytes and "
            "requests through the counting proxy: MB is what reached DuckDB; "
            "aborted is how many requests it hung up on mid-body, and MB asked "
            "what all its requests asked for. Columns are the last cold run.",
            "",
            "| arm | stream | how | cold ms | warm ms | cold MB | cold requests | "
            "aborted (MB asked) | warm MB | cold bytes by column, MB (row groups) "
            "| result |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        ]
        for arm in arms:
            mine = [r for r in records if r["arm"] == arm and r["question"] == question]
            if not mine:
                continue

            def pick(
                path: str, phase: str, rows: list[dict[str, Any]] = mine
            ) -> list[dict[str, Any]]:
                return [r for r in rows if r["path"] == path and r["phase"] == phase]

            cold = pick("proxy", "cold")
            warm = pick("proxy", "warm")
            first = cold[-1]
            by_column = ", ".join(
                f"{c} {b / 1e6:.2f} ({first['row_groups_read'].get(c, '-')})"
                for c, b in list(first["columns"].items())[:6]
            )
            planned = (
                f" [{first['files_planned']} files planned]"
                if "files_planned" in first
                else ""
            )
            lines.append(
                f"| {first['name']} | {first['stream']} | {first['how']} | "
                f"{spread([r['seconds'] * 1e3 for r in pick('direct', 'cold')])} | "
                f"{spread([r['seconds'] * 1e3 for r in pick('direct', 'warm')])} | "
                f"{spread([r['bytes'] / 1e6 for r in cold], 2)} | "
                f"{spread([float(r['requests']) for r in cold])} | "
                f"{spread([float(r['aborted']) for r in cold])} "
                f"({spread([r['bytes_declared'] / 1e6 for r in cold], 2)}) | "
                f"{spread([r['bytes'] / 1e6 for r in warm], 2)} | "
                f"{by_column}{planned} | {json.dumps(first['result'])[:80]} |"
            )

    return "\n".join(lines) + "\n"


def versions() -> dict[str, Any]:
    """What the measurement ran on: the libraries, DuckDB's extensions, the store."""
    connection = litelink.duckdb_connection(s3_options=rustfs())
    extensions = dict(
        connection.execute(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE loaded AND extension_name IN ('iceberg', 'httpfs', 'avro', 'parquet')"
        ).fetchall()
    )
    connection.close()
    store = None
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        store = subprocess.run(
            ["docker", "inspect", "--format", "{{.Config.Image}}", "streamcast-rustfs"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    from importlib.metadata import version

    return {
        "python": sys.version.split()[0],
        "streamcast": version("streamcast"),
        "litelink": version("litelink"),
        "duckdb": duckdb.__version__,
        "duckdb_extensions": extensions,
        "pyarrow": pa.__version__,
        "xxhash": version("xxhash"),
        "store_endpoint": rustfs().endpoint,
        "store_image": store,
    }


def clean(args: argparse.Namespace) -> None:
    """Delete everything `build` published: on a small disk, the store's copy
    of a 20-million-row build is the largest thing on it."""
    prefix = json.loads((Path(args.out) / "build.json").read_text())["prefix"]
    filesystem(rustfs()).delete_dir(prefix.removeprefix("s3://"))
    print(f"deleted {prefix}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="generate and publish the four streams")
    b.add_argument("--rows", type=int, default=20_000_000)
    b.add_argument(
        "--concurrent", type=int, default=1_000, help="sessions open at once"
    )
    b.add_argument("--mean-lines", type=int, default=2_000, help="mean session length")
    b.add_argument("--batch", type=int, default=250_000)
    b.add_argument("--seed", type=int, default=180)
    b.add_argument(
        "--bucket", default=os.environ.get("STREAMCAST_TEST_BUCKET", "streamcast-demo")
    )
    b.add_argument("--out", default="bench-identity")
    m = sub.add_parser(
        "measure", help="run every arm, cold and warm, with and without the proxy"
    )
    m.add_argument("--out", default="bench-identity")
    m.add_argument("--repeat", type=int, default=5)
    m.add_argument(
        "--heavy-repeat", type=int, default=3, help="--repeat for dedupe and join"
    )
    m.add_argument("--memory-limit", default="1.5GB", help="DuckDB's, per database")
    m.add_argument(
        "--resume", action="store_true", help="skip arms already in records.jsonl"
    )
    m.add_argument("--arm", help=argparse.SUPPRESS)
    m.add_argument("--no-explain", dest="explain", action="store_false")
    m.add_argument("--only", nargs="*", help="run arms whose id contains one of these")
    c = sub.add_parser("clean", help="delete a build's tables from the store")
    c.add_argument("--out", default="bench-identity")
    args = parser.parse_args()
    if args.command == "clean":
        clean(args)
    else:
        asyncio.run(build(args) if args.command == "build" else measure(args))


if __name__ == "__main__":
    main()
