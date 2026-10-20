"""What `_streamcast_id` costs and saves, measured in object-store bytes and requests.

    just rustfs
    uv run --with numpy python benchmarks/identity.py build --rows 20000000 --out bench-identity
    uv run --with numpy python benchmarks/identity.py measure --out bench-identity

`build` writes ONE generated table twice, as two streams published to S3: the
same rows, one declared with `identity=("session_id", "line_number")`, so its
writer fills `_streamcast_id`, and one without. Both go through `Stream.new` and
`Stream.ingest`, the bulk path a backfill takes, and are published by litelink
exactly as a stream's maintainer would publish them: Iceberg tables, Parquet
with footer statistics. The rows are sessions-like: `--concurrent` sessions
open at once, each a run of dense `line_number`s, interleaved in time order as
a live feed writes them, so a file holds slices of many sessions and its
`session_id` bounds are wide.

`measure` reads both through `s3proxy.CountingProxy`, a forwarding proxy in
front of the store that records every request, and runs each query in
`QUERIES` against each stream. For every query it reports the requests, the
bytes the store sent back, and those bytes attributed to the Parquet column
chunks they fell in, from the files' own footers: so "did DuckDB read
`session_id` to answer a lookup by `_streamcast_id`" is answered by the byte
ranges it fetched, not by its plan. Each query runs two ways:

* `streamcast`: through `Stream.snapshot`, which is what the pull request
  changes. `filters=` that fix the whole identity get the key term added
  beside the natural ones (`_snapshot._keyed`).
* `raw`: the same predicate over `iceberg_scan` of the published table on a
  bare DuckDB connection, which is what DuckDB itself does with it, without
  the snapshot's offset bounds and union around it.

Reads are uncached (`memory_cache=False`, and a fresh connection for every
raw query), so every query pays for what it reads. Wall times are reported but
are not the store's: every byte crosses a Python proxy. Bytes and requests are
the measurement.

The store is the `just rustfs` container unless `AWS_ENDPOINT_URL` and the AWS
credentials say otherwise; `--bucket` names the bucket.
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
VARIANTS = {"natural": None, "keyed": IDENTITY}
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


# -- the table -----------------------------------------------------------------


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


async def publish(
    name: str,
    identity: tuple[str, ...] | None,
    shape: Shape,
    prefix: str,
    options: litelink.S3Options,
    work: Path,
) -> dict[str, Any]:
    """One stream of the table, published under `prefix`; what a reader needs."""
    root = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=work))
    try:
        stream = streamcast.Stream.new(
            name,
            root=root,
            schema=SCHEMA,
            published=prefix,
            s3_options=options,
            identity=identity,
        )
        await stream.aclose()

        samples: list[dict[str, Any]] = []
        reader = pa.RecordBatchReader.from_batches(
            SCHEMA_ARROW, sessions(shape, samples)
        )
        started = time.perf_counter()
        streamcast.Stream.ingest(
            name, reader, root=root, s3_options=options, publish=True, flush=True
        )
        loaded = time.perf_counter() - started

        stream = streamcast.Stream.new(
            name,
            root=root,
            schema=SCHEMA,
            published=prefix,
            s3_options=options,
            identity=identity,
        )
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
            "identity": identity,
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
    """Files, row groups and each column's compressed bytes, from the footers."""
    fs = filesystem(options)
    columns: dict[str, int] = {}
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
                columns[chunk.path_in_schema] = (
                    columns.get(chunk.path_in_schema, 0) + chunk.total_compressed_size
                )
                stats[chunk.path_in_schema] = chunk.is_stats_set

    return {
        "files": len(paths),
        "rows": rows,
        "row_groups": groups,
        "bytes": size,
        "column_bytes": columns,
        "column_has_stats": stats,
    }


async def build(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    options = rustfs()
    prefix = f"s3://{args.bucket}/identity-bench/{uuid.uuid4().hex[:12]}"
    shape = Shape(args.rows, args.concurrent, args.mean_lines, args.batch, args.seed)
    streams = {}
    for name, identity in VARIANTS.items():
        print(f"building {name}: {shape.rows:,} rows under {prefix}", file=sys.stderr)
        built = await publish(name, identity, shape, prefix, options, out)
        built["layout"] = layout(data_files(built["table"], options), options)
        print(
            f"  {built['ingest_seconds']:.1f} s to ingest; "
            f"{built['layout']['files']} files, {built['layout']['row_groups']} row groups, "
            f"{built['layout']['bytes'] / 1e6:.1f} MB",
            file=sys.stderr,
        )
        streams[name] = built

    manifest = {
        "prefix": prefix,
        "shape": shape.__dict__,
        "streams": streams,
        "versions": versions(),
    }
    (out / "build.json").write_text(json.dumps(manifest, indent=2, default=list))
    print(f"wrote {out / 'build.json'}", file=sys.stderr)


# -- the queries ---------------------------------------------------------------


def blob(key: bytes) -> str:
    return "'" + "".join(f"\\x{b:02x}" for b in key) + "'::BLOB"


def quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class Query:
    """One question, asked through a snapshot and as raw SQL.

    `streamcast` takes the snapshot and a sample and returns the reader;
    `raw` returns a SQL predicate-or-query over the relation `t`. Either is
    None where the question does not apply (a keyed predicate on the
    natural stream).
    """

    name: str
    keyed_only: bool
    streamcast: Callable[[Any, dict[str, Any]], Any] | None
    raw: Callable[[dict[str, Any]], str] | None


def lookup(sample: dict[str, Any]) -> str:
    return (
        f"session_id = {quoted(sample['session_id'])} "
        f"AND line_number = {sample['line_number']}"
    )


def keyed(sample: dict[str, Any]) -> str:
    return f'"_streamcast_id" = {blob(sample["key"])}'


def join_keys(samples: list[dict[str, Any]]) -> tuple[str, str]:
    natural = ", ".join(
        f"({quoted(s['session_id'])}, {s['line_number']})" for s in samples
    )
    hashed = ", ".join(f"({blob(s['key'])})" for s in samples)
    return natural, hashed


QUERIES = [
    Query(
        "lookup: filters= (key term added on keyed)",
        False,
        lambda snap, s: snap.scan(
            filters=[
                ("session_id", "==", s["session_id"]),
                ("line_number", "==", s["line_number"]),
            ]
        ),
        lambda s: f"SELECT * FROM t WHERE {lookup(s)}",
    ),
    Query(
        "lookup: natural predicate only",
        False,
        lambda snap, s: snap.sql(f"SELECT * FROM log WHERE {lookup(s)}"),
        lambda s: f"SELECT * FROM t WHERE {lookup(s)}",
    ),
    Query(
        "lookup: _streamcast_id only",
        True,
        lambda snap, s: snap.sql(
            f'SELECT * EXCLUDE ("_streamcast_id") FROM log WHERE {keyed(s)}'
        ),
        lambda s: f'SELECT * EXCLUDE ("_streamcast_id") FROM t WHERE {keyed(s)}',
    ),
    Query(
        "lookup: _streamcast_id and natural",
        True,
        lambda snap, s: snap.sql(
            f'SELECT * EXCLUDE ("_streamcast_id") FROM log WHERE {keyed(s)} AND {lookup(s)}'
        ),
        lambda s: (
            f'SELECT * EXCLUDE ("_streamcast_id") FROM t WHERE {keyed(s)} AND {lookup(s)}'
        ),
    ),
    Query(
        "whole session: filters= session_id",
        False,
        lambda snap, s: snap.scan(filters=[("session_id", "==", s["session_id"])]),
        lambda s: f"SELECT * FROM t WHERE session_id = {quoted(s['session_id'])}",
    ),
    Query(
        "distinct keys: natural columns",
        False,
        lambda snap, s: snap.sql(
            "SELECT count(*) FROM (SELECT DISTINCT session_id, line_number FROM log)"
        ),
        lambda s: (
            "SELECT count(*) FROM (SELECT DISTINCT session_id, line_number FROM t)"
        ),
    ),
    Query(
        "distinct keys: _streamcast_id",
        True,
        lambda snap, s: snap.sql('SELECT count(DISTINCT "_streamcast_id") FROM log'),
        lambda s: 'SELECT count(DISTINCT "_streamcast_id") FROM t',
    ),
    Query(
        "semi-join 64 keys: natural columns",
        False,
        lambda snap, s: snap.sql(
            "SELECT count(*) FROM log JOIN (VALUES "
            + s["join"][0]
            + ") k(s, n) ON log.session_id = k.s AND log.line_number = k.n"
        ),
        lambda s: (
            "SELECT count(*) FROM t JOIN (VALUES "
            + s["join"][0]
            + ") k(s, n) ON t.session_id = k.s AND t.line_number = k.n"
        ),
    ),
    Query(
        "semi-join 64 keys: _streamcast_id",
        True,
        lambda snap, s: snap.sql(
            "SELECT count(*) FROM log JOIN (VALUES "
            + s["join"][1]
            + ') k(id) ON log."_streamcast_id" = k.id'
        ),
        lambda s: (
            "SELECT count(*) FROM t JOIN (VALUES "
            + s["join"][1]
            + ') k(id) ON t."_streamcast_id" = k.id'
        ),
    ),
]


class Attribution:
    """Byte ranges of a table's files, mapped to the column chunks they fall in."""

    def __init__(
        self, chunks: dict[str, list[tuple[int, int, str]]], bucket_path: str
    ) -> None:
        self._chunks = chunks
        self._prefix = bucket_path

    @classmethod
    def open(cls, table: str, options: litelink.S3Options) -> Attribution:
        fs = filesystem(options)
        chunks: dict[str, list[tuple[int, int, str]]] = {}
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

                    spans.append(
                        (
                            first,
                            first + chunk.total_compressed_size,
                            chunk.path_in_schema,
                        )
                    )

            chunks["/" + path] = spans

        return cls(chunks, table)

    def columns(self, seen: Seen) -> dict[str, int]:
        """Bytes per column; what fell in no column chunk is `(footer/other)`,
        and what was read from no data file is `(metadata)`."""
        totals: dict[str, int] = {}
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
            for first, last, name in spans:
                overlap = min(high, last) - max(low, first)
                if overlap > 0:
                    totals[name] = totals.get(name, 0) + overlap
                    covered += overlap

            rest = request.bytes_out - covered
            if rest:
                totals["(footer/other)"] = totals.get("(footer/other)", 0) + rest

        return dict(sorted(totals.items(), key=lambda kv: -kv[1]))


def sample_inputs(built: dict[str, Any]) -> list[dict[str, Any]]:
    identity = _identity.Identity(IDENTITY, SCHEMA_ARROW)
    samples = [dict(s, key=identity.key(s)) for s in built["samples"]]
    joined = join_keys(samples)
    return [dict(s, join=joined) for s in samples]


def raw_connection(options: litelink.S3Options) -> duckdb.DuckDBPyConnection:
    """A bare DuckDB connection reading S3 through `options`, uncached."""
    connection = litelink.duckdb_connection(s3_options=options, memory_cache=False)
    connection.execute("SET enable_external_file_cache = false")
    return connection


@dataclass(frozen=True)
class Run:
    table: pa.Table
    seconds: float
    seen: Seen


async def through_snapshot(
    query: Query,
    built: dict[str, Any],
    sample: dict[str, Any],
    proxy: CountingProxy,
    options: litelink.S3Options,
) -> Run:
    assert query.streamcast is not None
    # The snapshot is opened OUTSIDE the measured block, so a query's bytes
    # are its own, not the metadata and manifest every open reads.
    snap = await streamcast.Stream.snapshot(
        built["metadata_uri"], s3_options=options, memory_cache=False
    )
    try:
        with proxy.measure() as seen:
            started = time.perf_counter()
            table = await query.streamcast(snap, sample).read_all()
            return Run(table, time.perf_counter() - started, seen)
    finally:
        await snap.close()


def through_duckdb(
    query: Query,
    built: dict[str, Any],
    sample: dict[str, Any],
    proxy: CountingProxy,
    options: litelink.S3Options,
) -> Run:
    assert query.raw is not None
    connection = raw_connection(options)
    metadata = litelink.current_metadata(built["table"], s3_options=options)
    sql = query.raw(sample).replace(" FROM t", f" FROM iceberg_scan('{metadata}') t")
    try:
        with proxy.measure() as seen:
            started = time.perf_counter()
            table = connection.execute(sql).to_arrow_table()
            return Run(table, time.perf_counter() - started, seen)
    finally:
        connection.close()


async def run_query(
    query: Query,
    mode: str,
    built: dict[str, Any],
    samples: list[dict[str, Any]],
    proxy: CountingProxy,
    options: litelink.S3Options,
    attribution: Attribution,
    repeat: int,
) -> dict[str, Any] | None:
    call = query.streamcast if mode == "streamcast" else query.raw
    if call is None or (query.keyed_only and built["identity"] is None):
        return None

    runs: list[Run] = []
    for i in range(repeat):
        sample = samples[(i * 17 + 5) % len(samples)]
        if mode == "streamcast":
            runs.append(await through_snapshot(query, built, sample, proxy, options))
        else:
            runs.append(through_duckdb(query, built, sample, proxy, options))

    last = runs[-1]
    sizes = [run.seen.bytes_out for run in runs]
    return {
        "query": query.name,
        "mode": mode,
        "rows": last.table.num_rows,
        "first": last.table.slice(0, 1).to_pylist()[0] if last.table.num_rows else None,
        "median_seconds": statistics.median(run.seconds for run in runs),
        "median_requests": statistics.median(run.seen.requests for run in runs),
        "median_bytes": statistics.median(sizes),
        "bytes_range": [min(sizes), max(sizes)],
        "columns": attribution.columns(last.seen),
        "methods": last.seen.methods(),
    }


async def measure(args: argparse.Namespace) -> None:
    out = Path(args.out)
    manifest = json.loads((out / "build.json").read_text())
    direct = rustfs()
    results: list[dict[str, Any]] = []
    with CountingProxy(direct.endpoint or "") as proxy:
        through = litelink.S3Options(
            endpoint=proxy.endpoint,
            access_key=direct.access_key,
            secret_key=direct.secret_key,
            region=direct.region,
        )
        for name, built in manifest["streams"].items():
            built["identity"] = tuple(built["identity"]) if built["identity"] else None
            samples = sample_inputs(built)
            attribution = Attribution.open(built["table"], direct)
            for query in QUERIES:
                if args.only and not any(o in query.name for o in args.only):
                    continue

                for mode in args.modes:
                    result = await run_query(
                        query,
                        mode,
                        built,
                        samples,
                        proxy,
                        through,
                        attribution,
                        args.repeat,
                    )
                    if result is None:
                        continue

                    result["stream"] = name
                    results.append(result)
                    print(
                        f"{name:8} {mode:10} {query.name:44} rows={result['rows']:<6} "
                        f"req={result['median_requests']:<5} "
                        f"bytes={result['median_bytes'] / 1e6:8.3f} MB "
                        f"{result['median_seconds'] * 1e3:8.1f} ms",
                        file=sys.stderr,
                    )

    report = {"build": manifest, "versions": versions(), "results": results}
    path = out / f"measure-{time.strftime('%Y%m%dT%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    print(table(results))
    print(f"\nwrote {path}", file=sys.stderr)


def table(results: list[dict[str, Any]]) -> str:
    lines = [
        "| query | mode | stream | rows | requests | MB read | ms | top columns by bytes |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for r in results:
        top = ", ".join(f"{c} {b / 1e6:.2f}" for c, b in list(r["columns"].items())[:4])
        lines.append(
            f"| {r['query']} | {r['mode']} | {r['stream']} | {r['rows']} | "
            f"{r['median_requests']:.0f} | {r['median_bytes'] / 1e6:.3f} | "
            f"{r['median_seconds'] * 1e3:.0f} | {top} |"
        )

    return "\n".join(lines)


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
    b = sub.add_parser("build", help="generate and publish both streams")
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
    m = sub.add_parser("measure", help="run the queries through the counting proxy")
    m.add_argument("--out", default="bench-identity")
    m.add_argument("--repeat", type=int, default=3)
    m.add_argument(
        "--modes",
        nargs="+",
        default=["streamcast", "raw"],
        choices=["streamcast", "raw"],
    )
    m.add_argument(
        "--only", nargs="*", help="run queries whose name contains one of these"
    )
    c = sub.add_parser("clean", help="delete a build's tables from the store")
    c.add_argument("--out", default="bench-identity")
    args = parser.parse_args()
    if args.command == "clean":
        clean(args)
    else:
        asyncio.run(build(args) if args.command == "build" else measure(args))


if __name__ == "__main__":
    main()
