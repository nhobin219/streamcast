"""What reading a stream's history costs: one log against several.

    just bench-snapshot
    just bench-snapshot --rows 1000000 --logs 8
    just bench-snapshot --rows 100000 --logs 1 4 16      # one table, three shapes

A stream of `--rows` rows is written as `--logs` logs, each a migration that
adds a column, so the read is the real one: a `UNION ALL BY NAME` across
differing schemas. `price` rises with the offset, so a filter on it can rule
out every log but the last, which is what makes pruning measurable.

Four things are worth separating:

**cold vs warm.** The first query in a process loads DuckDB's extensions and
resolves the first table. That lands on the first read a process makes, which
for a one-shot `Stream.scan` in a script is the only one.

**the floor.** Opening a snapshot (the metadata file, the manifest, the live
table's hint) and reading one row. Nothing a reader does costs less, and how it
grows with the log count is the cost of the stream's shape.

**pruning.** The same selective read written as `where=` (every log opened)
and as `filters=` (retired logs ruled out on the manifest), and the cost of
deciding — `_manifest.prune` on its own.

**the scan.** A full count, a full read into Arrow, and an aggregate, where the
log count should barely matter once the bytes dominate.

Each figure is the median of `--repeat` runs on one open snapshot, except
`open` and `one-shot`, which open their own. Numbers move with hardware;
compare runs from the same session on the same machine.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import streamcast
from streamcast import _manifest

BASE: dict[str, Any] = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
        "amount": {"type": "number"},
        "side": {"type": "integer", "format": "int32"},
    },
    "required": ["event_ts", "price", "amount", "side"],
}
BATCH = 10_000


def schema(version: int) -> dict[str, Any]:
    """The base columns plus one nullable column per migration so far."""
    properties = dict(BASE["properties"])
    for k in range(1, version):
        properties[f"extra{k}"] = {"type": ["string", "null"]}

    return {**BASE, "properties": properties}


def row(i: int) -> dict[str, object]:
    return {
        "event_ts": 1_790_038_800_000_000 + i,
        "price": 1_000.0 + i,
        "amount": 0.015,
        "side": i % 2,
    }


async def build(root: Path, rows: int, logs: int) -> str:
    """A stream of `rows` rows across `logs` logs; its metadata URI."""
    per_log = rows // logs
    stream = streamcast.Stream.new("trades", root=root, schema=schema(1))
    sent = 0
    for version in range(1, logs + 1):
        if version > 1:
            await stream.aclose()
            stream = streamcast.Stream.migrate(
                "trades", root=root, schema=schema(version)
            )

        last = rows if version == logs else sent + per_log
        for start in range(sent, last, BATCH):
            await stream.send_many(
                [row(i) for i in range(start, min(start + BATCH, last))]
            )

        sent = last

    log = stream.log
    assert log is not None
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)
    uri = stream.metadata_uri
    assert uri is not None
    stream.ensure_metadata()
    await stream.aclose()
    return uri


async def median(repeat: int, call) -> float:
    """Median wall time of `call()` in milliseconds."""
    times = []
    for _ in range(repeat):
        started = time.perf_counter()
        await call()
        times.append((time.perf_counter() - started) * 1e3)

    return statistics.median(times)


async def measure(uri: str, rows: int, repeat: int) -> dict[str, float]:
    results: dict[str, float] = {}
    # A price only the last log holds, so `filters` can rule out every other.
    selective = 1_000.0 + rows - 100

    started = time.perf_counter()
    async with await streamcast.Stream.snapshot(uri) as snap:
        await snap.scan(start_offset=rows, end_offset=rows + 1)

    results["cold: open + 1 row"] = (time.perf_counter() - started) * 1e3

    async def opened() -> None:
        await (await streamcast.Stream.snapshot(uri)).close()

    results["open"] = await median(repeat, opened)

    async def one_shot() -> None:
        await streamcast.Stream.scan(uri, start_offset=rows, end_offset=rows + 1)

    results["one-shot: open + 1 row + close"] = await median(repeat, one_shot)

    async with await streamcast.Stream.snapshot(uri) as snap:
        await snap.scan()  # opens every table, so what follows is warm

        async def last_row() -> None:
            await snap.scan(start_offset=rows, end_offset=rows + 1)

        async def count() -> None:
            await snap.sql("SELECT count(*) FROM log")

        async def full() -> None:
            await snap.scan()

        async def aggregate() -> None:
            await snap.sql(
                "SELECT side, sum(amount), max(price) FROM log GROUP BY side"
            )

        async def where() -> None:
            await snap.scan(where=f"price > {selective}")

        async def filtered() -> None:
            await snap.scan(filters=[("price", ">", selective)])

        async def middle() -> None:
            await snap.scan(start_offset=rows // 2, end_offset=rows // 2 + 1_000)

        results["1 row (warm)"] = await median(repeat, last_row)
        results["1,000-row offset range"] = await median(repeat, middle)
        results["selective, where= (no pruning)"] = await median(repeat, where)
        results["selective, filters= (pruned)"] = await median(repeat, filtered)
        results["count(*)"] = await median(repeat, count)
        results["group by"] = await median(repeat, aggregate)
        results["full scan to Arrow"] = await median(repeat, full)

        names = [piece.entry.name for piece in snap._pieces]  # noqa: SLF001
        manifest = snap._manifest  # noqa: SLF001

        async def prune() -> None:
            _manifest.prune(manifest, names, [("price", ">", selective)])

        results["prune decision alone"] = await median(repeat * 10, prune)

    return results


async def one(rows: int, logs: int, repeat: int) -> None:
    """One config, in this process, printed as JSON for `main` to collect."""
    with tempfile.TemporaryDirectory() as tmp:
        started = time.perf_counter()
        uri = await build(Path(tmp), rows, logs)
        built = time.perf_counter() - started
        print(f"built {rows:,} rows in {logs} log(s) in {built:.1f} s", file=sys.stderr)
        print(json.dumps(await measure(uri, rows, repeat)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rows", type=int, nargs="+", default=[100_000])
    parser.add_argument("--logs", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--repeat", type=int, default=15)
    parser.add_argument("--one", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.one:
        asyncio.run(one(args.rows[0], args.logs[0], args.repeat))
        return

    # **Each config in a fresh process**, so every one has a genuinely cold
    # first query: DuckDB's extensions load once per process, and a second
    # config in the same one would report a warm number as cold.
    configs = [(rows, logs) for rows in args.rows for logs in args.logs]
    columns: dict[tuple[int, int], dict[str, float]] = {}
    for rows, logs in configs:
        child = subprocess.run(
            [
                sys.executable,
                __file__,
                "--one",
                "--rows",
                str(rows),
                "--logs",
                str(logs),
                "--repeat",
                str(args.repeat),
            ],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        columns[(rows, logs)] = json.loads(child.stdout.strip().splitlines()[-1])

    heads = [f"{rows:,} × {logs}" for rows, logs in configs]
    print()
    print("| ms | " + " | ".join(heads) + " |")
    print("|---|" + "---:|" * len(heads))
    for name in next(iter(columns.values())):
        cells = [f"{columns[config][name]:.2f}" for config in configs]
        print(f"| {name} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
