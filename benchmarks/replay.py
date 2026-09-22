"""What a replay costs, and which layer it is spent in.

    just bench-replay
    just bench-replay --rows 200000

The numbers `docs/SPEC.md` §4 sizes `max_replay` against come from here, and
the reason this exists as a benchmark rather than a paragraph is that the
paragraph was wrong: it claimed ~1M rows/s and an 80,000 msg/s threshold, both
guesses stated as measurements, and both about 2x optimistic.

Three things worth separating, because only one of them is streamcast's:

**fixed vs marginal.** A replay is a DuckDB scan with real setup — resolving
the Iceberg table, opening Parquet files — and then a cheap per-row walk. A
subscriber resuming 100 rows pays almost the same as one resuming 10,000, so a
single us/row figure taken at one size is meaningless on its own.

**cold vs warm.** The first scan in a process loads DuckDB extensions and
resolves metadata. That cost lands on the first subscriber to resume after a
broker starts, which is exactly the subscriber a restart produces.

**where it goes.** Split across the DuckDB read, the Arrow-to-Python
conversion, and the encode. If the encode ever stops being noise, the wire
format is the thing to look at; today it is ~1%.
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from pathlib import Path

import litelink
import pyarrow as pa

import streamcast
from streamcast._log import _next_batch, columns, replay
from streamcast._protocol import OFFSET, encode_projected

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("side", pa.int64()),
    ]
)


def row(i: int) -> dict:
    return {
        "event_ts": 1_790_038_800_000_000 + i,
        "trade_id": 386_550_397 + i,
        "price": 85_565.0 + (i % 100),
        "amount": 0.015,
        "side": i % 2,
    }


async def drain(log, count: int) -> float:
    started = time.perf_counter()
    seen = 0
    async for _offset, _frame in replay(log, 1, count + 1):
        seen += 1

    assert seen == count, (seen, count)

    return time.perf_counter() - started


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--seal", type=int, default=1024 * 1024)
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp())
    config = litelink.LogConfig(target_seal_size=args.seal, compact_min_files=2)
    with litelink.new(
        root, "trades", schema=SCHEMA, sort_by=("event_ts",), config=config
    ) as log:
        stream = streamcast.Stream("trades", log=log)
        for start in range(0, args.rows, 500):
            await stream.send_many(
                [row(i) for i in range(start, min(start + 500, args.rows))]
            )

        while log.seal() is not None:
            pass

        print(f"{args.rows:,} rows, {log.table_files()} parquet file(s)\n")

        # --- cold vs warm, at full size ------------------------------------
        cold = await drain(log, args.rows)
        warm = min([await drain(log, args.rows) for _ in range(3)])
        print(f"  {'cold (first scan in the process)':<38} {cold * 1e3:>8.1f} ms")
        print(
            f"  {'warm':<38} {warm * 1e3:>8.1f} ms"
            f"   {warm / args.rows * 1e6:.2f} us/row   {args.rows / warm:,.0f} rows/s"
        )

        # --- fixed vs marginal ---------------------------------------------
        print("\n  scaling (warm, best of 3):")
        print(f"  {'replay size':>14}  {'wall':>9}  {'per row':>10}")
        small = min([await drain(log, 100) for _ in range(3)])
        for n in (100, 1_000, 10_000, args.rows):
            if n > args.rows:
                continue

            best = small if n == 100 else min([await drain(log, n) for _ in range(3)])
            print(f"  {n:>14,}  {best * 1e3:>7.1f} ms  {best / n * 1e6:>8.2f} us")

        marginal = (warm - small) / (args.rows - 100)
        print(
            f"\n  → fixed ≈ {small * 1e3:.0f} ms per scan, "
            f"marginal ≈ {marginal * 1e6:.2f} us/row ({1 / marginal:,.0f} rows/s)"
        )

        # --- where the warm time goes ---------------------------------------
        names = (OFFSET, *columns(log))

        started = time.perf_counter()
        reader = log.scan(columns=names, start_offset=1, end_offset=args.rows + 1)
        batches = []
        while (batch := _next_batch(reader)) is not None:
            batches.append(batch)

        reader.close()
        scan = time.perf_counter() - started

        # The path `_log.replay` actually takes: Arrow builds each dict, in
        # the order the scan projected, and the encoder takes it as-is.
        started = time.perf_counter()
        pulled = [b.to_pylist() for b in batches]
        topy = time.perf_counter() - started

        started = time.perf_counter()
        total = sum(len(encode_projected(r)) for rows in pulled for r in rows)
        enc = time.perf_counter() - started

        whole = scan + topy + enc
        print("\n  where the time goes:")
        for label, value in (
            ("DuckDB scan", scan),
            ("Arrow → Python", topy),
            ("encode (msgspec)", enc),
        ):
            print(
                f"  {label:<24} {value * 1e3:>8.1f} ms"
                f"   {value / whole * 100:>5.1f}%   {value / args.rows * 1e6:>6.2f} us/row"
            )

        print(f"\n  {total / 1e6:.1f} MB of frames produced")

        # --- what that means for the two settings ---------------------------
        rate = args.rows / warm
        window = streamcast.MAX_REPLAY / rate
        print(
            f"\n  at this rate, a max_replay of {streamcast.MAX_REPLAY:,} takes "
            f"{window:.2f} s,\n  so a feed above "
            f"{streamcast.MAX_BACKLOG / window:,.0f} msg/s would overflow a "
            f"max_backlog of {streamcast.MAX_BACKLOG:,} during it."
        )


if __name__ == "__main__":
    asyncio.run(main())
