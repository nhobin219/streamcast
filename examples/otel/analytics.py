"""Real-time analytics on the spans stream: each service's error rate, now against always.

    just demo otel        # the broker, the services, the exporter, and this

A `Stream.live` view of `/spans`: the published tables plus every span the
broker has sent since, so one SQL query answers over the whole history up to
the span that arrived a moment ago. Every few seconds it asks, per service,
for the error rate over the last `--window` seconds — by `streamcast_ts`,
when the broker took the span — against the error rate over everything, and
p95 latency, and prints a line. A service whose recent rate is well above its
long-term one is flagged.

No consumer loop, no running totals kept by hand: the view keeps itself
current, and the question is a query.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import time

import streamcast
from examples.otel import spans

BROKER = "ws://127.0.0.1:8766"
SERVER = 2
"""OTLP's `SPAN_KIND_SERVER`: one span per request a service handled."""

# Recent above long-term by this factor, over at least this many requests,
# is worth a line of its own. Small samples swing too far to mean anything.
HOT = 1.5
ENOUGH = 5


def query(window_us: int) -> str:
    """Per service: requests and errors over the window and over everything."""
    since = time.time_ns() // 1_000 - window_us
    recent = f"streamcast_ts >= {since}"
    failed = f"status_code = {spans.ERROR}"
    return f"""
        SELECT
            service,
            count(*) FILTER (WHERE {recent})                 AS recent,
            count(*) FILTER (WHERE {recent} AND {failed})    AS recent_errors,
            count(*)                                         AS total,
            count(*) FILTER (WHERE {failed})                 AS errors,
            quantile_cont(
                (end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95
            ) FILTER (WHERE {recent})                        AS p95_ms
        FROM log
        WHERE kind = {SERVER}
        GROUP BY service
        ORDER BY service
    """


def report(rows: list[dict], window: float, end_offset: int) -> None:
    stamp = time.strftime("%H:%M:%S")
    if not rows:
        print(f"[{stamp}] no requests yet", flush=True)
        return

    for row in rows:
        recent = row["recent"]
        recent_rate = row["recent_errors"] / recent if recent else 0.0
        overall = row["errors"] / row["total"] if row["total"] else 0.0
        hot = recent >= ENOUGH and recent_rate > overall * HOT
        p95 = row["p95_ms"]
        print(
            f"[{stamp}] {row['service']:<9} "
            f"last {window:.0f}s: {recent_rate:6.1%} errors of {recent:3} requests"
            f"  | all {row['total']:5}: {overall:6.1%}"
            f"  | p95 {'—' if p95 is None else f'{p95:6.1f} ms'}"
            f"{'  ▲ ABOVE LONG-TERM' if hot else ''}",
            flush=True,
        )

    print(f"[{stamp}] (as of offset {end_offset - 1})", flush=True)


async def main(broker: str, every: float, window: float) -> None:
    uri = f"{broker}/spans"
    while True:
        try:
            async with await streamcast.Stream.live(uri) as live:
                print(f"analysing {uri} live, every {every:.0f}s", flush=True)
                while True:
                    table = await live.sql(query(int(window * 1_000_000)))
                    report(table.to_pylist(), window, live.end_offset)
                    await asyncio.sleep(every)

        except OSError:
            await asyncio.sleep(1)  # the broker is not up yet


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--broker", default=BROKER, help="serving /spans")
    parser.add_argument(
        "--every", type=float, default=5.0, help="seconds between reports"
    )
    parser.add_argument(
        "--window", type=float, default=30.0, help="seconds counted as recent"
    )
    arguments = parser.parse_args()
    # As in `export.py`: started in the background, SIGINT arrives ignored.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.broker, arguments.every, arguments.window))
