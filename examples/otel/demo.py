"""OpenTelemetry logs, traces and metrics, through streams, into tables you can query.

    uv run python -m examples.otel.demo    # once, printing what it saw
    just demo otel                         # the same roles as processes, live

Nothing here is part of streamcast. The OTel schemas and conversions live in
`logs.py`, `spans.py` and `metrics.py`, built from ordinary column types. The OTel packages
are dev dependencies, for this example only.

The three roles, in one process so a test can run it: the broker, the
producer (`services.py`) and the subscribers are each what they would be as
separate processes, talking over sockets, and nothing below changes but the
URI. `just demo otel` runs them as separate processes.

1. **A broker** serves three streams, `logs`, `spans` and `metrics`, each
   with a log, accepting remote publishers.
2. **The producer**: two services, checkout and payments, handle traced
   requests, log through the standard `logging` module, and count orders and
   time requests. OTel's SDK turns each into records, spans and metric data
   points, published by the exporters in `logs.py`, `spans.py` and
   `metrics.py`.
3. **A live tail** follows the problems as they happen — `where=` a membership
   filter on `severity_text` — and **a replay** reads back one request by its
   trace id from both streams, `where={"trace_id": "<32 hex chars>"}`.
4. **The stored tables** answer what a log search, a trace view and a metrics
   dashboard would: errors per service, the slowest request, ingest lag,
   orders by outcome, mean request duration per service, and the failed
   order's exemplar, which names the failed request's trace.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import streamcast
from examples.otel import logs, metrics, spans
from examples.otel.services import exporting, traffic

if TYPE_CHECKING:
    pass

# -- the broker -------------------------------------------------------------------


def streams(
    root: Path,
) -> tuple[streamcast.Stream, streamcast.Stream, streamcast.Stream]:
    return (
        streamcast.Stream.new("logs", root=root, schema=logs.SCHEMA),
        streamcast.Stream.new("spans", root=root, schema=spans.SCHEMA),
        streamcast.Stream.new("metrics", root=root, schema=metrics.SCHEMA),
    )


# -- the one-shot demo ------------------------------------------------------------


def query(stream: streamcast.Stream, sql: str) -> list[dict[str, Any]]:
    assert stream.log is not None
    return stream.log.sql(sql).read_all().to_pylist()


async def one_trace(
    uri: str, stream: streamcast.Stream, trace: str
) -> list[dict[str, Any]]:
    """Every row of one trace, replayed by its id — hex text, as a tracing tool
    would hand it to you. A filtered replay's window is an upper bound, not a
    count (see `Greeting.replay`), so the table says how many rows to expect.
    """
    [counted] = query(
        stream, f"SELECT count(*) AS n FROM log WHERE lower(hex(trace_id)) = '{trace}'"
    )
    async with streamcast.connect(
        uri, offset=streamcast.EARLIEST, where={"trace_id": trace}
    ) as sub:
        return [(await sub.recv())[2] for _ in range(counted["n"])]


async def main(root: Path) -> dict[str, Any]:
    """Run the whole demo under `root`, print it, and return what it saw."""
    log_stream, span_stream, metric_stream = streams(root)
    # No maintainer, as in the migration demo: a run this short never fills a
    # log enough to seal it. Its five processes were still opening the logs
    # when the run ended, and cost the run several seconds. `just demo otel`'s
    # broker keeps `serve`'s default, which starts them.
    server = await streamcast.serve(
        [log_stream, span_stream, metric_stream],
        "127.0.0.1",
        0,
        maintain=False,
    )
    base = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        # **The live tail, subscribed before anything is logged.** A filter
        # list is membership, so this follows every error and warning as the
        # broker takes it — the view an on-call dashboard wants.
        async with streamcast.connect(
            # OTel's text for Python's WARNING level is "WARN".
            f"{base}/logs",
            where={"severity_text": ["ERROR", "WARN"]},
        ) as tail:
            async with exporting(base) as exporters:
                failed, problems = await asyncio.to_thread(traffic, exporters)

            live: list[dict[str, Any]] = [
                (await tail.recv())[2] for _ in range(problems)
            ]

        failed_logs = await one_trace(f"{base}/logs", log_stream, failed)
        failed_spans = await one_trace(f"{base}/spans", span_stream, failed)
        errors = query(
            log_stream,
            f"SELECT service, count(*) AS errors FROM log "
            f"WHERE severity_number >= {logs.ERROR} GROUP BY service ORDER BY service",
        )
        # A request's duration is its root span's: the one with no parent.
        [slowest] = query(
            span_stream,
            "SELECT max(end_time_unix_nano - start_time_unix_nano) / 1e6 AS ms "
            "FROM log WHERE parent_span_id IS NULL",
        )
        # `streamcast_ts` is when the broker took the row, in microseconds;
        # the record's own time is nanoseconds. The difference is how far
        # behind the broker each record arrived.
        [lag] = query(
            log_stream,
            "SELECT max(streamcast_ts * 1000 - time_unix_nano) / 1e6 AS ms FROM log",
        )
        # Each row of `shop.orders` is one interval's count (delta temporality),
        # so the total is a sum.
        outcomes = query(
            metric_stream,
            "SELECT attributes['outcome'].string_value AS outcome, "
            "sum(value_int)::BIGINT AS orders FROM log "
            "WHERE name = 'shop.orders' GROUP BY outcome ORDER BY outcome",
        )
        durations = query(
            metric_stream,
            "SELECT service, sum(count)::BIGINT AS requests, "
            "sum(sum) / sum(count) * 1e3 AS mean_ms FROM log "
            "WHERE name = 'http.server.request.duration' "
            "GROUP BY service ORDER BY service",
        )
        # The failed order was counted inside its request's span, so the SDK
        # kept it as an exemplar: from the metric straight to the trace.
        exemplars = query(
            metric_stream,
            "SELECT lower(hex(e.trace_id)) AS trace FROM ("
            "SELECT unnest(exemplars) AS e FROM log WHERE name = 'shop.orders' "
            "AND attributes['outcome'].string_value = 'failed')",
        )
        [stored_logs] = query(log_stream, "SELECT count(*) AS n FROM log")
        [stored_spans] = query(span_stream, "SELECT count(*) AS n FROM log")
        [stored_points] = query(metric_stream, "SELECT count(*) AS n FROM log")
    finally:
        server.close()
        await server.wait_closed()

    print(
        f"{stored_logs['n']} log records, {stored_spans['n']} spans and "
        f"{stored_points['n']} metric data points stored"
    )
    print("live tail, errors and warnings as they happened:")
    for message in live:
        print(
            f"  {message['service']:>8}  {message['severity_text']:<7} {message['body']['string_value']}"
        )

    print(f"the failed request, trace {failed}:")
    for span in sorted(failed_spans, key=lambda s: s["start_time_unix_nano"]):
        status = "ERROR" if span["status_code"] == spans.ERROR else ""
        print(f"  {span['service']:>8}  span    {span['name']:<14} {status}")

    for message in failed_logs:
        print(
            f"  {message['service']:>8}  {message['severity_text']:<7} {message['body']['string_value']}"
        )

    print(f"errors per service: {errors}")
    print(f"slowest request: {slowest['ms']:.1f} ms")
    print(f"ingest lag, worst: {lag['ms']:.1f} ms")
    print(f"orders by outcome: {outcomes}")
    for row in durations:
        print(
            f"  {row['service']:>8}  {row['requests']} requests, "
            f"mean {row['mean_ms']:.2f} ms"
        )

    print(f"the failed order's exemplar: trace {[e['trace'] for e in exemplars]}")

    return {
        "logs": stored_logs["n"],
        "spans": stored_spans["n"],
        "failed": failed,
        "live": live,
        "failed_logs": failed_logs,
        "failed_spans": failed_spans,
        "errors": errors,
        "slowest_ms": slowest["ms"],
        "lag_ms": lag["ms"],
        "points": stored_points["n"],
        "outcomes": outcomes,
        "durations": durations,
        "exemplars": [e["trace"] for e in exemplars],
    }


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(main(Path(directory)))
