"""Re-export the OTel streams as OTLP, to any OpenTelemetry receiver.

    just demo otel        # the broker, this exporter and otel-gui's dashboard

A streamcast subscriber on one side and OpenTelemetry's own OTLP exporters on
the other. It turns each row of `logs.py`'s and `spans.py`'s schemas back into
an SDK log record or span and hands it to the SDK's batch processor and OTLP
exporter — so the batching, the protobuf encoding and the retries are
OpenTelemetry's, and the receiver can be anything that speaks OTLP/HTTP: a
local viewer like [otel-gui](https://github.com/metafab/otel-gui), a
Collector, or a vendor's endpoint.

Metrics have no batch processor to hand a data point to: the SDK's metric
pipeline starts at instruments, not at points. So rows of `metrics.py`'s
schema are collected for half a second, grouped back into the
resource → scope → metric nesting an export carries, and handed to OTel's
OTLP metric exporter as one `MetricsData`.

It exists because OTel viewers are RECEIVERS: data is pushed to them, and none
subscribes to a WebSocket. A stream is where the telemetry lives; this is how
any of them reads it.

It replays each stream from the start, then follows it live, and retries until
the broker is up, so the pieces can start in any order. The exporters retry a
receiver that is not up yet.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import signal
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import ReadWriteLogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import Exemplar
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Buckets,
    ExponentialHistogram,
    ExponentialHistogramDataPoint,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    MetricsData,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import (
    Link,
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
    TraceState,
    set_span_in_context,
)

import streamcast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from opentelemetry.context import Context

BROKER = "ws://127.0.0.1:8766"
RECEIVER = "http://127.0.0.1:4318"

# -- rows back to OTel's values ---------------------------------------------------


def value(any_value: Mapping[str, Any] | None) -> Any:  # noqa: ANN401 — OTel's AnyValue
    """A row's `AnyValue` struct back to the Python value OTel's SDK takes."""
    if not any_value:
        return None

    for field in (
        "string_value",
        "bool_value",
        "int_value",
        "double_value",
        "bytes_value",
    ):
        if any_value.get(field) is not None:
            return any_value[field]

    if any_value.get("json_value") is not None:
        # Arrays and key-value lists were stored as JSON text; parsed back,
        # the exporter encodes them as OTLP's own array and kvlist values.
        return json.loads(any_value["json_value"])

    return None


def attributes(values: Mapping[str, Any] | None) -> dict[str, Any]:
    return {key: value(item) for key, item in (values or {}).items()}


def frozen(values: Mapping[str, Any] | None) -> BoundedAttributes:
    """Attributes as a live SDK span holds them, arrays as tuples.

    A `LogRecord` does this to its own attributes; a `ReadableSpan` is built
    finished, so it is done here.
    """
    return BoundedAttributes(attributes=attributes(values), immutable=True)


def scope(row: Mapping[str, Any]) -> InstrumentationScope | None:
    stored = row["scope"] or {}
    if not stored.get("name"):
        return None

    return InstrumentationScope(stored["name"], stored.get("version"))


def ident(stored: bytes | None) -> int:
    """The client hands binary columns back as bytes; OTel keeps ids as ints."""
    return int.from_bytes(stored, "big") if stored else 0


# -- a log row back to a log record -----------------------------------------------


def record(row: Mapping[str, Any]) -> ReadWriteLogRecord:
    """One stored log row as the SDK's log record: the inverse of `logs.row`."""
    severity = row["severity_number"]
    log = LogRecord(
        timestamp=row["time_unix_nano"],
        observed_timestamp=row["observed_time_unix_nano"],
        context=span_context(row),
        severity_text=row["severity_text"],
        severity_number=None if severity is None else SeverityNumber(severity),
        body=value(row["body"]),
        attributes=attributes(row["attributes"]),
        event_name=row["event_name"],
    )

    return ReadWriteLogRecord(
        log,
        resource=Resource(attributes(row["resource"])),
        instrumentation_scope=scope(row),
    )


def span_context(row: Mapping[str, Any]) -> Context | None:
    """The log row's trace and span, as the context OTel attaches a record to.

    A context rather than `trace_id=`/`span_id=` on the record, which OTel
    deprecated in 1.35.
    """
    if not row["trace_id"]:
        return None

    span = SpanContext(
        trace_id=ident(row["trace_id"]),
        span_id=ident(row["span_id"]),
        is_remote=True,
        trace_flags=TraceFlags(row["trace_flags"] or 0),
    )
    return set_span_in_context(NonRecordingSpan(span))


# -- a span row back to a span ----------------------------------------------------


def span(row: Mapping[str, Any]) -> ReadableSpan:
    """One stored span row as the SDK's finished span: the inverse of `spans.row`."""
    trace = ident(row["trace_id"])
    flags = TraceFlags(row["flags"] or 0)
    state = TraceState.from_header([row["trace_state"]]) if row["trace_state"] else None
    parent = row["parent_span_id"]

    return ReadableSpan(
        name=row["name"],
        context=SpanContext(trace, ident(row["span_id"]), False, flags, state),
        parent=SpanContext(trace, ident(parent), False, flags) if parent else None,
        resource=Resource(attributes(row["resource"])),
        attributes=frozen(row["attributes"]),
        events=[
            Event(e["name"], frozen(e["attributes"]), e["time_unix_nano"])
            for e in row["events"] or ()
        ],
        links=[
            Link(
                SpanContext(ident(link["trace_id"]), ident(link["span_id"]), True),
                frozen(link["attributes"]),
            )
            for link in row["links"] or ()
        ],
        # Stored as OTLP's number, which is Python's `SpanKind` plus one.
        kind=SpanKind(max(row["kind"] - 1, 0)),
        status=Status(StatusCode(row["status_code"]), row["status_message"]),
        start_time=row["start_time_unix_nano"],
        end_time=row["end_time_unix_nano"],
        instrumentation_scope=scope(row),
    )


# -- metric rows back to an export ------------------------------------------------


def number(row: Mapping[str, Any]) -> float | None:
    """`value_int` or `value_double`, whichever the row holds."""
    return row["value_int"] if row["value_int"] is not None else row["value_double"]


def exemplars(row: Mapping[str, Any]) -> list[Exemplar]:
    # An exemplar always has a value; a null one was non-finite, stored as
    # null because a stream refuses NaN and ±inf. NaN is the closest return.
    return [
        Exemplar(
            frozen(e["filtered_attributes"]),
            math.nan if number(e) is None else number(e),  # ty: ignore[invalid-argument-type]
            e["time_unix_nano"],
            ident(e["span_id"]) or None,
            ident(e["trace_id"]) or None,
        )
        for e in row["exemplars"] or ()
    ]


def buckets(stored: Mapping[str, Any] | None) -> Buckets:
    stored = stored or {}
    return Buckets(stored.get("offset") or 0, list(stored.get("bucket_counts") or ()))


def point(
    row: Mapping[str, Any],
) -> NumberDataPoint | HistogramDataPoint | ExponentialHistogramDataPoint:
    """One stored data point back as the SDK's: the inverse of `metrics.rows`."""
    shared = {
        "attributes": frozen(row["attributes"]),
        # None for a gauge, which has no start, as the SDK leaves it.
        "start_time_unix_nano": row["start_time_unix_nano"],
        "time_unix_nano": row["time_unix_nano"],
        "exemplars": exemplars(row),
    }
    if row["type"] in ("sum", "gauge"):
        return NumberDataPoint(value=number(row), **shared)  # ty: ignore[invalid-argument-type]

    histogram = {
        "count": row["count"],
        "sum": row["sum"],
        "min": row["min"],
        "max": row["max"],
    }
    if row["type"] == "histogram":
        return HistogramDataPoint(
            bucket_counts=tuple(row["bucket_counts"] or ()),
            explicit_bounds=tuple(row["explicit_bounds"] or ()),
            **histogram,
            **shared,
        )

    return ExponentialHistogramDataPoint(
        scale=row["scale"],
        zero_count=row["zero_count"],
        positive=buckets(row["positive"]),
        negative=buckets(row["negative"]),
        flags=row["flags"] or 0,
        **histogram,
        **shared,
    )


def data(
    row: Mapping[str, Any], points: list
) -> Sum | Gauge | Histogram | ExponentialHistogram:
    """The metric's data, of the row's type, holding `points`."""
    kind = row["type"]
    if kind == "gauge":
        return Gauge(points)

    temporality = AggregationTemporality(row["temporality"])
    if kind == "sum":
        return Sum(points, temporality, bool(row["is_monotonic"]))

    if kind == "histogram":
        return Histogram(points, temporality)

    return ExponentialHistogram(points, temporality)


def metrics_data(rows: Iterable[Mapping[str, Any]]) -> MetricsData:
    """Stored data points regrouped as one export: resource, scope, metric, points.

    A metric is identified as OTLP identifies one: by its resource and scope,
    and its name, unit, type and temporality. Points keep their order.
    """
    grouped: dict[str, dict[str, dict[tuple, list[Mapping[str, Any]]]]] = {}
    for row in rows:
        resource = json.dumps(row["resource"], sort_keys=True, default=str)
        scope_key = json.dumps(row["scope"], sort_keys=True)
        metric = (
            row["name"],
            row["description"],
            row["unit"],
            row["type"],
            row["temporality"],
            row["is_monotonic"],
        )
        by_scope = grouped.setdefault(resource, {})
        by_scope.setdefault(scope_key, {}).setdefault(metric, []).append(row)

    resource_metrics = []
    for resource, by_scope in grouped.items():
        scope_metrics = []
        for by_metric in by_scope.values():
            found = []
            for points in by_metric.values():
                first = points[0]
                found.append(
                    Metric(
                        first["name"],
                        first["description"] or "",
                        first["unit"] or "",
                        data(first, [point(row) for row in points]),
                    )
                )

            first = next(iter(by_metric.values()))[0]
            scope_metrics.append(
                ScopeMetrics(scope(first) or InstrumentationScope(""), found, "")
            )

        resource_metrics.append(
            ResourceMetrics(
                Resource(attributes(json.loads(resource))), scope_metrics, ""
            )
        )

    return MetricsData(resource_metrics)


async def export_metrics(
    exporter: OTLPMetricExporter,
    pending: list[Mapping[str, Any]],
    every: float = 0.5,
) -> None:
    """Every `every` seconds, what has arrived goes out as one OTLP export."""
    while True:
        await asyncio.sleep(every)
        if pending:
            batch = pending[:]
            pending.clear()
            # Off the loop: the exporter's HTTP and retries block.
            await asyncio.to_thread(exporter.export, metrics_data(batch))


# -- following the streams --------------------------------------------------------


async def follow(uri: str, emit: Callable[[Mapping[str, Any]], None]) -> None:
    """Every row from the start of the stream, then live, handed to `emit`."""
    while True:
        try:
            try:
                async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                    await pump(uri, sub, emit)

            except streamcast.NotReplayable:
                # The log holds nothing yet: follow it live instead.
                async with streamcast.connect(uri) as sub:
                    await pump(uri, sub, emit)

        except OSError:
            await asyncio.sleep(1)  # the broker is not up yet


async def pump(
    uri: str, sub: streamcast.Subscription, emit: Callable[[Mapping[str, Any]], None]
) -> None:
    print(f"following {uri}", flush=True)
    async for _offset, _ts, row in sub:
        emit(row)


async def main(broker: str, receiver: str) -> None:
    # A short delay, so a dashboard sees each record within half a second.
    # `on_emit` and `on_end` only queue: each batch processor's own thread
    # does the encoding and the HTTP, never this event loop.
    log_processor = BatchLogRecordProcessor(
        OTLPLogExporter(endpoint=f"{receiver}/v1/logs"), schedule_delay_millis=500
    )
    span_processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint=f"{receiver}/v1/traces"), schedule_delay_millis=500
    )
    metric_exporter = OTLPMetricExporter(endpoint=f"{receiver}/v1/metrics")
    metric_rows: list[Mapping[str, Any]] = []
    print(f"exporting OTLP to {receiver}", flush=True)
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                follow(f"{broker}/logs", lambda row: log_processor.on_emit(record(row)))
            )
            group.create_task(
                follow(f"{broker}/spans", lambda row: span_processor.on_end(span(row)))
            )
            group.create_task(follow(f"{broker}/metrics", metric_rows.append))
            group.create_task(export_metrics(metric_exporter, metric_rows))
    finally:
        log_processor.shutdown()
        span_processor.shutdown()
        if metric_rows:
            metric_exporter.export(metrics_data(metric_rows))

        metric_exporter.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--broker", default=BROKER, help="serving /logs, /spans and /metrics"
    )
    parser.add_argument(
        "--receiver", default=RECEIVER, help="an OTLP/HTTP base URL, without /v1/..."
    )
    arguments = parser.parse_args()
    # As in `demo.py`: started in the background, SIGINT arrives ignored.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.broker, arguments.receiver))
