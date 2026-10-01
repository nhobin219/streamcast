"""Re-export the OTel streams as OTLP, to any OpenTelemetry receiver.

    just demo otel        # the broker, this exporter and otel-gui's dashboard

A streamcast subscriber on one side and OpenTelemetry's own OTLP exporters on
the other. It turns each row of `logs.py`'s and `spans.py`'s schemas back into
an SDK log record or span and hands it to the SDK's batch processor and OTLP
exporter — so the batching, the protobuf encoding and the retries are
OpenTelemetry's, and the receiver can be anything that speaks OTLP/HTTP: a
local viewer like [otel-gui](https://github.com/metafab/otel-gui), a
Collector, or a vendor's endpoint.

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
import signal
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import ReadWriteLogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
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
    from collections.abc import Callable, Mapping

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
    print(f"exporting OTLP to {receiver}", flush=True)
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                follow(f"{broker}/logs", lambda row: log_processor.on_emit(record(row)))
            )
            group.create_task(
                follow(f"{broker}/spans", lambda row: span_processor.on_end(span(row)))
            )
    finally:
        log_processor.shutdown()
        span_processor.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--broker", default=BROKER, help="serving /logs and /spans")
    parser.add_argument(
        "--receiver", default=RECEIVER, help="an OTLP/HTTP base URL, without /v1/..."
    )
    arguments = parser.parse_args()
    # As in `demo.py`: started in the background, SIGINT arrives ignored.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.broker, arguments.receiver))
