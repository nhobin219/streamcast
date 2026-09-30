"""Re-export a stream of OTel log rows as OTLP, to any OpenTelemetry receiver.

    just demo-otel        # the broker, this exporter and otel-gui's dashboard

A streamcast subscriber on one side and OpenTelemetry's own OTLP exporter on
the other. It turns each row of `examples/otel/logs.py`'s schema back into an
SDK log record and hands it to the SDK's `BatchLogRecordProcessor` and
`OTLPLogExporter` — so the batching, the protobuf encoding and the retries are
OpenTelemetry's, and the receiver can be anything that speaks OTLP/HTTP: a
local viewer like [otel-gui](https://github.com/metafab/otel-gui), a
Collector, or a vendor's endpoint.

It exists because OTel viewers are RECEIVERS: data is pushed to them, and none
subscribes to a WebSocket. A stream is where the logs live; this is how any of
them reads it.

It replays from the start of the stream, then follows it live, and retries
until the broker is up, so the pieces can start in any order. The exporter
retries a receiver that is not up yet.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import ReadWriteLogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    set_span_in_context,
)

import streamcast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from opentelemetry.context import Context

BROKER = "ws://127.0.0.1:8766/logs"
RECEIVER = "http://127.0.0.1:4318/v1/logs"


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


def record(row: Mapping[str, Any]) -> ReadWriteLogRecord:
    """One stored row as the SDK's log record: the inverse of `logs.row`."""
    severity = row["severity_number"]
    scope = row["scope"] or {}
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
        instrumentation_scope=(
            InstrumentationScope(scope["name"], scope.get("version"))
            if scope.get("name")
            else None
        ),
    )


def span_context(row: Mapping[str, Any]) -> Context | None:
    """The row's trace and span, as the context OTel attaches a log record to.

    A context rather than `trace_id=`/`span_id=` on the record, which OTel
    deprecated in 1.35. The client hands binary columns back as bytes, and
    OTel keeps ids as ints.
    """
    if not row["trace_id"]:
        return None

    span = SpanContext(
        trace_id=int.from_bytes(row["trace_id"], "big"),
        span_id=int.from_bytes(row["span_id"], "big") if row["span_id"] else 0,
        is_remote=True,
        trace_flags=TraceFlags(row["trace_flags"] or 0),
    )
    return set_span_in_context(NonRecordingSpan(span))


async def follow(broker: str, processor: BatchLogRecordProcessor) -> None:
    """Every row from the start of the stream, then live, into the processor."""
    while True:
        try:
            try:
                async with streamcast.connect(
                    broker, offset=streamcast.EARLIEST
                ) as sub:
                    await pump(broker, sub, processor)

            except streamcast.NotReplayable:
                # The log holds nothing yet: follow it live instead.
                async with streamcast.connect(broker) as sub:
                    await pump(broker, sub, processor)

        except OSError:
            await asyncio.sleep(1)  # the broker is not up yet


async def pump(
    broker: str, sub: streamcast.Subscription, processor: BatchLogRecordProcessor
) -> None:
    print(f"following {broker}", flush=True)
    async for _offset, row in sub:
        # `on_emit` only queues: the batch processor's own thread does the
        # encoding and the HTTP, never this event loop.
        processor.on_emit(record(row))


async def main(broker: str, receiver: str) -> None:
    exporter = OTLPLogExporter(endpoint=receiver)
    # A short delay, so a dashboard sees each record within half a second.
    processor = BatchLogRecordProcessor(exporter, schedule_delay_millis=500)
    print(f"exporting OTLP to {receiver}", flush=True)
    try:
        await follow(broker, processor)
    finally:
        processor.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument(
        "--receiver", default=RECEIVER, help="an OTLP/HTTP logs endpoint"
    )
    arguments = parser.parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(arguments.broker, arguments.receiver))
