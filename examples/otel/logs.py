"""OTel's log record as a stream's row, and an exporter that publishes it.

**How a record maps onto a row** (streamcast#40 has the reasoning):

* `trace_id` and `span_id` are fixed-size binary, written as hex on the wire,
  as OTLP/JSON writes them. An id of 0 means "not in a trace" and is null.
* `body` and every attribute value are OTel's `AnyValue`, a recursive union.
  No column type is recursive, so it is a struct with one field per scalar
  kind; arrays and key-value lists go into `json_value` as JSON text.
* A non-finite double is stored as null: a stream refuses NaN and ±inf, and
  dropping a whole record over one attribute is the wrong failure for logs.
* `service` is promoted out of the resource into its own column, because it
  is what you filter and group on.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult

from examples.otel import common

if TYPE_CHECKING:
    from collections.abc import Sequence

    from opentelemetry.sdk._logs import ReadableLogRecord

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "time_unix_nano": {"type": "integer"},
        "observed_time_unix_nano": {"type": "integer"},
        "severity_number": {"type": ["integer", "null"], "format": "int32"},
        "severity_text": {"type": ["string", "null"]},
        "service": {"type": ["string", "null"]},
        "body": {**common.ANY_VALUE, "type": ["object", "null"]},
        "trace_id": common.TRACE_ID,
        "span_id": common.SPAN_ID,
        "trace_flags": {"type": ["integer", "null"], "format": "int32"},
        "event_name": {"type": ["string", "null"]},
        "scope": common.SCOPE,
        "attributes": common.ATTRIBUTES,
        "resource": common.ATTRIBUTES,
    },
    "required": ["time_unix_nano", "observed_time_unix_nano"],
}

ERROR = 17
"""OTel's `SeverityNumber.ERROR`; everything from here up is an error."""


def row(record: ReadableLogRecord) -> dict[str, object]:
    """One OTel log record as one row of `SCHEMA`."""
    log = record.log_record
    resource = dict(record.resource.attributes) if record.resource else {}
    severity = log.severity_number

    return {
        "time_unix_nano": log.timestamp or log.observed_timestamp,
        "observed_time_unix_nano": log.observed_timestamp,
        "severity_number": None if severity is None else severity.value,
        "severity_text": log.severity_text,
        "service": resource.get("service.name"),
        "body": common.any_value(log.body),
        "trace_id": common.trace_id(log.trace_id or 0),
        "span_id": common.span_id(log.span_id or 0),
        "trace_flags": None if log.trace_flags is None else int(log.trace_flags),
        "event_name": log.event_name,
        "scope": common.scope(record.instrumentation_scope),
        "attributes": common.attributes(log.attributes),
        "resource": common.attributes(resource),
    }


class StreamLogExporter(LogRecordExporter):
    """Publishes each batch of OTel log records to a stream."""

    def __init__(self, publisher: common.Publisher) -> None:
        self._publisher = publisher

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        rows = [row(record) for record in batch]
        if self._publisher.send(rows):
            return LogRecordExportResult.SUCCESS

        return LogRecordExportResult.FAILURE

    def force_flush(self, timeout_millis: int = 30_000) -> bool:  # noqa: ARG002
        # Nothing buffered here: `export` returns only once the stream has
        # acknowledged the batch. The processor above this does the buffering.
        return True

    def shutdown(self) -> None:
        pass
