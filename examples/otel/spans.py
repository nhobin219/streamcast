"""OTel's span as a stream's row, and an exporter that publishes it.

**How a span maps onto a row**, beyond what it shares with a log record:

* `kind` is OTLP's number (`SPAN_KIND_SERVER` is 2), not Python's `SpanKind`,
  which counts from one lower. The table holds the data model's value, so it
  reads the same whichever SDK wrote it.
* `flags` are the W3C trace flags. They must survive: an exporter drops a
  span that is not marked sampled.
* `events` and `links` are lists of structs, each with its own attributes.
* The SDK's dropped-attribute, -event and -link counts are not kept.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from examples.otel import common

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Sequence

    from opentelemetry.sdk.trace import ReadableSpan

    import streamcast


def _struct(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": [],
        "additionalProperties": False,
    }


SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "trace_id": common.TRACE_ID,
        "span_id": common.SPAN_ID,
        "parent_span_id": common.SPAN_ID,
        "trace_state": {"type": ["string", "null"]},
        "flags": {"type": ["integer", "null"], "format": "int32"},
        "name": {"type": "string"},
        "kind": {"type": "integer", "format": "int32"},
        "start_time_unix_nano": {"type": "integer"},
        "end_time_unix_nano": {"type": "integer"},
        "service": {"type": ["string", "null"]},
        "status_code": {"type": "integer", "format": "int32"},
        "status_message": {"type": ["string", "null"]},
        "attributes": common.ATTRIBUTES,
        "events": {
            "type": ["array", "null"],
            "items": _struct(
                {
                    "time_unix_nano": {"type": ["integer", "null"]},
                    "name": {"type": ["string", "null"]},
                    "attributes": common.ATTRIBUTES,
                }
            ),
        },
        "links": {
            "type": ["array", "null"],
            "items": _struct(
                {
                    "trace_id": common.TRACE_ID,
                    "span_id": common.SPAN_ID,
                    "attributes": common.ATTRIBUTES,
                }
            ),
        },
        "scope": common.SCOPE,
        "resource": common.ATTRIBUTES,
    },
    "required": [
        "trace_id",
        "span_id",
        "name",
        "kind",
        "start_time_unix_nano",
        "end_time_unix_nano",
        "status_code",
    ],
}

ERROR = 2
"""OTel's `StatusCode.ERROR`, the same number in Python and in OTLP."""


def row(span: ReadableSpan) -> dict[str, object]:
    """One finished OTel span as one row of `SCHEMA`."""
    context = span.get_span_context()
    assert context is not None  # a finished SDK span always has one
    resource = dict(span.resource.attributes) if span.resource else {}

    return {
        "trace_id": common.trace_id(context.trace_id),
        "span_id": common.span_id(context.span_id),
        "parent_span_id": common.span_id(span.parent.span_id) if span.parent else None,
        "trace_state": context.trace_state.to_header() or None,
        "flags": int(context.trace_flags),
        "name": span.name,
        "kind": span.kind.value + 1,
        "start_time_unix_nano": span.start_time,
        "end_time_unix_nano": span.end_time,
        "service": resource.get("service.name"),
        "status_code": span.status.status_code.value,
        "status_message": span.status.description,
        "attributes": common.attributes(span.attributes),
        "events": [
            {
                "time_unix_nano": event.timestamp,
                "name": event.name,
                "attributes": common.attributes(event.attributes),
            }
            for event in span.events
        ]
        or None,
        "links": [
            {
                "trace_id": common.trace_id(link.context.trace_id),
                "span_id": common.span_id(link.context.span_id),
                "attributes": common.attributes(link.attributes),
            }
            for link in span.links
        ]
        or None,
        "scope": common.scope(span.instrumentation_scope),
        "resource": common.attributes(resource),
    }


class StreamSpanExporter(SpanExporter):
    """Publishes each batch of finished OTel spans to a stream."""

    def __init__(
        self, publication: streamcast.Publication, loop: asyncio.AbstractEventLoop
    ) -> None:
        self._publication = publication
        self._loop = loop

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        rows = [row(span) for span in spans]
        if common.publish(self._publication, self._loop, rows):
            return SpanExportResult.SUCCESS

        return SpanExportResult.FAILURE

    def force_flush(self, timeout_millis: int = 30_000) -> bool:  # noqa: ARG002
        return True  # nothing buffered here; see `StreamLogExporter`

    def shutdown(self) -> None:
        pass
