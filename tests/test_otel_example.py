"""`examples/otel/`, run rather than read.

An example that is only read is one that rots: an SDK renames a class, a
record grows a field, and nobody notices until a reader copies it. So the
demo runs end to end here — OTel SDK, exporter, broker, live tail, replay by
trace id, SQL over the table — and the two conversions are pinned case by
case, because they are what a reader copies.
"""

from __future__ import annotations

import math
import re
from typing import Any

import pytest

pytest.importorskip("opentelemetry.sdk", reason="the OTel example's dev dependency")

from examples.otel import common, demo, export, logs, spans  # noqa: E402


class TestTheDemo:
    async def test_it_runs_start_to_finish(self, tmp_path):
        seen = await demo.main(tmp_path)

        # Two services, three orders, one of them declined: 3 received + 3
        # outcomes from checkout, 3 from payments.
        assert seen["logs"] == 9

        # The live tail saw exactly the problems, as they happened.
        assert sorted(m["severity_text"] for m in seen["live"]) == ["ERROR", "WARN"]

        # Replayed by trace id: the failed request's three records, and only them.
        assert re.fullmatch(r"[0-9a-f]{32}", seen["failed"])
        # As a set: each service's batch processor exports on its own
        # schedule, so the two services' records interleave either way.
        assert sorted(
            (m["service"], m["body"]["string_value"]) for m in seen["failed_logs"]
        ) == [
            ("checkout", "order failed"),
            ("checkout", "order received"),
            ("payments", "card declined"),
        ]
        assert all(m["trace_id"].hex() == seen["failed"] for m in seen["failed_logs"])

        # And the table answers a log search's questions.
        assert seen["errors"] == [{"service": "payments", "errors": 1}]
        assert seen["lag_ms"] >= 0

    async def test_the_failed_request_is_one_trace_across_both_services(self, tmp_path):
        seen = await demo.main(tmp_path)
        # Three spans a request: checkout's request, its call, payments' charge.
        assert seen["spans"] == 9
        assert seen["slowest_ms"] > 0

        by_name = {s["name"]: s for s in seen["failed_spans"]}
        request, call, charge = (
            by_name["POST /orders"],
            by_name["charge card"],
            by_name["POST /charge"],
        )
        # The parent links cross the services: that is the service map's edge.
        assert request["parent_span_id"] is None
        assert call["parent_span_id"] == request["span_id"]
        assert charge["parent_span_id"] == call["span_id"]
        assert [s["service"] for s in (request, call, charge)] == [
            "checkout",
            "checkout",
            "payments",
        ]
        # OTLP's numbers: SERVER is 2 and CLIENT 3, one above Python's.
        assert [s["kind"] for s in (request, call, charge)] == [2, 3, 2]
        assert all(s["status_code"] == spans.ERROR for s in (request, call, charge))
        [declined] = charge["events"]
        assert declined["name"] == "card declined"
        assert declined["attributes"]["amount"]["double_value"] == 250.0

        # payments' error is logged inside payments' span.
        [error] = [m for m in seen["failed_logs"] if m["severity_text"] == "ERROR"]
        assert error["span_id"] == charge["span_id"]


class TestAnyValue:
    """OTel's recursive union, as a struct with one field per scalar kind."""

    @pytest.mark.parametrize(
        ("value", "stored"),
        [
            ("text", {"string_value": "text"}),
            # bool before int: in Python True IS 1, and must not be stored so.
            (True, {"bool_value": True}),
            (42, {"int_value": 42}),
            (2**70, {"json_value": str(2**70)}),
            (1.5, {"double_value": 1.5}),
            # A stream refuses non-finite floats; the record is kept.
            (math.nan, {"double_value": None}),
            (math.inf, {"double_value": None}),
            (b"\x00\xff", {"bytes_value": b"\x00\xff"}),
            (("book", "pen"), {"json_value": '["book", "pen"]'}),
            ({"k": 1}, {"json_value": '{"k": 1}'}),
            (None, None),
        ],
    )
    def test_each_kind(self, value, stored):
        assert common.any_value(value) == stored


def emitted() -> list:
    """Real SDK log records: `logging` through OTel's handler, captured in memory."""
    import logging

    from opentelemetry.instrumentation.logging.handler import LoggingHandler
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import (
        InMemoryLogRecordExporter,
        SimpleLogRecordProcessor,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

    captured = InMemoryLogRecordExporter()
    provider = LoggerProvider(resource=Resource({"service.name": "payments"}))
    provider.add_log_record_processor(SimpleLogRecordProcessor(captured))
    logger = logging.getLogger("test.otel.export")
    logger.propagate = False
    logger.addHandler(LoggingHandler(logger_provider=provider))
    with TracerProvider().get_tracer("t").start_as_current_span("request"):
        logger.error(
            "card declined",
            extra={
                "order_id": 2,
                "amount": 250.0,
                "items": ("book", "pen"),
                "raw": b"\x01",
            },
        )

    return list(captured.get_finished_logs())


def finished() -> list:
    """A real finished SDK span, child of another, with an event and a link."""
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace import Link, SpanKind, Status, StatusCode

    captured = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource({"service.name": "payments"}))
    provider.add_span_processor(SimpleSpanProcessor(captured))
    tracer = provider.get_tracer("t", "1.0")
    with tracer.start_as_current_span("POST /orders") as parent:
        other = parent.get_span_context()
        with tracer.start_as_current_span(
            "POST /charge",
            kind=SpanKind.SERVER,
            attributes={"amount": 250.0, "items": ("book", "pen")},
            links=[Link(other, {"cause": "retry"})],
        ) as span:
            span.add_event("card declined", {"amount": 250.0})
            span.set_status(Status(StatusCode.ERROR, "card declined"))

    return [s for s in captured.get_finished_spans() if s.name == "POST /charge"]


class TestTheExporter:
    """A stored row back to an SDK log record, and out through OTel's exporter."""

    def test_a_record_survives_the_round_trip_through_a_row(self):
        [original] = emitted()
        again = export.record(logs.row(original))

        was, now = original.log_record, again.log_record
        assert now.trace_id == was.trace_id != 0
        assert now.span_id == was.span_id
        assert now.severity_number == was.severity_number
        assert now.severity_text == was.severity_text
        assert now.body == was.body
        assert now.timestamp == was.timestamp
        assert again.resource is not None
        assert dict(again.resource.attributes)["service.name"] == "payments"
        assert again.instrumentation_scope is not None
        assert original.instrumentation_scope is not None
        assert again.instrumentation_scope.name == original.instrumentation_scope.name
        # Every attribute returns exactly — an array included: stored as JSON
        # text, parsed back to a list, and frozen to a tuple by the SDK as the
        # original was.
        assert dict(now.attributes) == dict(was.attributes)  # ty: ignore[no-matching-overload]
        assert dict(now.attributes)["items"] == ("book", "pen")  # ty: ignore[no-matching-overload]

    def test_a_span_survives_the_round_trip_through_a_row(self):
        [original] = finished()
        again = export.span(spans.row(original))

        assert again.name == original.name
        assert again.context == original.context
        assert again.context.trace_flags.sampled  # else an exporter drops it
        assert again.parent is not None
        assert original.parent is not None
        assert again.parent.span_id == original.parent.span_id
        assert again.kind == original.kind
        assert again.status.status_code == original.status.status_code
        assert again.status.description == original.status.description
        assert (again.start_time, again.end_time) == (
            original.start_time,
            original.end_time,
        )
        assert dict(again.attributes or {}) == dict(original.attributes or {})
        assert [
            (e.name, e.timestamp, dict(e.attributes or {})) for e in again.events
        ] == [(e.name, e.timestamp, dict(e.attributes or {})) for e in original.events]
        [link] = again.links
        assert link.context.span_id == original.links[0].context.span_id
        assert dict(link.attributes or {}) == {"cause": "retry"}
        assert again.resource.attributes["service.name"] == "payments"

    def test_it_reaches_an_otlp_receiver_as_otel_encodes_it(self):
        """OTel's own batch processor and OTLP/HTTP exporter, to a local receiver.

        The receiver decodes the protobuf with OTel's own message type, so this
        reads what otel-gui or a Collector would.
        """
        import http.server
        import threading

        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
            ExportLogsServiceRequest,
        )
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
            ExportTraceServiceRequest,
        )
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        received: dict[str, bytes] = {}

        class Receiver(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 — the http.server name
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received[self.path] = body
                self.send_response(200)
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ANN401
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Receiver)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            receiver = f"http://127.0.0.1:{server.server_port}"
            log_processor = BatchLogRecordProcessor(
                OTLPLogExporter(endpoint=f"{receiver}/v1/logs")
            )
            [original] = emitted()
            log_processor.on_emit(export.record(logs.row(original)))
            assert log_processor.force_flush(timeout_millis=10_000)
            log_processor.shutdown()

            span_processor = BatchSpanProcessor(
                OTLPSpanExporter(endpoint=f"{receiver}/v1/traces")
            )
            [original_span] = finished()
            span_processor.on_end(export.span(spans.row(original_span)))
            assert span_processor.force_flush(timeout_millis=10_000)
            span_processor.shutdown()
        finally:
            server.shutdown()

        assert sorted(received) == ["/v1/logs", "/v1/traces"]
        request = ExportLogsServiceRequest.FromString(received["/v1/logs"])
        [resource] = request.resource_logs
        [scope] = resource.scope_logs
        [log] = scope.log_records
        assert {a.key: a.value.string_value for a in resource.resource.attributes} == {
            "service.name": "payments"
        }
        assert log.trace_id == original.log_record.trace_id.to_bytes(16, "big")
        assert log.body.string_value == "card declined"
        assert log.severity_text == "ERROR"

        traces = ExportTraceServiceRequest.FromString(received["/v1/traces"])
        [span] = traces.resource_spans[0].scope_spans[0].spans
        assert span.name == "POST /charge"
        assert span.kind == 2  # OTLP's SPAN_KIND_SERVER, as stored
        assert span.parent_span_id == original_span.parent.span_id.to_bytes(8, "big")
        assert span.status.code == 2  # STATUS_CODE_ERROR
        assert [e.name for e in span.events] == ["card declined"]
