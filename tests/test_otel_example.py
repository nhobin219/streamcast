"""`examples/otel_logs.py` and `examples/otel_export.py`, run rather than read.

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

from examples import otel_export, otel_logs  # noqa: E402


class TestTheDemo:
    async def test_it_runs_start_to_finish(self, tmp_path):
        seen = await otel_logs.main(tmp_path)

        # Two services, three orders, one of them declined: 3 received + 3
        # outcomes from checkout, 3 from payments.
        assert seen["stored"] == 9

        # The live tail saw exactly the problems, as they happened.
        assert sorted(m["severity_text"] for m in seen["live"]) == ["ERROR", "WARN"]

        # Replayed by trace id: the failed request's three records, and only them.
        assert re.fullmatch(r"[0-9a-f]{32}", seen["failed"])
        # As a set: each service's batch processor exports on its own
        # schedule, so the two services' records interleave either way.
        assert sorted(
            (m["service"], m["body"]["string_value"]) for m in seen["one_request"]
        ) == [
            ("checkout", "order failed"),
            ("checkout", "order received"),
            ("payments", "card declined"),
        ]
        assert all(m["trace_id"].hex() == seen["failed"] for m in seen["one_request"])

        # And the table answers a log search's questions.
        assert seen["errors"] == [{"service": "payments", "errors": 1}]
        assert seen["lag_ms"] >= 0


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
        assert otel_logs.any_value(value) == stored


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


class TestTheExporter:
    """A stored row back to an SDK log record, and out through OTel's exporter."""

    def test_a_record_survives_the_round_trip_through_a_row(self):
        [original] = emitted()
        again = otel_export.record(otel_logs.row(original))

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

    def test_it_reaches_an_otlp_receiver_as_otel_encodes_it(self):
        """OTel's own batch processor and OTLP/HTTP exporter, to a local receiver.

        The receiver decodes the protobuf with OTel's own message type, so this
        reads what otel-gui or a Collector would.
        """
        import http.server
        import threading

        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
            ExportLogsServiceRequest,
        )
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

        received: list[tuple[str, bytes]] = []

        class Receiver(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 — the http.server name
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received.append((self.path, body))
                self.send_response(200)
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ANN401
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Receiver)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}/v1/logs"
            processor = BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint))
            [original] = emitted()
            processor.on_emit(otel_export.record(otel_logs.row(original)))
            assert processor.force_flush(timeout_millis=10_000)
            processor.shutdown()
        finally:
            server.shutdown()

        [(path, body)] = received
        assert path == "/v1/logs"
        request = ExportLogsServiceRequest.FromString(body)
        [resource] = request.resource_logs
        [scope] = resource.scope_logs
        [log] = scope.log_records
        assert {a.key: a.value.string_value for a in resource.resource.attributes} == {
            "service.name": "payments"
        }
        assert log.trace_id == original.log_record.trace_id.to_bytes(16, "big")
        assert log.body.string_value == "card declined"
        assert log.severity_text == "ERROR"
