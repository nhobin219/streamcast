"""`examples/otel/`, run rather than read.

An example that is only read is one that rots: an SDK renames a class, a
record grows a field, and nobody notices until a reader copies it. So the
demo runs end to end here — OTel SDK, exporter, broker, live tail, replay by
trace id, SQL over the table — and the two conversions are pinned case by
case, because they are what a reader copies.
"""

from __future__ import annotations

import asyncio
import math
import re
from typing import Any

import litelink
import pytest

pytest.importorskip("opentelemetry.sdk", reason="the OTel example's dev dependency")

import streamcast  # noqa: E402
from examples.otel import (  # noqa: E402
    analytics,
    common,
    demo,
    export,
    logs,
    metrics,
    spans,
)


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

    async def test_the_metrics_count_what_happened(self, tmp_path):
        """Totals, not row counts: how many rows a run makes depends on how
        many one-second collections it spans; what they add up to does not."""
        seen = await demo.main(tmp_path)

        assert seen["points"] > 0
        assert seen["outcomes"] == [
            {"outcome": "confirmed", "orders": 2},
            {"outcome": "failed", "orders": 1},
        ]
        assert [(d["service"], d["requests"]) for d in seen["durations"]] == [
            ("checkout", 3),
            ("payments", 3),
        ]
        assert all(d["mean_ms"] > 0 for d in seen["durations"])
        # The one failed order was counted inside its request's span: its
        # exemplar is that request's trace.
        assert seen["exemplars"] == [seen["failed"]]

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


class TestThePublisher:
    """What every exporter publishes through: telemetry must never fail the app."""

    async def test_a_broker_restart_costs_only_the_batches_sent_while_it_was_down(
        self, tmp_path, capsys
    ):
        schema = {
            "type": "object",
            "properties": {"i": {"type": "integer"}},
            "required": ["i"],
        }

        async def broker(port: int = 0):  # noqa: ANN202
            stream = streamcast.Stream.new("t", root=tmp_path, schema=schema)
            return await streamcast.serve(
                stream, "127.0.0.1", port, publish=True, maintain=False
            )

        server = await broker()
        port = server.sockets[0].getsockname()[1]
        publisher = common.Publisher(
            f"ws://127.0.0.1:{port}/t", asyncio.get_running_loop()
        )
        # `send` blocks on the loop, as OTel's export thread does.
        assert await asyncio.to_thread(publisher.send, [{"i": 1}])

        server.close()
        await server.wait_closed()
        # Down: dropped, not raised. Twice, but reported once.
        assert not await asyncio.to_thread(publisher.send, [{"i": 2}])
        assert not await asyncio.to_thread(publisher.send, [{"i": 3}])

        server = await broker(port)
        try:
            assert await asyncio.to_thread(publisher.send, [{"i": 4}])
            await publisher.aclose()
        finally:
            server.close()
            await server.wait_closed()

        # One line when the outage starts, one when it ends: not one a batch.
        printed = capsys.readouterr().out.splitlines()
        assert len(printed) == 2
        assert "is being dropped" in printed[0]
        assert "is being published again" in printed[1]
        with litelink.open(tmp_path, "t", read_only=True) as log:
            stored = log.sql("SELECT i FROM log ORDER BY litelink_offset").read_all()

        assert stored.column("i").to_pylist() == [1, 4]


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
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
            ExportLogsServiceRequest,
        )
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
            ExportMetricsServiceRequest,
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

            metric_exporter = OTLPMetricExporter(endpoint=f"{receiver}/v1/metrics")
            original_metrics, trace = collected()
            stored = metrics.rows(original_metrics)
            metric_exporter.export(export.metrics_data(stored))
            metric_exporter.shutdown()
        finally:
            server.shutdown()

        assert sorted(received) == ["/v1/logs", "/v1/metrics", "/v1/traces"]
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

        sent = ExportMetricsServiceRequest.FromString(received["/v1/metrics"])
        by_name = {m.name: m for m in sent.resource_metrics[0].scope_metrics[0].metrics}
        orders = by_name["shop.orders"].sum
        assert orders.aggregation_temporality == 1  # DELTA, as stored
        assert orders.is_monotonic
        [orders_point] = orders.data_points
        assert orders_point.as_int == 2
        [exemplar] = orders_point.exemplars
        assert exemplar.trace_id == trace.to_bytes(16, "big")
        [duration] = by_name["http.server.request.duration"].histogram.data_points
        assert list(duration.bucket_counts) == [1, 1, 0]


def collected():
    """Real SDK metrics of every kind, as a reader collects them.

    A counter measured inside a sampled span, so it carries an exemplar; an
    up-down counter, a gauge, an explicit-bucket and an exponential histogram.
    """
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.metrics.view import (
        ExponentialBucketHistogramAggregation,
        View,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

    reader = InMemoryMetricReader(preferred_temporality=metrics.PREFERRED_TEMPORALITY)
    provider = MeterProvider(
        [reader],
        resource=Resource({"service.name": "checkout"}),
        views=[
            View(
                instrument_name="shop.amount",
                aggregation=ExponentialBucketHistogramAggregation(),
            )
        ],
    )
    meter = provider.get_meter("shop", "1.0")
    with TracerProvider().get_tracer("t").start_as_current_span("request") as span:
        meter.create_counter("shop.orders", unit="{order}").add(
            2, {"outcome": "failed", "items": ("book", "pen")}
        )
        trace = span.get_span_context().trace_id

    meter.create_up_down_counter("shop.in_flight").add(-3)
    meter.create_gauge("shop.temperature", unit="Cel").set(21.5, {"room": "a"})
    duration = meter.create_histogram(
        "http.server.request.duration",
        unit="s",
        explicit_bucket_boundaries_advisory=[0.01, 0.1],
    )
    # Two buckets of three filled, unevenly: counts that read the same
    # backwards would hide a reversal.
    duration.record(0.005)
    duration.record(0.05)
    amount = meter.create_histogram("shop.amount")
    amount.record(0.3)
    amount.record(0)
    return reader.get_metrics_data(), trace


class TestTheMetrics:
    """A metrics export as rows, one per data point, and back."""

    def test_every_kind_survives_the_round_trip_as_otlp_encodes_it(self):
        """Compared as OTLP, encoded by OTel's own encoder: what a receiver gets."""
        from opentelemetry.exporter.otlp.proto.common.metrics_encoder import (
            encode_metrics,
        )

        original, _trace = collected()
        again = export.metrics_data(metrics.rows(original))

        assert encode_metrics(again) == encode_metrics(original)

    def test_a_row_is_one_data_point_carrying_its_metric(self):
        original, trace = collected()
        by_name = {row["name"]: row for row in metrics.rows(original)}

        orders = by_name["shop.orders"]
        assert (orders["type"], orders["temporality"], orders["is_monotonic"]) == (
            "sum",
            metrics.DELTA,
            True,
        )
        assert (orders["value_int"], orders["value_double"]) == (2, None)
        exemplars = orders["exemplars"]
        assert isinstance(exemplars, list)
        [exemplar] = exemplars
        assert exemplar["trace_id"] == trace.to_bytes(16, "big")

        # The SDK's `delta` preference keeps an up-down counter cumulative.
        assert by_name["shop.in_flight"]["temporality"] == metrics.CUMULATIVE
        assert by_name["shop.in_flight"]["value_int"] == -3
        gauge = by_name["shop.temperature"]
        assert (gauge["type"], gauge["temporality"], gauge["value_double"]) == (
            "gauge",
            None,
            21.5,
        )
        duration = by_name["http.server.request.duration"]
        assert duration["bucket_counts"] == [1, 1, 0]
        assert duration["explicit_bounds"] == [0.01, 0.1]
        amount = by_name["shop.amount"]
        assert (amount["type"], amount["count"], amount["zero_count"]) == (
            "exponential_histogram",
            2,
            1,
        )

    def test_a_non_finite_double_is_stored_as_null(self):
        """A stream refuses NaN and ±inf; the point is kept, the value is not."""
        from opentelemetry.sdk.metrics.export import (
            Gauge,
            Metric,
            MetricsData,
            NumberDataPoint,
            ResourceMetrics,
            ScopeMetrics,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope

        point = NumberDataPoint({}, None, 1, math.inf)  # ty: ignore[invalid-argument-type]
        data = MetricsData(
            [
                ResourceMetrics(
                    Resource({}),
                    [
                        ScopeMetrics(
                            InstrumentationScope("s"),
                            [Metric("g", "", "", Gauge([point]))],
                            "",
                        )
                    ],
                    "",
                )
            ]
        )
        [row] = metrics.rows(data)
        assert (row["value_int"], row["value_double"]) == (None, None)

    def test_the_exporter_asks_for_delta_counters_and_histograms(self):
        from opentelemetry.sdk.metrics import Counter, Histogram, UpDownCounter
        from opentelemetry.sdk.metrics.export import AggregationTemporality

        exporter = metrics.StreamMetricExporter(None)  # ty: ignore[invalid-argument-type]
        preferred = exporter._preferred_temporality  # noqa: SLF001
        assert preferred is not None
        assert preferred[Counter] == AggregationTemporality.DELTA
        assert preferred[Histogram] == AggregationTemporality.DELTA
        assert preferred[UpDownCounter] == AggregationTemporality.CUMULATIVE


def span_row(i: int, service: str, *, kind: int = 2, failed: bool = False) -> dict:
    """The fields `spans.SCHEMA` requires, plus the service: enough to count."""
    return {
        "trace_id": i.to_bytes(16, "big"),
        "span_id": i.to_bytes(8, "big"),
        "name": "POST /orders" if service == "checkout" else "POST /charge",
        "kind": kind,
        "start_time_unix_nano": 1_000_000_000 * i,
        "end_time_unix_nano": 1_000_000_000 * i + 5_000_000,
        "service": service,
        "status_code": spans.ERROR if failed else 1,
    }


class TestTheAnalytics:
    async def test_the_query_counts_each_services_requests_and_errors(
        self, tmp_path, serve
    ):
        """Through `Stream.live`, as the demo runs it: server spans only."""
        stream = streamcast.Stream.new("spans", root=tmp_path, schema=spans.SCHEMA)
        rows = [
            *(span_row(i, "checkout", failed=i == 0) for i in range(4)),
            *(span_row(10 + i, "payments", failed=i < 2) for i in range(4)),
            span_row(
                20, "checkout", kind=3, failed=True
            ),  # a client call: not a request
        ]
        await stream.send_many(rows)
        try:
            async with serve(stream, maintain=False) as uri:
                async with await streamcast.Stream.live(uri) as live:
                    await live.wait_for(len(rows))
                    table = await live.sql(analytics.query(3_600 * 1_000_000))

            counts = [
                {
                    k: r[k]
                    for k in ("service", "recent", "recent_errors", "total", "errors")
                }
                for r in table.to_pylist()
            ]
            assert counts == [
                {
                    "service": "checkout",
                    "recent": 4,
                    "recent_errors": 1,
                    "total": 4,
                    "errors": 1,
                },
                {
                    "service": "payments",
                    "recent": 4,
                    "recent_errors": 2,
                    "total": 4,
                    "errors": 2,
                },
            ]
            assert all(r["p95_ms"] == pytest.approx(5.0) for r in table.to_pylist())
        finally:
            await stream.aclose()

    def test_a_service_running_hot_is_flagged(self, capsys):
        hot = {
            "service": "payments",
            "recent": 10,
            "recent_errors": 5,
            "total": 100,
            "errors": 10,
            "p95_ms": 12.0,
        }
        steady = {
            "service": "checkout",
            "recent": 10,
            "recent_errors": 1,
            "total": 100,
            "errors": 10,
            "p95_ms": 3.0,
        }
        few = {
            "service": "billing",
            "recent": 2,
            "recent_errors": 2,
            "total": 100,
            "errors": 10,
            "p95_ms": None,
        }
        analytics.report([hot, steady, few], 30, 101)

        lines = capsys.readouterr().out.splitlines()
        flagged = [line for line in lines if "ABOVE LONG-TERM" in line]
        assert len(flagged) == 1
        assert "payments" in flagged[0]
        assert "50.0%" in flagged[0]
