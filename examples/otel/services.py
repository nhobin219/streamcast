"""A producer: two services logging and tracing through OTel, into a broker.

    just demo otel    # otel-gui, a broker, this, and the OTLP exporter

Two simulated services, checkout and payments, handle orders as traced
requests, log through the standard `logging` module, and record metrics: an
order count by outcome and each request's duration. OTel's SDK turns each
into log records, spans and metric data points, and the exporters in
`logs.py`, `spans.py` and `metrics.py` publish them, as a client, to the
broker's `logs`, `spans` and `metrics` streams. About one order in five
fails: payments declines the card, and the failed request is one trace
across both services.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import math
import random
import signal
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

from examples.otel import common, logs, metrics, spans

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter, Histogram
    from opentelemetry.trace import Tracer

DURATION_BOUNDS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25)
"""Request-duration buckets, in seconds: these requests take milliseconds."""

# -- the services ---------------------------------------------------------------


@dataclass
class Exporters:
    logs: logs.StreamLogExporter
    spans: spans.StreamSpanExporter
    metrics: metrics.StreamMetricExporter


@dataclass
class Service:
    logger: logging.Logger
    tracer: Tracer
    log_provider: LoggerProvider
    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    handler: logging.Handler
    orders: Counter
    """Orders handled, by `outcome`: what a dashboard's error rate is."""
    duration: Histogram
    """`http.server.request.duration`, OTel's semantic convention, in seconds."""

    def detach(self) -> None:
        """Take the handler off the logger, which outlives this service.

        `logging.getLogger` is process-wide: a handler left on it sends a
        later run's records to this run's exporters as well.
        """
        self.logger.removeHandler(self.handler)

    def shutdown(self) -> None:
        """Detach, then flush and stop every provider."""
        self.detach()
        self.tracer_provider.shutdown()
        self.log_provider.shutdown()
        # Last: its final collection is the requests made before this call.
        self.meter_provider.shutdown()


def instrument(name: str, exporters: Exporters) -> Service:
    """A service's logger and tracer, exported through OTel to the streams."""
    resource = Resource.create({"service.name": name})
    log_provider = LoggerProvider(resource=resource)
    # Half a second for both: the SDK's defaults (1 s for logs, 5 s for spans)
    # would show a dashboard each request's logs seconds before its trace.
    log_provider.add_log_record_processor(
        BatchLogRecordProcessor(exporters.logs, schedule_delay_millis=500)
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(exporters.spans, schedule_delay_millis=500)
    )
    # Every second, so a chart moves; each collection is one row per series.
    meter_provider = MeterProvider(
        [PeriodicExportingMetricReader(exporters.metrics, export_interval_millis=1000)],
        resource=resource,
    )
    meter = meter_provider.get_meter("shop")

    logger = logging.getLogger(f"shop.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = LoggingHandler(logger_provider=log_provider)
    logger.addHandler(handler)
    return Service(
        logger,
        tracer_provider.get_tracer("shop"),
        log_provider,
        tracer_provider,
        meter_provider,
        handler,
        meter.create_counter(
            "shop.orders", unit="{order}", description="Orders handled"
        ),
        meter.create_histogram(
            "http.server.request.duration",
            unit="s",
            description="Duration of HTTP server requests",
            explicit_bucket_boundaries_advisory=DURATION_BOUNDS,
        ),
    )


def order(
    checkout: Service, payments: Service, order_id: int, amount: float
) -> str | None:
    """One traced request through both services. Returns its trace id if it failed."""
    failed = Status(StatusCode.ERROR, "card declined")
    started = time.perf_counter()
    with checkout.tracer.start_as_current_span(
        "POST /orders", kind=SpanKind.SERVER, attributes={"order.id": order_id}
    ) as request:
        checkout.logger.info(
            "order received", extra={"order_id": order_id, "items": ("book", "pen")}
        )
        # In production the call crosses the network and a `traceparent`
        # header carries the context; in one process the current span does.
        with (
            checkout.tracer.start_as_current_span(
                "charge card", kind=SpanKind.CLIENT
            ) as call,
            payments.tracer.start_as_current_span(
                "POST /charge", kind=SpanKind.SERVER, attributes={"amount": amount}
            ) as charge,
        ):
            charging = time.perf_counter()
            declined = amount > 100
            if declined:
                charge.add_event("card declined", {"amount": amount})
                charge.set_status(failed)
                call.set_status(failed)
                payments.logger.error(
                    "card declined",
                    extra={
                        "order_id": order_id,
                        "amount": amount,
                        "retry_ratio": math.inf,
                    },
                )
            else:
                payments.logger.info(
                    "card charged", extra={"order_id": order_id, "amount": amount}
                )

            # Inside the span, so the SDK can keep this measurement as an
            # exemplar: a metric's link back to the trace it measured.
            payments.duration.record(
                time.perf_counter() - charging,
                {
                    "http.route": "/charge",
                    "http.response.status_code": 402 if declined else 200,
                },
            )

        if declined:
            request.set_status(failed)
            checkout.logger.warning("order failed", extra={"order_id": order_id})
        else:
            checkout.logger.info("order confirmed", extra={"order_id": order_id})

        outcome = "failed" if declined else "confirmed"
        checkout.orders.add(1, {"outcome": outcome})
        checkout.duration.record(
            time.perf_counter() - started,
            {
                "http.route": "/orders",
                "http.response.status_code": 402 if declined else 201,
            },
        )
        return format(request.get_span_context().trace_id, "032x") if declined else None


def traffic(exporters: Exporters) -> tuple[str, int]:
    """Two services handling three requests, one of which fails.

    Returns the failed request's trace id, and how many ERROR or WARN
    records were logged — which is what tells the live tail when to stop.

    Runs on a worker thread: the exporters block on the event loop, so a
    flush from the loop's own thread would wait on itself.
    """
    checkout = instrument("checkout", exporters)
    payments = instrument("payments", exporters)
    failed = ""
    for order_id, amount in [(1, 19.99), (2, 250.0), (3, 5.25)]:
        failed = order(checkout, payments, order_id, amount) or failed

    checkout.shutdown()
    payments.shutdown()
    return failed, 2


def orders_forever(exporters: Exporters, stop: threading.Event) -> None:
    """Random orders until `stop`, for a dashboard to watch. About one in five fails."""
    checkout = instrument("checkout", exporters)
    payments = instrument("payments", exporters)
    order_id = 0
    try:
        while not stop.wait(random.uniform(0.3, 1.2)):
            order_id += 1
            declined = random.random() < 0.2
            amount = round(
                random.uniform(120, 400) if declined else random.uniform(5, 90), 2
            )
            failed = order(checkout, payments, order_id, amount)
            print(
                f"order {order_id:>4}  {amount:>7.2f}  {'FAILED ' + failed if failed else 'ok'}",
                flush=True,
            )

    finally:
        # `run` joins this thread before it closes the publications,
        # so the last batches flush into an open stream.
        checkout.shutdown()
        payments.shutdown()


@contextlib.asynccontextmanager
async def exporting(base: str):  # noqa: ANN201
    """A publisher on each stream, wrapped as the exporters OTel's SDK calls."""
    loop = asyncio.get_running_loop()
    publishers = [
        common.Publisher(f"{base}/{name}", loop)
        for name in ("logs", "spans", "metrics")
    ]
    try:
        yield Exporters(
            logs.StreamLogExporter(publishers[0]),
            spans.StreamSpanExporter(publishers[1]),
            metrics.StreamMetricExporter(publishers[2]),
        )
    finally:
        for publisher in publishers:
            await publisher.aclose()


async def run(broker: str) -> None:
    """Orders until cancelled, published to `broker`'s three streams."""
    async with exporting(broker) as exporters:
        stop = threading.Event()
        # A thread of its own, joined on the way out: the services flush their
        # last batches into publications that are still open.
        services = threading.Thread(target=orders_forever, args=(exporters, stop))
        services.start()
        try:
            await asyncio.Event().wait()  # until cancelled
        finally:
            stop.set()
            await asyncio.to_thread(services.join)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--broker",
        default="ws://127.0.0.1:8766",
        help="serving /logs, /spans and /metrics",
    )
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(arguments.broker))
