"""OpenTelemetry logs and traces, through two streams, into tables you can query.

    uv run python -m examples.otel.demo            # once, printing what it saw
    uv run python -m examples.otel.demo --serve    # a broker with live traffic

Nothing here is part of streamcast. The OTel schemas and conversions live in
`logs.py` and `spans.py`, built from ordinary column types. The OTel packages
are dev dependencies, for this example only.

What it does, in one process for the demo — in production the services and
the broker are different machines, and nothing below changes but the URI:

1. **A broker** serves two streams, `logs` and `spans`, each with a log,
   accepting remote publishers (`publish=True`).
2. **Two services**, checkout and payments, handle traced requests and log
   through the standard `logging` module. OTel's SDK turns each into records
   and spans, and the exporters in `logs.py` and `spans.py` publish them.
3. **A live tail** follows the problems as they happen — `where=` a membership
   filter on `severity_text` — and **a replay** reads back one request by its
   trace id from both streams, `where={"trace_id": "<32 hex chars>"}`.
4. **The stored tables** answer what a log search and a trace view would:
   errors per service, the slowest request, and ingest lag.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import math
import random
import signal
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

import streamcast
from examples.otel import logs, spans

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

# -- the services ---------------------------------------------------------------


@dataclass
class Exporters:
    logs: logs.StreamLogExporter
    spans: spans.StreamSpanExporter


@dataclass
class Service:
    logger: logging.Logger
    tracer: Tracer
    log_provider: LoggerProvider
    tracer_provider: TracerProvider

    def flush(self) -> None:
        self.tracer_provider.force_flush()
        self.log_provider.force_flush()


def instrument(name: str, exporters: Exporters) -> Service:
    """A service's logger and tracer, exported through OTel to the streams."""
    resource = Resource.create({"service.name": name})
    log_provider = LoggerProvider(resource=resource)
    log_provider.add_log_record_processor(BatchLogRecordProcessor(exporters.logs))
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(exporters.spans))

    logger = logging.getLogger(f"shop.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(LoggingHandler(logger_provider=log_provider))
    return Service(
        logger, tracer_provider.get_tracer("shop"), log_provider, tracer_provider
    )


def order(
    checkout: Service, payments: Service, order_id: int, amount: float
) -> str | None:
    """One traced request through both services. Returns its trace id if it failed."""
    failed = Status(StatusCode.ERROR, "card declined")
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

        if declined:
            request.set_status(failed)
            checkout.logger.warning("order failed", extra={"order_id": order_id})
            return format(request.get_span_context().trace_id, "032x")

        checkout.logger.info("order confirmed", extra={"order_id": order_id})
        return None


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

    checkout.flush()
    payments.flush()
    return failed, 2


def orders_forever(exporters: Exporters, stop: threading.Event) -> None:
    """Random orders until `stop`, for a dashboard to watch. About one in five fails."""
    checkout = instrument("checkout", exporters)
    payments = instrument("payments", exporters)
    order_id = 0
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


# -- the broker -------------------------------------------------------------------


def streams(root: Path) -> tuple[streamcast.Stream, streamcast.Stream]:
    return (
        streamcast.Stream.new("logs", root=root, schema=logs.SCHEMA),
        streamcast.Stream.new("spans", root=root, schema=spans.SCHEMA),
    )


@contextlib.asynccontextmanager
async def exporting(base: str):  # noqa: ANN201
    """A publisher on each stream, wrapped as the exporters OTel's SDK calls."""
    loop = asyncio.get_running_loop()
    async with (
        streamcast.publish(f"{base}/logs") as log_publication,
        streamcast.publish(f"{base}/spans") as span_publication,
    ):
        yield Exporters(
            logs.StreamLogExporter(log_publication, loop),
            spans.StreamSpanExporter(span_publication, loop),
        )


async def serve_forever(port: int) -> None:
    """The broker on `port`, with the two services placing orders until Ctrl-C.

    What `just demo-otel` runs: `examples/otel/export.py` follows both
    streams and re-exports each row as OTLP to otel-gui.
    """
    with tempfile.TemporaryDirectory() as directory:
        server = await streamcast.serve(
            streams(Path(directory)), "127.0.0.1", port, publish=True
        )
        stop = threading.Event()
        base = f"ws://127.0.0.1:{port}"
        print(f"broker on {base}/logs and {base}/spans — Ctrl-C to stop", flush=True)
        try:
            async with exporting(base) as exporters:
                await asyncio.to_thread(orders_forever, exporters, stop)
        finally:
            stop.set()
            server.close()
            await server.wait_closed()


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
        return [(await sub.recv())[1] for _ in range(counted["n"])]


async def main(root: Path) -> dict[str, Any]:
    """Run the whole demo under `root`, print it, and return what it saw."""
    log_stream, span_stream = streams(root)
    server = await streamcast.serve(
        [log_stream, span_stream], "127.0.0.1", 0, publish=True
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
                (await tail.recv())[1] for _ in range(problems)
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
        [stored_logs] = query(log_stream, "SELECT count(*) AS n FROM log")
        [stored_spans] = query(span_stream, "SELECT count(*) AS n FROM log")
    finally:
        server.close()
        await server.wait_closed()

    print(f"{stored_logs['n']} log records and {stored_spans['n']} spans stored")
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
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--serve",
        action="store_true",
        help="run the broker with live traffic until Ctrl-C, for a dashboard to watch",
    )
    parser.add_argument("--port", type=int, default=8766)
    arguments = parser.parse_args()
    # Started in the background, as `just demo-otel` starts it, a script
    # inherits SIGINT ignored; and a supervisor stops it with SIGTERM. Both
    # become KeyboardInterrupt, so every stop unwinds the same way — the
    # broker stopping its maintainer rather than orphaning it.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    if arguments.serve:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(serve_forever(arguments.port))
    else:
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(main(Path(directory)))
