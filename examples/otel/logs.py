"""OpenTelemetry logs, through a stream, into a table you can query.

    uv run python examples/otel/logs.py

Nothing here is part of streamcast. The OTel log record's schema and the
record-to-row conversion live in this file: streamcast carries binary ids,
maps and nested values as ordinary column types, and an OTel record is one
thing you can build from them. `opentelemetry-sdk` is a dev dependency, for
this example only.

What it does, in one process for the demo — in production the services and
the broker are different machines, and nothing below changes but the URI:

1. **A broker** serves a stream called `logs`, with a log, accepting remote
   publishers (`publish=True`).
2. **Two services** log through the standard `logging` module, inside traced
   requests. OTel's SDK turns each record into a `LogRecord`, and
   `StreamExporter` publishes it as a row.
3. **A live tail** follows the problems as they happen — `where=` a membership
   filter on `severity_text` — and **a replay** reads back one request by its
   trace id, `where={"trace_id": "<32 hex chars>"}`, the id as every tracing
   tool shows it.
4. **The stored table** answers the questions a log search would: errors per
   service, and how far behind the broker each record arrived.

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

import argparse
import asyncio
import contextlib
import json
import logging
import math
import random
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    LogRecordExporter,
    LogRecordExportResult,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

import streamcast

if TYPE_CHECKING:
    from collections.abc import Sequence

    from opentelemetry.sdk._logs import ReadableLogRecord
    from opentelemetry.trace import Tracer

# -- the schema: OTel's log data model, in streamcast's column types -------------

ANY_VALUE: dict[str, Any] = {
    "type": "object",
    "properties": {
        "string_value": {"type": ["string", "null"]},
        "bool_value": {"type": ["boolean", "null"]},
        "int_value": {"type": ["integer", "null"]},
        "double_value": {"type": ["number", "null"]},
        "bytes_value": {"type": ["string", "null"], "contentEncoding": "base64"},
        "json_value": {"type": ["string", "null"]},
    },
    "required": [],
    "additionalProperties": False,
}
"""OTel's `AnyValue`: exactly one field set, or none for an empty value."""

ATTRIBUTES: dict[str, Any] = {
    "type": ["object", "null"],
    "additionalProperties": ANY_VALUE,
}

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "time_unix_nano": {"type": "integer"},
        "observed_time_unix_nano": {"type": "integer"},
        "severity_number": {"type": ["integer", "null"], "format": "int32"},
        "severity_text": {"type": ["string", "null"]},
        "service": {"type": ["string", "null"]},
        "body": {**ANY_VALUE, "type": ["object", "null"]},
        "trace_id": {
            "type": ["string", "null"],
            "contentEncoding": "base16",
            "format": "bytes16",
        },
        "span_id": {
            "type": ["string", "null"],
            "contentEncoding": "base16",
            "format": "bytes8",
        },
        "trace_flags": {"type": ["integer", "null"], "format": "int32"},
        "event_name": {"type": ["string", "null"]},
        "scope": {
            "type": ["object", "null"],
            "properties": {
                "name": {"type": ["string", "null"]},
                "version": {"type": ["string", "null"]},
            },
            "required": [],
            "additionalProperties": False,
        },
        "attributes": ATTRIBUTES,
        "resource": ATTRIBUTES,
    },
    "required": ["time_unix_nano", "observed_time_unix_nano"],
}

ERROR = 17
"""OTel's `SeverityNumber.ERROR`; everything from here up is an error."""


# -- the conversion ---------------------------------------------------------------


def any_value(value: object) -> dict[str, object] | None:
    """An OTel attribute or body value, as the `AnyValue` struct."""
    if value is None:
        return None

    # bool before int: in Python a bool IS an int, and True is not 1 here.
    if isinstance(value, bool):
        return {"bool_value": value}

    if isinstance(value, int):
        if -(2**63) <= value < 2**63:
            return {"int_value": value}

        return {"json_value": json.dumps(value)}

    if isinstance(value, float):
        # A stream refuses NaN and ±inf, so they are stored as null — the
        # record is kept, the one unrepresentable value is not.
        return {"double_value": value if math.isfinite(value) else None}

    if isinstance(value, str):
        return {"string_value": value}

    if isinstance(value, (bytes, bytearray)):
        return {"bytes_value": bytes(value)}

    # Arrays and key-value lists: no column type is recursive, so JSON text.
    return {"json_value": json.dumps(value, default=str)}


def attributes(values: object) -> dict[str, object] | None:
    if not values:
        return None

    return {str(key): any_value(value) for key, value in dict(values).items()}  # ty: ignore[no-matching-overload]


def row(record: ReadableLogRecord) -> dict[str, object]:
    """One OTel log record as one row of `SCHEMA`."""
    log = record.log_record
    resource = dict(record.resource.attributes) if record.resource else {}
    scope = record.instrumentation_scope
    severity = log.severity_number

    return {
        "time_unix_nano": log.timestamp or log.observed_timestamp,
        "observed_time_unix_nano": log.observed_timestamp,
        "severity_number": None if severity is None else severity.value,
        "severity_text": log.severity_text,
        "service": resource.get("service.name"),
        "body": any_value(log.body),
        # An id of 0 is OTel's "not in a trace"; stored as null, not as zeros.
        "trace_id": log.trace_id.to_bytes(16, "big") if log.trace_id else None,
        "span_id": log.span_id.to_bytes(8, "big") if log.span_id else None,
        "trace_flags": None if log.trace_flags is None else int(log.trace_flags),
        "event_name": log.event_name,
        "scope": None
        if scope is None
        else {"name": scope.name, "version": scope.version or None},
        "attributes": attributes(log.attributes),
        "resource": attributes(resource),
    }


# -- the exporter ----------------------------------------------------------------


class StreamExporter(LogRecordExporter):
    """Publishes each batch of OTel log records to a stream.

    OTel's `BatchLogRecordProcessor` calls `export` on its own worker thread,
    and a streamcast publisher lives on an event loop — so the batch is handed
    to the loop and the thread waits for the stream's acknowledgement. That
    makes a flush mean the rows are DURABLE, not merely sent.
    """

    def __init__(
        self, publication: streamcast.Publication, loop: asyncio.AbstractEventLoop
    ) -> None:
        self._publication = publication
        self._loop = loop

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        rows = [row(record) for record in batch]
        sent = asyncio.run_coroutine_threadsafe(
            self._publication.send_many(rows), self._loop
        )
        try:
            sent.result(timeout=10)
        except Exception:  # noqa: BLE001 — the SDK wants a result, not an exception
            return LogRecordExportResult.FAILURE

        return LogRecordExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30_000) -> bool:  # noqa: ARG002
        # Nothing buffered here: `export` returns only once the stream has
        # acknowledged the batch. The processor above this does the buffering.
        return True

    def shutdown(self) -> None:
        pass


# -- the demo ---------------------------------------------------------------------


def instrument(
    service: str, exporter: StreamExporter
) -> tuple[logging.Logger, LoggerProvider]:
    """A service's logger: standard `logging`, exported through OTel."""
    provider = LoggerProvider(resource=Resource.create({"service.name": service}))
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    logger = logging.getLogger(f"shop.{service}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(LoggingHandler(logger_provider=provider))

    return logger, provider


def order(
    tracer: Tracer,
    checkout: logging.Logger,
    payments: logging.Logger,
    order_id: int,
    amount: float,
) -> str | None:
    """One traced request through both services. Returns its trace id if it failed."""
    with tracer.start_as_current_span("POST /orders") as span:
        checkout.info(
            "order received", extra={"order_id": order_id, "items": ("book", "pen")}
        )
        if amount > 100:
            payments.error(
                "card declined",
                extra={"order_id": order_id, "amount": amount, "retry_ratio": math.inf},
            )
            checkout.warning("order failed", extra={"order_id": order_id})
            return format(span.get_span_context().trace_id, "032x")

        payments.info("card charged", extra={"order_id": order_id, "amount": amount})
        checkout.info("order confirmed", extra={"order_id": order_id})
        return None


def traffic(exporter: StreamExporter) -> tuple[str, int]:
    """Two services handling three requests, one of which fails.

    Returns the failed request's trace id, and how many ERROR or WARN
    records were logged — which is what tells the live tail when to stop.

    Runs on a worker thread: the exporter blocks on the event loop, so a flush
    from the loop's own thread would wait on itself.
    """
    tracer = TracerProvider().get_tracer("shop")
    checkout, checkout_logs = instrument("checkout", exporter)
    payments, payments_logs = instrument("payments", exporter)
    failed = ""
    for order_id, amount in [(1, 19.99), (2, 250.0), (3, 5.25)]:
        failed = order(tracer, checkout, payments, order_id, amount) or failed

    checkout_logs.force_flush()
    payments_logs.force_flush()
    return failed, 2


def orders_forever(exporter: StreamExporter, stop: threading.Event) -> None:
    """Random orders until `stop`, for a dashboard to watch. About one in five fails."""
    tracer = TracerProvider().get_tracer("shop")
    checkout, _ = instrument("checkout", exporter)
    payments, _ = instrument("payments", exporter)
    order_id = 0
    while not stop.wait(random.uniform(0.3, 1.2)):
        order_id += 1
        amount = round(
            random.uniform(120, 400)
            if random.random() < 0.2
            else random.uniform(5, 90),
            2,
        )
        failed = order(tracer, checkout, payments, order_id, amount)
        print(
            f"order {order_id:>4}  {amount:>7.2f}  {'FAILED ' + failed if failed else 'ok'}",
            flush=True,
        )


async def serve_forever(port: int) -> None:
    """The broker on `port`, with the two services placing orders until Ctrl-C.

    What `just demo-otel` runs: `examples/otel/export.py` subscribes to
    `ws://127.0.0.1:<port>/logs` and re-exports each row as OTLP to otel-gui.
    """
    with tempfile.TemporaryDirectory() as directory:
        stream = streamcast.Stream.new("logs", root=Path(directory), schema=SCHEMA)
        server = await streamcast.serve(stream, "127.0.0.1", port, publish=True)
        stop = threading.Event()
        print(f"broker on ws://127.0.0.1:{port}/logs — Ctrl-C to stop", flush=True)
        try:
            async with streamcast.publish(f"ws://127.0.0.1:{port}/logs") as publication:
                exporter = StreamExporter(publication, asyncio.get_running_loop())
                await asyncio.to_thread(orders_forever, exporter, stop)
        finally:
            stop.set()
            server.close()
            await server.wait_closed()


async def main(root: Path) -> dict[str, Any]:
    """Run the whole demo under `root`, print it, and return what it saw."""
    stream = streamcast.Stream.new("logs", root=root, schema=SCHEMA)
    server = await streamcast.serve(stream, "127.0.0.1", 0, publish=True)
    uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/logs"
    try:
        # **The live tail, subscribed before anything is logged.** A filter
        # list is membership, so this follows every error and warning as the
        # broker takes it — the view an on-call dashboard wants.
        async with streamcast.connect(
            # OTel's text for Python's WARNING level is "WARN".
            uri,
            where={"severity_text": ["ERROR", "WARN"]},
        ) as tail:
            async with streamcast.publish(uri) as publication:
                exporter = StreamExporter(publication, asyncio.get_running_loop())
                failed, problems = await asyncio.to_thread(traffic, exporter)

            live: list[dict[str, Any]] = [
                (await tail.recv())[1] for _ in range(problems)
            ]

        # **One request, replayed by its trace id** — hex text, as a tracing
        # tool would hand it to you. A filtered replay's window is an upper
        # bound, not a count (see `Greeting.replay`), so the table says how
        # many rows to expect.
        assert stream.log is not None
        expected = (
            stream.log.sql(
                f"SELECT count(*) AS n FROM log WHERE lower(hex(trace_id)) = '{failed}'"
            )
            .read_all()
            .to_pylist()[0]["n"]
        )
        async with streamcast.connect(
            uri, offset=streamcast.EARLIEST, where={"trace_id": failed}
        ) as sub:
            one_request: list[dict[str, Any]] = [
                (await sub.recv())[1] for _ in range(expected)
            ]

        assert stream.log is not None
        errors = (
            stream.log.sql(
                f"SELECT service, count(*) AS errors FROM log "
                f"WHERE severity_number >= {ERROR} GROUP BY service ORDER BY service"
            )
            .read_all()
            .to_pylist()
        )
        # `streamcast_ts` is when the broker took the row, in microseconds;
        # the record's own time is nanoseconds. The difference is how far
        # behind the broker each record arrived.
        lag = (
            stream.log.sql(
                "SELECT max(streamcast_ts * 1000 - time_unix_nano) / 1e6 AS max_ms FROM log"
            )
            .read_all()
            .to_pylist()[0]["max_ms"]
        )
        stored = (
            stream.log.sql("SELECT count(*) AS n FROM log")
            .read_all()
            .to_pylist()[0]["n"]
        )
    finally:
        server.close()
        await server.wait_closed()

    print(f"{stored} log records stored")
    print("live tail, errors and warnings as they happened:")
    for message in live:
        print(
            f"  {message['service']:>8}  {message['severity_text']:<7} {message['body']['string_value']}"
        )

    print(f"the failed request, trace {failed}:")
    for message in one_request:
        print(
            f"  {message['service']:>8}  {message['severity_text']:<7} {message['body']['string_value']}"
        )

    print(f"errors per service: {errors}")
    print(f"ingest lag, worst: {lag:.1f} ms")

    return {
        "stored": stored,
        "failed": failed,
        "live": live,
        "one_request": one_request,
        "errors": errors,
        "lag_ms": lag,
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
    if arguments.serve:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(serve_forever(arguments.port))
    else:
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(main(Path(directory)))
