"""OTel's metric data point as a stream's row, and an exporter that publishes it.

**How a metric maps onto rows**, beyond what it shares with a log record:

* One row per DATA POINT, not per export. An export nests resource, scope,
  metric and points; the point is what you query, so each is a row carrying
  its metric's name, unit, type and temporality beside its own values.
* `type` is one of `sum`, `gauge`, `histogram` and `exponential_histogram`,
  and only that kind's columns are set. A summary is not here: the Python SDK
  never produces one.
* A number is `value_int` or `value_double`, as OTLP keeps it: an int64
  counter must not round through a double.
* `temporality` is OTLP's number: 1 delta, 2 cumulative, null for a gauge.
  The exporter asks for DELTA for counters and histograms, the SDK's
  `delta` preference: each row is then what happened in its own interval, so
  a window's total is a `sum()` and a histogram heatmap reads directly.
  Up-down counters stay cumulative, as that preference keeps them: their
  current value is the useful one.
* `exemplars` link a point to the traces it measured, by trace and span id.
  The SDK records one only for a measurement made inside a sampled span.
* A non-finite double is stored as null, as in a log record.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    ExponentialHistogram,
    Gauge,
    MetricExporter,
    MetricExportResult,
    Sum,
)

from examples.otel import common

if TYPE_CHECKING:
    import asyncio

    from opentelemetry.sdk.metrics import Exemplar
    from opentelemetry.sdk.metrics.export import MetricsData

    import streamcast


def _list_of(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": ["array", "null"], "items": items}


def _struct(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "properties": properties,
        "required": [],
        "additionalProperties": False,
    }


COUNTS = _list_of({"type": "integer"})
BUCKETS = _struct(
    {
        "offset": {"type": ["integer", "null"], "format": "int32"},
        "bucket_counts": COUNTS,
    }
)
"""One side of an exponential histogram: counts from bucket `offset` up."""

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "time_unix_nano": {"type": "integer"},
        "start_time_unix_nano": {"type": ["integer", "null"]},
        "service": {"type": ["string", "null"]},
        "name": {"type": "string"},
        "description": {"type": ["string", "null"]},
        "unit": {"type": ["string", "null"]},
        "type": {"type": "string"},
        "temporality": {"type": ["integer", "null"], "format": "int32"},
        "is_monotonic": {"type": ["boolean", "null"]},
        "attributes": common.ATTRIBUTES,
        # A sum or a gauge: exactly one of the two.
        "value_int": {"type": ["integer", "null"]},
        "value_double": {"type": ["number", "null"]},
        # A histogram, either kind.
        "count": {"type": ["integer", "null"]},
        "sum": {"type": ["number", "null"]},
        "min": {"type": ["number", "null"]},
        "max": {"type": ["number", "null"]},
        # An explicit-bucket histogram: `len(bounds) + 1` counts.
        "bucket_counts": COUNTS,
        "explicit_bounds": _list_of({"type": "number"}),
        # An exponential histogram.
        "scale": {"type": ["integer", "null"], "format": "int32"},
        "zero_count": {"type": ["integer", "null"]},
        "positive": BUCKETS,
        "negative": BUCKETS,
        "flags": {"type": ["integer", "null"], "format": "int32"},
        "exemplars": _list_of(
            _struct(
                {
                    "time_unix_nano": {"type": ["integer", "null"]},
                    "value_int": {"type": ["integer", "null"]},
                    "value_double": {"type": ["number", "null"]},
                    "trace_id": common.TRACE_ID,
                    "span_id": common.SPAN_ID,
                    "filtered_attributes": common.ATTRIBUTES,
                }
            )
        ),
        "scope": common.SCOPE,
        "resource": common.ATTRIBUTES,
    },
    "required": ["time_unix_nano", "name", "type"],
}

DELTA = 1
CUMULATIVE = 2
"""OTLP's `AggregationTemporality` numbers, the same in Python's SDK."""

PREFERRED_TEMPORALITY: dict[type, AggregationTemporality] = {
    Counter: AggregationTemporality.DELTA,
    UpDownCounter: AggregationTemporality.CUMULATIVE,
    Histogram: AggregationTemporality.DELTA,
    ObservableCounter: AggregationTemporality.DELTA,
    ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
    ObservableGauge: AggregationTemporality.CUMULATIVE,
}
"""The SDK's `delta` temporality preference, as OTLP exporters spell it."""


def _finite(value: float | None) -> float | None:
    return value if value is None or math.isfinite(value) else None


def _number(value: float) -> dict[str, object]:
    """`value_int` for an int, `value_double` for anything else, as OTLP does."""
    if isinstance(value, int) and not isinstance(value, bool):
        return {"value_int": value, "value_double": None}

    return {"value_int": None, "value_double": _finite(float(value))}


def _exemplar(exemplar: Exemplar) -> dict[str, object]:
    return {
        "time_unix_nano": exemplar.time_unix_nano,
        **_number(exemplar.value),
        "trace_id": common.trace_id(exemplar.trace_id or 0),
        "span_id": common.span_id(exemplar.span_id or 0),
        "filtered_attributes": common.attributes(exemplar.filtered_attributes),
    }


def _kind(data: object) -> str:
    if isinstance(data, Sum):
        return "sum"

    if isinstance(data, Gauge):
        return "gauge"

    if isinstance(data, ExponentialHistogram):
        return "exponential_histogram"

    return "histogram"


def rows(metrics_data: MetricsData) -> list[dict[str, object]]:
    """One OTel metrics export as rows of `SCHEMA`, one per data point."""
    out: list[dict[str, object]] = []
    for resource_metrics in metrics_data.resource_metrics:
        resource = dict(resource_metrics.resource.attributes)
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                data = metric.data
                kind = _kind(data)
                temporality = getattr(data, "aggregation_temporality", None)
                monotonic = getattr(data, "is_monotonic", None)
                for point in data.data_points:
                    row: dict[str, object] = {
                        "time_unix_nano": point.time_unix_nano,
                        "start_time_unix_nano": getattr(
                            point, "start_time_unix_nano", None
                        )
                        or None,
                        "service": resource.get("service.name"),
                        "name": metric.name,
                        "description": metric.description or None,
                        "unit": metric.unit or None,
                        "type": kind,
                        "temporality": None
                        if temporality is None
                        else temporality.value,
                        "is_monotonic": monotonic,
                        "attributes": common.attributes(point.attributes),
                        "exemplars": [_exemplar(e) for e in point.exemplars or ()]
                        or None,
                        "scope": common.scope(scope_metrics.scope),
                        "resource": common.attributes(resource),
                    }
                    if kind in ("sum", "gauge"):
                        row.update(_number(point.value))  # ty: ignore[unresolved-attribute]
                    else:
                        row.update(
                            count=point.count,  # ty: ignore[unresolved-attribute]
                            sum=_finite(point.sum),  # ty: ignore[unresolved-attribute]
                            min=_finite(point.min),  # ty: ignore[unresolved-attribute]
                            max=_finite(point.max),  # ty: ignore[unresolved-attribute]
                        )

                    if kind == "histogram":
                        row.update(
                            bucket_counts=list(point.bucket_counts),  # ty: ignore[unresolved-attribute]
                            explicit_bounds=list(point.explicit_bounds),  # ty: ignore[unresolved-attribute]
                        )

                    if kind == "exponential_histogram":
                        row.update(
                            scale=point.scale,  # ty: ignore[unresolved-attribute]
                            zero_count=point.zero_count,  # ty: ignore[unresolved-attribute]
                            flags=point.flags,  # ty: ignore[unresolved-attribute]
                            positive={
                                "offset": point.positive.offset,  # ty: ignore[unresolved-attribute]
                                "bucket_counts": list(point.positive.bucket_counts),  # ty: ignore[unresolved-attribute]
                            },
                            negative={
                                "offset": point.negative.offset,  # ty: ignore[unresolved-attribute]
                                "bucket_counts": list(point.negative.bucket_counts),  # ty: ignore[unresolved-attribute]
                            },
                        )

                    out.append(row)

    return out


class StreamMetricExporter(MetricExporter):
    """Publishes each collection of OTel metrics to a stream, a row per data point.

    Asks for delta temporality for counters and histograms (see the module
    docstring); pass `preferred_temporality` to choose otherwise.
    """

    def __init__(
        self,
        publication: streamcast.Publication,
        loop: asyncio.AbstractEventLoop,
        preferred_temporality: dict[type, AggregationTemporality] | None = None,
    ) -> None:
        super().__init__(
            preferred_temporality=preferred_temporality or PREFERRED_TEMPORALITY
        )
        self._publication = publication
        self._loop = loop

    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,  # noqa: ARG002
        **kwargs: object,  # noqa: ARG002
    ) -> MetricExportResult:
        batch = rows(metrics_data)
        if not batch or common.publish(self._publication, self._loop, batch):
            return MetricExportResult.SUCCESS

        return MetricExportResult.FAILURE

    def force_flush(self, timeout_millis: float = 10_000) -> bool:  # noqa: ARG002
        return True  # nothing buffered here; see `StreamLogExporter`

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
        pass
