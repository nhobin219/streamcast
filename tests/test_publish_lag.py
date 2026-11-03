"""ACK-time lag, with controlled clocks and a real SDK only in tests."""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace
from typing import Any, cast

import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import streamcast
from streamcast import _metrics, _outbox, _publish
from streamcast._protocol import greeting, parse_greeting

from .conftest import trade

BOUNDS = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)


@pytest.fixture
def lag(monkeypatch):
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    original = _metrics.PUBLISH_LAG
    with monkeypatch.context() as patch:
        patch.setattr(metrics, "get_meter", provider.get_meter)
        importlib.reload(_metrics)

    monkeypatch.setattr(_publish, "PUBLISH_LAG", _metrics.PUBLISH_LAG)
    _metrics.PUBLISH_LAG = original
    yield reader
    provider.shutdown()


def points(reader):
    data = reader.get_metrics_data()
    if data is None:
        return []

    return [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == "streamcast.publish.lag" and metric.unit == "s"
        for point in metric.data.data_points
    ]


class Replies:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.protocol = SimpleNamespace(close_exc=RuntimeError("closed"))

    async def send(self, frame, *, text):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        frame = await self.frames.get()
        if frame is None:
            raise StopAsyncIteration

        return frame


@pytest.mark.parametrize(
    "first_ack, second_ack, total, bucket", [(12.5, 14.0, 9.0, 4), (9.0, 8.0, 0.0, 0)]
)
@pytest.mark.parametrize("publisher", [None, "p"])
async def test_plain_publication_records_each_row_at_ack(
    lag, monkeypatch, publisher, first_ack, second_ack, total, bucket
):
    clock = SimpleNamespace(monotonic=lambda: 10.0)
    monkeypatch.setattr(_publish, "time", clock)
    wire = Replies()
    out = streamcast.Publication(
        cast(Any, wire),
        parse_greeting(
            greeting(stream="events", end_offset=1, replay=None, durable=False)
        ),
        "events",
        publisher=publisher,
    )
    first = await out.submit_many([{}, {}], seq=1 if publisher else None)
    second = await out.submit_many([{}], seq=3 if publisher else None)
    assert points(lag) == [], "submission is not an ACK"
    clock.monotonic = lambda: first_ack
    await wire.frames.put('{"ok":[1,2]}')
    assert await first == [1, 2]
    clock.monotonic = lambda: second_ack
    await wire.frames.put('{"ok":[3]}')
    assert await second == [3]
    await wire.frames.put(None)
    assert out._reader is not None
    await out._reader
    [point] = points(lag)
    assert point.count == 3
    assert point.sum == total
    assert point.attributes == {"stream": "events"}
    assert point.explicit_bounds == BOUNDS
    assert point.bucket_counts[bucket] == 3


async def test_failed_and_cancelled_sends_only_count_successful_acks(lag, monkeypatch):
    monkeypatch.setattr(_publish, "time", SimpleNamespace(monotonic=lambda: 10.0))
    wire = Replies()
    out = streamcast.Publication(
        cast(Any, wire),
        parse_greeting(
            greeting(stream="events", end_offset=1, replay=None, durable=False)
        ),
        "events",
    )
    rejected = await out.submit_many([{}])
    cancelled = await out.submit_many([{}])
    cancelled.cancel()
    await wire.frames.put('{"error":"rejected","message":"bad row"}')
    with pytest.raises(streamcast.Rejected):
        await rejected

    await wire.frames.put('{"ok":[2]}')
    await wire.frames.put(None)
    assert out._reader is not None
    await out._reader
    [point] = points(lag)
    assert point.count == 1, "cancelling the waiter does not undo the ACK"
    assert point.sum == 0


@pytest.mark.parametrize("ack_us, expected", [(40_000_000, 59.0), (1_000_000, 0.0)])
async def test_outbox_uses_original_row_times_once(
    lag, monkeypatch, tmp_path, serve, log, ack_us, expected
):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")
    monkeypatch.setattr(_outbox, "_now_us", lambda: 10_000_000)
    async with store.transaction() as tx:
        await _outbox.enqueue(tx, "trades", "p", [trade(0)])

    monkeypatch.setattr(_outbox, "_now_us", lambda: 11_000_000)
    async with store.transaction() as tx:
        await _outbox.enqueue(tx, "trades", "p", [trade(1)])
        rows = await _outbox.pending(tx, "trades", "p", limit=2)
        assert [at for _, _, at in rows] == [10_000_000, 11_000_000]

    monkeypatch.setattr(
        _publish,
        "time",
        SimpleNamespace(monotonic=lambda: 999.0, time_ns=lambda: ack_us * 1_000),
    )
    stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
    async with serve(stream, maintain=False) as uri:
        async with streamcast.outbox(uri, publisher="p", store=store) as out:
            await out.flush(timeout=10)

    [point] = points(lag)
    assert point.count == 2
    assert point.sum == expected
    assert point.attributes == {"stream": "trades"}
    assert await out.pending_rows() == 0
    await store.close()


async def test_reconciliation_has_no_ack_time_and_rejected_rows_are_not_recorded(
    lag, monkeypatch, tmp_path, serve, log
):
    store = await streamcast.SqliteStore.open(tmp_path / "state.db")
    monkeypatch.setattr(_outbox, "_now_us", lambda: 10_000_000)
    monkeypatch.setattr(
        _publish,
        "time",
        SimpleNamespace(monotonic=lambda: 0.0, time_ns=lambda: 40_000_000_000),
    )
    stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
    async with serve(stream, maintain=False) as uri:
        async with store.transaction() as tx:
            await _outbox.enqueue(tx, "trades", "p", [trade(0)])

        async with streamcast.publish(uri, publisher="p") as direct:
            await direct.send(trade(0), seq=1)

        assert points(lag)[0].count == 1
        async with store.transaction() as tx:
            await _outbox.enqueue(tx, "trades", "p", [{"event_ts": "bad"}, trade(2)])

        async with streamcast.outbox(uri, publisher="p", store=store) as out:
            await out.flush(timeout=10)

    [point] = points(lag)
    assert point.count == 2, (
        "one direct ACK, one sift ACK; no reconciliation or rejection"
    )
    assert point.sum == 30.0
    assert log.end_offset() == 3
    await store.close()
