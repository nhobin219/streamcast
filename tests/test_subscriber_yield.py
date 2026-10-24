"""`subscriber_yield_every`: the data frames a subscriber is sent before its
task yields to the event loop, counted across replay and live delivery.

A send to a writable socket need not suspend, which is the whole reason the
checkpoint exists: the peer here never suspends, so every point at which
another task runs is a checkpoint the pump took itself.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import TYPE_CHECKING

import pytest

import streamcast
from streamcast._subscriber import Subscriber
from tests.test_broker import config, managed, serving  # noqa: F401 — the fixture

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class Writable:
    """A peer with room in its buffer: `send` returns without suspending."""

    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    async def send(self, message: str | bytes, /, *, text: bool | None = None) -> None:
        self.trace.append("frame")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass

    async def wait_closed(self) -> None:
        await asyncio.Event().wait()

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        raise NotImplementedError("a subscriber is never read")


async def ready(count: int) -> AsyncIterator[tuple[int, bytes]]:
    """A replay whose rows are all ready: iterating it never suspends."""
    for offset in range(1, count + 1):
        yield offset, b"[]"


async def runs(yield_every: int, *, replayed: int, live: int) -> list[int]:
    """The frames sent between one turn of another task and the next."""
    trace: list[str] = []
    subscriber = Subscriber(Writable(trace), max_backlog=100, yield_every=yield_every)
    for _ in range(live):
        subscriber.offer({}, b"[]")

    async def other() -> None:
        while True:
            trace.append("other")
            await asyncio.sleep(0)

    neighbour = asyncio.create_task(other())
    await asyncio.sleep(0)
    pump = asyncio.create_task(subscriber.pump(ready(replayed)))
    while trace.count("frame") < replayed + live:
        await asyncio.sleep(0)

    pump.cancel()
    neighbour.cancel()
    await asyncio.gather(pump, neighbour, return_exceptions=True)
    lengths, run = [], 0
    for event in trace:
        if event == "frame":
            run += 1
        elif run:
            lengths.append(run)
            run = 0

    return [*lengths, run] if run else lengths


@pytest.mark.parametrize(("every", "expected"), [(1, [1] * 16), (4, [4] * 4)])
async def test_replay_and_live_share_one_count(every, expected):
    """10 replayed and 6 live frames at 4 are four runs of 4, not 4, 4, 2
    and then a fresh count for the live queue."""
    assert await runs(every, replayed=10, live=6) == expected


async def test_an_interval_longer_than_the_backlog_never_yields_within_it():
    assert await runs(64, replayed=10, live=6) == [16]


@pytest.mark.parametrize("value", [0, -1, True, 1.0])
def test_the_interval_is_a_positive_integer(value):
    with pytest.raises(ValueError, match="positive integer"):
        streamcast.BrokerConfig(subscriber_yield_every=value)


async def test_a_static_stream_takes_the_brokers_interval():
    stream = streamcast.Stream("trades")
    app = streamcast.create_app(
        streams=stream,
        config=streamcast.BrokerConfig(subscriber_yield_every=3, maintain=False),
    )
    async with app.router.lifespan_context(app):
        assert stream._subscriber_yield_every == 3  # noqa: SLF001


async def test_a_managed_stream_takes_the_brokers_interval(config):  # noqa: F811
    async with managed(dataclasses.replace(config, subscriber_yield_every=5)) as (
        owner,
        client,
        _,
    ):
        stream = await serving(owner, client)
        assert stream._subscriber_yield_every == 5  # noqa: SLF001
