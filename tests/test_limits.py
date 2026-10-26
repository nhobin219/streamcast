"""Queue limits belong to each handle and use the portable policy's rules."""

from __future__ import annotations

import pytest

import streamcast
from streamcast._limits import MAX_BACKLOG, MAX_IN_FLIGHT, MAX_INBOUND
from tests.hosting import application


def bounds(handle):
    return handle._max_backlog, handle._max_inbound, handle._max_in_flight


def test_defaults_and_independent_handle_settings():
    trades = streamcast.StreamHandle(
        "trades", max_backlog=16, max_inbound=32, max_in_flight=8
    )
    quotes = streamcast.StreamHandle("quotes")
    application([trades, quotes], maintain=False)
    assert bounds(trades) == (16, 32, 8)
    assert bounds(quotes) == (MAX_BACKLOG, MAX_INBOUND, MAX_IN_FLIGHT)
    assert trades.definition is quotes.definition is None


@pytest.mark.parametrize(
    "field",
    [
        "max_publishers",
        "max_backlog",
        "max_inbound",
        "max_in_flight",
        "max_message_bytes",
    ],
)
@pytest.mark.parametrize("bad", [0, -1, True, "3", 1.5])
def test_positive_integer_limits_are_strict(field, bad):
    with pytest.raises(ValueError):
        streamcast.StreamHandle("t", **{field: bad})


@pytest.mark.parametrize("bad", [-1, True, "3", 1.5])
def test_replay_limit_is_strict_and_nonnegative(bad):
    with pytest.raises(ValueError):
        streamcast.StreamHandle("t", max_replay=bad)


@pytest.mark.parametrize("value", [0, None, 3])
def test_replay_allows_zero_and_unbounded(value):
    assert streamcast.StreamHandle("t", max_replay=value)._max_replay == value


async def test_a_subscriber_gets_its_handles_backlog(serve):
    trades = streamcast.StreamHandle("trades", max_backlog=16)
    quotes = streamcast.StreamHandle("quotes", max_backlog=64)
    async with serve(trades, quotes) as uri:
        async with streamcast.connect(uri.rsplit("/", 1)[0] + "/quotes"):
            (subscriber,) = quotes._subscribers
            assert subscriber._backlog == 64
