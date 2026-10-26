"""Each handle validates and retains its own queue and message bounds."""

from __future__ import annotations

import pytest

import streamcast
from tests.hosting import application as asgi


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
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "3"])
def test_a_bound_must_be_a_strict_positive_int(field, bad):
    with pytest.raises(ValueError):
        streamcast.StreamHandle("trades", **{field: bad})


def test_message_bound_can_be_unspecified():
    assert (
        streamcast.StreamHandle("trades", max_message_bytes=None)._max_message_bytes
        is None
    )  # noqa: SLF001


async def test_serving_keeps_each_handles_own_bounds(serve):
    trades = streamcast.StreamHandle(
        "trades", max_backlog=16, max_inbound=32, max_in_flight=8
    )
    quotes = streamcast.StreamHandle(
        "quotes", max_backlog=64, max_inbound=7, max_in_flight=9
    )
    async with serve(trades, quotes) as uri:
        assert (trades._max_backlog, trades._max_inbound, trades._max_in_flight) == (
            16,
            32,
            8,
        )  # noqa: SLF001
        assert (quotes._max_backlog, quotes._max_inbound, quotes._max_in_flight) == (
            64,
            7,
            9,
        )  # noqa: SLF001
        async with streamcast.connect(uri.rsplit("/", 1)[0] + "/quotes"):
            (subscriber,) = quotes._subscribers  # noqa: SLF001
            assert subscriber._backlog == 64  # noqa: SLF001


def test_app_refuses_an_oversized_policy_without_touching_handles():
    stream = streamcast.StreamHandle("trades", max_backlog=16, max_message_bytes=1025)
    with pytest.raises(ValueError, match="transport_max_bytes"):
        asgi(stream, transport_max_bytes=1024)

    assert stream._max_backlog == 16  # noqa: SLF001
    assert stream._max_message_bytes == 1025  # noqa: SLF001
