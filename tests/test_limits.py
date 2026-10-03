"""The queue bounds as `serve` and `asgi` take them: one int, or one per stream.

They are the serving process's settings, not the streams', so they arrive at
`serve` — and a per-stream map that leaves a stream out or names one not
served is refused at the call, before any stream is changed.
"""

from __future__ import annotations

import pytest

import streamcast
from streamcast.asgi import asgi


def streams() -> tuple[streamcast.Stream, streamcast.Stream]:
    return streamcast.Stream("trades"), streamcast.Stream("quotes")


def bounds(stream: streamcast.Stream) -> tuple[int, int, int]:
    return (
        stream._max_backlog,  # noqa: SLF001
        stream._max_inbound,  # noqa: SLF001
        stream._max_in_flight,  # noqa: SLF001
    )


class TestServe:
    async def test_the_defaults_until_told_otherwise(self, serve):
        trades, quotes = streams()
        async with serve(trades, quotes):
            defaults = (
                streamcast.MAX_BACKLOG,
                streamcast._limits.MAX_INBOUND,  # noqa: SLF001
                streamcast._limits.MAX_IN_FLIGHT,  # noqa: SLF001
            )
            assert bounds(trades) == bounds(quotes) == defaults

    async def test_one_int_applies_to_every_stream(self, serve):
        trades, quotes = streams()
        async with serve(
            trades, quotes, max_backlog=16, max_inbound=32, max_in_flight=8
        ):
            assert bounds(trades) == bounds(quotes) == (16, 32, 8)

    async def test_a_map_applies_each_streams_own(self, serve):
        trades, quotes = streams()
        async with serve(
            trades,
            quotes,
            max_backlog={"trades": 16, "quotes": 64},
            max_in_flight=8,  # an int beside a map: each keyword is its own
        ):
            assert bounds(trades)[0::2] == (16, 8)
            assert bounds(quotes)[0::2] == (64, 8)

    async def test_a_subscriber_gets_its_streams_backlog(self, serve):
        trades, quotes = streams()
        async with serve(
            trades, quotes, max_backlog={"trades": 16, "quotes": 64}
        ) as uri:
            quotes_uri = uri.rsplit("/", 1)[0] + "/quotes"
            async with streamcast.connect(quotes_uri):
                (subscriber,) = quotes._subscribers  # noqa: SLF001
                assert subscriber._backlog == 64  # noqa: SLF001

    async def test_a_map_missing_a_stream_is_refused(self, serve):
        trades, quotes = streams()
        with pytest.raises(ValueError, match="max_inbound=.*nothing for 'quotes'"):
            async with serve(trades, quotes, max_inbound={"trades": 10}):
                pass

    async def test_a_map_naming_a_stream_not_served_is_refused(self, serve):
        trades, quotes = streams()
        with pytest.raises(ValueError, match="'trdes', which is not served"):
            async with serve(
                trades, quotes, max_backlog={"trades": 10, "quotes": 10, "trdes": 10}
            ):
                pass

    @pytest.mark.parametrize("bad", [0, -1, True, 1.5])
    async def test_a_bound_below_one_or_not_an_int_is_refused(self, serve, bad):
        trades, quotes = streams()
        with pytest.raises(ValueError, match=r"max_in_flight\['quotes'\]"):
            async with serve(
                trades, quotes, max_in_flight={"trades": 4, "quotes": bad}
            ):
                pass

        with pytest.raises(ValueError, match="max_backlog="):
            async with serve(trades, quotes, max_backlog=bad):
                pass

    async def test_a_refused_call_changes_no_stream(self, serve):
        """Every keyword is checked before any stream is set."""
        trades, quotes = streams()
        before = bounds(trades), bounds(quotes)
        with pytest.raises(ValueError, match="max_in_flight"):
            async with serve(trades, quotes, max_backlog=16, max_in_flight=0):
                pass

        assert (bounds(trades), bounds(quotes)) == before


class TestAsgi:
    def test_it_takes_the_same_keywords(self):
        trades, quotes = streams()
        asgi([trades, quotes], maintain=False, max_inbound={"trades": 7, "quotes": 9})
        assert bounds(trades)[1] == 7
        assert bounds(quotes)[1] == 9

    def test_a_map_missing_a_stream_is_refused(self):
        trades, quotes = streams()
        with pytest.raises(ValueError, match="nothing for 'trades'"):
            asgi([trades, quotes], maintain=False, max_backlog={"quotes": 7})
