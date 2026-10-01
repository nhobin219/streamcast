"""`where=` — a subscriber is served only the rows it asked for.

The two things filtering can get wrong are both quiet. It can deliver rows
nobody asked for, which a test catches easily. Or it can make a REPLAY differ
from the live stream it continues, so a resume hands a consumer something its
live connection never would — and nothing about that is visible from either
side. Most of this file is about the second.
"""

from __future__ import annotations

import asyncio

import pytest

import streamcast
from streamcast._filter import compile_where, validate

SCHEMA = {
    "type": "object",
    "properties": {
        "client_id": {"type": "integer"},
        "ticker": {"type": "string"},
        "qty": {"type": "integer"},
        "px": {"type": "number"},
    },
    "required": ["client_id", "ticker", "qty", "px"],
}

TICKERS = ("AAPL", "MSFT", "NVDA")


def trade(i: int) -> dict:
    return {
        "client_id": i % 3,
        "ticker": TICKERS[i % 3],
        "qty": 100 + i,
        "px": 1.0 * i,
    }


@pytest.fixture
def stream(tmp_path):
    made = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
    try:
        yield made

    finally:
        made._owned = None  # closed by the fixture's own handle  # noqa: SLF001


@pytest.fixture
async def served(stream, serve):
    async with serve(stream, maintain=False) as uri:
        yield stream, uri


class TestTheLivePath:
    async def test_only_matching_rows_arrive(self, served):
        stream, uri = served
        async with streamcast.connect(uri, where={"ticker": "AAPL"}) as sub:
            await stream.send_many([trade(i) for i in range(9)])
            got = [(await sub.recv())[2] for _ in range(3)]

        assert [row["ticker"] for row in got] == ["AAPL"] * 3
        assert [row["qty"] for row in got] == [100, 103, 106]

    async def test_a_list_value_means_membership(self, served):
        stream, uri = served
        async with streamcast.connect(uri, where={"ticker": ["AAPL", "NVDA"]}) as sub:
            await stream.send_many([trade(i) for i in range(9)])
            got = [(await sub.recv())[2]["ticker"] for _ in range(6)]

        assert got == ["AAPL", "NVDA"] * 3

    async def test_two_terms_are_and(self, served):
        stream, uri = served
        where = {"ticker": "AAPL", "client_id": 0}
        async with streamcast.connect(uri, where=where) as sub:
            await stream.send_many([trade(i) for i in range(9)])
            first = (await sub.recv())[2]

        assert (first["ticker"], first["client_id"]) == ("AAPL", 0)

    async def test_an_empty_filter_is_no_filter(self, served):
        """`where={}` must not mean "match nothing", which a naive `all()` over
        zero terms would also get right by accident — asserted so it stays."""
        stream, uri = served
        async with streamcast.connect(uri, where={}) as sub:
            await stream.send_many([trade(i) for i in range(4)])
            got = [(await sub.recv())[0] for _ in range(4)]

        assert got == [1, 2, 3, 4]

    async def test_one_subscriber_s_filter_does_not_reach_another(self, served):
        """**The property the whole fan-out rests on.**

        Each subscriber owns its predicate, and the frame they share is
        encoded once. A filter that leaked would either silence a subscriber
        that asked for everything or deliver to one that asked for less.
        """
        stream, uri = served
        async with (
            streamcast.connect(uri, where={"ticker": "AAPL"}) as narrow,
            streamcast.connect(uri) as everything,
        ):
            await stream.send_many([trade(i) for i in range(9)])
            wide = [(await everything.recv())[0] for _ in range(9)]
            some = [(await narrow.recv())[2]["ticker"] for _ in range(3)]

        assert wide == list(range(1, 10)), "the unfiltered subscriber lost rows"
        assert some == ["AAPL"] * 3

    async def test_a_type_mismatch_does_not_match_and_does_not_raise(self, served):
        """`offer` promises never to raise, and the predicate runs inside it.

        Comparing a str column against an int is not an error, it is a
        non-match — so a filter cannot take down a fan-out.
        """
        stream, uri = served
        async with (
            streamcast.connect(uri, where={"ticker": 7}) as absurd,
            streamcast.connect(uri) as everything,
        ):
            await stream.send_many([trade(i) for i in range(4)])
            assert [(await everything.recv())[0] for _ in range(4)] == [1, 2, 3, 4]

            with pytest.raises(TimeoutError):
                await asyncio.wait_for(absurd.recv(), timeout=0.3)


class TestTheReplayAgrees:
    """A resume must deliver what the live stream would have."""

    async def test_a_replay_is_filtered_the_same_way(self, served):
        stream, uri = served
        await stream.send_many([trade(i) for i in range(9)])

        async with streamcast.connect(
            uri, offset=streamcast.EARLIEST, where={"ticker": "AAPL"}
        ) as sub:
            got = [(await sub.recv())[2]["ticker"] for _ in range(3)]

        assert got == ["AAPL"] * 3

    async def test_live_and_replayed_frames_are_identical(self, served):
        """I6, across the filter. Both paths project the same columns, so a
        filtered replay must produce the same bytes the live send did."""
        websockets = pytest.importorskip("websockets")
        stream, uri = served
        where = "%7B%22ticker%22%3A%22AAPL%22%7D"

        async with websockets.connect(f"{uri}?where={where}") as raw:
            await raw.recv()
            await stream.send(trade(0))
            live = await raw.recv()

        async with websockets.connect(f"{uri}?offset=1&where={where}") as raw:
            await raw.recv()
            replayed = await raw.recv()

        assert replayed == live

    async def test_resuming_above_the_last_seen_offset_loses_nothing(self, served):
        """The resume contract, with a filter: `offset + 1` and the same
        predicate delivers exactly the matching rows that came after."""
        stream, uri = served
        where = {"ticker": "AAPL"}
        await stream.send_many([trade(i) for i in range(9)])

        async with streamcast.connect(
            uri, offset=streamcast.EARLIEST, where=where
        ) as a:
            first = await a.recv()

        assert first[0] is not None, "a durable stream must offset"

        async with streamcast.connect(uri, offset=first[0] + 1, where=where) as b:
            rest = [(await b.recv())[0] for _ in range(2)]

        assert first[0] == 1
        assert rest == [4, 7], "a filtered resume skipped or repeated"

    async def test_a_filter_does_not_make_an_intact_log_look_evicted(self, served):
        """**The subtle one, and the reason `_log.replay` yields its first row
        whatever the filter says.**

        `_replay_from` reads one row early to check it against the offset that
        was asked for — a log whose retention passed it would otherwise serve
        from wherever it does start, which is a hole at the join. If the filter
        applied to that row, the first MATCHING row would stand in for the
        log's floor, and a selective filter over a perfectly intact log would
        refuse itself as `evicted`.

        Here offset 1 exists but does not match; the first match is offset 2.

        Falsify by filtering the first row in `_log.replay`.
        """
        stream, uri = served
        await stream.send_many([trade(i) for i in range(9)])

        # Offset 1 is trade(0) -> AAPL; ask for MSFT, whose first row is 2.
        async with streamcast.connect(uri, offset=1, where={"ticker": "MSFT"}) as sub:
            got = [(await sub.recv())[0] for _ in range(3)]

        assert got == [2, 5, 8], "the replay did not start where it should"

    async def test_the_first_row_is_dropped_when_it_does_not_match(self, served):
        """Having used it to measure the log's floor, it must not be sent."""
        stream, uri = served
        await stream.send_many([trade(i) for i in range(3)])

        async with streamcast.connect(uri, offset=1, where={"ticker": "MSFT"}) as sub:
            offset, ts, row = await sub.recv()

        assert (offset, row["ticker"]) == (2, "MSFT"), "row 1 leaked past the filter"
        assert isinstance(ts, int)


class TestTheGreeting:
    async def test_it_echoes_the_filter(self, served):
        """A filter's failure mode is silence, so a subscriber needs to see
        that the server understood the one it sent."""
        _stream, uri = served
        async with streamcast.connect(uri, where={"ticker": "AAPL"}) as sub:
            assert sub.info.where == {"ticker": "AAPL"}

    async def test_it_is_none_without_one(self, served):
        _stream, uri = served
        async with streamcast.connect(uri) as sub:
            assert sub.info.where is None

    async def test_replay_is_an_upper_bound_when_filtered(self, served):
        """`end - start` stops being a count. Documented on `Greeting.replay`,
        and asserted here so the documentation cannot quietly become false."""
        stream, uri = served
        await stream.send_many([trade(i) for i in range(9)])

        async with streamcast.connect(
            uri, offset=streamcast.EARLIEST, where={"ticker": "AAPL"}
        ) as sub:
            assert sub.info.replay is not None
            low, high = sub.info.replay
            delivered = [(await sub.recv())[0] for _ in range(3)]

        assert high - low == 9, "the window should still be the offset range"
        assert len(delivered) == 3, "and the count delivered is less"


class TestWhatItRefuses:
    async def test_a_column_the_schema_does_not_have(self, served):
        """Refused, not served as a subscription that never delivers — which
        is indistinguishable from a stream that has gone quiet."""
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="does not have"):
            await streamcast.connect(uri, where={"tikcer": "AAPL"})

    async def test_a_value_that_is_not_a_scalar(self, served):
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="scalar"):
            await streamcast.connect(uri, where={"ticker": {"eq": "AAPL"}})

    async def test_an_empty_list_matches_nothing_and_says_so(self, served):
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="matches nothing"):
            await streamcast.connect(uri, where={"ticker": []})

    async def test_malformed_json_on_the_wire(self, served):
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="not JSON"):
            await streamcast.connect(f"{uri}?where=notjson")

    async def test_a_filter_that_is_not_an_object(self, served):
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="JSON object"):
            await streamcast.connect(f"{uri}?where=%5B1%5D")

    async def test_publish_takes_no_filter(self, served):
        """A publisher receives nothing, so filtering what it receives is a
        request with no meaning."""
        _stream, uri = served
        with pytest.raises(streamcast.ProtocolError, match="receives nothing"):
            await streamcast.connect(f"{uri}?publish&where=%7B%7D")

    async def test_given_twice_it_raises_rather_than_picking(self, served):
        """Same rule as `offset`: the two disagreeing means filtering on
        something the caller did not ask for."""
        _stream, uri = served
        with pytest.raises(ValueError, match="given twice"):
            streamcast.connect(f"{uri}?where=%7B%7D", where={"ticker": "AAPL"})

    async def test_a_live_only_stream_validates_no_names(self):
        """It declares no schema, so there is nothing to check a name against.

        A typo does silently match nothing there, which is a property of
        having no schema rather than of the filter.
        """
        validate({"anything": 1}, None)  # does not raise

        with pytest.raises(streamcast.ProtocolError, match="does not have"):
            validate({"anything": 1}, ("ticker",))


class TestThePredicate:
    """`compile_where` directly — no socket, no server."""

    @pytest.mark.parametrize(
        ("where", "row", "expected"),
        [
            ({}, {"a": 1}, True),
            ({"a": 1}, {"a": 1}, True),
            ({"a": 1}, {"a": 2}, False),
            ({"a": 1}, {}, False),
            ({"a": None}, {"a": None}, True),
            ({"a": None}, {}, True),
            ({"a": 1, "b": 2}, {"a": 1, "b": 2}, True),
            ({"a": 1, "b": 2}, {"a": 1, "b": 3}, False),
            ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}, True),
            ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 4}, False),
            ({"a": [1, 2]}, {"a": 2}, True),
            ({"a": [1, 2]}, {"a": 3}, False),
            ({"a": "x"}, {"a": 1}, False),
        ],
    )
    def test_cases(self, where, row, expected):
        assert compile_where(where)(row) is expected

    def test_a_missing_key_matching_none_is_deliberate(self):
        """`{"a": None}` matches a row that omits `a`, because a row that omits
        a nullable column stores NULL — the filter agrees with the log."""
        assert compile_where({"a": None})({}) is True

    def test_it_never_raises_on_any_row(self):
        """The promise `offer` depends on."""
        predicate = compile_where({"a": 1, "b": "x"})
        for row in ({}, {"a": object()}, {"a": None, "b": []}, {"b": {"n": 1}}):
            assert predicate(row) in (True, False)
