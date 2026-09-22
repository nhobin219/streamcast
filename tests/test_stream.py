"""`Stream` without a socket: offsets, durability ordering, and the schema gate.

The broadcast is deliberately separable from the transport, and this file is
what that buys — every guarantee below is checked against the object itself
rather than inferred from what came out of a WebSocket.
"""

from __future__ import annotations

import litelink
import pyarrow as pa
import pytest
from litelink.log import OFFSET as COLUMN

import streamcast
from streamcast._protocol import decode, encode
from tests.conftest import SCHEMA, trade


class TestOffsets:
    async def test_a_live_only_stream_assigns_no_offsets_at_all(self):
        """None, not a counter.

        An in-memory sequence would look exactly like a resume cursor to every
        subscriber and every operator reading a greeting, and would be wrong
        the moment the process restarted. Nothing assigned an offset, so
        nothing reports one.
        """
        stream = streamcast.Stream()
        assert stream.end_offset is None
        assert await stream.send(trade(0)) is None
        assert await stream.send(trade(1)) is None
        assert await stream.send_many([trade(2), trade(3)]) == [None, None]
        assert stream.end_offset is None
        assert stream.durable is False

    async def test_a_durable_stream_takes_its_offsets_from_the_log(self, log):
        stream = streamcast.Stream("trades", log=log)
        assert stream.end_offset == log.end_offset()

        offsets = [await stream.send(trade(i)) for i in range(5)]
        assert offsets == [1, 2, 3, 4, 5]
        assert stream.end_offset == 6

    async def test_it_resumes_the_counter_from_a_log_that_already_holds_rows(self, log):
        first = streamcast.Stream("trades", log=log)
        await first.send_many([trade(i) for i in range(10)])

        # A server restart against the same log. The offsets must continue,
        # never restart — a restart that reset them would hand the same
        # integers to different data and every consumer cursor in the system
        # would silently point somewhere else.
        second = streamcast.Stream("trades", log=log)
        assert second.end_offset == 11
        assert await second.send(trade(99)) == 11

    async def test_send_many_assigns_contiguous_offsets_and_returns_them(self, log):
        stream = streamcast.Stream("trades", log=log)
        assert await stream.send_many([trade(i) for i in range(3)]) == [1, 2, 3]
        assert stream.end_offset == 4

    async def test_an_empty_group_assigns_nothing(self, log):
        stream = streamcast.Stream("trades", log=log)
        assert await stream.send_many([]) == []
        assert stream.end_offset == 1

    async def test_a_row_is_durable_before_send_returns(self, log):
        stream = streamcast.Stream("trades", log=log)
        offset = await stream.send(trade(0))

        # Read it back through litelink with no seal, no flush and no
        # cooperation from the stream. This is the ordering the whole recovery
        # story rests on: a row a subscriber can have seen is always a row the
        # log already holds.
        assert offset is not None
        rows = (
            log.scan(start_offset=offset, end_offset=offset + 1).read_all().to_pylist()
        )
        assert len(rows) == 1
        assert rows[0]["price"] == 85_565.0
        assert rows[0]["side"] == 0
        assert rows[0]["tag"] == "t0"


class TestTheLogIsATable:
    """The whole reason litelink is underneath this rather than a flat file."""

    async def test_every_column_is_queryable(self, log):
        stream = streamcast.Stream("trades", log=log)
        await stream.send_many([trade(i) for i in range(20)])

        summary = (
            log.sql("SELECT count(*) n, max(price) high, sum(amount) vol FROM log")
            .read_all()
            .to_pylist()[0]
        )
        assert summary["n"] == 20
        assert summary["high"] == 85_565.0 + 19

    async def test_a_predicate_prunes_on_a_real_column(self, log):
        # Against a blob `payload` column there would be nothing to prune on,
        # and this query would have to parse JSON in SQL.
        stream = streamcast.Stream("trades", log=log)
        await stream.send_many([trade(i) for i in range(20)])

        sells = log.scan(columns=["litelink_offset", "price"], where="side = 1")
        rows = sells.read_all().to_pylist()
        assert len(rows) == 10
        assert set(rows[0]) == {"litelink_offset", "price"}

    async def test_a_nullable_column_the_caller_omitted_reads_back_null(self, log):
        stream = streamcast.Stream("trades", log=log)
        await stream.send_many([trade(i) for i in range(4)])

        tags = [r["tag"] for r in log.scan(columns=["tag"]).read_all().to_pylist()]
        assert tags == ["t0", None, "t2", None]


class TestTheSchemaIsTheCallers:
    def test_streamcast_declares_no_columns_of_its_own(self):
        # It used to export a SCHEMA and own three columns. litelink's own
        # example says the opposite in as many words: declare a schema rather
        # than store the frame whole.
        assert not hasattr(streamcast, "SCHEMA")

    async def test_any_shape_of_log_works(self, tmp_path):
        other = litelink.new(
            tmp_path / "data",
            "book",
            schema=pa.schema(
                [
                    pa.field("level", pa.int32(), nullable=False),
                    pa.field("bid", pa.float64()),
                ]
            ),
        )
        with other:
            stream = streamcast.Stream("book", log=other)
            assert await stream.send({"level": 1, "bid": 85_560.0}) == 1
            assert other.scan().read_all().to_pylist()[0]["bid"] == 85_560.0

    async def test_the_wire_uses_the_logs_column_order(self, log):
        stream = streamcast.Stream("trades", log=log)
        row = trade(0)
        offset = await stream.send(row)
        # Built from the log's schema, not from the dict the caller passed.
        assert encode(offset, row, tuple(SCHEMA.names)) == encode(
            offset, dict(reversed(list(row.items()))), tuple(SCHEMA.names)
        )

    async def test_a_row_the_schema_refuses_broadcasts_nothing(self, log):
        stream = streamcast.Stream("trades", log=log)
        with pytest.raises(Exception):  # noqa: B017, PT011 — litelink names the column
            await stream.send({"event_ts": 1, "price": "not a number"})

        # The offset space is not a place to leave holes for a bad row.
        assert stream.end_offset == 1
        assert await stream.send(trade(0)) == 1


class TestInputs:
    async def test_an_unknown_column_is_refused_by_litelink(self, log):
        stream = streamcast.Stream("trades", log=log)
        with pytest.raises(Exception):  # noqa: B017, PT011
            await stream.send({**trade(0), "venue": "bitstamp"})

        assert stream.end_offset == 1


async def test_repr_says_what_it_is(log):
    stream = streamcast.Stream("trades", log=log)
    assert "trades" in repr(stream)
    assert "durable" in repr(stream)
    assert "live-only" in repr(streamcast.Stream())


def test_the_message_never_carries_an_offset_column(log):
    """The frame is `[offset, msg]`; `msg` is the publisher's row, period.

    litelink's column lives in the TABLE, where it belongs — the caller never
    declares it and litelink refuses a schema that does (I11) — and it never
    reaches the wire under any name.
    """
    assert COLUMN == "litelink_offset"
    assert COLUMN not in SCHEMA.names
    assert COLUMN in log.scan().read_all().schema.names

    frame = encode(1861, trade(0), tuple(SCHEMA.names))
    offset, message = decode(frame)
    assert offset == 1861
    assert set(message) <= set(SCHEMA.names)
    assert COLUMN not in message
    assert "offset" not in message
