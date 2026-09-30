"""A stream with no log can declare a schema, and then refuses what a log would (#9).

The property: **a stream accepts exactly the same rows with or without a
log**, so attaching storage later is a change of configuration and not of
behaviour. It is checked the direct way — the same bad row, sent to a
live-only stream with a schema and to a durable one, raises the same
exception with the same message — because the check is litelink's own
`validate_row`, the DDL and helpers `append` uses, rather than a copy of it.
"""

from __future__ import annotations

import asyncio
import json
import math
from typing import Any

import pytest
import websockets

import streamcast

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
        "tag": {"type": ["string", "null"]},
        "trace_id": {
            "type": ["string", "null"],
            "contentEncoding": "base16",
            "format": "bytes16",
        },
        "attrs": {
            "type": ["object", "null"],
            "additionalProperties": {"type": ["number", "null"]},
        },
    },
    "required": ["event_ts", "price"],
}

GOOD = {"event_ts": 1, "price": 2.5}

BAD = [
    pytest.param({**GOOD, "venue": "x"}, id="unknown column"),
    pytest.param({"event_ts": 1}, id="missing required"),
    pytest.param({**GOOD, "price": "cheap"}, id="wrong type"),
    pytest.param({**GOOD, "price": math.nan}, id="nan"),
    pytest.param({**GOOD, "price": math.inf}, id="inf"),
    pytest.param({**GOOD, "attrs": {"k": -math.inf}}, id="nested -inf"),
    pytest.param({**GOOD, "trace_id": b"short"}, id="fixed size"),
]


def live() -> streamcast.Stream:
    return streamcast.Stream("trades", schema=SCHEMA)


class TestTheSameRule:
    @pytest.mark.parametrize("row", BAD)
    async def test_a_bad_row_is_refused_exactly_as_a_log_refuses_it(
        self, row, tmp_path
    ):
        durable = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            with pytest.raises(ValueError) as with_log:
                await durable.send(row)
        finally:
            await durable.aclose()

        with pytest.raises(ValueError) as without_log:
            await live().send(row)

        # Identical, up to one clause: an unknown-column refusal goes on to
        # list the log's declared columns, and a durable log also carries
        # `streamcast_ts`, which a live-only stream has no reason to. The
        # refusal — type, reason, offending column — is the same.
        assert type(without_log.value) is type(with_log.value)
        assert (
            str(without_log.value).split(". Declared:")[0]
            == str(with_log.value).split(". Declared:")[0]
        )

    async def test_a_good_row_is_accepted(self):
        assert await live().send(GOOD) is None, "no log, so no offset"

    async def test_one_bad_row_refuses_the_whole_group(self):
        stream = live()
        with pytest.raises(ValueError, match="venue"):
            await stream.send_many([GOOD, {**GOOD, "venue": "x"}])

    async def test_a_map_sent_as_pairs_is_refused(self):
        with pytest.raises(ValueError, match="'attrs' is a map"):
            await live().send({**GOOD, "attrs": [("k", 1.0)]})

    def test_the_owned_column_cannot_be_declared(self):
        with pytest.raises(ValueError, match="another name"):
            streamcast.Stream(
                "trades",
                schema={
                    **SCHEMA,
                    "properties": {
                        **SCHEMA["properties"],
                        "streamcast_ts": {"type": ["integer", "null"]},
                    },
                },
            )

    def test_a_log_and_a_schema_together_are_refused(self, log):
        with pytest.raises(ValueError, match="not both"):
            streamcast.Stream("trades", log=log, schema=SCHEMA)


class TestOnTheWire:
    async def test_a_refused_row_reaches_no_subscriber(self, serve):
        stream = live()
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            with pytest.raises(ValueError):
                await stream.send({**GOOD, "price": math.nan})

            await stream.send({**GOOD, "event_ts": 2})
            offset, row = await sub.recv()

        assert offset is None
        assert row["event_ts"] == 2, "the refused row was never fanned out"

    async def test_the_greeting_publishes_the_schema(self, serve):
        stream = live()
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.schema is not None
            assert list(sub.info.schema["properties"]) == list(SCHEMA["properties"])  # ty: ignore[invalid-argument-type]
            assert sub.info.durable is False

    async def test_frames_take_the_declared_key_order(self, serve):
        stream = live()
        async with serve(stream) as uri, websockets.connect(uri) as raw:
            await raw.recv()
            await stream.send({"price": 1.0, "event_ts": 3})
            _offset, message = json.loads(await raw.recv())

        assert list(message) == list(SCHEMA["properties"])
        assert message["tag"] is None, (
            "an omitted nullable column is null, as a log stores it"
        )

    async def test_binary_columns_work_without_a_log(self, serve):
        stream = live()
        trace = bytes(range(16))
        async with serve(stream) as uri:
            async with websockets.connect(uri) as raw, streamcast.connect(uri) as sub:
                await raw.recv()
                await stream.send({**GOOD, "trace_id": trace})
                _offset, on_the_wire = json.loads(await raw.recv())
                _offset, decoded = await sub.recv()

        assert on_the_wire["trace_id"] == trace.hex()
        assert decoded["trace_id"] == trace

    async def test_a_remote_publisher_is_told_why(self, serve):
        stream = live()
        async with serve(stream, publish=True) as uri:
            async with streamcast.publish(uri) as producer:
                with pytest.raises(Exception, match="venue"):  # noqa: B017, PT011
                    await producer.send({**GOOD, "venue": "x"})

                assert await producer.send(GOOD) is None

    async def test_where_is_checked_against_the_declared_columns(self, serve):
        stream = live()
        async with serve(stream) as uri:
            with pytest.raises(streamcast.ProtocolError, match="does not have"):
                await streamcast.connect(uri, where={"ticker": "AAPL"})


class TestWithoutASchema:
    """Unchanged: a relay that declares nothing checks nothing, on purpose."""

    async def test_any_row_is_accepted(self):
        stream = streamcast.Stream("relay")
        await stream.send({"anything": [1, 2], "goes": {"here": True}})

    async def test_a_non_finite_float_arrives_as_null(self, serve):
        """JSON has no NaN; msgspec writes it as null. Declare a schema to refuse it."""
        stream = streamcast.Stream("relay")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send({"x": math.nan})
            _offset, row = await asyncio.wait_for(sub.recv(), 5)

        assert row == {"x": None}
