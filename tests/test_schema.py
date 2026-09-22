"""JSON Schema in, Arrow out — the layer that lets a caller import one thing.

litelink speaks Arrow and is deliberately general about what it stores;
streamcast is specifically about JSON websockets. The mapping sits on the side
that knows about JSON, and these are its edges.
"""

from __future__ import annotations

import json

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast._schema import from_arrow, to_arrow


def properties(schema: dict) -> dict:
    """`schema["properties"]`, typed. `from_arrow` returns `dict[str, object]`."""
    found = schema["properties"]
    assert isinstance(found, dict)

    return found


TRADES = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
        "side": {"type": "integer", "format": "int32"},
        "live": {"type": "boolean"},
        "tag": {"type": "string"},
    },
    "required": ["event_ts", "price", "side", "live"],
}


class TestTypes:
    @pytest.mark.parametrize(
        ("spec", "arrow"),
        [
            ({"type": "boolean"}, pa.bool_()),
            ({"type": "integer"}, pa.int64()),
            ({"type": "integer", "format": "int64"}, pa.int64()),
            ({"type": "integer", "format": "int32"}, pa.int32()),
            ({"type": "number"}, pa.float64()),
            ({"type": "number", "format": "double"}, pa.float64()),
            ({"type": "number", "format": "float"}, pa.float32()),
            ({"type": "string"}, pa.string()),
        ],
    )
    def test_every_json_type_maps(self, spec, arrow):
        schema = to_arrow({"properties": {"c": spec}, "required": ["c"]})
        assert schema.field("c").type == arrow

    def test_a_missing_format_takes_the_wider_of_each_pair(self):
        """`integer` and `number` do not carry a width, so one is chosen.

        The wide one: a feed that overflows an int32 is a silent wrong answer,
        where a feed that would have fitted one costs four bytes a row.
        """
        schema = to_arrow(
            {"properties": {"i": {"type": "integer"}, "n": {"type": "number"}}}
        )
        assert schema.field("i").type == pa.int64()
        assert schema.field("n").type == pa.float64()

    def test_required_decides_nullability(self):
        schema = to_arrow(TRADES)
        assert not schema.field("event_ts").nullable
        assert schema.field("tag").nullable

    def test_a_null_union_is_the_other_spelling_of_nullable(self):
        # What a schema written by a tool will emit.
        schema = to_arrow(
            {"properties": {"c": {"type": ["integer", "null"]}}, "required": ["c"]}
        )
        assert schema.field("c").type == pa.int64()
        assert schema.field("c").nullable

    def test_column_order_is_the_order_properties_are_written(self):
        # Which is the order every frame's keys appear in, so it is part of
        # the wire contract rather than a detail.
        assert to_arrow(TRADES).names == list(TRADES["properties"])
        # And it survives a trip through JSON, because dicts keep order.
        assert to_arrow(json.loads(json.dumps(TRADES))).names == list(
            TRADES["properties"]
        )


class TestRefusals:
    """It refuses up front what litelink would refuse at the first append."""

    @pytest.mark.parametrize(
        ("spec", "match"),
        [
            ({"type": "object"}, "nested objects are not a column"),
            ({"type": "array"}, "arrays are not a column"),
            ({"type": "null"}, "carries nothing"),
            ({"type": "string", "format": "date-time"}, "epoch integers"),
            ({"type": "string", "format": "byte"}, "refuses binary columns"),
            ({"type": "integer", "format": "int8"}, "is not a column type"),
            ({"type": "integer", "format": "uint64"}, "is not a column type"),
            ({"type": "wat"}, "is not a column type"),
            ({}, "missing a 'type'"),
            ({"type": ["integer", "string"]}, "not a column"),
        ],
    )
    def test_what_litelink_cannot_store_is_refused_here(self, spec, match):
        with pytest.raises(TypeError, match=match):
            to_arrow({"properties": {"c": spec}})

    def test_the_message_names_the_column(self):
        with pytest.raises(TypeError, match="column 'price'"):
            to_arrow({"properties": {"price": {"type": "array"}}})

    @pytest.mark.parametrize(
        ("schema", "match"),
        [
            ({"properties": {}}, "non-empty 'properties'"),
            ({}, "non-empty 'properties'"),
            (
                {"type": "array", "properties": {"c": {"type": "integer"}}},
                "not 'array'",
            ),
            (
                {"properties": {"c": {"type": "integer"}}, "required": "c"},
                "a list of column names",
            ),
            (
                {"properties": {"c": {"type": "integer"}}, "required": ["typo"]},
                "not in 'properties'",
            ),
        ],
    )
    def test_a_malformed_schema_says_what_is_wrong(self, schema, match):
        with pytest.raises(TypeError, match=match):
            to_arrow(schema)

    def test_a_required_typo_is_caught_rather_than_ignored(self):
        # Ignoring it would silently make a column nullable, and the first
        # sign would be a NULL in a column the author believed was required.
        with pytest.raises(TypeError, match=r"not in 'properties': \['evnt_ts'\]"):
            to_arrow(
                {
                    "properties": {"event_ts": {"type": "integer"}},
                    "required": ["evnt_ts"],
                }
            )

    def test_everything_arrow_accepts_that_litelink_does_not_is_unreachable(self):
        # The mapping's whole point: nothing it produces can be refused one
        # layer down.
        from litelink._types import column_type

        for spec in (
            {"type": "boolean"},
            {"type": "integer"},
            {"type": "integer", "format": "int32"},
            {"type": "number"},
            {"type": "number", "format": "float"},
            {"type": "string"},
        ):
            field = to_arrow({"properties": {"c": spec}}).field("c")
            column_type(field.type)  # raises if litelink would refuse it


class TestRoundTrip:
    def test_arrow_to_json_and_back_is_the_same_schema(self):
        arrow = to_arrow(TRADES)
        assert to_arrow(from_arrow(arrow)) == arrow

    def test_widths_are_stated_explicitly_on_the_way_out(self):
        """An `integer` published with no format reads back as int64.

        Which is wrong for an int32 column — so the published shape says
        which one it is, and that is what makes the round trip hold.
        """
        published = properties(from_arrow(to_arrow(TRADES)))
        assert published["side"] == {"type": "integer", "format": "int32"}
        assert published["event_ts"]["format"] == "int64"

    def test_large_string_comes_back_as_string(self):
        # litelink stores `string` and can hand back `large_string`; both are
        # one JSON type.
        arrow = pa.schema([pa.field("c", pa.large_string(), nullable=False)])
        assert properties(from_arrow(arrow))["c"] == {"type": "string"}

    def test_a_type_with_no_json_spelling_says_so(self):
        arrow = pa.schema([pa.field("c", pa.binary())])
        with pytest.raises(TypeError, match="no JSON Schema spelling"):
            from_arrow(arrow)

    def test_it_is_json(self):
        # It is published in a greeting, so it has to survive a serialiser.
        assert json.loads(json.dumps(from_arrow(to_arrow(TRADES)))) == from_arrow(
            to_arrow(TRADES)
        )


class TestTheStreamOwnsItsLog:
    """`root=` + `schema=` — the whole reason a caller imports one thing."""

    async def test_it_creates_the_log(self, tmp_path):
        stream = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        try:
            assert stream.durable
            assert (tmp_path / "trades" / "buffer.db").exists()
            assert (
                await stream.send(
                    {"event_ts": 1, "price": 1.0, "side": 0, "live": True}
                )
                == 1
            )
        finally:
            await stream.aclose()

    async def test_it_reopens_an_existing_one_and_continues_the_offsets(self, tmp_path):
        first = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        await first.send_many(
            [{"event_ts": i, "price": 1.0, "side": 0, "live": True} for i in range(5)]
        )
        await first.aclose()

        # A restart. `new` would raise FileExistsError; the offsets must
        # continue, never restart.
        second = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        try:
            assert second.end_offset == 6
        finally:
            await second.aclose()

    async def test_a_declaration_that_disagrees_with_disk_is_refused(self, tmp_path):
        """`open` takes none of the shape, so a disagreement would be ignored.

        Every send would then be validated against columns the caller never
        wrote down — the failure this convenience would otherwise introduce.
        """
        await streamcast.Stream("trades", root=tmp_path, schema=TRADES).aclose()

        other = {
            "properties": {"different": {"type": "string"}},
            "required": ["different"],
        }
        with pytest.raises(ValueError, match="has columns"):
            streamcast.Stream("trades", root=tmp_path, schema=other)

    async def test_a_log_it_was_handed_is_not_closed(self, log):
        # The caller may be sharing it. A library that closes a borrowed
        # handle is one you cannot lend to.
        stream = streamcast.Stream("trades", log=log)
        await stream.aclose()
        assert log.end_offset() >= 1 or log.end_offset() == 1  # still usable

    async def test_both_at_once_is_refused(self, log, tmp_path):
        with pytest.raises(ValueError, match="not both"):
            streamcast.Stream("trades", log=log, root=tmp_path, schema=TRADES)

    async def test_one_without_the_other_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="go together"):
            streamcast.Stream("trades", root=tmp_path)

        with pytest.raises(ValueError, match="go together"):
            streamcast.Stream("trades", schema=TRADES)

    async def test_serve_closes_a_log_the_stream_created(self, tmp_path):
        stream = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=False):
            await stream.send({"event_ts": 1, "price": 1.0, "side": 0, "live": True})

        # Closed on the way out, after the connections were done.
        assert stream.log is not None
        with pytest.raises(Exception):  # noqa: B017, PT011 — litelink names it
            stream.log.end_offset()


class TestTheGreetingPublishesIt:
    async def test_a_subscriber_learns_the_columns_without_this_repo(
        self, serve, tmp_path
    ):
        stream = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        async with serve(stream) as uri:
            async with streamcast.connect(uri) as sub:
                assert sub.info.schema == from_arrow(to_arrow(TRADES))
                # And it is enough to rebuild the Arrow schema exactly.
                assert to_arrow(sub.info.schema) == to_arrow(TRADES)

        await stream.aclose()

    async def test_a_live_only_stream_publishes_none(self, serve):
        stream = streamcast.Stream("live")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.schema is None
            assert stream.schema is None

    async def test_it_is_readable_by_a_plain_websocket_client(self, serve, tmp_path):
        import websockets

        stream = streamcast.Stream("trades", root=tmp_path, schema=TRADES)
        async with serve(stream) as uri:
            async with websockets.connect(uri) as raw:
                greeting = json.loads(await raw.recv())

            assert greeting["schema"]["properties"]["side"]["format"] == "int32"
            assert "event_ts" in greeting["schema"]["required"]

        await stream.aclose()


def test_the_exported_converters_are_the_public_surface():
    assert streamcast.to_arrow is to_arrow
    assert streamcast.from_arrow is from_arrow
    # And a log made with one is an ordinary litelink log.
    assert isinstance(to_arrow(TRADES), pa.Schema)
    assert litelink is not None
