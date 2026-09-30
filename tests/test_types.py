"""Binary and nested columns: declared in JSON Schema, carried as JSON, stored as Arrow.

The property that holds this together is invariant 10: a replayed frame is
byte-identical to the live one. For scalars that came for free. For these
types it did not — a map comes back from Arrow as a list of pairs, and bytes
have no JSON form at all — so every test here that matters sends a row live,
seals it, replays it, and compares the bytes.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
import websockets

import streamcast
from streamcast import _log, _metadata, _schema

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "ts": {"type": "integer"},
        "trace_id": {
            "type": "string",
            "contentEncoding": "base16",
            "format": "bytes16",
        },
        "blob": {"type": ["string", "null"], "contentEncoding": "base64"},
        "attrs": {
            "type": "object",
            "additionalProperties": {"type": ["string", "null"]},
        },
        "res": {
            "type": "object",
            "properties": {
                "service": {"type": "string"},
                "pid": {"type": ["integer", "null"]},
            },
            "required": ["service"],
            "additionalProperties": False,
        },
        "tags": {"type": ["array", "null"], "items": {"type": "string"}},
        "nested": {
            "type": ["object", "null"],
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "b": {"type": ["string", "null"], "contentEncoding": "base16"}
                },
                "required": [],
                "additionalProperties": False,
            },
        },
    },
    "required": ["ts", "trace_id", "attrs", "res"],
}

TRACE = bytes(range(16))


def row(i: int = 0) -> dict[str, object]:
    return {
        "ts": i,
        "trace_id": TRACE,
        "blob": b"\x00\xff" + bytes([i]),
        "attrs": {"k": "v", "a": None, "n": str(i)},
        "res": {"service": "api", "pid": i},
        "tags": ["x", "y"],
        "nested": {"m": {"b": b"\x01\x02"}},
    }


def stream_at(tmp_path) -> streamcast.Stream:
    return streamcast.Stream.new("otel", root=tmp_path, schema=SCHEMA)


class TestTheSchema:
    def test_every_spelling_round_trips(self):
        arrow = _schema.to_arrow(SCHEMA)
        assert _schema.to_arrow(_schema.from_arrow(arrow)).equals(
            arrow, check_metadata=True
        )

    def test_the_types_are_what_litelink_stores(self):
        arrow = _schema.to_arrow(SCHEMA)
        assert str(arrow.field("trace_id").type) == "fixed_size_binary[16]"
        assert str(arrow.field("attrs").type) == "map<string, string>"
        assert arrow.field("res").type.num_fields == 2
        assert _schema.encoding(arrow.field("trace_id")) == "base16"
        assert _schema.encoding(arrow.field("blob")) == "base64"

    async def test_it_is_published_with_its_encodings(self, tmp_path):
        stream = stream_at(tmp_path)
        try:
            assert stream.schema is not None
            published = stream.schema["properties"]
            assert published["trace_id"] == {  # ty: ignore[not-subscriptable]
                "type": "string",
                "contentEncoding": "base16",
                "format": "bytes16",
            }
        finally:
            await stream.aclose()

    async def test_the_encoding_survives_reopening_the_log(self, tmp_path):
        await stream_at(tmp_path).aclose()
        again = stream_at(tmp_path)
        try:
            assert again.schema is not None
            assert again.schema["properties"]["trace_id"]["contentEncoding"] == "base16"  # ty: ignore[not-subscriptable]
        finally:
            await again.aclose()


class TestInvariant10:
    """A replayed frame is byte-identical to the live one, for every new type."""

    async def test_live_and_replayed_frames_match(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        assert stream.log is not None
        async with serve(stream, maintain=False) as uri:
            async with websockets.connect(uri) as raw:
                await raw.recv()
                for i in range(3):
                    await stream.send(row(i))

                live = [await raw.recv() for _ in range(3)]

            # Sealed, so the replay reads Parquet — where maps come back as
            # pairs unless the read asks otherwise.
            while stream.log.seal() is not None:
                pass

            async with websockets.connect(uri + "?offset=1") as raw:
                await raw.recv()
                replayed = [await raw.recv() for _ in range(3)]

        assert replayed == live

    async def test_the_wire_carries_each_encoding_and_maps_as_objects(
        self, tmp_path, serve
    ):
        stream = stream_at(tmp_path)
        async with serve(stream, maintain=False) as uri, websockets.connect(uri) as raw:
            await raw.recv()
            await stream.send(row())
            _offset, message = json.loads(await raw.recv())

        assert message["trace_id"] == TRACE.hex()
        assert message["blob"] == "AP8A"  # base64 of 00 ff 00
        assert message["attrs"] == {"k": "v", "a": None, "n": "0"}
        assert message["nested"] == {"m": {"b": "0102"}}

    async def test_send_many_frames_match_too(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        assert stream.log is not None
        async with serve(stream, maintain=False) as uri:
            async with websockets.connect(uri) as raw:
                await raw.recv()
                await stream.send_many([row(i) for i in range(3)])
                live = [await raw.recv() for _ in range(3)]

            while stream.log.seal() is not None:
                pass

            async with websockets.connect(uri + "?offset=1") as raw:
                await raw.recv()
                assert [await raw.recv() for _ in range(3)] == live


class TestAMapIsAnObject:
    async def test_a_map_sent_as_pairs_is_refused_before_it_is_stored(self, tmp_path):
        stream = stream_at(tmp_path)
        try:
            with pytest.raises(ValueError, match="'attrs' is a map"):
                await stream.send({**row(), "attrs": [("k", "v")]})

            with pytest.raises(ValueError, match="'attrs' is a map"):
                await stream.send_many([row(), {**row(), "attrs": [("k", "v")]}])

            assert stream.end_offset == 1, "nothing appended"
        finally:
            await stream.aclose()


def test_a_map_nested_in_a_list_is_checked_too():
    import pyarrow as pa

    from streamcast._codec import compile_codec

    codec = compile_codec(
        pa.schema([pa.field("groups", pa.list_(pa.map_(pa.string(), pa.string())))])
    )
    assert codec.check is not None
    codec.check({"groups": [{"a": "b"}, None]})
    with pytest.raises(ValueError, match=r"'groups\[\]' is a map"):
        codec.check({"groups": [{"a": "b"}, [("k", "v")]]})


class TestRemotePublishers:
    async def test_binary_arrives_as_text_and_is_stored_as_bytes(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        assert stream.log is not None
        wire = {
            **row(),
            "trace_id": TRACE.hex(),
            "blob": "AP8A",
            "nested": {"m": {"b": "0102"}},
        }
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                assert await producer.send(wire) == 1

            # Inside the block: `serve` closes a log it owns on the way out.
            stored = stream.log.scan(columns=["trace_id", "blob", "nested"]).read_all()

        found = stored.to_pylist(maps_as_pydicts="strict")[0]
        assert found == {
            "trace_id": TRACE,
            "blob": b"\x00\xff\x00",
            "nested": {"m": {"b": b"\x01\x02"}},
        }

    async def test_text_that_is_not_its_encoding_is_rejected(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                with pytest.raises(Exception, match="binary in base16"):  # noqa: B017, PT011
                    await producer.send({**row(), "trace_id": "not hex"})

                # The connection survives it, as for any refused row.
                assert (
                    await producer.send(
                        {**row(), "trace_id": TRACE.hex(), "blob": None, "nested": None}
                    )
                    == 1
                )


class TestTheClient:
    async def test_a_subscriber_gets_bytes_back(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send(row())
                _offset, got = await sub.recv()

        assert got == row()

    async def test_it_matches_what_catch_up_reads_from_the_archive(
        self, tmp_path, serve
    ):
        """Catch-up hands the consumer `_log.rows` output; the socket, a frame.

        Both have to be the same row, or a consumer sees `bytes` for rows it
        caught up on and text for the rest.
        """
        stream = stream_at(tmp_path)
        assert stream.log is not None
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send(row(4))
                _offset, from_socket = await sub.recv()

            from_archive = [
                message async for _o, message in _log.rows(stream.log, 1, 2)
            ]

        assert from_archive == [from_socket]


class TestWhere:
    async def test_a_binary_column_filters_on_its_text(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        other = bytes(16)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri, where={"trace_id": TRACE.hex()}) as sub:
                await stream.send({**row(1), "trace_id": other})
                await stream.send(row(2))
                offset, got = await sub.recv()

            assert offset == 2
            assert got["trace_id"] == TRACE

            # And on replay, through the same predicate.
            async with streamcast.connect(
                uri, offset=1, where={"trace_id": [TRACE.hex()]}
            ) as sub:
                offset, _got = await sub.recv()

            assert offset == 2

    async def test_a_replay_whose_first_row_matches_delivers_it(self, tmp_path, serve):
        """The first replayed row is checked from its decoded FRAME.

        There a binary value is text again, so it has to be decoded before the
        filter compares it — or a first row that matches is silently dropped.
        """
        stream = stream_at(tmp_path)
        async with serve(stream, maintain=False) as uri:
            await stream.send(row(1))  # matches
            await stream.send({**row(2), "trace_id": bytes(16)})
            await stream.send(row(3))  # matches
            async with streamcast.connect(
                uri, offset=1, where={"trace_id": TRACE.hex()}
            ) as sub:
                first, _row = await sub.recv()

        assert first == 1

    async def test_a_nested_column_is_refused(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        async with serve(stream, maintain=False) as uri:
            # The close reason is 123 bytes, so the sentence is trimmed; the
            # column and its type come first for that reason.
            with pytest.raises(streamcast.ProtocolError, match="'attrs', which is map"):
                await streamcast.connect(uri, where={"attrs": "x"})

    async def test_text_that_is_not_the_encoding_is_refused(self, tmp_path, serve):
        stream = stream_at(tmp_path)
        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.ProtocolError, match="binary in base16"):
                await streamcast.connect(uri, where={"trace_id": "zz"})


async def test_the_metadata_records_the_encodings(tmp_path, serve):
    stream = stream_at(tmp_path)
    async with serve(stream, maintain=False):
        pass

    metadata = _metadata.load(tmp_path, "otel")
    assert metadata is not None
    spelled = metadata.live_log.schema["properties"]["trace_id"]  # ty: ignore[not-subscriptable]
    assert spelled["contentEncoding"] == "base16"
