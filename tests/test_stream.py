"""`Stream` without a socket: offsets, durability ordering, and the schema gate.

The broadcast is deliberately separable from the transport, and this file is
what that buys — every guarantee below is checked against the object itself
rather than inferred from what came out of a WebSocket.
"""

from __future__ import annotations

import base64

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast._log import SCHEMA
from streamcast._protocol import BINARY, TEXT


class TestOffsets:
    async def test_a_live_only_stream_counts_from_one(self):
        stream = streamcast.Stream()
        assert stream.end_offset == 1
        assert await stream.send("a") == 1
        assert await stream.send("b") == 2
        assert stream.end_offset == 3
        assert stream.durable is False

    async def test_a_durable_stream_takes_its_offsets_from_the_log(self, log):
        stream = streamcast.Stream("trades", log=log)
        assert stream.end_offset == log.end_offset()

        offsets = [await stream.send(f"m{i}") for i in range(5)]
        assert offsets == [1, 2, 3, 4, 5]
        assert stream.end_offset == 6

    async def test_it_resumes_the_counter_from_a_log_that_already_holds_rows(self, log):
        first = streamcast.Stream("trades", log=log)
        await first.send_many([f"m{i}" for i in range(10)])

        # A broker restart against the same log. The offsets must continue,
        # never restart — this is the whole of why the counter is seeded from
        # `end_offset()` rather than from 1.
        second = streamcast.Stream("trades", log=log)
        assert second.end_offset == 11
        assert await second.send("after the restart") == 11

    async def test_send_many_assigns_contiguous_offsets_and_returns_them(self, log):
        stream = streamcast.Stream("trades", log=log)
        offsets = await stream.send_many(["a", "b", "c"])
        assert offsets == [1, 2, 3]
        assert stream.end_offset == 4

    async def test_an_empty_group_assigns_nothing(self, log):
        stream = streamcast.Stream("trades", log=log)
        assert await stream.send_many([]) == []
        assert stream.end_offset == 1

    async def test_a_message_is_durable_before_send_returns(self, log):
        stream = streamcast.Stream("trades", log=log)
        offset = await stream.send('{"price": 78501.62}')

        # Read it back through litelink with no seal, no flush and no
        # cooperation from the stream. This is the ordering the whole recovery
        # story rests on: a message a subscriber can have seen is always a
        # message the log already holds.
        rows = (
            log.scan(start_offset=offset, end_offset=offset + 1).read_all().to_pylist()
        )
        assert len(rows) == 1
        assert rows[0]["payload"] == '{"price": 78501.62}'
        assert rows[0]["kind"] == TEXT
        assert rows[0]["recv_ts"] > 0

    async def test_a_binary_message_is_stored_base64_and_comes_back_as_bytes(self, log):
        stream = streamcast.Stream("trades", log=log)
        payload = b"\x00\x01\xff\xfe not UTF-8"
        offset = await stream.send(payload)

        rows = (
            log.scan(start_offset=offset, end_offset=offset + 1).read_all().to_pylist()
        )
        assert rows[0]["kind"] == BINARY
        # litelink refuses `binary` columns today, so this is text on disk —
        # and the cost is visible right here, 4/3 the size.
        assert rows[0]["payload"] == base64.b64encode(payload).decode()


class TestSchemaGate:
    def test_a_log_with_another_shape_is_refused_at_construction(self, tmp_path):
        # The failure this prevents: a broker that opens fine, brings up its
        # upstream subscription, and raises inside `append` on the first
        # message — with the feed live and nowhere to put it.
        other = litelink.new(
            tmp_path / "data",
            "other",
            schema=pa.schema([pa.field("price", pa.float64(), nullable=False)]),
        )
        with other, pytest.raises(ValueError, match="not a streamcast log"):
            streamcast.Stream("other", log=other)

    def test_the_message_says_what_to_create_it_with(self, tmp_path):
        other = litelink.new(
            tmp_path / "data", "other", schema=pa.schema([pa.field("x", pa.int64())])
        )
        with other, pytest.raises(ValueError, match=r"schema=streamcast\.SCHEMA"):
            streamcast.Stream("other", log=other)

    def test_litelink_hands_back_the_arrow_types_it_was_given(self, log, tmp_path):
        """`validate` compares types exactly, and this is why it may.

        litelink documents that `string` is stored and returned as
        `large_string`, which would make an exact comparison reject a good
        log. It does not apply to a handle whose schema comes from the log's
        own `meta` — which is every handle a `Stream` can take. If that ever
        changes, this fails by name rather than `Stream` failing with
        "not a streamcast log" against a log streamcast itself created.
        """
        assert list(log.schema) == list(SCHEMA)

        # And again after a close and reopen, which is the interesting case:
        # the schema then comes back from the log's `meta` rather than from
        # the argument `new` was given. A separate log rather than a second
        # handle to this one — litelink is explicit that a reader in the
        # writer's own process is the case to avoid.
        litelink.new(tmp_path / "reopen", "trades", schema=SCHEMA).close()
        with litelink.open(tmp_path / "reopen", "trades") as reopened:
            assert list(reopened.schema) == list(SCHEMA)
            streamcast.Stream("trades", log=reopened)

    def test_the_schema_declares_exactly_three_columns(self):
        # Pinned, because the shape is the contract between streamcast and
        # every log it has ever written. Adding a column is a migration.
        assert SCHEMA.names == ["recv_ts", "kind", "payload"]


class TestInputs:
    async def test_a_message_that_is_not_str_or_bytes_is_refused(self):
        stream = streamcast.Stream()
        with pytest.raises(TypeError, match="str or bytes"):
            await stream.send({"not": "a message"})  # ty: ignore[invalid-argument-type]

    async def test_nothing_is_assigned_to_a_message_that_was_refused(self, log):
        stream = streamcast.Stream("trades", log=log)
        with pytest.raises(TypeError):
            await stream.send(12345)  # ty: ignore[invalid-argument-type]

        # The offset space is not a place to leave holes for a caller's typo.
        assert stream.end_offset == 1
        assert await stream.send("real") == 1


async def test_repr_says_what_it_is(log):
    stream = streamcast.Stream("trades", log=log)
    assert "trades" in repr(stream)
    assert "durable" in repr(stream)
    assert "live-only" in repr(streamcast.Stream())
