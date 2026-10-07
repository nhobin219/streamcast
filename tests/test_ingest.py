"""`Stream.ingest`: Arrow loaded straight into a stopped stream's log.

The rows take the next offsets at the end of the current log, as any append
does, and a subscriber reads them like any other — by offset, after the server
starts again. What streamcast adds to litelink's `ingest` is its own column,
`streamcast_ts`, and the refusals a stream has that a log does not.
"""

from __future__ import annotations

import time
from typing import Any

import litelink
import pyarrow as pa
import pytest

import streamcast

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "i": {"type": "integer"},
        "blob": {"type": ["string", "null"], "contentEncoding": "base64"},
    },
    "required": ["i"],
}


def source(lo: int, hi: int) -> pa.Table:
    return pa.table(
        {
            "i": pa.array(range(lo, hi), pa.int64()),
            "blob": pa.array(
                [bytes([n % 256]) * 3 for n in range(lo, hi)], pa.binary()
            ),
        }
    )


async def stopped(root, rows: int = 3) -> None:
    """A stream with `rows` sent through the server path, then stopped."""
    stream = streamcast.Stream.new("t", root=root, schema=SCHEMA)
    try:
        await stream.send_many([{"i": n, "blob": None} for n in range(rows)])
        stream.ensure_metadata()
    finally:
        await stream.aclose()


class TestIngesting:
    async def test_rows_take_the_next_offsets_and_replay_like_any_other(
        self, tmp_path, serve
    ):
        await stopped(tmp_path)
        before = time.time_ns() // 1_000
        span = streamcast.Stream.ingest("t", source(100, 150), root=tmp_path)
        after = time.time_ns() // 1_000

        assert span == (4, 54), "after the three sent rows, dense"
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        assert stream.end_offset == 54
        async with serve(stream) as uri:
            async with streamcast.connect(uri, offset=3) as sub:
                got = [await sub.recv() for _ in range(51)]

        assert [offset for offset, _ts, _row in got] == list(range(3, 54))
        # Sent in its encoding and decoded on arrival, as a live row's is.
        assert got[1][2] == {"i": 100, "blob": b"ddd"}
        stamps = [ts for _o, ts, _r in got[1:]]
        assert all(ts is not None and before <= ts <= after for ts in stamps), (
            "stamped at load"
        )

    async def test_it_publishes_what_it_loaded(self, tmp_path):
        await stopped(tmp_path)
        streamcast.Stream.ingest("t", source(0, 10), root=tmp_path, flush=True)
        with litelink.open(tmp_path, "t", read_only=True) as log:
            assert log.published_through() == 13

    async def test_a_reader_is_loaded_batch_by_batch(self, tmp_path):
        await stopped(tmp_path, rows=0)
        table = source(0, 30)
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(10))
        assert streamcast.Stream.ingest("t", reader, root=tmp_path) == (1, 31)

    async def test_an_empty_source_takes_nothing(self, tmp_path):
        await stopped(tmp_path)
        assert streamcast.Stream.ingest("t", source(0, 0), root=tmp_path) is None

    async def test_a_migrated_stream_loads_into_its_current_log(self, tmp_path):
        await stopped(tmp_path)
        wider = {
            **SCHEMA,
            "properties": {
                **SCHEMA["properties"],
                "note": {"type": ["string", "null"]},
            },
        }
        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=wider)
        await migrated.aclose()

        rows = source(0, 5).append_column("note", pa.array(["n"] * 5, pa.string()))
        assert streamcast.Stream.ingest("t", rows, root=tmp_path) == (4, 9)
        with litelink.open(tmp_path, "t-v2", read_only=True) as log:
            assert log.end_offset() == 9


class TestWhatItRefuses:
    async def test_a_source_that_carries_the_stamp(self, tmp_path):
        await stopped(tmp_path)
        stamped = source(0, 3).append_column(
            "streamcast_ts", pa.array([1, 2, 3], pa.int64())
        )
        with pytest.raises(ValueError, match="stamped by the server"):
            streamcast.Stream.ingest("t", stamped, root=tmp_path)

    async def test_a_missing_column_before_anything_is_reserved(self, tmp_path):
        await stopped(tmp_path)
        with pytest.raises(ValueError, match="missing"):
            streamcast.Stream.ingest("t", source(0, 3).drop(["blob"]), root=tmp_path)

        # No hole: the next load takes the next offset.
        assert streamcast.Stream.ingest("t", source(0, 1), root=tmp_path) == (4, 5)

    async def test_a_null_in_a_required_column(self, tmp_path):
        await stopped(tmp_path)
        rows = source(0, 3).set_column(0, "i", pa.array([1, None, 3], pa.int64()))
        with pytest.raises(ValueError, match="null"):
            streamcast.Stream.ingest("t", rows, root=tmp_path)

        assert streamcast.Stream.ingest("t", source(0, 1), root=tmp_path) == (4, 5)

    async def test_a_retired_stream(self, tmp_path):
        await stopped(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        with pytest.raises(streamcast.StreamRetired, match="revive=True"):
            streamcast.Stream.ingest("t", source(0, 3), root=tmp_path)

    def test_a_stream_that_is_not_there(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="no stream 't'"):
            streamcast.Stream.ingest("t", source(0, 3), root=tmp_path)
