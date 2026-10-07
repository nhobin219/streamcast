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
        with pytest.raises(streamcast.IngestFailed, match="missing") as raised:
            streamcast.Stream.ingest("t", source(0, 3).drop(["blob"]), root=tmp_path)

        assert (raised.value.loaded, raised.value.gap) == (None, None)

        # No hole: the next load takes the next offset.
        assert streamcast.Stream.ingest("t", source(0, 1), root=tmp_path) == (4, 5)

    async def test_a_null_in_a_required_column(self, tmp_path):
        await stopped(tmp_path)
        rows = source(0, 3).set_column(0, "i", pa.array([1, None, 3], pa.int64()))
        with pytest.raises(streamcast.IngestFailed, match="null") as raised:
            streamcast.Stream.ingest("t", rows, root=tmp_path)

        assert (raised.value.batch, raised.value.row) == (0, 1)

        assert streamcast.Stream.ingest("t", source(0, 1), root=tmp_path) == (4, 5)

    async def test_a_retired_stream(self, tmp_path):
        await stopped(tmp_path)
        streamcast.Stream.retire("t", root=tmp_path)
        with pytest.raises(streamcast.StreamRetired, match="revive=True"):
            streamcast.Stream.ingest("t", source(0, 3), root=tmp_path)

    def test_a_stream_that_is_not_there(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="no stream 't'"):
            streamcast.Stream.ingest("t", source(0, 3), root=tmp_path)


PARTIAL: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "x": {"type": "number"}},
    "required": ["i", "x"],
}
PARTIAL_ARROW = pa.schema([("i", pa.int64()), ("x", pa.float64())])


async def small_files(root) -> None:
    """Three rows sent, and files small enough that a load commits in groups.

    litelink commits a load 20 files at a time, so a failure past the 20th
    file leaves the first 20 in the log; 500-row row groups at a tiny target
    make every file 1,000 rows."""
    stream = streamcast.Stream.new(
        "t",
        root=root,
        schema=PARTIAL,
        config=litelink.LogConfig(
            target_compact_size=8 * 1024, target_row_group_rows=500
        ),
    )
    try:
        await stream.send_many([{"i": -1, "x": 0.0}] * 3)
    finally:
        await stream.aclose()


def batch(lo: int, hi: int, nan_at: int | None = None) -> pa.RecordBatch:
    xs = [n * 1.37 for n in range(lo, hi)]
    if nan_at is not None:
        xs[nan_at] = float("nan")

    return pa.record_batch(
        [pa.array(range(lo, hi), pa.int64()), pa.array(xs)], schema=PARTIAL_ARROW
    )


class TestAFailureMidway:
    async def test_it_says_where_what_landed_and_where_to_resume(self, tmp_path):
        await small_files(tmp_path)
        batches = [batch(n, n + 1_000) for n in range(0, 30_000, 1_000)]
        batches += [batch(30_000, 31_000, nan_at=7), batch(31_000, 32_000)]
        reader = pa.RecordBatchReader.from_batches(PARTIAL_ARROW, iter(batches))

        with pytest.raises(streamcast.IngestFailed, match="nan") as raised:
            streamcast.Stream.ingest("t", reader, root=tmp_path)

        failed = raised.value
        assert (failed.batch, failed.row) == (30, 30_007)
        assert failed.loaded == (4, 20_004), "the first 20 files, committed"
        assert failed.rows_loaded == 20_000
        assert failed.gap == (20_004, 30_004), "reserved, never committed"
        assert isinstance(failed.__cause__, ValueError), "litelink's own, kept"

        # Resuming from the row it names, with the bad value fixed, finishes
        # the load: every source row once, in order, around the gap.
        fixed = [batch(n, n + 1_000) for n in range(0, 32_000, 1_000)]
        rest = pa.Table.from_batches(fixed).slice(failed.rows_loaded)
        assert streamcast.Stream.ingest("t", rest, root=tmp_path) == (30_004, 42_004)
        with litelink.open(tmp_path, "t", read_only=True) as log:
            loaded = log.scan(columns=["i"], start_offset=4).read_all()

        assert loaded.column("i").to_pylist() == list(range(32_000))

    async def test_a_bad_row_early_in_a_row_group_is_still_found(self, tmp_path):
        """litelink fails a row group, which is several 100-row batches here:
        by then the bad row's batch is not the newest one read."""
        await small_files(tmp_path)
        batches = [batch(n, n + 100) for n in range(0, 2_000, 100)]
        batches[5] = batch(500, 600, nan_at=7)  # the first of a 500-row group
        reader = pa.RecordBatchReader.from_batches(PARTIAL_ARROW, iter(batches))

        with pytest.raises(streamcast.IngestFailed) as raised:
            streamcast.Stream.ingest("t", reader, root=tmp_path)

        assert (raised.value.batch, raised.value.row) == (5, 507)

    async def test_a_failure_that_is_not_the_data_keeps_its_message(
        self, tmp_path, monkeypatch
    ):
        """A full disk, a killed process: nothing to locate, litelink's word."""
        await small_files(tmp_path)

        def full(self, reader, **_):  # noqa: ANN001, ANN003, ANN202
            for _ in reader:
                msg = "No space left on device"
                raise OSError(msg)

        monkeypatch.setattr(litelink.WriteHandle, "ingest", full)
        reader = pa.RecordBatchReader.from_batches(PARTIAL_ARROW, iter([batch(0, 10)]))
        with pytest.raises(streamcast.IngestFailed, match="No space") as raised:
            streamcast.Stream.ingest("t", reader, root=tmp_path)

        assert (raised.value.batch, raised.value.row) == (None, None)
        assert raised.value.loaded is None
