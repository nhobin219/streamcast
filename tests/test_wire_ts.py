"""`streamcast_ts` on the wire: element 1 of every frame, `[offset, ts, msg]`.

Carried the way the offset is — beside the row, never a key in it — and held
to the same rule (I10): a replayed frame is byte-for-byte the live frame it
repeats, stamp included, so `ts` is the value the log stored, not a time
taken at replay.
"""

from __future__ import annotations

import json
import time
from typing import Any

import litelink
import pytest
import websockets

import streamcast
from streamcast import LATEST, _log

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}


def row(i: int) -> dict[str, object]:
    return {"event_ts": i, "price": 100.0 + i}


def stored(log: litelink.WriteHandle) -> dict[int, int | None]:
    """`streamcast_ts` by offset, as the log holds it."""
    table = log.scan(columns=[_log.COLUMN, _log.STAMP]).read_all()
    return {r[_log.COLUMN]: r[_log.STAMP] for r in table.to_pylist()}


async def frames(uri: str, count: int, *, offset: int | None = None) -> list[bytes]:
    """The raw bytes of the first `count` data frames, after the greeting."""
    target = uri if offset is None else f"{uri}?offset={offset}"
    async with websockets.connect(target) as raw:
        await raw.recv()  # the greeting
        return [await raw.recv(decode=False) for _ in range(count)]


class TestTheLiveFrame:
    async def test_it_carries_the_stamp_the_log_stored(self, tmp_path, serve):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    await stream.send(row(0))
                    offset, ts, msg = await sub.recv()

                assert stream.log is not None
                expected = stored(stream.log)

            assert offset is not None
            assert ts == expected[offset]
            assert msg == row(0)  # and not a key in the row
        finally:
            await stream.aclose()

    async def test_a_group_shares_one_stamp(self, tmp_path, serve):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    await stream.send_many([row(i) for i in range(3)])
                    received = [await sub.recv() for _ in range(3)]

                assert stream.log is not None
                expected = stored(stream.log)

            stamps = {ts for _, ts, _ in received}
            assert len(stamps) == 1
            assert stamps == set(expected.values())
        finally:
            await stream.aclose()

    async def test_a_stream_with_no_log_sends_its_send_time(self, serve):
        stream = streamcast.Stream("trades", schema=SCHEMA)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            before = time.time_ns() // 1_000
            await stream.send(row(0))
            after = time.time_ns() // 1_000
            offset, ts, _msg = await sub.recv()

        assert offset is None
        assert ts is not None
        assert before <= ts <= after


class TestTheReplayedFrame:
    async def test_it_is_the_live_frame_byte_for_byte(self, tmp_path, serve):
        """I10, with the stamp: read from the log, not taken at replay."""
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        try:
            async with serve(stream, maintain=False) as uri:
                async with websockets.connect(uri) as raw:
                    await raw.recv()  # the greeting
                    await stream.send(row(0))
                    await stream.send_many([row(1), row(2)])
                    live = [await raw.recv(decode=False) for _ in range(3)]

                replayed = await frames(uri, 3, offset=1)

            assert replayed == live
            assert all(isinstance(json.loads(f)[1], int) for f in live)
        finally:
            await stream.aclose()


class TestALogWithoutTheStamp:
    """A log created before `streamcast_ts` existed: null, live and replayed.

    Null live too, because its replay can only send what it stored — and a
    live frame that differed from its own replay would break I10.
    """

    async def test_it_sends_null_both_ways(self, tmp_path, serve):
        legacy = litelink.new(tmp_path, "trades", schema=streamcast.to_arrow(SCHEMA))
        stream = streamcast.Stream("trades", log=legacy)
        try:
            async with serve(stream, maintain=False) as uri:
                async with websockets.connect(uri) as raw:
                    await raw.recv()
                    await stream.send(row(0))
                    live = await raw.recv(decode=False)

                [replayed] = await frames(uri, 1, offset=1)

            assert json.loads(live)[1] is None
            assert replayed == live
        finally:
            legacy.close()


class TestRowsReadFromTheTables:
    async def test_a_snapshot_carries_the_stored_stamps(self, tmp_path, serve):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        await stream.send_many([row(i) for i in range(3)])
        log = stream.log
        assert log is not None
        while log.seal() is not None:
            pass

        log.publish(push_unsettled=True)
        await stream.send(row(3))  # only on the broker
        expected = stored(log)
        uri = stream.metadata_uri
        assert uri is not None
        try:
            async with serve(stream, maintain=False) as broker:
                async with await streamcast.Stream.snapshot(
                    uri, as_of_offset=LATEST, broker=broker
                ) as snap:
                    read = [(offset, ts) async for offset, ts, _ in snap.rows(1)]
                    table = await snap.scan()

            # Published rows and the broker's tail alike: the tail's stamps
            # came off the wire, and they are the stored ones.
            assert dict(read) == expected
            stamps = dict(
                zip(
                    table.column(_log.COLUMN).to_pylist(),
                    table.column(_log.STAMP).to_pylist(),
                    strict=True,
                )
            )
            assert stamps == expected
        finally:
            await stream.aclose()

    @pytest.mark.replication
    async def test_catch_up_delivers_the_stored_stamps(
        self, tmp_path, s3, bucket, serve
    ):
        stream = streamcast.Stream.new(
            "trades",
            root=tmp_path,
            schema=SCHEMA,
            published=bucket,
            s3=s3,
            max_replay=2,
        )
        await stream.send_many([row(i) for i in range(10)])
        log = stream.log
        assert log is not None
        while log.seal() is not None:
            pass

        log.publish(push_unsettled=True)
        expected = stored(log)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(
                    uri, offset=1, catch_up=True, s3=s3
                ) as sub:
                    received = [await sub.recv() for _ in range(10)]

            assert {offset: ts for offset, ts, _ in received} == expected
        finally:
            await stream.aclose()
