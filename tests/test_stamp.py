"""`streamcast_ts`: stamped on every row a server stores, and never sent.

The one column streamcast owns, on litelink's terms for its own offset — the
writer fills it, the table holds it, and nothing on the wire or in the shape a
subscriber is told mentions it. Invariant 10 is the one it could break: a
replayed frame has to carry exactly the keys the live one did.
"""

from __future__ import annotations

import json
import time
from typing import Any

import litelink
import pyarrow as pa
import pytest
import websockets

import streamcast
from streamcast import _log
from tests.conftest import SCHEMA, trade

SCHEMA_JSON: dict[str, Any] = streamcast.from_arrow(SCHEMA)


def stored(log) -> list[int]:
    """The `streamcast_ts` of every row in `log`, in offset order."""
    table = log.scan(columns=["litelink_offset", _log.STAMP]).read_all()
    return [row[_log.STAMP] for row in table.sort_by("litelink_offset").to_pylist()]


class TestItIsStamped:
    async def test_every_row_carries_the_time_the_server_took_it(self, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        before = time.time_ns() // 1_000
        await stream.send(trade(0))
        after = time.time_ns() // 1_000

        [ts] = stored(stamped)
        # Microseconds, the unit `event_ts` uses: the two subtract directly.
        assert before <= ts <= after

    async def test_a_group_shares_one_stamp(self, stamped):
        # One transaction, one moment of durability. Distinct values would
        # claim an ordering in time that the commit does not have.
        stream = streamcast.Stream("trades", log=stamped)
        await stream.send_many([trade(i) for i in range(5)])
        await stream.send(trade(5))

        stamps = stored(stamped)
        assert len(set(stamps[:5])) == 1
        assert stamps[5] >= stamps[0]

    async def test_stats_report_the_same_moment_the_log_stored(self, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        await stream.send(trade(0))

        [ts] = stored(stamped)
        assert stream.stats.last_send_ts == pytest.approx(ts / 1e6, abs=1e-6)

    async def test_the_callers_row_is_not_modified(self, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        row = trade(0)
        await stream.send(row)
        await stream.send_many([row])

        assert row == trade(0)


class TestItIsNeverSent:
    async def test_the_greeting_and_the_frames_agree_on_the_keys(self, serve, stamped):
        """The schema a subscriber is told is the keys it receives, both ways.

        Two expressions read `log.schema` — one for the greeting, one for the
        frame's key order — and filtering only one tells a subscriber about a
        column no frame will ever carry.
        """
        stream = streamcast.Stream("trades", log=stamped)
        await stream.send(trade(0))

        async with serve(stream) as uri, websockets.connect(uri + "?offset=1") as raw:
            greeting = json.loads(await raw.recv())
            _offset, replayed = json.loads(await raw.recv())
            await stream.send(trade(2))
            _offset, live = json.loads(await raw.recv())

        advertised = list(greeting["schema"]["properties"])
        assert _log.STAMP not in advertised
        assert _log.STAMP not in replayed
        assert _log.STAMP not in live
        assert list(live) == [c for c in advertised if c in live]

    async def test_a_replayed_frame_is_byte_identical_to_the_live_one(
        self, serve, stamped
    ):
        # Invariant 10, on a log that holds a column the wire does not.
        stream = streamcast.Stream("trades", log=stamped)
        async with serve(stream) as uri:
            async with websockets.connect(uri) as raw:
                await raw.recv()
                await stream.send(trade(0))
                await stream.send(trade(1))
                await raw.recv()
                live = await raw.recv()

            # Seal, so the replay reads Parquet rather than the buffer.
            while stamped.seal() is not None:
                pass

            async with websockets.connect(uri + "?offset=2") as raw:
                await raw.recv()
                replayed = await raw.recv()

        assert replayed == live

    async def test_the_greeting_names_the_columns_the_log_owns(self, serve, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.log is not None
            assert sub.info.log.owned == ("litelink_offset", _log.STAMP)

    async def test_a_filter_cannot_name_it(self, serve, stamped):
        # It is not a column a subscriber can see, so it is not one it can
        # select on either — the same answer as for any undeclared column.
        stream = streamcast.Stream("trades", log=stamped)
        async with serve(stream) as uri:
            with pytest.raises(streamcast.ProtocolError, match="does not have"):
                await streamcast.connect(uri, where={_log.STAMP: 1})


def test_a_system_columns_type_never_changes():
    """Pinned, because a change here breaks every stream that spans it.

    A stream's logs are read with `UNION ALL BY NAME`, where a column that
    changed type across a seam coerces silently. So an entry already here is
    FROZEN: a column that needs a different type is a new name beside it
    (`streamcast_ts_v2`). Adding a system column is fine and needs no change
    to this test.
    """
    frozen = {_log.STAMP: {"type": "integer", "format": "int64"}}
    for name, spec in frozen.items():
        assert dict(_log.SYSTEM[name]) == spec, (
            f"{name!r} changed type. System column types never change; add a "
            f"new column under a new name instead."
        )


class TestItIsTheServersToFill:
    async def test_a_row_that_supplies_it_is_refused(self, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        with pytest.raises(ValueError, match="stamped by the server"):
            await stream.send({**trade(0), _log.STAMP: 1})

        with pytest.raises(ValueError, match="stamped by the server"):
            await stream.send_many([trade(0), {**trade(1), _log.STAMP: 1}])

        # Nothing committed, nothing consumed.
        assert stream.end_offset == 1
        assert stored(stamped) == []

    async def test_a_remote_publisher_is_told_why(self, serve, stamped):
        stream = streamcast.Stream("trades", log=stamped)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                with pytest.raises(Exception, match="stamped by the server"):  # noqa: B017, PT011
                    await producer.send({**trade(0), _log.STAMP: 1})

                # And the connection survives it, as for any refused row.
                assert await producer.send(trade(0)) == 1

    def test_a_declaration_cannot_claim_it(self, tmp_path):
        schema = {
            **SCHEMA_JSON,
            "properties": {
                **SCHEMA_JSON["properties"],
                _log.STAMP: {"type": "integer"},
            },
            "required": [*SCHEMA_JSON.get("required", []), _log.STAMP],
        }
        with pytest.raises(ValueError, match="another name"):
            streamcast.Stream.new("trades", root=tmp_path, schema=schema)

    def test_a_column_by_that_name_of_another_type_is_refused(self, tmp_path):
        handle = litelink.new(
            tmp_path,
            "trades",
            schema=pa.schema(
                [pa.field("n", pa.int64()), pa.field(_log.STAMP, pa.string())]
            ),
        )
        with handle, pytest.raises(ValueError, match="streamcast owns that name"):
            streamcast.Stream("trades", log=handle)


class TestLogsWithoutIt:
    async def test_stream_new_creates_every_log_with_it(self, tmp_path):
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        try:
            assert stream.log is not None
            assert stream.log.schema.names[-1] == _log.STAMP
            assert stream.schema is not None
            properties = stream.schema["properties"]
            assert isinstance(properties, dict)
            assert _log.STAMP not in properties
            await stream.send(trade(0))
            assert len(stored(stream.log)) == 1
        finally:
            await stream.aclose()

    async def test_reopening_a_stamped_log_accepts_the_same_declaration(self, tmp_path):
        first = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        await first.aclose()

        again = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        await again.aclose()

    async def test_a_log_without_it_opens_and_is_left_alone(self, tmp_path):
        # Created before the column existed. It keeps its shape — adding a
        # column is not a side effect an open should have.
        litelink.new(tmp_path, "trades", schema=SCHEMA).close()

        stream = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA_JSON)
        try:
            assert stream.log is not None
            assert _log.STAMP not in stream.log.schema.names
            assert await stream.send(trade(0)) == 1
            info = stream._log_info()
            assert info is not None
            assert info[2] == ("litelink_offset",)
        finally:
            await stream.aclose()

    async def test_a_handed_in_log_without_it_is_never_stamped(self, log, serve):
        stream = streamcast.Stream("trades", log=log)
        await stream.send(trade(0))

        assert _log.STAMP not in log.schema.names
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.log is not None
            assert sub.info.log.owned == ("litelink_offset",)
