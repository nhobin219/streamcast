"""`Stream.live`: the published tables plus the broker's rows, kept current.

Served directly rather than through the `serve` fixture where a test needs the
server itself — to drop its connections and watch the view come back.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import time
from collections.abc import AsyncIterator
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _live, _log

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}


def row(i: int) -> dict[str, object]:
    return {"event_ts": i, "price": 100.0 + i}


def publish(stream: streamcast.Stream) -> None:
    log = stream.log
    assert log is not None
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)


@contextlib.asynccontextmanager
async def served(stream: streamcast.Stream) -> AsyncIterator[tuple[Any, str]]:
    server = await streamcast.serve(stream, "127.0.0.1", 0, maintain=False)
    try:
        port = server.sockets[0].getsockname()[1]
        yield server, f"ws://127.0.0.1:{port}/{stream.name}"
    finally:
        server.close()
        await server.wait_closed()


async def offsets(live: _live.Live) -> list[int]:
    table = await live.scan(columns=[_log.COLUMN])
    return table.column(_log.COLUMN).to_pylist()


def held(live: _live.Live) -> int:
    """Rows the view holds in memory: its tail, converted or not."""
    return len(live._pending) + sum(t.num_rows for t in live._tail)  # noqa: SLF001


@pytest.fixture
async def stream(tmp_path) -> AsyncIterator[streamcast.Stream]:
    """Three rows published, two more only on the broker."""
    made = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
    await made.send_many([row(i) for i in range(3)])
    publish(made)
    await made.send_many([row(i) for i in range(3, 5)])
    try:
        yield made
    finally:
        await made.aclose()


class TestItIsCurrent:
    async def test_published_and_broker_rows_read_as_one_table(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker) as live:
                await live.wait_for(5)
                assert await offsets(live) == [1, 2, 3, 4, 5]

                await stream.send_many([row(i) for i in range(5, 8)])
                await live.wait_for(8)
                assert await offsets(live) == list(range(1, 9))
                assert live.end_offset == 9

    async def test_sql_sees_what_scan_sees(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker) as live:
                await live.wait_for(5)
                result = await live.sql(
                    "SELECT count(*) AS n, max(price) AS top FROM log"
                )
                assert result.to_pylist() == [{"n": 5, "top": 104.0}]

    async def test_broker_rows_carry_their_stamps(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker) as live:
                await live.wait_for(5)
                table = await live.scan(columns=[_log.COLUMN, _log.STAMP])

        assert all(ts is not None for ts in table.column(_log.STAMP).to_pylist())

    async def test_a_stream_with_no_log_has_no_history_to_keep(self):
        live_only = streamcast.Stream("trades", schema=SCHEMA)
        async with served(live_only) as (_server, broker):
            with pytest.raises(ValueError, match="no log"):
                await streamcast.Stream.live(broker)


class TestMemory:
    async def test_a_rebase_drops_what_is_now_published(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker, rebase_every=3600) as live:
                await live.wait_for(5)
                before = await offsets(live)
                assert held(live) == 2  # offsets 4 and 5: not yet published

                publish(stream)
                await live.rebase()

                assert held(live) == 0
                assert await offsets(live) == before

    async def test_a_row_the_base_already_holds_is_not_counted_twice(self, stream):
        """The base can get ahead of the socket: published past what arrived.

        Such a row then arrives late, and the base already has it.
        """
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker, rebase_every=3600) as live:
                await live.wait_for(5)
                publish(stream)
                await live.rebase()

                await live._append(4, 1, row(3))  # noqa: SLF001 — late, and published

                assert await offsets(live) == [1, 2, 3, 4, 5]
                assert held(live) == 0
                assert live.end_offset == 6  # and the view did not go backwards

    async def test_a_row_received_twice_is_kept_once(self, stream):
        """Not yet published, so only the receive guard stands between it and a
        duplicate in the tail."""
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker, rebase_every=3600) as live:
                await live.wait_for(5)
                await live._append(5, 1, row(4))  # noqa: SLF001 — a repeat

                assert await offsets(live) == [1, 2, 3, 4, 5]
                assert live.end_offset == 6


class TestReconnecting:
    async def test_a_dropped_connection_comes_back_without_a_gap(self, stream):
        async with served(stream) as (server, broker):
            async with await streamcast.Stream.live(broker, rebase_every=3600) as live:
                await live.wait_for(5)
                original = live._base  # noqa: SLF001
                for connection in list(server.connections):
                    await connection.close()

                # Sent while the view is reconnecting: replayed when it is back.
                await stream.send_many([row(i) for i in range(5, 9)])
                await live.wait_for(9)

                assert await offsets(live) == list(range(1, 10))
                # Re-pinned on the way back, not on the hour-long timer: a
                # restart is when a migration happens, and its new log has to
                # be read.
                assert live._base is not original  # noqa: SLF001

    async def test_a_failure_it_cannot_fix_is_raised_by_the_next_query(
        self, stream, monkeypatch
    ):
        async with served(stream) as (server, broker):
            async with await streamcast.Stream.live(broker) as live:
                await live.wait_for(5)

                async def refused() -> None:
                    raise streamcast.NotReplayable("ahead", offset=6, end_offset=6)

                monkeypatch.setattr(live, "_connect", refused)
                for connection in list(server.connections):
                    await connection.close()

                with pytest.raises(RuntimeError, match="stopped"):
                    # Bounded, so a view that never records the failure fails
                    # here rather than hanging the suite.
                    await asyncio.wait_for(live.wait_for(100), timeout=5)

                with pytest.raises(RuntimeError, match="stopped"):
                    await live.scan()


class TestWaitingForATime:
    """`wait_for(ts=T)`: every row stamped at or before T is visible.

    Known only once a row stamped after T is here — the documented sharp
    edge on a quiet stream. Every wait below is bounded, so a regression
    fails rather than hangs.
    """

    async def test_it_returns_once_a_later_row_arrives(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker) as live:
                await live.wait_for(5)
                now = time.time_ns() // 1_000
                waiting = asyncio.create_task(live.wait_for(ts=now))
                for _ in range(20):
                    await asyncio.sleep(0)

                # Nothing stamped after `now` exists yet, so nothing proves it.
                assert not waiting.done()

                await stream.send(row(5))
                await asyncio.wait_for(waiting, timeout=5)

    async def test_published_rows_past_it_need_no_new_row(self, tmp_path):
        # Everything published, nothing more sent: an idle stream asked about
        # a time its published rows have already passed.
        idle = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
        await idle.send_many([row(i) for i in range(3)])
        publish(idle)
        try:
            async with served(idle) as (_server, broker):
                async with await streamcast.Stream.live(broker) as live:
                    await asyncio.wait_for(live.wait_for(ts=0), timeout=5)
        finally:
            await idle.aclose()

    async def test_one_point_or_the_other(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker) as live:
                with pytest.raises(ValueError, match="not both"):
                    await live.wait_for(5, ts=1)

                with pytest.raises(ValueError, match="not neither"):
                    await live.wait_for()

    async def test_a_log_without_stamps_has_no_time_to_wait_for(self, tmp_path):
        legacy = litelink.new(tmp_path, "trades", schema=streamcast.to_arrow(SCHEMA))
        unstamped = streamcast.Stream("trades", log=legacy)
        await unstamped.send_many([row(i) for i in range(3)])
        while legacy.seal(flush=True) is not None:
            pass

        legacy.publish(flush=True)
        try:
            async with served(unstamped) as (_server, broker):
                async with await streamcast.Stream.live(broker) as live:
                    with pytest.raises(ValueError, match="without streamcast_ts"):
                        await asyncio.wait_for(live.wait_for(ts=1), timeout=5)
        finally:
            legacy.close()


class TestNarrowing:
    async def test_where_narrows_the_view_on_both_sides(self, stream):
        """Published rows by `filters=`, broker rows by the subscription."""
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(
                broker, where={"price": [101.0, 103.0, 106.0]}, rebase_every=3600
            ) as live:
                await live.wait_for(4)
                await stream.send_many([row(5), row(6)])  # 105, 106
                await live.wait_for(7)

                assert await offsets(live) == [2, 4, 7]
                # Narrowed on the wire too: only the matching tail is held.
                assert held(live) == 2

    async def test_where_refuses_what_the_two_sides_read_differently(self, stream):
        async with served(stream) as (_server, broker):
            with pytest.raises(ValueError, match="non-null scalars"):
                await streamcast.Stream.live(broker, where={"price": None})

            with pytest.raises(ValueError, match="does not declare"):
                await streamcast.Stream.live(broker, where={"venue": "x"})

    async def test_where_refuses_a_binary_column(self, tmp_path):
        schema = {
            "type": "object",
            "properties": {
                **SCHEMA["properties"],
                "trace": {"type": ["string", "null"], "contentEncoding": "base16"},
            },
            "required": SCHEMA["required"],
        }
        binary = streamcast.Stream.new("trades", root=tmp_path, schema=schema)
        try:
            async with served(binary) as (_server, broker):
                with pytest.raises(ValueError, match="binary column"):
                    await streamcast.Stream.live(broker, where={"trace": "00ff"})
        finally:
            await binary.aclose()


class TestStartingPoint:
    async def test_from_an_offset(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(broker, start_offset=3) as live:
                await live.wait_for(5)
                assert await offsets(live) == [3, 4, 5]
                assert await offsets_where(live, start_offset=1) == [3, 4, 5]

    async def test_latest_is_from_now_and_stays_so_after_a_rebase(self, stream):
        async with served(stream) as (_server, broker):
            async with await streamcast.Stream.live(
                broker, start_offset=streamcast.LATEST, rebase_every=3600
            ) as live:
                assert await offsets(live) == []

                await stream.send(row(5))
                await live.wait_for(6)
                assert await offsets(live) == [6]

                # Published now, so read from the tables — still from 6 on.
                publish(stream)
                await live.rebase()
                assert held(live) == 0
                assert await offsets(live) == [6]


async def offsets_where(live: _live.Live, **kwargs: Any) -> list[int]:
    table = await live.scan(columns=[_log.COLUMN], **kwargs)
    return table.column(_log.COLUMN).to_pylist()


class TestAMigrationUnderAnOpenView:
    async def test_it_reads_across_the_seam_with_the_new_columns(self, tmp_path):
        """Stop, migrate, restart on the same address; the view carries on.

        One listening socket, bound for the whole test, with a `dup()` handed
        to each server: the address never goes away, so a reconnect waits in
        the socket's backlog for the new server instead of racing for a port.
        """
        v2 = {
            "type": "object",
            "properties": {
                **SCHEMA["properties"],
                "venue": {"type": ["string", "null"]},
            },
            "required": SCHEMA["required"],
        }
        listening = socket.socket()
        listening.bind(("127.0.0.1", 0))
        listening.listen()
        port = listening.getsockname()[1]
        broker = f"ws://127.0.0.1:{port}/trades"
        try:
            old = streamcast.Stream.new("trades", root=tmp_path, schema=SCHEMA)
            await old.send_many([row(i) for i in range(3)])
            first = await streamcast.serve(old, sock=listening.dup(), maintain=False)
            async with await streamcast.Stream.live(broker, rebase_every=3600) as live:
                await live.wait_for(3)

                first.close()
                await first.wait_closed()
                await old.aclose()

                new = streamcast.Stream.migrate("trades", root=tmp_path, schema=v2)
                await new.send_many([row(i) | {"venue": "x"} for i in range(3, 5)])
                second = await streamcast.serve(
                    new, sock=listening.dup(), maintain=False
                )
                try:
                    await asyncio.wait_for(live.wait_for(5), timeout=10)
                    table = await live.scan()
                finally:
                    second.close()
                    await second.wait_closed()
                    await new.aclose()

            assert table.column(_log.COLUMN).to_pylist() == [1, 2, 3, 4, 5]
            assert table.column("venue").to_pylist() == [None, None, None, "x", "x"]
        finally:
            listening.close()
