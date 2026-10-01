"""`Stream.live`: the published tables plus the broker's rows, kept current.

Served directly rather than through the `serve` fixture where a test needs the
server itself — to drop its connections and watch the view come back.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

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
    while log.seal() is not None:
        pass

    log.publish(push_unsettled=True)


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
