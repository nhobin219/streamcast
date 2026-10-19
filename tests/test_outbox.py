"""The outbox: rows enqueued in the caller's transaction, shipped exactly once.

Each test runs on SQLite and Postgres (`opener`) against a real broker on
loopback, serving an `exactly_once=True` stream.
"""

from __future__ import annotations

import asyncio
import socket
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _outbox

from .conftest import SCHEMA, trade


@pytest.fixture
def log(tmp_path):
    handle = litelink.new(tmp_path / "data", "trades", schema=SCHEMA)
    with handle:
        yield handle


def landed(log) -> list[float]:
    """The prices in the log, in offset order: each trade's `i`, recoverable."""
    table = log.sql('SELECT price FROM log ORDER BY "litelink_offset"').read_all()
    return [row["price"] - 85_565.0 for row in table.to_pylist()]


async def outstanding(store) -> list[int]:
    async with store.transaction() as tx:
        rows = await tx.fetch("SELECT streamcast_seq FROM streamcast_outbox ORDER BY 1")

    return [int(seq) for (seq,) in rows]


FAST: dict[str, Any] = {"poll_every": 0.05, "backoff": (0.01,)}


class TestEnqueueing:
    async def test_rows_ship_in_seq_order_after_commit_and_leave_the_outbox(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.outbox(
                uri, publisher="p", store=store, **FAST
            ) as out:
                async with store.transaction() as tx:
                    assert await out.send(trade(0), tx=tx) == 1
                    assert await out.send_many([trade(1), trade(2)], tx=tx) == 2
                    await asyncio.sleep(0.2)
                    assert log.end_offset() == 1, "nothing is sent before COMMIT"

                assert await out.landed(3) == 3
                await out.flush(timeout=10)
                assert await out.pending() == 0

        assert landed(log) == [0.0, 1.0, 2.0]
        await store.close()

    async def test_a_rollback_returns_its_seqs(self, serve, log, opener):
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.outbox(
                uri, publisher="p", store=store, **FAST
            ) as out:
                with pytest.raises(RuntimeError):
                    async with store.transaction() as tx:
                        assert await out.send(trade(9), tx=tx) == 1
                        raise RuntimeError

                async with store.transaction() as tx:
                    assert await out.send(trade(0), tx=tx) == 1, "no hole"

                await out.flush(timeout=10)

        assert landed(log) == [0.0]
        await store.close()

    async def test_two_enqueuers_on_one_publisher_commit_in_seq_order(
        self, serve, log, opener
    ):
        """Two connections to one database. The second's enqueue waits on the
        first's lock until it commits, so it takes the next seq, never the same.

        Falsify by reading `next_seq` before the UPDATE in `enqueue`.
        """
        first, second = await opener(), await opener()
        async with first.transaction() as tx:  # the publisher's row exists already
            await _outbox.enqueue(tx, "trades", "p", [trade(9)])

        holding = asyncio.Event()

        async def a() -> int:
            async with first.transaction() as tx:
                seq = await _outbox.enqueue(tx, "trades", "p", [trade(0)])
                holding.set()
                await asyncio.sleep(0.2)
                return seq

        async def b() -> int:
            await holding.wait()
            async with second.transaction() as tx:
                return await _outbox.enqueue(tx, "trades", "p", [trade(1)])

        assert await asyncio.gather(a(), b()) == [2, 3]
        assert await outstanding(first) == [1, 2, 3]
        await first.close()
        await second.close()


class TestRecovery:
    async def test_rows_the_broker_holds_are_deleted_unsent_after_a_crash(
        self, serve, log, opener, monkeypatch
    ):
        """A sender that died after the broker committed and before it deleted
        the rows: the next one's greeting says they landed, and they are not
        sent again."""
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with store.transaction() as tx:
                await _outbox.enqueue(tx, "trades", "p", [trade(0), trade(1), trade(2)])

            # What the dead sender did: shipped rows 1 and 2, deleted nothing.
            async with streamcast.publish(uri, publisher="p") as direct:
                await direct.send_many([trade(0), trade(1)], seq=1)

            sent: list[int | None] = []
            original = streamcast.Publication.submit_many

            async def recording(self, rows, *, single=False, seq=None):
                sent.append(seq)
                return await original(self, rows, single=single, seq=seq)

            monkeypatch.setattr(streamcast.Publication, "submit_many", recording)
            async with streamcast.outbox(
                uri, publisher="p", store=store, **FAST
            ) as out:
                await out.flush(timeout=10)

        assert sent == [3], "the landed rows were not sent again"
        assert landed(log) == [0.0, 1.0, 2.0], "each row once"
        await store.close()

    async def test_a_refused_row_is_dropped_and_the_rest_renumbered(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.outbox(
                uri, publisher="p", store=store, **FAST
            ) as out:
                async with store.transaction() as tx:
                    await out.send_many(
                        [trade(0), {"event_ts": "not an int"}, trade(2), trade(3)],
                        tx=tx,
                    )

                await out.flush(timeout=10)

            async with streamcast.publish(uri, publisher="p") as direct:
                assert direct.info.publisher is not None
                assert direct.info.publisher.last_seq == 3, "seqs 1..3, gapless"

        assert landed(log) == [0.0, 2.0, 3.0]
        await store.close()

    async def test_a_broker_that_lost_acknowledged_rows_stops_the_sender(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.outbox(
                uri, publisher="p", store=store, **FAST
            ) as out:
                async with store.transaction() as tx:
                    await out.send(trade(0), tx=tx)

                await out.flush(timeout=10)

            # As if the broker's buffer were lost: its mark goes back to 0.
            async with store.transaction() as tx:
                await _outbox.enqueue(tx, "trades", "p", [trade(1)])

            writer = stream._writer  # noqa: SLF001
            assert writer is not None
            await asyncio.to_thread(writer.close)
            with log.transaction() as ltx:
                ltx.execute("DELETE FROM streamcast_publishers")

            reborn = streamcast.Stream("trades", log=log, exactly_once=True)
            async with serve(reborn, maintain=False) as uri2:
                with pytest.raises(streamcast.AcknowledgedRowsLost):
                    async with streamcast.outbox(
                        uri2, publisher="p", store=store, **FAST
                    ) as out:
                        await out.flush(timeout=10)

        await store.close()

    async def test_a_database_behind_the_broker_stops_the_sender(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri, publisher="p") as direct:
                await direct.send_many([trade(0), trade(1), trade(2)], seq=1)

            with pytest.raises(streamcast.StoreBehindBroker):
                async with streamcast.outbox(
                    uri, publisher="p", store=store, **FAST
                ) as out:
                    async with store.transaction() as tx:
                        await out.send(trade(3), tx=tx)  # seq 1: the broker is at 3

                    await out.flush(timeout=10)

        await store.close()

    async def test_a_recreated_stream_is_refused(self, serve, log, opener):
        store = await opener()
        async with store.transaction() as tx:
            await _outbox.enqueue(tx, "trades", "p", [trade(0)])
            await tx.execute(
                "UPDATE streamcast_publisher_state SET streamcast_stream_id = 'old'"
            )

        stream = streamcast.Stream("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri, publisher="probe") as direct:
                assert direct.info.stream_id, "the refusal must be about a real id"

            with pytest.raises(streamcast.StreamMismatch, match="'old'"):
                async with streamcast.outbox(
                    uri, publisher="p", store=store, **FAST
                ) as out:
                    await out.flush(timeout=10)

        assert log.end_offset() == 1, "nothing was sent"
        await store.close()


class TestAnOutage:
    async def test_it_is_said_once_and_the_rows_go_when_the_broker_returns(
        self, log, opener
    ):
        store = await opener()
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        uri = f"ws://127.0.0.1:{port}/trades"
        said: list[str] = []
        async with streamcast.outbox(
            uri, publisher="p", store=store, notice=said.append, **FAST
        ) as out:
            async with store.transaction() as tx:
                await out.send(trade(0), tx=tx)

            await asyncio.sleep(0.3)  # several attempts, all refused
            assert len(said) == 1, said
            stream = streamcast.Stream("trades", log=log, exactly_once=True)
            server = await streamcast.serve([stream], "127.0.0.1", port, maintain=False)
            try:
                await out.flush(timeout=10)
            finally:
                server.close()
                await server.wait_closed()

        assert len(said) == 2 and "again" in said[1], said
        assert landed(log) == [0.0]
        await store.close()
