"""The inbox: rows received into the caller's database, processed exactly once.

Each test runs on SQLite and Postgres (`opener`) against a real broker on
loopback. The receive-window tests use small bounds so a few rows fill them.
"""

from __future__ import annotations

import asyncio
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _inbox

from .conftest import SCHEMA, trade


@pytest.fixture
def log(tmp_path):
    handle = litelink.new(tmp_path / "data", "trades", schema=SCHEMA)
    with handle:
        yield handle


FAST: dict[str, Any] = {"poll_every": 0.05, "backoff": (0.01,)}


async def held(store) -> list[int]:
    """The offsets in the inbox now."""
    async with store.transaction() as tx:
        rows = await tx.fetch("SELECT litelink_offset FROM streamcast_inbox ORDER BY 1")

    return [int(offset) for (offset,) in rows]


async def received(store) -> int:
    async with store.transaction() as tx:
        [(last,)] = await tx.fetch(
            "SELECT streamcast_last_received_offset FROM streamcast_subscriber_state"
        )

    return int(last)


async def until(condition, timeout: float = 10.0) -> None:
    async with asyncio.timeout(timeout):
        while not await condition():
            await asyncio.sleep(0.02)


class TestReceivingAndProcessing:
    async def test_each_row_comes_once_with_its_ts_and_a_transaction(
        self, serve, tmp_path, opener
    ):
        from streamcast import _log

        stamped = litelink.new(
            tmp_path / "s", "trades", schema=_log.with_system(SCHEMA)
        )
        store = await opener()
        with stamped:
            stream = streamcast.Stream("trades", log=stamped)
            async with serve(stream, maintain=False) as uri:
                async with streamcast.inbox(
                    uri, subscriber="s", store=store, **FAST
                ) as sub:
                    await stream.send_many([trade(i) for i in range(3)])
                    got = []
                    async for offset, ts, row, tx in sub:
                        assert tx.store is store
                        assert isinstance(ts, int)
                        got.append((offset, row["price"]))
                        if offset == 3:
                            break

        assert got == [
            (1, trade(0)["price"]),
            (2, trade(1)["price"]),
            (3, trade(2)["price"]),
        ]
        assert await held(store) == [], "the clean exit committed the last row's delete"
        assert await received(store) == 3
        await store.close()

    async def test_a_raise_rolls_back_and_the_row_is_delivered_again(
        self, serve, log, opener
    ):
        """Crash and redelivery: the row's delete commits only with its work."""
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            with pytest.raises(RuntimeError, match="crash"):
                async with streamcast.inbox(
                    uri, subscriber="s", store=store, **FAST
                ) as sub:
                    await stream.send(trade(0))
                    async for _offset, _ts, _row, tx in sub:
                        await tx.execute("CREATE TABLE IF NOT EXISTS work (n INTEGER)")
                        await tx.execute("INSERT INTO work VALUES (1)")
                        raise RuntimeError("crash")

            assert await held(store) == [1]
            async with store.transaction() as tx:
                assert await tx.fetch("SELECT count(*) FROM streamcast_inbox") == [(1,)]
                tables = await tx.fetch(
                    "SELECT count(*) FROM streamcast_schema"  # the work table rolled back too
                )
                assert tables == [(1,)]

            async with streamcast.inbox(
                uri, subscriber="s", store=store, **FAST
            ) as sub:
                offset, _ts, row, tx = await sub.recv()
                assert (offset, row["price"]) == (1, trade(0)["price"])

        assert await held(store) == []
        await store.close()

    async def test_resume_is_from_the_received_position_and_unprocessed_rows_come_first(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, **FAST
            ) as sub:
                await stream.send_many([trade(i) for i in range(3)])
                await until(lambda: _is(received(store), 3))
                offset, *_ = await sub.recv()  # 1 processed; 2 and 3 only received
                assert offset == 1

            await stream.send_many([trade(i) for i in range(3, 5)])  # 4, 5 while down
            offsets = []
            async with streamcast.inbox(
                uri, subscriber="s", store=store, **FAST
            ) as sub:
                for _ in range(4):
                    offset, *_ = await sub.recv()
                    offsets.append(offset)

        assert offsets == [2, 3, 4, 5], "nothing skipped, nothing repeated"
        await store.close()

    async def test_a_new_subscriber_starts_at_the_latest_or_where_it_is_told(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([trade(i) for i in range(3)])
            async with streamcast.inbox(
                uri, subscriber="late", store=store, **FAST
            ) as sub:
                await stream.send(trade(3))
                offset, *_ = await sub.recv()
                assert offset == 4, "the latest: nothing from before it opened"

            async with streamcast.inbox(
                uri, subscriber="placed", store=store, offset=2, **FAST
            ) as sub:
                offset, *_ = await sub.recv()
                assert offset == 2

            with pytest.raises(ValueError, match="saved position"):
                async with streamcast.inbox(
                    uri, subscriber="placed", store=store, offset=1, **FAST
                ):
                    pass

        await store.close()

    async def test_a_recreated_stream_is_refused_loudly(self, serve, log, opener):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(uri, subscriber="s", store=store, **FAST):
                pass

            async with store.transaction() as tx:
                await tx.execute(
                    "UPDATE streamcast_subscriber_state SET streamcast_stream_id = 'old'"
                )

            with pytest.raises(streamcast.StreamMismatch, match="'old'"):
                async with streamcast.inbox(uri, subscriber="s", store=store, **FAST):
                    pass

        await store.close()

    async def test_an_empty_inbox_is_not_caught_up(self, serve, log, opener):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_rows=2, **FAST
            ) as sub:
                await stream.send_many([trade(i) for i in range(6)])
                await until(lambda: _now(sub.paused))
                await sub.recv_many(2)
                await sub.commit()
                assert await held(store) == []
                assert not sub.caught_up(), "empty, and four rows still on the broker"
                seen = 2
                while seen < 6:
                    rows, _tx = await sub.recv_many(6)
                    seen += len(rows)

                await sub.commit()
                await until(lambda: _now(sub.caught_up()))

        await store.close()


class TestTheReceiveWindow:
    async def test_a_slow_processor_holds_the_inbox_at_its_bound_and_loses_nothing(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        peak = 0
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_rows=5, **FAST
            ) as sub:
                await stream.send_many([trade(i) for i in range(40)])
                # The receiver runs ahead of a processor that has not started,
                # and stops at the bound.
                await until(lambda: _now(sub.paused))
                await asyncio.sleep(0.2)
                assert await held(store) == [1, 2, 3, 4, 5]
                offsets = []
                while len(offsets) < 40:
                    offset, *_ = await sub.recv()
                    offsets.append(offset)
                    await asyncio.sleep(0.005)  # slow
                    await sub.commit()
                    peak = max(peak, len(await held(store)))

        assert offsets == list(range(1, 41))
        assert peak <= 5, peak
        await store.close()

    async def test_a_paused_or_disconnected_subscriber_does_not_grow(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_rows=3, **FAST
            ) as sub:
                await stream.send_many([trade(i) for i in range(5)])
                await until(lambda: _now(sub.paused))
                assert await held(store) == [1, 2, 3]
                await until(lambda: _now(stream.subscribers == 0))
                await stream.send_many([trade(i) for i in range(5, 50)])
                await asyncio.sleep(0.3)
                assert await held(store) == [1, 2, 3], "nothing received while paused"
                assert await received(store) == 3

            await stream.send_many([trade(i) for i in range(50, 60)])
            assert await held(store) == [1, 2, 3], "nor while disconnected"
            assert await received(store) == 3

        await store.close()

    async def test_the_byte_bound_alone_stops_receiving(self, serve, log, opener):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        size = len(_inbox._ENCODE.encode(trade(0)))  # noqa: SLF001
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_bytes=3 * size, **FAST
            ) as sub:
                await stream.send_many([trade(i) for i in range(10)])
                await until(lambda: _now(sub.paused))
                assert len(await held(store)) == 3

        await store.close()

    async def test_receiving_resumes_only_below_the_low_water_mark(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri,
                subscriber="s",
                store=store,
                inbox_max_rows=4,
                inbox_resume_at=0.5,
                **FAST,
            ) as sub:
                await stream.send_many([trade(i) for i in range(20)])
                await until(lambda: _now(sub.paused))
                await sub.recv()
                await sub.commit()  # 3 held: not below 2
                await asyncio.sleep(0.3)
                assert sub.paused
                assert await held(store) == [2, 3, 4]
                await sub.recv_many(2)
                await sub.commit()  # 1 held: below 2
                await until(lambda: _now(not sub.paused))
                await until(lambda: _is_more(held(store), 1))

        await store.close()

    async def test_an_oversized_row_is_delivered_into_an_empty_inbox(
        self, serve, log, opener
    ):
        store = await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.inbox(
                uri, subscriber="s", store=store, inbox_max_bytes=10, **FAST
            ) as sub:
                await stream.send_many([trade(0), trade(1)])
                offset, *_ = await sub.recv()
                assert offset == 1
                await sub.commit()
                assert await held(store) in ([], [2]), (
                    "one at a time, each past the bound"
                )
                offset, *_ = await sub.recv()
                assert offset == 2

        await store.close()


class TestConsumeTransformPublish:
    async def test_one_transaction_holds_the_input_the_state_and_the_output(
        self, serve, tmp_path, opener
    ):
        """The input row's delete, the caller's state and the output row commit
        together. A crash before the commit leaves the input to redeliver and
        nothing enqueued; downstream sees each input's output exactly once."""
        store = await opener()
        source = litelink.new(tmp_path / "in", "orders", schema=SCHEMA)
        sink = litelink.new(tmp_path / "out", "enriched", schema=SCHEMA)
        with source, sink:
            inputs = streamcast.Stream("orders", log=source)
            outputs = streamcast.Stream("enriched", log=sink, exactly_once=True)
            async with serve(inputs, outputs, maintain=False) as uri:
                out_uri = uri.replace("/orders", "/enriched")
                async with store.transaction() as tx:
                    await tx.execute("CREATE TABLE totals (n INTEGER)")
                    await tx.execute("INSERT INTO totals VALUES (0)")

                crash: dict[str, int | None] = {"at": 2}

                async def run(start: int | None) -> None:
                    async with (
                        streamcast.inbox(
                            uri, subscriber="e", store=store, offset=start, **FAST
                        ) as sub,
                        streamcast.outbox(
                            out_uri, publisher="e", store=store, **FAST
                        ) as out,
                    ):
                        async for offset, _ts, row, tx in sub:
                            await tx.execute("UPDATE totals SET n = n + 1")
                            await out.send({**row, "tag": f"from {offset}"}, tx=tx)
                            if offset == crash["at"]:
                                crash["at"] = None
                                raise RuntimeError("crash mid-transaction")

                            if offset == 4:
                                await sub.commit()
                                await out.flush(timeout=10)
                                return

                await inputs.send_many([trade(i) for i in range(4)])
                with pytest.raises(RuntimeError, match="crash"):
                    await run(1)  # placed at 1: the rows were sent before it opened

                await run(None)  # resumed from its saved position

            table = sink.sql(
                'SELECT tag FROM log ORDER BY "litelink_offset"'
            ).read_all()
            assert [r["tag"] for r in table.to_pylist()] == [
                "from 1",
                "from 2",
                "from 3",
                "from 4",
            ]
            async with store.transaction() as tx:
                assert await tx.fetch("SELECT n FROM totals") == [(4,)]

        await store.close()


async def _is(value, expected) -> bool:
    return await value == expected


async def _is_more(value, than) -> bool:
    return len(await value) > than


async def _now(value) -> bool:
    return bool(value)


class TestTwoWorkersOnOneSubscriber:
    async def test_a_lagging_receiver_never_inserts_what_another_processed(
        self, serve, log, opener
    ):
        """Two workers, each with its own store and its own receiver, on one
        subscriber id. B's receiver is held back while A receives and
        processes every row; released, B must insert none of them again.

        Falsify by judging inserts against the receiver's memory rather than
        the state row read in `_insert`.
        """
        first, second = await opener(), await opener()
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with (
                streamcast.inbox(uri, subscriber="w", store=first, **FAST) as a,
                streamcast.inbox(uri, subscriber="w", store=second, **FAST) as b,
            ):
                # Hold B's store lock, so B's receiver cannot insert.
                hold = second._lock._lock  # noqa: SLF001
                await hold.acquire()
                try:
                    await stream.send_many([trade(i) for i in range(3)])
                    for expected in (1, 2, 3):
                        offset, *_ = await a.recv()
                        assert offset == expected

                    await a.commit()
                    assert await held(first) == []
                finally:
                    hold.release()

                await asyncio.sleep(0.5)  # B's receiver gets the store back
                assert await held(first) == [], "nothing processed comes back"
                assert b.last_received == 3

        await first.close()
        await second.close()
