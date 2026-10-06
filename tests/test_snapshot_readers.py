"""Reading a snapshot as it streams: `rows`, and the readers `sql` and `scan` return.

A streaming DuckDB result on a shared connection ends — silently, exactly as
at its true end — when anything else runs a query on that connection. So
every read that is pulled across awaits holds a cursor of its own, and these
tests interleave reads on one snapshot to prove it. `_published.BATCH` is made
small so a few hundred rows span several batches.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _published

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "side": {"type": "integer"}},
    "required": ["i", "side"],
}
ROWS = 600


@pytest.fixture
def small_batches(monkeypatch):
    monkeypatch.setattr(_published, "BATCH", 50)


async def published(root, count: int = ROWS) -> str:
    """A stream of `count` rows, every one published, and its metadata URI."""
    stream = streamcast.Stream.new(
        "t", root=root, schema=SCHEMA, published=(root / "published").as_uri()
    )
    try:
        await stream.send_many([{"i": i, "side": i % 2} for i in range(count)])
        log: litelink.WriteHandle | None = stream.log
        assert log is not None
        while log.seal(flush=True) is not None:
            pass

        log.publish(flush=True)
        stream.ensure_metadata()
        uri = stream.metadata_uri
        assert uri is not None
        return uri
    finally:
        await stream.aclose()


class TestRows:
    async def test_a_query_while_rows_are_read_does_not_end_them(
        self, tmp_path, small_batches
    ):
        """On the shared connection this stopped at the first batch boundary,
        50 rows of 600, and reported success."""
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            got = []
            async for offset, _ts, _row in snap.rows(1):
                got.append(offset)
                if len(got) == 10:
                    counted = await snap.sql("SELECT count(*) AS n FROM log").read_all()
                    assert counted["n"][0].as_py() == ROWS

        assert got == list(range(1, ROWS + 1))


def offsets(batches) -> list[int]:
    return [o for batch in batches for o in batch.column("litelink_offset").to_pylist()]


class TestAReader:
    async def test_a_result_larger_than_a_batch_arrives_whole_and_in_order(
        self, tmp_path
    ):
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            batches = [batch async for batch in snap.scan(batch_size=50)]

        assert len(batches) == ROWS // 50
        assert offsets(batches) == list(range(1, ROWS + 1))

    async def test_read_all_is_the_batches_put_together(self, tmp_path):
        uri = await published(tmp_path)
        query = "SELECT side, count(*) AS n FROM log GROUP BY side ORDER BY side"
        async with await streamcast.Stream.snapshot(uri) as snap:
            whole = await snap.sql(query).read_all()
            batches = [batch async for batch in snap.sql(query, batch_size=1)]

        assert whole.to_pylist() == [{"side": 0, "n": 300}, {"side": 1, "n": 300}]
        assert [row for b in batches for row in b.to_pylist()] == whole.to_pylist()

    async def test_two_readers_on_one_snapshot_both_finish_whole(self, tmp_path):
        """Read alternately, a batch each. On one shared connection the second
        query ended the first at its first batch, and `log`, a view on that
        connection, was the second's."""
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            evens = snap.sql(
                "SELECT litelink_offset FROM log WHERE side = 0 ORDER BY 1",
                batch_size=50,
            )
            odds = snap.sql(
                "SELECT litelink_offset FROM log WHERE side = 1 ORDER BY 1",
                batch_size=50,
            )
            got: dict[str, list] = {"evens": [], "odds": []}
            async with evens, odds:
                running = {"evens": aiter(evens), "odds": aiter(odds)}
                while running:
                    for name, reader in list(running.items()):
                        batch = await anext(reader, None)
                        if batch is None:
                            del running[name]
                        else:
                            got[name].append(batch)

        assert offsets(got["evens"]) == list(range(1, ROWS + 1, 2))
        assert offsets(got["odds"]) == list(range(2, ROWS + 1, 2))

    async def test_its_schema_is_there_once_the_query_has_run(self, tmp_path):
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            reader = snap.scan(columns=["litelink_offset", "i"])
            with pytest.raises(RuntimeError, match="not run yet"):
                _ = reader.schema

            async with reader:
                assert reader.schema.names == ["litelink_offset", "i"]

    async def test_the_event_loop_runs_while_it_reads(self, tmp_path):
        uri = await published(tmp_path)
        ticks = 0

        async def tick() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.005)
                ticks += 1

        async with await streamcast.Stream.snapshot(uri) as snap:
            ticker = asyncio.create_task(tick())
            started = time.perf_counter()
            # A cross join, to give DuckDB a few hundred milliseconds of work.
            await snap.sql("SELECT sum(i * r) FROM log, range(200000) t(r)").read_all()
            took = time.perf_counter() - started
            ticker.cancel()

        assert took > 0.05, "too quick to tell"
        assert ticks >= took / 0.005 / 4, f"{ticks} ticks in {took:.2f}s"


class TestItsLifetime:
    async def test_leaving_it_early_releases_its_cursor(self, tmp_path):
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            async with snap.scan(batch_size=50) as reader:
                await anext(reader)

            assert snap._readers == 0  # noqa: SLF001

        assert snap._shut  # noqa: SLF001

    async def test_a_batch_after_the_snapshot_closed_raises(self, tmp_path):
        """Never an early, quiet end: that is a short answer."""
        uri = await published(tmp_path)
        snap = await streamcast.Stream.snapshot(uri)
        reader = snap.scan(batch_size=50)
        await anext(reader)
        await snap.close()
        with pytest.raises(RuntimeError, match="closed"):
            await anext(reader)

        assert snap._shut, "and the connection is closed once it lets go"  # noqa: SLF001

    async def test_one_made_after_the_snapshot_closed_raises(self, tmp_path):
        uri = await published(tmp_path)
        async with await streamcast.Stream.snapshot(uri) as snap:
            pass

        with pytest.raises(RuntimeError, match="closed"):
            snap.sql("SELECT 1")

    async def test_rows_after_the_snapshot_closed_raises(self, tmp_path, small_batches):
        uri = await published(tmp_path)
        snap = await streamcast.Stream.snapshot(uri)
        rows = snap.rows(1)
        await anext(rows)
        await snap.close()
        with pytest.raises(RuntimeError, match="closed"):
            await anext(rows)  # the very next row, not the rest of the log first


class TestTheOneShotForms:
    """`Stream.sql` and `Stream.scan`: a reader with a snapshot of its own."""

    async def test_it_streams_and_closes_its_snapshot_once_done(self, tmp_path):
        uri = await published(tmp_path)
        reader = streamcast.Stream.scan(uri, columns=["litelink_offset"], batch_size=50)
        batches = [batch async for batch in reader]

        assert offsets(batches) == list(range(1, ROWS + 1))
        assert reader._snapshot is not None  # noqa: SLF001
        assert reader._snapshot._shut  # noqa: SLF001

    async def test_read_all_is_the_whole_answer(self, tmp_path):
        uri = await published(tmp_path)
        table = await streamcast.Stream.sql(
            uri, "SELECT count(*) AS n FROM log WHERE side = 1"
        ).read_all()
        assert table.to_pylist() == [{"n": ROWS // 2}]

    async def test_a_refusal_is_raised_at_the_first_read(self, tmp_path):
        uri = await published(tmp_path)
        reader = streamcast.Stream.scan(uri, as_of_offset=ROWS + 10)
        with pytest.raises(streamcast.SnapshotUnavailable, match="broker="):
            await reader.read_all()

    async def test_one_closed_unread_opens_nothing(self, tmp_path, monkeypatch):
        uri = await published(tmp_path)
        opened = []
        real = streamcast.Stream.snapshot

        async def recording(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            opened.append(args)
            return await real(*args, **kwargs)

        monkeypatch.setattr(streamcast.Stream, "snapshot", recording)
        reader = streamcast.Stream.sql(uri, "SELECT 1")
        await reader.aclose()
        assert opened == []
