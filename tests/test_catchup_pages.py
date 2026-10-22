"""What a catch-up reads from the published tables, and how much of it at once.

On a local `file://` table, so these run without an endpoint. A gap read in
one query held the whole gap in memory before its first row: a 200,000-row
gap took a reader from 220 to 679 MiB. `Snapshot.rows` reads a page to a
query instead, at most `page_bytes` of Arrow and `page_rows` rows, and a
subscriber the server accepts reads no table at all. The byte budget's tests
read an Arrow table through DuckDB, so rows a megabyte wide cost no Parquet.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pyarrow as pa
import pytest

import streamcast
from streamcast import _log, _published, _snapshot

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}
ROWS = 300
MAX_REPLAY = 50


@pytest.fixture
async def published(tmp_path):
    """A stream whose every row is published, and that replays only `MAX_REPLAY`."""
    stream = streamcast.Stream.open(
        "t",
        create=True,
        root=tmp_path,
        schema=SCHEMA,
        published=(tmp_path / "published").as_uri(),
        max_replay=MAX_REPLAY,
    )
    try:
        await stream.send_many([{"i": i} for i in range(ROWS)])
        log = stream.log
        assert log is not None
        while log.seal(flush=True) is not None:
            pass

        log.publish(flush=True)
        stream.ensure_metadata()
        assert log.published_through() == ROWS, "the fixture must publish every row"
        yield stream
    finally:
        await stream.aclose()


@pytest.fixture
def scans(monkeypatch) -> list[tuple[int | None, int | None]]:
    """The `[start, end)` of every query a published table is read with."""
    seen: list[tuple[int | None, int | None]] = []
    scan = _published.Table.scan

    def recording(self, *, start_offset=None, end_offset=None, **kwargs):
        seen.append((start_offset, end_offset))
        return scan(self, start_offset=start_offset, end_offset=end_offset, **kwargs)

    monkeypatch.setattr(_published.Table, "scan", recording)
    return seen


class TestOnlyARefusalReadsTheTables:
    async def test_an_offset_the_server_accepts_reads_no_table(
        self, serve, published, monkeypatch
    ):
        """`catch_up=True` is a fallback, not a mode: within the window the
        server serves the rows and the published tables are never opened."""
        opened = []

        async def refuse(*args, **kwargs):
            opened.append(args)
            msg = "a catch-up opened a snapshot for an offset the server accepts"
            raise AssertionError(msg)

        monkeypatch.setattr(_snapshot, "snapshot", refuse)
        start = ROWS - MAX_REPLAY + 1
        async with (
            serve(published, maintain=False) as uri,
            streamcast.connect(uri, offset=start, catch_up=True) as sub,
        ):
            got = [(await sub.recv())[0] for _ in range(ROWS - start + 1)]

        assert got == list(range(start, ROWS + 1))
        assert opened == []

    async def test_an_offset_it_refuses_does(self, serve, published, scans):
        """The control: the same subscriber one row further back reads the gap."""
        start = ROWS - MAX_REPLAY - 1
        async with (
            serve(published, maintain=False) as uri,
            streamcast.connect(uri, offset=start, catch_up=True) as sub,
        ):
            got = [(await sub.recv())[0] for _ in range(ROWS - start + 1)]

        assert got == list(range(start, ROWS + 1))
        assert scans, "the gap came from the published table"


@pytest.fixture
def pages(monkeypatch) -> list[tuple[int, int]]:
    """`(rows, Arrow bytes)` of every page `_paged` reads, as it measured them."""
    seen: list[tuple[int, int]] = []
    close = _snapshot._Measured.close  # noqa: SLF001

    def recording(self):
        seen.append((self.rows, self.nbytes))
        close(self)

    monkeypatch.setattr(_snapshot._Measured, "close", recording)  # noqa: SLF001
    return seen


class TestEveryCallThatPagesTakesTheBudgets:
    async def test_connect(self, serve, published, scans):
        async with (
            serve(published, maintain=False) as uri,
            streamcast.connect(
                uri, offset=1, catch_up=True, page_bytes=None, page_rows=32
            ) as sub,
        ):
            got = [await sub.recv() for _ in range(ROWS)]

        assert [offset for offset, _ts, _row in got] == list(range(1, ROWS + 1))
        assert [row["i"] for _offset, _ts, row in got] == list(range(ROWS))
        assert len(scans) > 1
        assert all(end - start <= 32 for start, end in scans), scans
        # Contiguous: each page starts where the last one ended.
        assert all(a[1] == b[0] for a, b in zip(scans, scans[1:], strict=False))

    async def test_snapshot_rows(self, published, scans):
        uri = published.metadata_uri
        async with await streamcast.Stream.snapshot(uri) as snap:
            rows = snap.rows(5, 260, page_bytes=None, page_rows=100)
            offsets = [offset async for offset, _ts, _row in rows]

        assert offsets == list(range(5, 260))
        assert scans == [(5, 105), (105, 205), (205, 260)]

    async def test_live_passes_them_to_its_catch_up(
        self, serve, published, monkeypatch
    ):
        """A live view catches up when it reconnects, through `connect`."""
        from streamcast import _client  # noqa: PLC0415 — patched where `_live` finds it

        asked = []
        connect = _client.connect

        def recording(*args, **kwargs):
            if kwargs.get("catch_up"):
                asked.append((kwargs["page_bytes"], kwargs["page_rows"]))

            return connect(*args, **kwargs)

        monkeypatch.setattr(_client, "connect", recording)
        async with (
            serve(published, maintain=False) as uri,
            await streamcast.Stream.live(uri, page_bytes=12_345, page_rows=67),
        ):
            pass

        assert asked == [(12_345, 67)]

    @pytest.mark.parametrize(
        "budget", [{"page_bytes": 0}, {"page_rows": -1}, {"page_rows": True}]
    )
    async def test_a_budget_below_one_is_refused_at_the_call(self, budget):
        with pytest.raises(ValueError, match="at least 1"):
            streamcast.connect("ws://127.0.0.1:1/t", catch_up=True, **budget)

    async def test_and_at_the_first_row_of_snapshot_rows(self, published):
        uri = published.metadata_uri
        async with await streamcast.Stream.snapshot(uri) as snap:
            with pytest.raises(ValueError, match="page_rows=0"):
                await anext(snap.rows(1, page_rows=0))


NARROW = 300
WIDE = 1_000_000


class TestTheByteBudget:
    """Through `_paged` on an Arrow-backed table, as `Snapshot.rows` reads a log."""

    async def test_narrow_rows_take_a_probe_then_one_page(self, scans, pages):
        """The first page is one offset; the rest of a narrow range fits the
        default budget, so it is one more query, as one was before paging."""
        got = await read(arrowed(NARROW), page_bytes=streamcast.PAGE_BYTES)

        assert got == list(range(1, NARROW + 1))
        assert scans == [(1, 2), (2, NARROW + 1)]

    async def test_wide_rows_stay_within_it(self, pages):
        budget = 4 * 1024 * 1024
        got = await read(arrowed(12, width=WIDE), page_bytes=budget)

        assert got == list(range(1, 13))
        assert len(pages) > 3
        assert all(nbytes <= budget for _rows, nbytes in pages), pages

    async def test_a_row_wider_than_the_budget_is_read_alone(self, pages):
        got = await read(arrowed(5, width=WIDE), page_bytes=1_000)

        assert got == list(range(1, 6))
        assert [rows for rows, _nbytes in pages] == [1] * 5

    async def test_the_row_budget_caps_a_page_too(self, scans):
        got = await read(
            arrowed(NARROW), page_bytes=streamcast.PAGE_BYTES, page_rows=50
        )

        assert got == list(range(1, NARROW + 1))
        assert all(end - start <= 50 for start, end in scans), scans
        assert len(scans) == 1 + -(-(NARROW - 1) // 50)

    async def test_with_neither_a_log_is_one_query(self, scans):
        got = await read(arrowed(NARROW), page_bytes=None, page_rows=None)

        assert got == list(range(1, NARROW + 1))
        assert scans == [(1, NARROW + 1)]

    async def test_rows_that_widen_come_back_within_it(self, pages):
        """The documented excess: the page that meets the wider rows was sized
        for narrow ones, and the one after by a mixed average. Every row still
        arrives once, in order, and the pages after the first read from the
        wide rows alone are within the budget again."""
        budget = 4 * 1024 * 1024
        widths = [10] * 40 + [WIDE] * 30
        table = arrowed(len(widths), widths=widths)
        got = await read(table, page_bytes=budget, page_rows=20)

        assert got == list(range(1, len(widths) + 1))
        all_wide = [
            n for n, (rows, nbytes) in enumerate(pages) if nbytes >= rows * WIDE
        ]
        assert all_wide, pages
        assert all(nbytes <= budget for _rows, nbytes in pages[all_wide[0] + 1 :])
        assert len(pages) > all_wide[0] + 1, "a page after it, to be within budget"


async def read(table: _published.Table, **budgets: Any) -> list[int]:
    try:
        return [
            offset
            async for offset, _ts, _row in _snapshot._paged(  # noqa: SLF001
                table, 1, 2**41, **{"page_bytes": None, "page_rows": None, **budgets}
            )
        ]
    finally:
        table._connection.close()  # noqa: SLF001


class _Arrowed(_published.Table):
    """A published table backed by an Arrow table rather than Iceberg files."""

    __slots__ = ()

    def relation(self) -> str:
        return "arrowed"


def arrowed(
    count: int,
    *,
    width: int = 10,
    widths: list[int] | None = None,
    offsets: list[int] | None = None,
) -> _published.Table:
    """`count` rows, each with a string `width` bytes long (or `widths[n]`)."""
    offsets = offsets or list(range(1, count + 1))
    widths = widths or [width] * count
    stamp = _log.with_system(streamcast.to_arrow(SCHEMA)).field(_log.STAMP).type
    data = pa.table(
        {
            _log.COLUMN: pa.array(offsets, pa.int64()),
            _log.STAMP: pa.array([0] * count, stamp),
            "i": pa.array(range(count), pa.int64()),
            "pad": pa.array(["x" * w for w in widths], pa.string()),
        }
    )
    connection = duckdb.connect()
    # A table, not a registered view: a view is the connection's own, and
    # every read here is on a cursor of its own.
    connection.execute("CREATE TABLE arrowed AS SELECT * FROM data")
    return _Arrowed(
        "arrowed",
        connection,
        "unused",
        data.schema,
        (offsets[0], offsets[-1] + 1),
        record_count=count,
    )


class TestAFence:
    async def test_an_empty_page_steps_to_the_next_row(self, scans):
        """A restore fences 2**40 offsets without a WAL replica. Walked a page at
        a time that is 2**24 empty queries; found, it is one."""
        below = list(range(1, 7))
        above = list(range(2**40 + 7, 2**40 + 13))
        got = await read(arrowed(12, offsets=below + above), page_rows=4)

        assert got == below + above
        # Two pages below, one empty one at the fence, two above.
        assert len(scans) == 5, scans
