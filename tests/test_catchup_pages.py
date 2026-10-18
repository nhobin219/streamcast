"""What a catch-up reads from the published tables, and how much of it at once.

On a local `file://` table, so these run without an endpoint. A gap read in
one query held the whole gap in memory before its first row: a 200,000-row
gap took a reader from 220 to 679 MiB. `Snapshot.rows` reads `PAGE` offsets
to a query instead, and a subscriber the server accepts reads no table at all.
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
    stream = streamcast.Stream.new(
        "t",
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


class TestTheGapIsReadInPages:
    async def test_no_query_spans_more_than_a_page(
        self, serve, published, scans, monkeypatch
    ):
        monkeypatch.setattr(_snapshot, "PAGE", 32)
        async with (
            serve(published, maintain=False) as uri,
            streamcast.connect(uri, offset=1, catch_up=True) as sub,
        ):
            got = [await sub.recv() for _ in range(ROWS)]

        assert [offset for offset, _ts, _row in got] == list(range(1, ROWS + 1))
        assert [row["i"] for _offset, _ts, row in got] == list(range(ROWS))
        assert len(scans) > 1
        assert all(end - start <= 32 for start, end in scans), scans
        # Contiguous: each page starts where the last one ended.
        assert all(a[1] == b[0] for a, b in zip(scans, scans[1:], strict=False))

    async def test_a_snapshot_reads_its_rows_in_pages_too(
        self, published, scans, monkeypatch
    ):
        monkeypatch.setattr(_snapshot, "PAGE", 100)
        uri = published.metadata_uri
        async with await streamcast.Stream.snapshot(uri) as snap:
            offsets = [offset async for offset, _ts, _row in snap.rows(5, 260)]

        assert offsets == list(range(5, 260))
        assert scans == [(5, 105), (105, 205), (205, 260)]


class _Arrowed(_published.Table):
    """A published table backed by an Arrow table rather than Iceberg files."""

    __slots__ = ()

    def relation(self) -> str:
        return "fenced"


def fenced(offsets: list[int]) -> _published.Table:
    stamp = _log.with_system(streamcast.to_arrow(SCHEMA)).field(_log.STAMP).type
    data = pa.table(
        {
            _log.COLUMN: pa.array(offsets, pa.int64()),
            _log.STAMP: pa.array([0] * len(offsets), stamp),
            "i": pa.array(range(len(offsets)), pa.int64()),
        }
    )
    connection = duckdb.connect()
    # A table, not a registered view: a view is the connection's own, and
    # every read here is on a cursor of its own.
    connection.execute("CREATE TABLE fenced AS SELECT * FROM data")
    return _Arrowed(
        "fenced",
        connection,
        "unused",
        data.schema,
        (offsets[0], offsets[-1] + 1),
        record_count=len(offsets),
    )


class TestAFence:
    async def test_an_empty_page_steps_to_the_next_row(self, monkeypatch, scans):
        """A restore fences 2**40 offsets without a WAL replica. Walked a page at
        a time that is 2**24 empty queries; found, it is one."""
        monkeypatch.setattr(_snapshot, "PAGE", 4)
        below = list(range(1, 7))
        above = list(range(2**40 + 7, 2**40 + 13))
        table = fenced(below + above)
        try:
            got = [
                offset
                async for offset, _ts, _row in _snapshot._paged(table, 1, 2**41)  # noqa: SLF001
            ]
        finally:
            table._connection.close()  # noqa: SLF001

        assert got == below + above
        # Two pages below, one empty one at the fence, two above.
        assert len(scans) == 5, scans
