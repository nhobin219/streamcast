"""Reading a snapshot as it streams.

A streaming DuckDB result on a shared connection ends — silently, exactly as
at its true end — when anything else runs a query on that connection. So
every read that is pulled across awaits holds a cursor of its own, and these
tests interleave reads on one snapshot to prove it. `_published.BATCH` is made
small so a few hundred rows span several batches.
"""

from __future__ import annotations

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
                    counted = await snap.sql("SELECT count(*) AS n FROM log")
                    assert counted["n"][0].as_py() == ROWS

        assert got == list(range(1, ROWS + 1))
