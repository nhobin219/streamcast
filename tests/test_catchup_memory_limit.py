"""`connect(catch_up=True, memory_limit=)`: the catch-up's DuckDB is capped too.

`memory_limit=` reached `Stream.snapshot`, `scan`, `sql` and `live`, but not
the snapshot a catch-up opens to read its gap, which sized itself at DuckDB's
default: 80% of what the process may use. Two catching-up consumers in one
cgroup were allowed 160% of it between them, and `connect` passed the keyword
on to the websocket, which refused it from inside `create_connection`.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pyarrow as pa
import pytest

import streamcast
from streamcast import _published

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"n": {"type": "integer"}, "text": {"type": "string"}},
    "required": ["n", "text"],
}
ROWS = 2_000
# An odd size, so no other test's database in this process has it.
LIMIT = "77MB"


@pytest.fixture
async def stream(tmp_path):
    """A stream whose published table holds every row, served with a replay
    window so short that a consumer at offset 1 must catch up from it."""
    made = streamcast.Stream.new("rows", root=tmp_path, schema=SCHEMA)
    assert made.log is not None
    made.log.close()
    table = pa.table({"n": list(range(ROWS)), "text": ["x" * 50] * ROWS})
    streamcast.Stream.ingest("rows", table, root=tmp_path, publish=True, flush=True)
    served = streamcast.Stream.new("rows", root=tmp_path, schema=SCHEMA, max_replay=10)
    try:
        yield served
    finally:
        await served.aclose()


@pytest.fixture
def read_through(monkeypatch):
    """The `memory_limit` DuckDB reports for each database a read goes through."""
    seen: list[str] = []
    original = _published.connection

    def recording(*args: Any, **kwargs: Any) -> Any:
        connected = original(*args, **kwargs)
        seen.append(
            connected.execute("SELECT current_setting('memory_limit')").fetchall()[0][0]
        )
        return connected

    monkeypatch.setattr(_published, "connection", recording)
    return seen


def setting(limit: str) -> str:
    """`limit` as DuckDB reports it back."""
    with duckdb.connect() as reference:
        reference.execute("SET memory_limit = ?", [limit])
        return reference.execute("SELECT current_setting('memory_limit')").fetchall()[
            0
        ][0]


async def test_the_catch_up_reads_through_a_database_with_the_limit(
    stream, serve, read_through
):
    async with serve(stream, maintain=False) as uri:
        async with streamcast.connect(
            uri, offset=1, catch_up=True, memory_limit=LIMIT
        ) as sub:
            offset, _ts, row = await sub.recv()  # from the published table
            assert (offset, row["n"]) == (1, 0)

    assert read_through, "the catch-up read the published table"
    assert set(read_through) == {setting(LIMIT)}


async def test_without_it_the_catch_up_leaves_duckdbs_default(
    stream, serve, read_through
):
    async with serve(stream, maintain=False) as uri:
        async with streamcast.connect(uri, offset=1, catch_up=True) as sub:
            assert (await sub.recv())[0] == 1

    assert read_through, "the catch-up read the published table"
    assert setting(LIMIT) not in read_through


def test_a_limit_duckdb_cannot_read_is_refused_at_the_call():
    with pytest.raises(ValueError, match="memory_limit='plenty'"):
        streamcast.connect(
            "ws://127.0.0.1:1/rows", catch_up=True, memory_limit="plenty"
        )
