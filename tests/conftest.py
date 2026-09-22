"""Fixtures. No network, no container, no credentials — by construction.

Every test here binds to port 0 on loopback and every log is a `tmp_path`
directory, so the suite is a pure local-file workload and a change that breaks
that is a change to reject.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import litelink
import pytest

import streamcast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from pathlib import Path

    from litelink import WriteHandle
    from websockets.asyncio.server import Server

    from streamcast import Stream


@pytest.fixture
def log(tmp_path: Path) -> Iterator[WriteHandle]:
    """A streamcast log, created with the schema the library owns.

    `target_seal_size` is small so a test that seals actually seals rather
    than leaving every row in the SQLite buffer — a replay that never reads
    Parquet is a replay that never exercised the tier it is testing.
    """
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        schema=streamcast.SCHEMA,
        config=litelink.LogConfig(target_seal_size=4 * 1024, compact_min_files=2),
    )
    with handle:
        yield handle


@pytest.fixture
def serve() -> Callable[..., contextlib.AbstractAsyncContextManager[str]]:
    """`async with serve(stream) as uri:` — a broker on an ephemeral port.

    Returns the URI rather than the server, because every test that starts one
    immediately wants to connect to it and port 0 means nobody can spell the
    URI ahead of time. The server itself is reachable through the stream for
    the handful of tests that need it.
    """

    @contextlib.asynccontextmanager
    async def _serve(*streams: Stream, **kwargs: Any) -> AsyncIterator[str]:
        server: Server = await streamcast.serve(list(streams), "127.0.0.1", 0, **kwargs)
        try:
            port = server.sockets[0].getsockname()[1]
            name = streams[0].name
            yield f"ws://127.0.0.1:{port}/{name}"
        finally:
            server.close()
            await server.wait_closed()

    return _serve
