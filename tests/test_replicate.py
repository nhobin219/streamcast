"""WAL replication: `serve` runs litestream, and never two of them.

`wal_replication` is opt-in on a litelink log, and it is what makes the log
survive losing its machine. These tests need somewhere to ship to — `just
rustfs` starts one — and SKIP without it, which is how a tier goes unchecked;
`STREAMCAST_REQUIRE_S3` turns that skip into a failure, and `just check-all`
sets it.
"""

from __future__ import annotations

import asyncio

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast._replicate import Sidecar, SidecarUnavailable

from .conftest import filesystem

pytestmark = pytest.mark.replication

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
    ]
)


@pytest.fixture
def shipped(tmp_path, s3, bucket):
    """A log that replicates its WAL to the test bucket."""
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=litelink.LogConfig(wal_replication=True),
        archive=bucket,
        s3=s3,
    )
    with handle:
        yield handle


async def wal_objects(s3, bucket: str) -> list[str]:
    prefix = f"{bucket.removeprefix('s3://')}/trades/_wal"
    fs = filesystem(s3)
    return await asyncio.to_thread(lambda: fs.find(prefix) if fs.exists(prefix) else [])


async def settle(s3, bucket, *, timeout=30.0) -> list[str]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        found = await wal_objects(s3, bucket)
        if found:
            return found

        await asyncio.sleep(0.5)

    return []


class TestItActuallyReplicates:
    @pytest.mark.slow
    async def test_serve_ships_the_wal_with_no_second_process_to_run(
        self, shipped, s3, bucket
    ):
        """The whole point: `serve` is the one thing you start.

        Before this, a log with `wal_replication=True` served by streamcast
        sealed and maintained while NOTHING shipped — the operator believing
        they had continuous RPO protection with none of it.
        """
        stream = streamcast.Stream("trades", log=shipped)
        async with streamcast.serve(stream, "127.0.0.1", 0):
            await stream.send_many(
                [{"event_ts": i, "price": 1.0 * i} for i in range(2_000)]
            )
            found = await settle(s3, bucket)

        assert found, "litestream shipped nothing"
        assert any("buffer.db" in key for key in found), found[:5]
        assert any(key.endswith(".ltx") for key in found), found[:5]

    async def test_replicate_false_starts_nothing(self, shipped):
        # For a deployment running its own, more finely tuned, litestream.
        stream = streamcast.Stream("trades", log=shipped)
        server = await streamcast.serve(stream, "127.0.0.1", 0, replicate=False)
        try:
            assert [c for c in server._children if isinstance(c, Sidecar)] == []  # noqa: SLF001
        finally:
            server.close()
            await server.wait_closed()

    async def test_a_log_without_wal_replication_starts_nothing(self, log):
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0, maintain=False)
        try:
            assert [c for c in server._children if isinstance(c, Sidecar)] == []  # noqa: SLF001
        finally:
            server.close()
            await server.wait_closed()


class TestNeverTwo:
    """Two litestream instances on one database is what litestream forbids."""

    async def test_the_lock_lives_beside_the_log_not_the_root(self, shipped):
        # litelink's own example locks `log.root / "litestream.lock"`, which
        # is right for the single log it runs and wrong here: `log.root` is
        # the PARENT, so two streams under one root would contend for one
        # lock and only one would ever replicate.
        sidecar = Sidecar(shipped)
        assert sidecar.config.parent.name == "trades"
        assert sidecar.config.parent != shipped.root

    async def test_a_second_sidecar_stands_by_rather_than_starting_one(self, shipped):
        first = Sidecar(shipped)
        second = Sidecar(shipped)
        first.start()
        second.start()
        try:
            for _ in range(60):
                if first.owner or second.owner:
                    break

                await asyncio.sleep(0.1)

            assert first.owner != second.owner, "both or neither took the lock"
            standby = second if first.owner else first
            # The standby holds no child at all — not a stopped one.
            assert standby._process is None  # noqa: SLF001

        finally:
            for sidecar in (first, second):
                sidecar.terminate()
                await sidecar.wait_closed()

    async def test_the_lock_is_released_so_the_next_server_takes_over(self, shipped):
        first = Sidecar(shipped)
        first.start()
        for _ in range(60):
            if first.owner:
                break

            await asyncio.sleep(0.1)

        assert first.owner
        first.terminate()
        await first.wait_closed()

        # Answered once, a standby would never become the owner.
        second = Sidecar(shipped)
        assert second._claim() is True  # noqa: SLF001
        second.terminate()
        await second.wait_closed()


class TestItFailsLoudly:
    async def test_a_missing_binary_raises_at_serve(self, shipped, monkeypatch):
        """Not at the first missed push.

        A server that came up and replicated nothing would leave the operator
        believing they had protection they did not have — the failure this
        module exists to prevent.
        """
        monkeypatch.setattr(
            "streamcast._replicate.litestream_binary",
            lambda override=None: "/nonexistent/litestream",
        )
        stream = streamcast.Stream("trades", log=shipped)
        with pytest.raises(SidecarUnavailable, match="litestream was not found"):
            streamcast.serve(stream, "127.0.0.1", 0)

    async def test_the_message_names_the_way_out(self, shipped, monkeypatch):
        monkeypatch.setattr(
            "streamcast._replicate.litestream_binary",
            lambda override=None: "/nonexistent/litestream",
        )
        stream = streamcast.Stream("trades", log=shipped)
        with pytest.raises(SidecarUnavailable) as raised:
            streamcast.serve(stream, "127.0.0.1", 0)

        assert "replicate=False" in str(raised.value)
        assert "'trades'" in str(raised.value)
