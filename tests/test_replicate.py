"""WAL replication: `serve` runs litestream, and never two of them.

`wal_replication` is opt-in on a litelink log, and it is what makes the log
survive losing its machine. These tests need somewhere to ship to — `just
rustfs` starts one — and SKIP without it, which is how a tier goes unchecked;
`STREAMCAST_REQUIRE_S3` turns that skip into a failure, and `just check-all`
sets it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

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


@pytest.fixture
def two_shipped(tmp_path, s3, bucket):
    """Two logs under one root, both replicating to the test bucket."""
    handles = [
        litelink.new(
            tmp_path / "data",
            name,
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=litelink.LogConfig(wal_replication=True),
            archive=bucket,
            s3=s3,
        )
        for name in ("trades", "quotes")
    ]
    try:
        yield handles

    finally:
        for handle in handles:
            handle.close()


async def wal_objects(s3, bucket: str, name: str = "trades") -> list[str]:
    prefix = f"{bucket.removeprefix('s3://')}/{name}/_wal"
    fs = filesystem(s3)
    return await asyncio.to_thread(lambda: fs.find(prefix) if fs.exists(prefix) else [])


async def settle(s3, bucket, *, name: str = "trades", timeout=30.0) -> list[str]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        found = await wal_objects(s3, bucket, name)
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

    @pytest.mark.slow
    async def test_one_process_ships_every_log(self, two_shipped, s3, bucket):
        """**The whole of #14's replication half, end to end.**

        One litestream, a merged `dbs` config, two logs — and both must
        actually arrive in object storage. Each process was 40-170 MB, so
        four small streams spent most of a gigabyte replicating a producer of
        ~460 MB, and a stream taking a row a minute cost the same as the
        busiest one.

        Falsify by dropping the merge in `Sidecar._write_config` and writing
        only the first log's config: `quotes` then ships nothing.
        """
        streams = [streamcast.Stream(h.name, log=h) for h in two_shipped]
        async with streamcast.serve(streams, "127.0.0.1", 0) as server:
            sidecars = [c for c in server._children if isinstance(c, Sidecar)]  # noqa: SLF001

            assert len(sidecars) == 1, "one litestream per log is the defect"

            # Ownership is taken by the supervise task, not by `start`, so a
            # standby can keep retrying. Wait for it rather than racing it.
            for _ in range(100):
                if sidecars[0].owned == {"trades", "quotes"}:
                    break

                await asyncio.sleep(0.1)

            assert sidecars[0].owned == {"trades", "quotes"}

            for stream in streams:
                await stream.send_many(
                    [{"event_ts": i, "price": 1.0 * i} for i in range(2_000)]
                )

            shipped_trades = await settle(s3, bucket, name="trades")
            shipped_quotes = await settle(s3, bucket, name="quotes")

        assert shipped_trades, "trades shipped nothing"
        assert shipped_quotes, "quotes shipped nothing from the same process"
        assert any(key.endswith(".ltx") for key in shipped_quotes), shipped_quotes[:5]

    async def test_the_merged_config_names_every_owned_database(self, two_shipped):
        """Three databases per log — buffer, catalog, archive — under one
        `dbs:`. Asserted on the file, so a merge that silently dropped a log
        fails here rather than as an absence in object storage an hour later.
        """
        sidecar = Sidecar.new(two_shipped)
        try:
            assert sidecar._claim() is True  # noqa: SLF001
            sidecar._write_config()  # noqa: SLF001
            text = sidecar.config.read_text()

            assert text.startswith("dbs:")
            assert text.count("- path:") == 6, text
            for name in ("trades", "quotes"):
                assert f"/{name}/buffer.db" in text
                assert f"/{name}/catalog.db" in text

        finally:
            sidecar.terminate()
            await sidecar.wait_closed()

    async def test_a_log_locked_elsewhere_is_left_out_not_a_standstill(
        self, two_shipped
    ):
        """**Per-log locks are what make one process safe.**

        The flock protects the DATABASE, not the replicator. So a server that
        finds one log already replicated takes the others and retries that
        one — rather than standing by for all of them, which sharing a
        process would otherwise turn a single contended log into.
        """
        holder = Sidecar.new(two_shipped[:1])  # owns `trades` only
        assert holder._claim() is True  # noqa: SLF001

        second = Sidecar.new(two_shipped)
        try:
            second._claim()  # noqa: SLF001

            assert second.owned == {"quotes"}, "it did not divide the logs"
            assert second.owner is True, "it stood still over one contended log"

        finally:
            for sidecar in (second, holder):
                sidecar.terminate()
                await sidecar.wait_closed()

    async def test_a_freed_log_is_picked_up_without_a_restart_of_the_server(
        self, two_shipped
    ):
        """Answered once, a log released by a dying server is never taken."""
        holder = Sidecar.new(two_shipped[:1])
        assert holder._claim() is True  # noqa: SLF001

        second = Sidecar.new(two_shipped)
        try:
            second._claim()  # noqa: SLF001
            assert second.owned == {"quotes"}

            holder.terminate()
            await holder.wait_closed()

            assert second._claim() is True, "the freed lock was not retaken"  # noqa: SLF001
            assert second.owned == {"trades", "quotes"}

        finally:
            second.terminate()
            await second.wait_closed()

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

    async def test_the_lock_lives_beside_each_log_not_the_root(self, shipped):
        # litelink's own example locks `log.root / "litestream.lock"`, which
        # is right for the single log it runs and wrong here: `log.root` is
        # the PARENT, so two streams under one root would contend for one
        # lock and only one would ever replicate.
        #
        # **Sharing the PROCESS does not share the LOCK.** The lock protects
        # the database, not the replicator, so it stays beside each log even
        # though one litestream now covers all of them.
        sidecar = Sidecar.new([shipped])
        lock = sidecar._lock_path(Path(shipped.write_replication_config()))  # noqa: SLF001

        assert lock.parent.name == "trades"
        assert lock.parent != Path(shipped.root)
        assert lock.name == "litestream.lock"

    async def test_a_second_sidecar_stands_by_rather_than_starting_one(self, shipped):
        first = Sidecar.new([shipped])
        second = Sidecar.new([shipped])
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
        first = Sidecar.new([shipped])
        first.start()
        for _ in range(60):
            if first.owner:
                break

            await asyncio.sleep(0.1)

        assert first.owner
        first.terminate()
        await first.wait_closed()

        # Answered once, a standby would never become the owner.
        second = Sidecar.new([shipped])
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
