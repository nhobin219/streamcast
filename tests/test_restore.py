"""Producer-side failover: standing a stream up where its log has never been.

The consumer half of this already worked — `connect(cursor=)` moves a consumer
between boxes. This is the other half, and the property that makes it usable is
in `test_a_consumer_cursor_survives_the_move`: offsets are FENCED rather than
reissued, so a consumer that reconnects after a producer move sees a gap and
never sees an offset it already holds carrying different data.

Needs an endpoint and litestream — `just rustfs` and `just litestream` — and
skips without either.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import litelink
import pytest

import streamcast

pytestmark = pytest.mark.replication

SCHEMA = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}
SEAL_SIZE = 64 * 1024


def row(i: int) -> dict[str, object]:
    return {"event_ts": 1_790_000_000_000_000 + i, "price": 100.0 + i}


@pytest.fixture
def litestream() -> Path:
    """The binary, or a skip. A restore cannot run without it."""
    binary = Path(litelink.__file__).parent / ".bin" / "litestream"
    if not os.access(binary, os.X_OK):
        from shutil import which

        found = which("litestream")
        if found is None:
            pytest.skip("litestream is not provisioned")

        return Path(found)

    return binary


def ship(config: str, s3: litelink.S3Options, binary: Path) -> None:
    """Run the sidecar once, so a replica exists to restore from.

    `-exec` bounds its life: it replicates, runs the command, and exits. The
    alternative is starting a daemon and guessing when to stop it.
    """
    environment = dict(os.environ)
    resolved = s3.resolved()
    if resolved.access_key and resolved.secret_key:
        environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key
        environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key

    subprocess.run(
        [str(binary), "replicate", "-config", config, "-exec", "sleep 4"],
        env=environment,
        check=False,
        capture_output=True,
        timeout=90,
    )


def produce(root: Path, bucket: str, s3: litelink.S3Options, count: int = 200):
    """A stream on the box that is about to be lost."""
    handle = litelink.new(
        root,
        "trades",
        schema=streamcast.to_arrow(SCHEMA),
        archive=bucket,
        s3=s3,
        config=litelink.LogConfig(target_seal_size=SEAL_SIZE, wal_replication=True),
    )
    stream = streamcast.Stream("trades", log=handle)

    return stream, handle


class TestItStandsUpElsewhere:
    async def test_a_restored_stream_serves_and_appends(
        self, tmp_path, s3, bucket, serve, litestream
    ):
        """The headline: a box that has never seen this log serves it."""
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal() is not None:
                pass

            handle.maintain()
            handle.sync(push_unsettled=True)
            ship(handle.write_replication_config(), s3, litestream)
            before = handle.end_offset()

        # Box B has never held this log — a different root entirely.
        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            archive=bucket,
            s3=s3,
            binary=str(litestream),
            replay_archive=True,
        )
        try:
            assert revived.log is not None
            assert revived.end_offset is not None
            assert revived.end_offset > before, (
                "a restored stream must resume ABOVE what the old box served"
            )

            # It appends, and it serves.
            async with serve(revived, maintain=False) as uri:
                offset = await revived.send(row(9_999))
                async with streamcast.connect(uri) as sub:
                    live = await revived.send(row(10_000))
                    got, _payload = await sub.recv()

            assert offset is not None
            assert got == live

        finally:
            await revived.aclose()

    async def test_a_consumer_cursor_survives_the_move(
        self, tmp_path, s3, bucket, serve, litestream
    ):
        """**The property the whole feature rests on.**

        litelink fences offsets rather than reissuing them, so nothing the old
        producer served is ever handed out again carrying different data. A
        consumer keeps the cursor it had, reconnects to the new producer, and
        sees a GAP — which `recv` allows, because it refuses a step backwards
        and not a jump forward.

        Falsify by making the restore reuse offsets: the consumer would then
        receive an offset it had already processed, with different rows behind
        it, and nothing would say so.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        cursor = tmp_path / "consumer.offset"

        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal() is not None:
                pass

            handle.maintain()
            handle.sync(push_unsettled=True)

            # A consumer reads part of the stream and records where it got to.
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(
                    uri, offset=streamcast.EARLIEST, cursor=cursor
                ) as sub:
                    seen = [await sub.recv() for _ in range(50)]

            ship(handle.write_replication_config(), s3, litestream)

        handled = seen[-1][0]
        assert handled is not None
        assert cursor.read_text().strip() == str(handled)

        # **`max_replay=None` is what makes the move transparent**, and it is
        # not obvious. The fence puts the new frontier 2**20 above the old
        # one, so every existing cursor looks a million messages behind and a
        # bounded server refuses it as `too_old` — measured: "offset 51 is
        # 1048746 messages behind and this server replays at most 100000".
        # `catch_up` does not rescue it either: the gap is the fence, and the
        # archive does not hold the offsets inside it because they were never
        # issued.
        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            archive=bucket,
            s3=s3,
            binary=str(litestream),
            replay_archive=True,
            max_replay=None,
        )
        try:
            async with serve(revived, maintain=False) as uri:
                await revived.send_many([row(i) for i in range(500, 520)])

                # The SAME cursor file, untouched. The consumer does not know
                # the producer moved.
                async with streamcast.connect(uri, cursor=cursor) as sub:
                    after = [await sub.recv() for _ in range(20)]

            offsets = [offset for offset, _payload in after]
            assert all(o is not None for o in offsets), "a durable stream numbers them"
            assert offsets == sorted(offsets), (  # ty: ignore[invalid-argument-type]
                "offsets must not go backwards"
            )
            assert handled is not None
            assert offsets[0] is not None
            assert offsets[0] > handled, (
                f"resumed at {offsets[0]} having handled {handled}: a restored "
                f"producer must never reissue an offset a consumer holds"
            )

        finally:
            await revived.aclose()


class TestWhatItCostsToSkipHydrate:
    async def test_an_unhydrated_log_is_locally_empty(
        self, tmp_path, s3, bucket, litestream
    ):
        """`hydrate` is a parameter and not a default, so say what skipping it means.

        The Parquet is on the machine that is gone and only the archive has
        it, so the local table comes back empty. A handle that reads local
        files only sees nothing — which is correct, and surprising if nobody
        wrote it down.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal() is not None:
                pass

            handle.maintain()
            handle.sync(push_unsettled=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            archive=bucket,
            s3=s3,
            binary=str(litestream),
        )
        try:
            assert revived.log is not None
            assert revived.log.table_extent() is None, (
                "the local table comes back empty; its Parquet was on the "
                "machine that is gone"
            )
        finally:
            await revived.aclose()

    async def test_hydrate_brings_the_local_tier_back(
        self, tmp_path, s3, bucket, litestream
    ):
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal() is not None:
                pass

            handle.maintain()
            handle.sync(push_unsettled=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            archive=bucket,
            s3=s3,
            binary=str(litestream),
            hydrate=timedelta(hours=1),
        )
        try:
            assert revived.log is not None
            assert revived.log.table_extent() is not None, (
                "hydrate must re-register archived files into the local table"
            )
        finally:
            await revived.aclose()


class TestTheServerStartsWhatItNeeds:
    async def test_a_restored_stream_keeps_its_replication_settings(
        self, tmp_path, s3, bucket, litestream
    ):
        """`wal_replication` comes back with the log, so `serve` starts a sidecar.

        A restored producer that quietly stopped replicating would be one
        failure away from having nothing to restore FROM next time — the
        failure mode being invisible until it mattered.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal() is not None:
                pass

            handle.maintain()
            handle.sync(push_unsettled=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            archive=bucket,
            s3=s3,
            binary=str(litestream),
        )
        try:
            assert revived.log is not None
            assert revived.log.config.wal_replication is True, (
                "a restored producer must still replicate its WAL, or the "
                "next failover has nothing to restore from"
            )
            assert revived.log.archive == bucket
        finally:
            await revived.aclose()
