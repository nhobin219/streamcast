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
import re
import subprocess
import time
from pathlib import Path

import litelink
import pytest

import streamcast
from streamcast import _metadata

from .conftest import filesystem

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
    """Run the sidecar until the replica holds every database, then stop it.

    **Polled, not slept.** litestream does no final sync when an `-exec`
    command exits — measured: `-exec true` left nothing to restore — so a
    fixed sleep has to guess how long the first snapshot takes, which is
    seconds wasted on a fast box and a flake on a slow one. Nothing writes
    while this runs, so each database's snapshot (litestream's level 9)
    holds its whole state, and every one of them existing is the condition.
    """
    environment = dict(os.environ)
    resolved = s3.resolved()
    if resolved.access_key and resolved.secret_key:
        environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key
        environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key

    # Only a database that exists has anything to snapshot: a log that has
    # published nothing has no `published.db` yet, and waiting on its replica
    # would wait for ever.
    replicas = [
        f"{bucket}/{path}/0009"
        for local, bucket, path in re.findall(
            r"- path: (\S+)\n\s+replica:\n(?:\s+\S.*\n)*?\s+bucket: (\S+)\n"
            r"\s+path: (\S+)",
            Path(config).read_text(),
        )
        if Path(local).exists()
    ]
    assert replicas, f"no database to replicate in {config}"
    fs = filesystem(s3)
    sidecar = subprocess.Popen(  # noqa: S603
        [str(binary), "replicate", "-config", config],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            fs.invalidate_cache()
            if all(fs.exists(replica) and fs.ls(replica) for replica in replicas):
                return

            time.sleep(0.1)

        msg = f"litestream shipped no snapshot of {replicas} within 60s"
        raise AssertionError(msg)
    finally:
        sidecar.terminate()
        sidecar.wait(timeout=10)


def produce(root: Path, bucket: str, s3: litelink.S3Options, count: int = 200):
    """A stream on the box that is about to be lost."""
    handle = litelink.new(
        root,
        "trades",
        schema=streamcast.to_arrow(SCHEMA),
        published=bucket,
        s3_options=s3,
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
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)
            before = handle.end_offset()

        # Box B has never held this log — a different root entirely.
        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
            replay_published=True,
        )
        try:
            assert revived.log is not None
            assert revived.end_offset is not None
            assert revived.end_offset > before, (
                "a restored stream must resume ABOVE what the old box served"
            )

            # It appends, and it serves.
            async with serve(revived, maintain=False, replicate=False) as uri:
                offset = await revived.send(row(9_999))
                async with streamcast.connect(uri) as sub:
                    live = await revived.send(row(10_000))
                    got, _ts, _payload = await sub.recv()

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
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)

            # A consumer reads part of the stream and records where it got to.
            async with serve(stream, maintain=False, replicate=False) as uri:
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
        #
        # `catch_up` recovers the DATA but not the live join, which is worth
        # being precise about: measured, it delivered offsets 51..200 out of
        # the published table — every row that existed — and then failed, because the
        # server still refuses 201 and the fence range above it was never
        # issued, so no published table will ever hold it. Raising the bound is the
        # only thing that closes it.
        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
            replay_published=True,
            max_replay=None,
        )
        try:
            async with serve(revived, maintain=False, replicate=False) as uri:
                await revived.send_many([row(i) for i in range(500, 520)])

                # The SAME cursor file, untouched. The consumer does not know
                # the producer moved.
                async with streamcast.connect(uri, cursor=cursor) as sub:
                    after = [await sub.recv() for _ in range(20)]

            offsets = [offset for offset, _ts, _payload in after]
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


class TestTheFenceIsNotDistance:
    async def test_a_fenced_cursor_resumes_with_no_workaround(
        self, tmp_path, s3, bucket, serve, litestream
    ):
        """A DEFAULT server, a plain connect, and the cursor the consumer had.

        `max_replay` bounds the work a replay costs, and that work is rows.
        Offset distance is a proxy for it, exact only while the offset space
        is dense — and a restore fences 2**20 offsets that were never issued.
        Measured before this was fixed: a consumer 150 rows behind a
        failed-over producer measured as 1,048,746 behind and was refused.

        So the distance check stays as the free first pass, and a subscribe it
        would refuse is asked what it actually costs before being turned away.

        Falsify by deleting the `_log.rows_from` call in `_resolve`: this
        raises `too_old` again, and recovering a server needs `max_replay=None`
        to be usable.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)

        # NOT `max_replay=None`. The default bound, which is the point.
        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
            replay_published=True,
        )
        try:
            async with serve(revived, maintain=False, replicate=False) as uri:
                await revived.send_many([row(i) for i in range(500, 520)])

                # No catch_up, no raised bound, no intervention.
                async with streamcast.connect(uri, offset=51) as sub:
                    received = [(await sub.recv())[0] for _ in range(170)]

            assert all(o is not None for o in received), "a durable stream numbers them"
            got = [o for o in received if o is not None]
            assert got[0] == 51, "the published rows come first"
            assert got[149] == 200, "then the rest of what the old box served"
            assert got[150] > 1_000_000, "then the live stream, above the fence"
            assert got == sorted(got)

        finally:
            await revived.aclose()

    async def test_a_genuinely_distant_cursor_is_still_refused(self, serve, log):
        """The bound still bounds. Counting rows is not removing the limit."""
        stream = streamcast.Stream("trades", log=log, max_replay=5)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([{"event_ts": i, "price": 1.0} for i in range(40)])
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=1)

        assert raised.value.why == "too_old"
        # And the number it reports is the work, not the distance.
        assert raised.value.fields.get("behind") == 40


class TestCatchUpOnTopOfIt:
    async def test_catch_up_is_harmless_now_that_the_fence_is_not_distance(
        self, tmp_path, s3, bucket, serve, litestream
    ):
        """It used to be the only route across a fence, and a broken one.

        Before `_resolve` counted rows, a fenced cursor was refused and
        `catch_up` was the suggested remedy. It half-worked: it delivered
        every published row — measured, 51..200 — and then could not rejoin,
        because the server still refused the offset the published table ended at and
        the fence range above it was never issued.

        With the refusal gone the server replays those rows itself, so
        `catch_up` has nothing to do. It must not get in the way.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
            replay_published=True,
        )
        try:
            async with serve(revived, maintain=False, replicate=False) as uri:
                await revived.send_many([row(i) for i in range(500, 520)])
                async with streamcast.connect(
                    uri, offset=51, catch_up=True, s3_options=s3
                ) as sub:
                    received = [(await sub.recv())[0] for _ in range(170)]

            got = [o for o in received if o is not None]
            assert len(got) == 170, "a durable stream numbers every message"
            assert got[0] == 51
            assert got[150] > 1_000_000, "it crossed the fence into the live stream"

        finally:
            await revived.aclose()


class TestTheStagingTableComesBackEmpty:
    async def test_a_restored_log_is_locally_empty(
        self, tmp_path, s3, bucket, litestream
    ):
        """Nothing copies published files back, so say what that means.

        The Parquet is on the machine that is gone and only the published table
        has it, so the staging table comes back empty. A handle that reads local
        files only sees nothing — which is correct, and surprising if nobody
        wrote it down.
        """
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
        )
        try:
            assert revived.log is not None
            assert revived.log.staging_extent() is None, (
                "the local table comes back empty; its Parquet was on the "
                "machine that is gone"
            )
        finally:
            await revived.aclose()

    async def test_replay_published_serves_the_history_anyway(
        self, tmp_path, s3, bucket, litestream, serve
    ):
        """What replaces copying files back: read them where they are."""
        stream, handle = produce(tmp_path / "box_a", bucket, s3)
        with handle:
            await stream.send_many([row(i) for i in range(200)])
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
            replay_published=True,
        )
        try:
            assert revived.log is not None
            assert revived.log.staging_extent() is None
            async with serve(revived, maintain=False, replicate=False) as uri:
                async with streamcast.connect(uri, offset=1) as sub:
                    first = [(await sub.recv())[0] for _ in range(3)]

            assert first == [1, 2, 3]
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
            while handle.seal(flush=True) is not None:
                pass

            handle.advance()
            handle.publish(flush=True)
            ship(handle.write_replication_config(), s3, litestream)

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
        )
        try:
            assert revived.log is not None
            assert revived.log.config.wal_replication is True, (
                "a restored producer must still replicate its WAL, or the "
                "next failover has nothing to restore from"
            )
            assert revived.log.published == bucket
        finally:
            await revived.aclose()


class TestAMigratedStream:
    async def test_restore_rebuilds_the_current_log_not_the_first(
        self, tmp_path, s3, bucket, litestream
    ):
        """The metadata beside the published tables is what says which log is current.

        Without it, a restore of `trades` rebuilds the stream's FIRST log and
        serves it as though no migration had happened.
        """
        stream = streamcast.Stream.new(
            "trades",
            root=tmp_path / "box_a",
            schema=SCHEMA,
            published=bucket,
            s3_options=s3,
            config=litelink.LogConfig(target_seal_size=SEAL_SIZE, wal_replication=True),
        )
        await stream.send_many([row(i) for i in range(20)])
        await stream.aclose()

        v2 = {
            **SCHEMA,
            "properties": {
                **SCHEMA["properties"],  # ty: ignore[invalid-argument-type]
                "venue": {"type": ["string", "null"]},
            },
        }
        migrated = streamcast.Stream.migrate(
            "trades", root=tmp_path / "box_a", schema=v2, s3_options=s3
        )
        assert migrated.log is not None
        await migrated.send_many([{**row(i), "venue": "x"} for i in range(20, 30)])
        ship(str(migrated.log.write_replication_config()), s3, litestream)
        await migrated.aclose()

        revived = streamcast.Stream.restore(
            "trades",
            root=tmp_path / "box_b",
            published=bucket,
            s3_options=s3,
            binary=str(litestream),
        )
        try:
            assert revived.log is not None
            assert revived.log.name == "trades-v2"
            assert revived.schema is not None
            assert "venue" in revived.schema["properties"]  # ty: ignore[unsupported-operator]
            # The metadata came down with it, so the next `Stream.new` on
            # this box opens the same log.
            assert _metadata.load(tmp_path / "box_b", "trades") is not None
            # And the first log is not on this box, so nothing maintains it.
            assert revived.retired == ()
        finally:
            await revived.aclose()
