"""Time-based retention (#118): a stream's rows older than its window leave it.

Off unless set. Set, a pass publishes the stream starting at its floor — the
smallest offset whose `streamcast_ts` is newer than the cutoff — and only
after readers' grace deletes what is below it, from the live log and every
retired one. The clock is the test's: rows are stamped at chosen times, and
passes run at chosen times, rather than waiting out a window.
"""

from __future__ import annotations

import asyncio
import ctypes
import gc
import json
import os
import subprocess
import sys
import threading
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import duckdb
import litelink
import pyarrow as pa
import pytest
from litelink import _handle

import streamcast
from streamcast import _maintain, _metadata, _published, _retention, _stream, _versions
from streamcast._maintain import Maintain, Supervisor

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}
WIDER: dict[str, Any] = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "note": {"type": ["string", "null"]}},
    "required": ["i"],
}

DAY = 86_400_000_000
T0 = 1_790_000_000_000_000
"""When the oldest rows were written."""
WINDOW = timedelta(days=1)
GRACE = int(_retention.GRACE.total_seconds() * 1_000_000)


async def write(
    stream: streamcast.StreamHandle, monkeypatch, at: int, values: list[int]
) -> None:
    """Rows stamped at `at`, in a file of their own, published."""
    with monkeypatch.context() as patched:
        patched.setattr(_stream.time, "time_ns", lambda: at * 1_000)
        await stream.send_many([{"i": value} for value in values])

    log = stream.log
    assert log is not None
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)


def lowest(log) -> int | None:
    """The lowest offset any tier of `log` holds."""
    coverage = log.coverage()
    starts = [
        tier[0]
        for tier in (coverage.published, coverage.staging, coverage.buffer)
        if tier is not None
    ]
    return min(starts, default=None)


def home(root) -> Any:
    return _metadata.home(root, "t")


async def offsets(root) -> list[int]:
    uri = (home(root) / "t.metadata" / "version-hint.text").resolve().as_uri()
    async with await streamcast.StreamHandle.snapshot(uri) as snap:
        read = await snap.scan(columns=["litelink_offset"]).read_all()

    return read.column("litelink_offset").to_pylist()


async def one_log(tmp_path, monkeypatch) -> streamcast.StreamHandle:
    """Three rows two days old, three rows new; a window of a day."""
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(root=Path(tmp_path).absolute()),
        ),
        initialization="create_if_absent",
    )
    await write(stream, monkeypatch, T0, [0, 1, 2])
    await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
    stream.ensure_metadata()
    streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
    return stream


NOW = T0 + 2 * DAY + 60_000_000
"""A minute after the new rows: the old ones are past the window."""


class TestTheSetting:
    async def test_it_is_off_unless_set(self, tmp_path, monkeypatch):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        try:
            await write(stream, monkeypatch, T0, [0, 1, 2])
            stream.ensure_metadata()
            assert stream.log is not None
            assert (
                _retention.run(home(tmp_path), "t", stream.log, now=T0 + 365 * DAY)
                is None
            )
        finally:
            await stream.aclose()

        assert await offsets(tmp_path) == [1, 2, 3]

    async def test_it_is_recorded_and_can_be_cleared(self, tmp_path):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        await stream.aclose()

        streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
        metadata = _metadata.load(home(tmp_path), "t")
        assert metadata is not None
        assert metadata.retention == DAY

        streamcast.StreamHandle.retain("t", root=tmp_path, max_age=None)
        cleared = _metadata.load(home(tmp_path), "t")
        assert cleared is not None
        assert cleared.retention is None

    def test_a_window_is_positive(self, tmp_path):
        with pytest.raises(ValueError, match="positive"):
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=timedelta(0))


class TestTheLiveLog:
    async def test_readers_lose_the_old_rows_before_the_files_go(
        self, tmp_path, monkeypatch
    ):
        stream = await one_log(tmp_path, monkeypatch)
        try:
            assert stream.log is not None
            begun = _retention.run(home(tmp_path), "t", stream.log, now=NOW)
            assert begun is not None
            assert begun.pending is not None
            assert begun.pending.floor == 4
            assert await offsets(tmp_path) == [4, 5, 6], "the floor, published"
            assert lowest(stream.log) == 1, "nothing deleted yet"

            # Inside the grace: nothing more.
            assert (
                _retention.run(home(tmp_path), "t", stream.log, now=NOW + 60_000_000)
                is None
            )

            done = _retention.run(home(tmp_path), "t", stream.log, now=NOW + GRACE)
            assert done is not None
            assert done.pending is None
            assert lowest(stream.log) == 4, "the old file is out of the log"
        finally:
            await stream.aclose()

    async def test_a_clock_that_steps_back_drops_nothing_newer(
        self, tmp_path, monkeypatch
    ):
        """The floor is the first row newer than the cutoff, not the last
        older one: an old stamp after a new one is kept."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        try:
            await write(stream, monkeypatch, T0, [0])
            await write(stream, monkeypatch, T0 + 2 * DAY, [1])
            await write(stream, monkeypatch, T0, [2])  # the clock stepped back
            stream.ensure_metadata()
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
            assert stream.log is not None
            begun = _retention.run(home(tmp_path), "t", stream.log, now=NOW)
            assert begun is not None
            assert begun.pending is not None
            assert begun.pending.floor == 2
            assert await offsets(tmp_path) == [2, 3]
        finally:
            await stream.aclose()

    async def test_a_held_claim_is_tried_again(self, tmp_path, monkeypatch):
        stream = await one_log(tmp_path, monkeypatch)
        try:
            log = stream.log
            assert log is not None
            _retention.run(home(tmp_path), "t", log, now=NOW)
            real = type(log).truncate

            def held(self, *, below):  # noqa: ANN001, ANN202, ARG001
                msg = "another owner holds a claim over this log; retry the truncate"
                raise RuntimeError(msg)

            monkeypatch.setattr(type(log), "truncate", held)
            with pytest.raises(RuntimeError, match="claim"):
                _retention.run(home(tmp_path), "t", log, now=NOW + GRACE)

            pending = _metadata.load(home(tmp_path), "t")
            assert pending is not None
            assert pending.pending is not None, "kept for the next pass"

            monkeypatch.setattr(type(log), "truncate", real)
            done = _retention.run(home(tmp_path), "t", log, now=NOW + GRACE)
            assert done is not None
            assert done.pending is None
        finally:
            await stream.aclose()


class TestRetiredLogs:
    async def test_one_wholly_older_is_dropped_then_deleted(
        self, tmp_path, monkeypatch
    ):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        await write(stream, monkeypatch, T0, [0, 1, 2])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
            migrated.ensure_metadata()
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
            assert migrated.log is not None

            begun = _retention.run(home(tmp_path), "t", migrated.log, now=NOW)
            assert begun is not None
            assert [entry.name for entry in begun.logs] == ["t-v2"], "out of the stream"
            assert begun.manifest is None, "its statistics with it"
            assert await offsets(tmp_path) == [4, 5, 6]
            assert (home(tmp_path) / "t").is_dir(), "deleted only after the grace"

            _retention.run(home(tmp_path), "t", migrated.log, now=NOW + GRACE)
            assert not (home(tmp_path) / "t").exists()
        finally:
            await migrated.aclose()

    async def test_one_the_floor_cuts_through_is_truncated(self, tmp_path, monkeypatch):
        """Whole files only: `retire` folds unfinished files into one, so the
        target is small enough that each sealed file is a finished one."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(),
                    config=litelink.LogConfig(target_compact_size=256),
                ),
            ),
            initialization="create_if_absent",
        )
        await write(stream, monkeypatch, T0, [0, 1, 2])
        await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [6])
            migrated.ensure_metadata()
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
            assert migrated.log is not None

            begun = _retention.run(home(tmp_path), "t", migrated.log, now=NOW)
            assert begun is not None
            assert begun.sealed_logs[0].start_offset == 4
            assert await offsets(tmp_path) == [4, 5, 6, 7]

            _retention.run(home(tmp_path), "t", migrated.log, now=NOW + GRACE)
            retired = begun.sealed_logs[0]
            assert retired.published is not None
            with litelink.open(home(tmp_path), "t", read_only=True) as old:
                assert lowest(old) == 4, "its old file is gone"

            # And it is still readable: its manifest row counts what it holds
            # now, or every read would refuse it as published short.
            assert await offsets(tmp_path) == [4, 5, 6, 7]
        finally:
            await migrated.aclose()


def resources() -> tuple[int, int]:
    """Current RSS in KiB and eventfds; maxrss cannot show reclamation."""
    rss = next(
        int(line.split()[1])
        for line in Path("/proc/self/status").read_text().splitlines()
        if line.startswith("VmRSS:")
    )
    eventfds = 0
    for fd in Path("/proc/self/fd").iterdir():
        try:
            eventfds += os.readlink(fd) == "anon_inode:[eventfd]"
        except FileNotFoundError:
            pass

    return rss, eventfds


@pytest.mark.replication
@pytest.mark.skipif(not Path("/proc/self/status").exists(), reason="Linux procfs")
async def test_clean_published_with_retention_keeps_resources_flat(
    tmp_path, monkeypatch, s3, bucket
):
    """A fixed retired S3 log must not allocate a client pool on each pass.

    The scheduler runs retention less frequently than published cleanup; run
    both directly to exercise that work without waiting for their intervals.
    """
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=tmp_path.absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="create_if_absent",
    )
    try:
        await write(stream, monkeypatch, T0, [1])
        stream.ensure_metadata()
    finally:
        await stream.aclose()

    migrated = streamcast.StreamHandle.migrate(
        "t", root=tmp_path, schema=WIDER, s3_options=s3
    )
    collected = gc.isenabled()
    try:
        streamcast.StreamHandle.retain(
            "t", root=tmp_path, max_age=WINDOW, s3_options=s3
        )
        log = migrated.log
        assert log is not None
        metadata = _metadata.load(home(tmp_path), "t")
        assert metadata is not None and len(metadata.sealed_logs) == 1
        # Warm the allocator with the same uncollected workload we measure.
        # A shorter warmup leaves arena growth from temporary Python objects
        # in the measured RSS, even when the S3 client pool is already warm.
        # Collect temporary objects before the baseline; keep shared I/O alive.
        gc.disable()
        for _ in range(50):
            assert (
                _retention.run(home(tmp_path), "t", log, now=T0, s3_options=s3) is None
            )
            _maintain.ROLES["clean-published"](log, False)

        # FileIO's bound-method cache forms a cycle. Do not let the collector
        # mask a fresh pool per pass, or depend on a Python version's schedule.
        gc.collect()
        samples = [(0, *resources())]
        for cycle in range(1, 51):
            assert (
                _retention.run(home(tmp_path), "t", log, now=T0, s3_options=s3) is None
            )
            _maintain.ROLES["clean-published"](log, False)
            if cycle % 10 == 0:
                samples.append((cycle, *resources()))

        print("cycles,rss_kib,eventfds")
        for sample in samples:
            print(",".join(map(str, sample)))

        _, rss, eventfds = samples[0]
        # RSS has page/allocator noise; eventfds must add none after warmup.
        assert all(
            current_rss <= rss + 2 * 1024 and current_eventfds <= eventfds
            for _, current_rss, current_eventfds in samples[1:]
        ), samples
        assert _metadata.load(home(tmp_path), "t") == metadata
    finally:
        if collected:
            gc.enable()

        gc.collect()
        await migrated.aclose()


def test_the_published_cleanup_is_told_the_streams(tmp_path):
    plan = Maintain()
    for role in ("seal", "clean-published"):
        argv = Supervisor(
            [(tmp_path, "t")],
            plan,
            role,
            streams=[(tmp_path, "t")] if role == "clean-published" else (),
        )._spawn_argv()  # noqa: SLF001
        assert ("--stream" in argv) == (role == "clean-published"), role


class TestReaders:
    async def test_a_catch_up_below_the_floor_is_told_it_is_retention(
        self, tmp_path, monkeypatch, serve
    ):
        """Not "lost": dropped on purpose, and the refusal says so."""
        stream = await one_log(tmp_path, monkeypatch)
        try:
            assert stream.log is not None
            _retention.run(home(tmp_path), "t", stream.log, now=NOW)
            _retention.run(home(tmp_path), "t", stream.log, now=NOW + GRACE)
            async with serve(stream, maintain=False) as uri:
                with pytest.raises(streamcast.CatchUpUnavailable, match="retention"):
                    await streamcast.connect(uri, offset=1, catch_up=True)
        finally:
            await stream.aclose()


def test_one_stream_failing_does_not_stop_the_others(tmp_path, monkeypatch, capsys):

    ran: list[str] = []

    def run(_home, name, _live, **_):  # noqa: ANN001, ANN003, ANN202
        ran.append(name)
        if name == "a":
            msg = "another owner holds a claim over this log; retry the truncate"
            raise RuntimeError(msg)

    monkeypatch.setattr(_maintain._retention, "run", run)  # noqa: SLF001
    monkeypatch.setattr(
        _maintain._metadata,  # noqa: SLF001
        "load",
        lambda _home, name: type(
            "M", (), {"live_log": type("E", (), {"name": name})()}
        )(),
    )
    _maintain.retain([(tmp_path, "a"), (tmp_path, "b")], {"a": object(), "b": object()})  # ty: ignore[invalid-argument-type]

    assert ran == ["a", "b"]
    assert "[retain] a: skipped" in capsys.readouterr().err


class TestRaces:
    async def test_a_row_written_while_the_floor_is_found_is_kept(
        self, tmp_path, monkeypatch
    ):
        """Nothing newer than the cutoff, and a row lands between the query
        and the end it reads: that row must not be under the floor."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        try:
            await write(stream, monkeypatch, T0, [0, 1, 2])
            stream.ensure_metadata()
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
            log = stream.log
            assert log is not None
            now = T0 + 2 * DAY
            loop = asyncio.get_running_loop()
            sent = threading.Event()

            async def send() -> None:
                with monkeypatch.context() as patched:
                    patched.setattr(_stream.time, "time_ns", lambda: now * 1_000)
                    await stream.send({"i": 99})

                sent.set()

            class Racing:
                """The live log, with a row landing as its query returns."""

                def __getattr__(self, name: str):  # noqa: ANN204
                    return getattr(log, name)

                def sql(self, query: str):  # noqa: ANN202
                    read = log.sql(query).read_all()
                    asyncio.run_coroutine_threadsafe(send(), loop)
                    sent.wait(10)
                    return type("Read", (), {"read_all": lambda _self: read})()

            begun = await asyncio.to_thread(
                _retention.run,
                home(tmp_path),
                "t",
                Racing(),  # ty: ignore[invalid-argument-type]
                now=now,
            )
            assert begun is not None
            assert begun.pending is not None
            assert begun.pending.floor == 4
            while log.seal(flush=True) is not None:
                pass

            log.publish(flush=True)
            assert await offsets(tmp_path) == [4]
        finally:
            await stream.aclose()

    async def test_a_pass_never_writes_over_a_newer_setting(
        self, tmp_path, monkeypatch
    ):
        """`StreamHandle.retain` committing while a pass is in flight: the pass is
        refused, rather than putting the old window back."""
        stream = await one_log(tmp_path, monkeypatch)
        try:
            assert stream.log is not None
            real = _retention._newest  # noqa: SLF001

            def widened(live, s3_options, *, duckdb=None):  # noqa: ANN001, ANN202
                streamcast.StreamHandle.retain("t", root=tmp_path, max_age=None)
                return real(live, s3_options, duckdb=duckdb)

            monkeypatch.setattr(_retention, "_newest", widened)
            with pytest.raises(_versions.Conflict):
                _retention.run(home(tmp_path), "t", stream.log, now=NOW)

            after = _metadata.load(home(tmp_path), "t")
            assert after is not None
            assert after.retention is None, "the operator's setting stands"
            assert after.pending is None
        finally:
            await stream.aclose()

    async def test_a_pass_reads_the_version_it_compares_not_the_plain_copy(
        self, tmp_path, monkeypatch
    ):
        """A plain `t.metadata.json` left beside the versions — by a build from
        before them, or a delete that failed — is the stream as it was. A pass
        built on it would put the old window back over the new."""
        stream = await one_log(tmp_path, monkeypatch)
        try:
            before = _metadata.load(home(tmp_path), "t")
            assert before is not None
            streamcast.StreamHandle.retain(
                "t", root=tmp_path, max_age=timedelta(days=30)
            )
            (home(tmp_path) / "t.metadata.json").write_text(before.to_json())

            assert stream.log is not None
            _retention.run(home(tmp_path), "t", stream.log, now=NOW)

            _version, current, _manifest = _versions.load(home(tmp_path), "t")
            assert current is not None
            assert current.retention == 30 * DAY, "the operator's window stands"
        finally:
            await stream.aclose()


class TestAFailedSecondHalf:
    async def test_the_cut_log_stays_readable_when_the_last_commit_fails(
        self, tmp_path, monkeypatch
    ):
        """Between `finish`'s truncate and its commit — a conflict, a crash —
        the log is short; it must not read as published short meanwhile."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(),
                    config=litelink.LogConfig(target_compact_size=256),
                ),
            ),
            initialization="create_if_absent",
        )
        await write(stream, monkeypatch, T0, [0, 1, 2])
        await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [6])
            migrated.ensure_metadata()
            streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
            assert migrated.log is not None
            _retention.run(home(tmp_path), "t", migrated.log, now=NOW)

            def conflicted(*_args, **_kwargs):  # noqa: ANN202
                msg = "the metadata changed since it was read"
                raise _versions.Conflict(msg)

            monkeypatch.setattr(_versions, "commit", conflicted)
            with pytest.raises(_versions.Conflict):
                _retention.run(home(tmp_path), "t", migrated.log, now=NOW + GRACE)

            monkeypatch.undo()
            assert await offsets(tmp_path) == [4, 5, 6, 7]
        finally:
            await migrated.aclose()


@pytest.mark.replication
class TestThePublishedCopy:
    async def test_the_grace_starts_when_readers_elsewhere_see_the_floor(
        self, tmp_path, monkeypatch, s3, bucket
    ):
        """A reader on another machine resolves the copy under the published
        prefix, so the floor reaches it before the home: if it cannot, the
        home does not record a grace those readers never had — not even once
        a later commit carries the same state there."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            await write(stream, monkeypatch, T0, [0, 1, 2])
            await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
            stream.ensure_metadata()
            streamcast.StreamHandle.retain(
                "t", root=tmp_path, max_age=WINDOW, s3_options=s3
            )
            assert stream.log is not None

            def unreachable(*_args, **_kwargs):  # noqa: ANN202
                msg = "S3 did not answer"
                raise OSError(msg)

            with monkeypatch.context() as patched:
                patched.setattr(_versions._Remote, "write", unreachable)  # noqa: SLF001
                with pytest.raises(OSError, match="did not answer"):
                    _retention.run(
                        home(tmp_path), "t", stream.log, s3_options=s3, now=NOW
                    )

            _version, metadata, _manifest = _versions.load(home(tmp_path), "t")
            assert metadata is not None
            assert metadata.pending is None, "no floor the published copy lacks"

            # A commit that reaches both copies meanwhile, as an operator's is.
            streamcast.StreamHandle.retain(
                "t", root=tmp_path, max_age=WINDOW, s3_options=s3
            )

            begun = _retention.run(
                home(tmp_path), "t", stream.log, s3_options=s3, now=NOW + GRACE
            )
            assert begun is not None
            assert begun.pending is not None
            assert begun.pending.at == NOW + GRACE, "the grace starts here"
            assert lowest(stream.log) == 1, "nothing deleted yet"

            done = _retention.run(
                home(tmp_path), "t", stream.log, s3_options=s3, now=NOW + 2 * GRACE
            )
            assert done is not None
            assert done.pending is None
            assert lowest(stream.log) == 4
        finally:
            await stream.aclose()

    async def test_a_restart_never_takes_the_floor_off_the_published_copy(
        self, tmp_path, monkeypatch, s3, bucket
    ):
        """A plain `t.metadata.json` left beside the versions is the stream as it
        was. The broker syncs the published copy at start; built on that, it
        would publish the stream without its floor while the home still
        deletes below it, with no grace for those readers."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
                storage=streamcast.Storage(
                    root=Path(tmp_path).absolute(), published=bucket, s3_options=s3
                ),
            ),
            initialization="create_if_absent",
        )
        try:
            await write(stream, monkeypatch, T0, [0, 1, 2])
            await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
            stream.ensure_metadata()
            streamcast.StreamHandle.retain(
                "t", root=tmp_path, max_age=WINDOW, s3_options=s3
            )
            assert stream.log is not None
            before = _metadata.load(home(tmp_path), "t")
            assert before is not None
            _retention.run(home(tmp_path), "t", stream.log, s3_options=s3, now=NOW)
            (home(tmp_path) / "t.metadata.json").write_text(before.to_json())

            stream.ensure_metadata()  # what the broker does at the restart

            hint = _versions.hint_uri(home(tmp_path), "t", stream.log.published)
            async with await streamcast.StreamHandle.snapshot(
                hint, s3_options=s3
            ) as snap:
                assert snap.metadata.pending is not None, "the floor is still there"
                read = await snap.scan(columns=["litelink_offset"]).read_all()

            assert read.column("litelink_offset").to_pylist() == [4, 5, 6]
        finally:
            await stream.aclose()


class RootProbe:
    """Forward real DuckDB calls while observing each root's lifetime."""

    def __init__(self, connection):
        self.connection = connection
        self.closed = False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def close(self):
        self.connection.close()
        self.closed = True


def probe_roots(monkeypatch):
    roots = []
    factory = litelink.duckdb_connection

    def connect(*args, **kwargs):
        root = RootProbe(factory(*args, **kwargs))
        assert root.execute(
            "SELECT current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('enable_external_file_cache')"
        ).fetchone() == ("64.0 MiB", 1, False)
        assert duckdb.sql("SELECT current_setting('memory_limit')").fetchone() == (
            "16.0 MiB",
        )
        roots.append(root)
        return root

    monkeypatch.setattr(litelink, "duckdb_connection", connect)
    monkeypatch.setattr(_handle, "duckdb_connection", connect)
    return roots


def child_passes(monkeypatch, root, live_name, after, *, memory=64, spill=None):
    # No wall-clock wait: a completed sweep is the deterministic pass boundary.
    count = 0

    def completed(_delay):
        nonlocal count
        count += 1
        after(count)
        if count == 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(_retention, "EVERY", 0)
    monkeypatch.setattr(_maintain.time, "sleep", completed)
    monkeypatch.setattr(_maintain.signal, "signal", lambda *_: None)
    args = [
        "--log",
        str(home(root)),
        live_name,
        "--stream",
        str(home(root)),
        "t",
        "--role",
        "clean-published",
        "--every",
        "0.01",
        "--duckdb-default-memory-mib",
        "16",
        "--duckdb-default-threads",
        "1",
        "--duckdb-retention-memory-mib",
        str(memory),
        "--duckdb-retention-threads",
        "1",
    ]
    if spill is not None:
        args.extend(
            ["--duckdb-spill-dir", str(spill), "--duckdb-retention-spill-mib", "128"]
        )

    old = duckdb.default_connection()
    try:
        assert _maintain.main(args) == 0
    finally:
        duckdb.set_default_connection(old)


@pytest.mark.parametrize("retired", [False, True])
async def test_clean_published_retention_roots_capped_and_closed(
    tmp_path, monkeypatch, retired
):
    stream = await one_log(tmp_path, monkeypatch)
    await stream.aclose()
    live_name = "t"
    if retired:
        migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
        assert migrated.log is not None
        live_name = migrated.log.name
        await migrated.aclose()

    roots = probe_roots(monkeypatch)
    monkeypatch.setattr(_retention, "now_us", lambda: T0)

    def after(count):
        assert len(roots) == count
        assert all(root.closed for root in roots)
        for root in roots:
            with pytest.raises(duckdb.ConnectionException):
                root.execute("SELECT 1")

    child_passes(monkeypatch, tmp_path, live_name, after)


async def test_retention_over_cap_leaves_metadata(tmp_path, monkeypatch, capsys):
    stream = await one_log(tmp_path, monkeypatch)
    await stream.aclose()
    version, before, _manifest = _versions.load(home(tmp_path), "t")
    real_query = litelink.LogHandle.query_once
    failures = []

    def allocate(log, _sql, *, duckdb, **kwargs):
        # Force an actual pool allocation beyond the configured cap; the
        # production retention entry point and query_once cleanup still run.
        try:
            return real_query(
                log,
                "CREATE TABLE heavy AS SELECT i, repeat('x', 100) FROM range(300000) t(i)",
                duckdb=duckdb,
                **kwargs,
            )
        except Exception as exc:
            failures.append(exc)
            raise

    monkeypatch.setattr(litelink.LogHandle, "query_once", allocate)
    monkeypatch.setattr(_retention, "now_us", lambda: NOW)

    def after(count):
        assert len(failures) == count
        assert isinstance(failures[-1], duckdb.OutOfMemoryException)
        current, metadata, _manifest = _versions.load(home(tmp_path), "t")
        assert current == version
        assert metadata == before

    child_passes(monkeypatch, tmp_path, "t", after, memory=16)
    assert capsys.readouterr().err.count("retrying next pass") == 3


async def test_child_clears_spill_dir_at_start(tmp_path, monkeypatch):
    stream = await one_log(tmp_path, monkeypatch)
    await stream.aclose()
    leaf = tmp_path / "missing" / "parents" / "maintain" / "clean-published-key"
    leaf.mkdir(parents=True)
    (leaf / "leftover").touch()
    roots = probe_roots(monkeypatch)
    real_open = litelink.open

    def open_log(*args, **kwargs):
        assert leaf.is_dir()
        assert list(leaf.iterdir()) == []
        return real_open(*args, **kwargs)

    monkeypatch.setattr(litelink, "open", open_log)
    monkeypatch.setattr(_retention, "now_us", lambda: T0)

    def after(count):
        assert len(roots) == count
        assert all(root.closed for root in roots)

    child_passes(monkeypatch, tmp_path, "t", after, spill=leaf)
    leaf.rmdir()
    # The child itself must also create the entire missing path.
    leaf.parent.rmdir()
    roots.clear()
    child_passes(monkeypatch, tmp_path, "t", after, spill=leaf)


async def test_child_spill_create_failure_is_nonfatal(tmp_path, monkeypatch, caplog):
    stream = await one_log(tmp_path, monkeypatch)
    await stream.aclose()
    leaf = tmp_path / "missing" / "maintain" / "child"
    real_mkdir = Path.mkdir

    def mkdir(path, *args, **kwargs):
        if path == leaf:
            raise PermissionError("read-only child spill directory")

        return real_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(_retention, "now_us", lambda: T0)
    roots = probe_roots(monkeypatch)

    def after(count):
        assert len(roots) == count
        assert all(root.closed for root in roots)

    child_passes(monkeypatch, tmp_path, "t", after, spill=leaf)
    assert "could not create DuckDB spill directory" in caplog.text
    assert not leaf.exists()


@pytest.mark.parametrize(
    "published", ["s3://example/tables", "file:///local/tables", "/local/tables"]
)
def test_published_root_only_resolves_s3_for_remote_tables(monkeypatch, published):
    options = litelink.S3Options(access_key="test-access", secret_key="test-secret")
    policy = litelink.DuckDBPolicy(64, 1, memory_cache=False)
    calls, closed = [], []
    root = SimpleNamespace(close=lambda: closed.append(True))

    def connect(**kwargs):
        calls.append(kwargs)
        return root

    monkeypatch.setattr(litelink, "duckdb_connection", connect)
    with _retention._published_root(published, options, policy) as opened:
        assert opened is root and not closed

    assert closed == [True]
    assert calls == [
        {
            "s3_options": options.resolved() if published.startswith("s3://") else None,
            "duckdb": policy,
        }
    ]


@pytest.mark.parametrize(
    "policy", [None, litelink.DuckDBPolicy(64, 1, memory_cache=False)]
)
async def test_local_published_retention_without_aws(tmp_path, monkeypatch, policy):
    # Local tables must work even when no credential chain can resolve.
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=tmp_path, config=litelink.LogConfig(target_compact_size=256)
            ),
        ),
        initialization="create_if_absent",
    )
    await write(stream, monkeypatch, T0, [0, 1, 2])
    await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
    stream.ensure_metadata()
    await stream.aclose()
    migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
    streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
    try:
        assert migrated.log is not None
        begun = _retention.run(
            home(tmp_path), "t", migrated.log, duckdb=policy, now=NOW
        )
        assert begun is not None and begun.pending is not None
        assert begun.sealed_logs[0].start_offset == 4
        done = _retention.run(
            home(tmp_path), "t", migrated.log, duckdb=policy, now=NOW + GRACE
        )
        assert done is not None and done.pending is None
        _version, _metadata, manifest = _versions.load(home(tmp_path), "t")
        assert manifest is not None and _retention._count(manifest, "t") == 3
        assert await offsets(tmp_path) == [4, 5, 6]
    finally:
        await migrated.aclose()


async def test_finish_recount_failure_then_retry_completes(
    tmp_path, monkeypatch, capsys
):
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=tmp_path, config=litelink.LogConfig(target_compact_size=256)
            ),
        ),
        initialization="create_if_absent",
    )
    await write(stream, monkeypatch, T0, [0, 1, 2])
    await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
    stream.ensure_metadata()
    await stream.aclose()
    migrated = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=WIDER)
    streamcast.StreamHandle.retain("t", root=tmp_path, max_age=WINDOW)
    assert migrated.log is not None
    live_name = migrated.log.name
    _retention.run(home(tmp_path), "t", migrated.log, now=NOW)
    await migrated.aclose()
    roots = probe_roots(monkeypatch)
    real_open = _published.Table.open
    attempts = 0

    def fail_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("recount unavailable")

        return real_open(*args, **kwargs)

    monkeypatch.setattr(_published.Table, "open", fail_once)
    monkeypatch.setattr(_retention, "now_us", lambda: NOW + GRACE)

    def after(count):
        _version, metadata, manifest = _versions.load(home(tmp_path), "t")
        assert metadata is not None
        assert manifest is not None
        if count == 1:
            assert metadata.pending is not None
            assert _retention._count(manifest, "t") is None
        else:
            assert metadata.pending is None
            assert _retention._count(manifest, "t") == 3

        assert all(root.closed for root in roots)

    child_passes(monkeypatch, tmp_path, live_name, after)
    assert attempts >= 2
    assert len(roots) >= 2
    assert "recount unavailable" in capsys.readouterr().err


def rss_child():
    """Run just the clean-published child in a fresh Linux interpreter."""
    samples = []
    retained_roots = []
    handles = []
    real_sweep = _maintain.sweep
    trim = ctypes.CDLL(None).malloc_trim
    trim.argtypes = [ctypes.c_size_t]
    trim.restype = ctypes.c_int

    def sample():
        gc.collect()
        pa.default_memory_pool().release_unused()
        trim(0)
        status = Path("/proc/self/status").read_text()
        return (
            int(
                next(
                    line.split()[1]
                    for line in status.splitlines()
                    if line.startswith("RssAnon:")
                )
            )
            * 1024
        )

    def sweep(*args, **kwargs):
        handles.extend(log for _name, log in args[0])
        # Extension code and allocator arenas are fixed interpreter costs.
        # Warm them before the idle baseline, with no retained root.
        litelink.duckdb_connection(
            s3_options=litelink.S3Options().resolved(),
            duckdb=litelink.DuckDBPolicy(64, 1, memory_cache=False),
        ).close()
        samples.append(sample())
        return real_sweep(*args, **kwargs)

    def completed(_delay):
        samples.append(sample())
        retained_roots.append(
            sum(log._reader._connection is not None for log in handles)
        )
        if len(samples) == 5:
            raise KeyboardInterrupt

    with (
        patch.object(_retention, "EVERY", 0),
        patch.object(_retention, "now_us", lambda: T0),
        patch.object(_maintain, "sweep", sweep),
        patch.object(_maintain.time, "sleep", completed),
    ):
        assert _maintain.main(sys.argv[1:]) == 0

    print(json.dumps({"rss": samples, "retained_roots": retained_roots}), flush=True)


@pytest.mark.slow
@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux RssAnon probe")
async def test_clean_published_rss_returns_after_passes(tmp_path, monkeypatch):
    args = []
    schema = {
        "type": "object",
        "properties": {"payload": {"type": "string"}},
        "required": ["payload"],
    }
    for index in range(5):
        name = f"t{index}"
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name=name, schema=schema),
                storage=streamcast.Storage(root=tmp_path),
            ),
            initialization="create_if_absent",
        )
        with monkeypatch.context() as patched:
            patched.setattr(_stream.time, "time_ns", lambda: T0 * 1000)
            await stream.send({"payload": "published history"})
            assert stream.log is not None
            stream.log.seal(flush=True)
            stream.log.publish(flush=True)
            await stream.send_many(
                [{"payload": "x" * (20 * 1024)} for _ in range(1000)]
            )
            stream.log.seal(flush=True)
            stream.log.publish(flush=True)

        stream.ensure_metadata()
        await stream.aclose()
        streamcast.StreamHandle.retain(
            name, root=tmp_path, max_age=timedelta(days=index % 3 + 1)
        )
        args.extend(["--log", str(_metadata.home(tmp_path, name)), name])
        args.extend(["--stream", str(_metadata.home(tmp_path, name)), name])

    args.extend(
        [
            "--role",
            "clean-published",
            "--every",
            "0.01",
            "--duckdb-default-memory-mib",
            "16",
            "--duckdb-default-threads",
            "1",
            "--duckdb-retention-memory-mib",
            "64",
            "--duckdb-retention-threads",
            "1",
        ]
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from tests.test_retention import rss_child; rss_child()",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=40,
        check=True,
    )
    measured = json.loads(result.stdout.splitlines()[-1])
    samples = measured["rss"]
    print(f"RssAnon bytes (baseline, four idle troughs): {samples}")
    # The RSS allowance covers allocator and extension overhead. A leaked
    # reader can remain below it on this fixture, so also check the resource
    # itself at the same idle boundary, before main closes the handles.
    assert measured["retained_roots"] == [0, 0, 0, 0], measured
    assert len(samples) == 5
    baseline, *troughs = samples
    assert max(troughs) <= baseline + 64 * 1024 * 1024, samples


@pytest.mark.skipif(
    os.geteuid() == 0, reason="requires unprivileged directory permissions"
)
async def test_child_untraversable_spill_parent_is_nonfatal(
    tmp_path, monkeypatch, caplog
):
    stream = await one_log(tmp_path, monkeypatch)
    await stream.aclose()
    locked = tmp_path / "locked"
    locked.mkdir()
    leaf = locked / "spill" / "child"
    roots = probe_roots(monkeypatch)
    monkeypatch.setattr(_retention, "now_us", lambda: T0)

    def after(count):
        assert len(roots) == count
        assert all(root.closed for root in roots)

    locked.chmod(0)
    try:
        child_passes(monkeypatch, tmp_path, "t", after, spill=leaf)
        assert "could not create DuckDB spill directory" in caplog.text
    finally:
        locked.chmod(0o700)

    assert not (locked / "spill").exists()
