"""Time-based retention (#118): a stream's rows older than its window leave it.

Off unless set. Set, a pass publishes the stream starting at its floor — the
smallest offset whose `streamcast_ts` is newer than the cutoff — and only
after readers' grace deletes what is below it, from the live log and every
retired one. The clock is the test's: rows are stamped at chosen times, and
passes run at chosen times, rather than waiting out a window.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import timedelta
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _metadata, _retention, _stream, _versions
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
    stream: streamcast.Stream, monkeypatch, at: int, values: list[int]
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
    async with await streamcast.Stream.snapshot(uri) as snap:
        read = await snap.scan(columns=["litelink_offset"]).read_all()

    return read.column("litelink_offset").to_pylist()


async def one_log(tmp_path, monkeypatch) -> streamcast.Stream:
    """Three rows two days old, three rows new; a window of a day."""
    stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
    await write(stream, monkeypatch, T0, [0, 1, 2])
    await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
    stream.ensure_metadata()
    streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
    return stream


NOW = T0 + 2 * DAY + 60_000_000
"""A minute after the new rows: the old ones are past the window."""


class TestTheSetting:
    async def test_it_is_off_unless_set(self, tmp_path, monkeypatch):
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
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
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        await stream.aclose()

        streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
        metadata = _metadata.load(home(tmp_path), "t")
        assert metadata is not None
        assert metadata.retention == DAY

        streamcast.Stream.retain("t", root=tmp_path, max_age=None)
        cleared = _metadata.load(home(tmp_path), "t")
        assert cleared is not None
        assert cleared.retention is None

    def test_a_window_is_positive(self, tmp_path):
        with pytest.raises(ValueError, match="positive"):
            streamcast.Stream.retain("t", root=tmp_path, max_age=timedelta(0))


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
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            await write(stream, monkeypatch, T0, [0])
            await write(stream, monkeypatch, T0 + 2 * DAY, [1])
            await write(stream, monkeypatch, T0, [2])  # the clock stepped back
            stream.ensure_metadata()
            streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
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
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        await write(stream, monkeypatch, T0, [0, 1, 2])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
            migrated.ensure_metadata()
            streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
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
        stream = streamcast.Stream.new(
            "t",
            root=tmp_path,
            schema=SCHEMA,
            config=litelink.LogConfig(target_compact_size=256),
        )
        await write(stream, monkeypatch, T0, [0, 1, 2])
        await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [6])
            migrated.ensure_metadata()
            streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
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
    from streamcast import _maintain  # noqa: PLC0415

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
        stream = streamcast.Stream.new("t", root=tmp_path, schema=SCHEMA)
        try:
            await write(stream, monkeypatch, T0, [0, 1, 2])
            stream.ensure_metadata()
            streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
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
        """`Stream.retain` committing while a pass is in flight: the pass is
        refused, rather than putting the old window back."""
        stream = await one_log(tmp_path, monkeypatch)
        try:
            assert stream.log is not None
            real = _retention._newest  # noqa: SLF001

            def widened(live, s3_options):  # noqa: ANN001, ANN202
                streamcast.Stream.retain("t", root=tmp_path, max_age=None)
                return real(live, s3_options)

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
        """The plain copy is written after the hint: between the two — or
        after a crash there — it is the old version under the new one's name.
        A pass built on it would put the old window back over the new."""
        stream = await one_log(tmp_path, monkeypatch)
        try:
            plain = home(tmp_path) / "t.metadata.json"
            before = plain.read_bytes()
            streamcast.Stream.retain("t", root=tmp_path, max_age=timedelta(days=30))
            plain.write_bytes(before)  # as if the copy had not been written yet

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
        stream = streamcast.Stream.new(
            "t",
            root=tmp_path,
            schema=SCHEMA,
            config=litelink.LogConfig(target_compact_size=256),
        )
        await write(stream, monkeypatch, T0, [0, 1, 2])
        await write(stream, monkeypatch, T0 + 2 * DAY, [3, 4, 5])
        stream.ensure_metadata()
        await stream.aclose()
        migrated = streamcast.Stream.migrate("t", root=tmp_path, schema=WIDER)
        try:
            await write(migrated, monkeypatch, T0 + 2 * DAY, [6])
            migrated.ensure_metadata()
            streamcast.Stream.retain("t", root=tmp_path, max_age=WINDOW)
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
