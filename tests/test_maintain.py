"""The maintainer: the half of the library that keeps a log from growing.

Nothing here sealed before `_maintain` existed, and the reason it went
unnoticed is worth knowing: litelink's `target_seal_size` defaults to 8 MiB,
so a short test or a short demo never crosses it and every log looks fine.
These tests deliberately cross the threshold — a small one, set on the log,
because what is under test is who calls `seal`, not where the line is,
and crossing 8 MiB took 100,000 rows and most of these tests' run time.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from pathlib import Path

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast import _maintain
from streamcast._maintain import ROLES, Maintain, Supervisor

# ~140 bytes a row, so ROWS is ~560 KB — comfortably past SEAL_SIZE, at
# which litelink's policy says a seal is due.
PAD = "x" * 120
SEAL_SIZE = 256 * 1024
ROWS = 4_000

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("tag", pa.string()),
    ]
)


def rows(lo: int, hi: int) -> list[dict]:
    return [
        {"event_ts": i, "price": 1.0 * i, "tag": f"{i:08}{PAD}"} for i in range(lo, hi)
    ]


@pytest.fixture
def wide_log(tmp_path):
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=litelink.LogConfig(target_seal_size=SEAL_SIZE),
    )
    with handle:
        yield handle


async def fill(stream, total=ROWS, chunk=500):
    for start in range(0, total, chunk):
        await stream.send_many(rows(start, start + chunk))
        # A publisher yields between groups; see `Stream.send`.
        await asyncio.sleep(0)


async def settle(log, *, want_files=1, timeout=20.0):
    """Wait for the maintainer to do something, or give up."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if log.staging_files() >= want_files:
            return True

        await asyncio.sleep(0.25)

    return False


class TestTheDefect:
    @pytest.mark.slow
    async def test_without_a_maintainer_nothing_ever_seals(self, wide_log):
        """The state this library shipped in, pinned.

        Not a litelink bug: `seal` respects the policy, and the policy is
        satisfied here — ~560 KB against a 256 KiB target. It simply never runs,
        because nothing calls it. The buffer grows for the life of the server
        and the DuckDB read cache mirrors it.
        """
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=False):
            await fill(stream)

            # **No sleep, deliberately.** This used to wait two seconds and
            # then check that nothing had sealed, which proves a negative by
            # hoping: on a slower box two seconds is not evidence, and on any
            # box it is two seconds of nothing. Nothing here is SCHEDULED to
            # seal — `maintain=False` starts no subprocess and the library
            # calls `seal` nowhere else — so there is no race for a wait
            # to lose, and a longer one could not catch anything.
            #
            # What proves the policy was satisfied is the POSITIVE CONTROL
            # below: `test_with_one_the_buffer_drains_into_parquet` runs the
            # same fixture through the same `fill` and does seal. An empty
            # table here means the maintainer, not an unmet threshold.
            # (`seal()` would answer it directly and must not be called —
            # it SEALS, which is the whole point of this test.)
            assert wide_log.buffered_rows() == ROWS
            assert wide_log.staging_files() == 0

    @pytest.mark.slow
    async def test_with_one_the_buffer_drains_into_parquet(self, wide_log):
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=FAST):
            await fill(stream)
            assert await settle(wide_log), "the maintainer sealed nothing"

            assert wide_log.staging_files() >= 1
            assert wide_log.buffered_rows() < ROWS

    @pytest.mark.slow
    async def test_sealed_rows_leave_the_buffer(self, wide_log):
        """Sealing moves rows; only `advance()` deletes them from `buffer.db`.

        `buffered_rows()` counts UNSEALED rows, so it drops on a seal alone and
        cannot see this: a maintainer that sealed and published but never ran
        litelink's `evict("buffer")` would pass every other test here while
        `buffer.db` held every row it was ever sent. Counted in the file.
        """
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=FAST):
            await fill(stream)
            assert await settle(wide_log)

            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and in_buffer(wide_log) >= ROWS:
                await asyncio.sleep(0.25)

            assert in_buffer(wide_log) < ROWS, "sealed rows were never evicted"

    @pytest.mark.slow
    async def test_a_replay_still_spans_the_seam_the_maintainer_made(self, wide_log):
        # The maintainer moves rows from the buffer into Parquet underneath a
        # running server. A replay has to cross that boundary without noticing.
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=FAST) as server:
            # All of ROWS: fewer would leave the buffer under the target,
            # with nothing due, and this would assert on a seal the policy
            # correctly declined to make.
            await fill(stream)
            assert await settle(wide_log)

            port = server.sockets[0].getsockname()[1]
            uri = f"ws://127.0.0.1:{port}/trades"
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                got = [await sub.recv() for _ in range(500)]

        assert [offset for offset, _ts, _row in got] == list(range(1, 501))
        assert got[0][2]["event_ts"] == 0


def _a_log(root, name):
    """Another durable log, for the tests about covering more than one."""
    import litelink
    import pyarrow as pa

    return litelink.new(
        root, name, schema=pa.schema([pa.field("n", pa.int64(), nullable=False)])
    )


class TestLifecycle:
    async def test_a_live_only_stream_starts_no_process(self, serve):
        # Nothing to sweep, so nothing is spawned — not a process with no work.
        from streamcast._server import _supervisors

        assert _supervisors({"a": streamcast.Stream("a")}, True) == []

    async def test_maintain_false_starts_nothing(self, log):
        from streamcast._server import _supervisors

        assert _supervisors({"a": streamcast.Stream("a", log=log)}, False) == []

    async def test_one_set_of_five_covers_every_log(self, log, tmp_path):
        """**Five processes for the server — one per role — not five per log.**

        Each maintainer is a full interpreter with litelink, pyarrow,
        pyiceberg and duckdb loaded — ~150-200 MB RSS — so a set per log made
        adding a stream that takes a row a minute cost as much as the busiest
        one. The roles are split from each other because their work is; the
        logs are not.

        Falsify by returning a set of `Supervisor`s per log in `_supervisors`.
        """
        from streamcast._server import _supervisors

        others = [_a_log(tmp_path / f"extra{i}", f"s{i}") for i in range(3)]
        try:
            routes = {
                "a": streamcast.Stream("a", log=log),
                "live": streamcast.Stream("live"),
            }
            routes.update(
                {
                    handle.name: streamcast.Stream(handle.name, log=handle)
                    for handle in others
                }
            )
            made = _supervisors(routes, True)

            assert sorted(sup.role for sup in made) == sorted(ROLES), (
                "one process per role, and only one"
            )
            # By the LOG's name, not the route key: the maintainer opens the
            # log, and `serve` may route it under a different path.
            for sup in made:
                assert {name for _root, name in sup.targets} == {
                    "trades",
                    "s0",
                    "s1",
                    "s2",
                }, f"a log was left out of {sup.role}"

        finally:
            for handle in others:
                handle.close()

    async def test_dedicated_names_a_log_that_gets_its_own(self, log, tmp_path):
        """The opt-out, for a log large or hot enough that its compaction
        would hold up everyone else's.

        The shared loop sweeps in series, so that delay is real — it is just a
        fine trade for the small streams sharing exists for.
        """
        from streamcast._maintain import Maintain
        from streamcast._server import _supervisors

        busy = _a_log(tmp_path / "busy", "busy")
        try:
            routes = {
                "a": streamcast.Stream("a", log=log),
                "busy": streamcast.Stream("busy", log=busy),
            }
            made = _supervisors(routes, Maintain(dedicated=("busy",)))

            by_role = {
                role: sorted(
                    sorted(n for _r, n in sup.targets)
                    for sup in made
                    if sup.role == role
                )
                for role in ROLES
            }
            # Every role twice: once for the dedicated log, once shared.
            assert by_role == {role: [["busy"], ["trades"]] for role in ROLES}

        finally:
            busy.close()

    async def test_dedicating_every_log_starts_no_empty_shared_process(
        self, log, tmp_path
    ):
        """**No maintainer with nothing to maintain.**

        `dedicated` naming every log leaves the shared set empty, and a
        `Supervisor` over no logs would be a full interpreter — 149 MB RSS
        measured — sweeping nothing for the life of the server. That is the
        cost this whole change exists to remove, reappearing as a rounding
        error in the opt-out.

        Falsify by appending the shared `Supervisor` unconditionally.
        """
        from streamcast._maintain import Maintain
        from streamcast._server import _supervisors

        other = _a_log(tmp_path / "other", "other")
        try:
            routes = {
                "a": streamcast.Stream("a", log=log),
                "b": streamcast.Stream("b", log=other),
            }
            made = _supervisors(routes, Maintain(dedicated=("trades", "other")))

            assert len(made) == 2 * len(ROLES), "an empty shared set was started"
            assert sorted(sorted(n for _r, n in s.targets) for s in made) == (
                [["other"]] * len(ROLES) + [["trades"]] * len(ROLES)
            )
            assert all(sup.targets for sup in made), "a maintainer sweeps nothing"

        finally:
            other.close()

    async def test_dedicating_a_name_nothing_serves_is_refused(self, log):
        """A typo would otherwise put the log back in the shared loop.

        Which is the one thing the caller named it to avoid, with the symptom
        being a latency problem they believe they already fixed. `_routes`
        refuses a name collision rather than resolving one, for the same
        reason.
        """
        from streamcast._maintain import Maintain
        from streamcast._server import _supervisors

        with pytest.raises(ValueError, match="does not serve with a log") as raised:
            _supervisors(
                {"a": streamcast.Stream("a", log=log)}, Maintain(dedicated=("trade",))
            )

        # It names what IS served, and that the name is the log's.
        assert "'trades'" in str(raised.value)
        assert "LOG's" in str(raised.value)

    async def test_a_live_only_stream_cannot_be_dedicated(self):
        """It has no log, so there is nothing for a maintainer to sweep."""
        from streamcast._maintain import Maintain
        from streamcast._server import _supervisors

        with pytest.raises(ValueError, match="does not serve with a log"):
            _supervisors({"a": streamcast.Stream("a")}, Maintain(dedicated=("a",)))

    async def test_the_maintainer_stops_when_the_server_closes(self, log):
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0)
        alive = list(server._children)  # noqa: SLF001
        assert len(alive) == len(ROLES)
        for child in alive:
            assert child._process is not None  # noqa: SLF001
            assert child._process.poll() is None  # noqa: SLF001

        server.close()
        await server.wait_closed()

        # Terminated AND reaped: a dropped reference here is one zombie per
        # server for the life of the parent.
        for child in alive:
            assert child._process is None  # noqa: SLF001
            assert child._stopped is None  # noqa: SLF001

    async def test_a_maintainer_that_dies_is_restarted(self, log):
        """The direction that matters.

        A maintainer dying unnoticed puts the server straight back to never
        sealing, which is invisible until the disk fills. litelink's leases
        lapse, so the replacement takes them cleanly.
        """
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0)
        supervisor = server._children[0]  # noqa: SLF001
        try:
            first = supervisor._process  # noqa: SLF001
            assert first is not None
            first.kill()

            for _ in range(200):
                current = supervisor._process  # noqa: SLF001
                if current is not None and current is not first:
                    break

                await asyncio.sleep(0.05)

            assert current is not first, "it was not restarted"
            assert current.poll() is None

        finally:
            server.close()
            await server.wait_closed()

    async def test_the_server_still_proxies_what_websockets_exposes(self, log):
        # `_Served` wraps rather than replaces, so everything else is reached
        # through `__getattr__` untouched.
        stream = streamcast.Stream("trades", log=log)
        async with streamcast.serve(stream, "127.0.0.1", 0) as server:
            assert server.sockets
            assert server.is_serving()
            async with streamcast.connect(
                f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/trades"
            ) as sub:
                await stream.send({"event_ts": 1, "price": 1.0})
                assert (await sub.recv())[0] == 1

            assert len(server.connections) >= 0


def test_the_cadences_differ_because_the_costs_do():
    # `seal` is an indexed read when idle; every other role reads table
    # metadata or waits on a network. A single interval would make one wrong.
    plan = Maintain()
    for role in ROLES:
        if role != "seal":
            assert plan.every(role) / plan.seal_every >= 10, role

    # The published table's cleanup lists a bucket: rarest of all.
    assert plan.clean_published_every > plan.clean_every


def test_there_is_no_thread_mode_to_get_wrong():
    """litelink measured appends 45.2 ms behind an in-process seal.

    On a fan-out server that is 45 ms with nothing fanned out and no keepalive
    answered — latency spikes that look like a network problem. A flag whose
    wrong setting is invisible until production should not exist, so this
    asserts the ABSENCE of one rather than its default.
    """
    import inspect

    assert "thread" not in inspect.signature(Maintain.__init__).parameters
    assert "thread" not in inspect.signature(streamcast.serve).parameters
    assert set(Maintain.__dataclass_fields__) == {
        "seal_every",
        "compact_every",
        "publish_every",
        "clean_every",
        "clean_published_every",
        # Names logs that get their own PROCESS, which is the opt-out from
        # sharing one — still not a thread, and there is no way to ask for
        # one.
        "dedicated",
        # When `seal` and `publish` pass `flush=True`: the RPO interval.
        "flush_every",
    }

    # And the child is a real subprocess, not a thread pretending to be one.
    spawn = inspect.getsource(Supervisor._spawn)
    assert "subprocess.Popen" in spawn
    assert "popen(" in spawn
    assert "sys.executable" in inspect.getsource(Supervisor._spawn_argv)


# `TestWalReplication` lived here and tested that `serve` REFUSED a log with
# `wal_replication` on, because the maintainer did not run litestream. It now
# does — see `test_replicate.py`, which exercises the sidecar against a real
# endpoint rather than asserting an apology.


# Every slow role once a second, so a test sees a whole pipeline turn over.
FAST = Maintain(
    compact_every=1.0, publish_every=1.0, clean_every=1.0, clean_published_every=1.0
)


def in_buffer(log) -> int:
    """Rows physically in `buffer.db`, sealed or not — read-only, beside the writer."""
    path = Path(log.root) / log.name / "buffer.db"
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
        return db.execute("SELECT count(*) FROM buffer").fetchone()[0]


class Recorder:
    """A log that records whether each call was flushed."""

    def __init__(self, failing: int = 0) -> None:
        self.calls: list[tuple[str, bool]] = []
        self._failing = failing

    def _record(self, step: str, flush: bool) -> None:  # noqa: FBT001
        self.calls.append((step, flush))
        if flush and self._failing:
            self._failing -= 1
            msg = "the bucket is unreachable"
            raise OSError(msg)

    def seal(self, *, flush: bool = False) -> None:
        self._record("seal", flush)

    def publish(self, *, flush: bool = False) -> None:
        self._record("publish", flush)

    def compact(self) -> None:
        self._record("compact", False)  # noqa: FBT003


def passes(monkeypatch, role: str, log: Recorder, at: list[float]) -> list[bool]:
    """Run `sweep` for one pass at each wall-clock time in `at`.

    The clock is the test's; every pass is due, since what is under test is
    which ones flush, not the cadence.
    """
    clock = iter(at)
    now = [next(clock)]

    def sleep(_: float) -> None:
        try:
            now[0] = next(clock)
        except StopIteration:
            raise KeyboardInterrupt from None

    monkeypatch.setattr(_maintain.time, "time", lambda: now[0])
    monkeypatch.setattr(_maintain.time, "sleep", sleep)
    with contextlib.suppress(KeyboardInterrupt):
        _maintain.sweep([("t", log)], role, 1e-9, flush_every=60.0)  # ty: ignore[invalid-argument-type]

    return [flush for _step, flush in log.calls]


class TestFlushing:
    """`flush_every`: the seal and publish passes that bound RPO."""

    def test_the_seal_flushes_once_per_boundary(self, monkeypatch):
        # Boundaries at 1020 and 1080: the first pass after each flushes.
        got = passes(monkeypatch, "seal", Recorder(), [1010, 1015, 1021, 1030, 1081])
        assert got == [False, False, True, False, True]

    def test_the_publish_flushes_after_the_seal_has(self, monkeypatch):
        """A few seconds past the boundary, so it pushes what the seal cut."""
        got = passes(monkeypatch, "publish", Recorder(), [1010, 1021, 1026, 1030])
        assert got == [False, False, True, False]

    def test_a_flush_that_failed_is_tried_again_next_pass(self, monkeypatch):
        got = passes(
            monkeypatch, "publish", Recorder(failing=1), [1010, 1026, 1027, 1030]
        )
        assert got == [False, True, True, False]

    def test_compaction_is_never_flushed(self, monkeypatch):
        """`compact(flush=True)` rewrites the in-progress file every call."""
        got = passes(monkeypatch, "compact", Recorder(), [1010, 1021, 1081])
        assert got == [False, False, False]

    def test_only_seal_and_publish_are_told_the_interval(self, tmp_path):
        plan = Maintain(flush_every=30.0)
        for role in ROLES:
            argv = Supervisor([(tmp_path, "t")], plan, role)._spawn_argv()  # noqa: SLF001
            assert ("--flush-every" in argv) == (role in {"seal", "publish"}), role

        argv = Supervisor([(tmp_path, "t")], Maintain(flush_every=None), "seal")
        assert "--flush-every" not in argv._spawn_argv()  # noqa: SLF001

    def test_the_interval_is_positive_or_none(self):
        with pytest.raises(ValueError, match="flush_every"):
            Maintain(flush_every=0)

    def test_a_graceful_stop_publishes_what_is_sealed(self, wide_log):
        """The publish role's last work, with no flush due: what is sealed
        leaves the machine. Called directly, since a server stopped at once
        would kill the child before its SIGTERM handler is installed."""
        for r in rows(0, 50):
            wide_log.append(r)

        while wide_log.seal(flush=True) is not None:
            pass

        _maintain.on_exit([("trades", wide_log)], "compact", 60.0)
        assert wide_log.published_through() == 0, "only the publish role pushes"

        _maintain.on_exit([("trades", wide_log)], "publish", 60.0)
        assert wide_log.published_through() == 50

    def test_none_publishes_nothing_on_the_way_out(self, wide_log):
        for r in rows(0, 50):
            wide_log.append(r)

        while wide_log.seal(flush=True) is not None:
            pass

        _maintain.on_exit([("trades", wide_log)], "publish", None)
        assert wide_log.published_through() == 0

    async def test_a_served_row_reaches_the_published_table(self, wide_log):
        """End to end: unflushed, litelink publishes only 512 MiB files, so on
        this stream nothing would be published at all."""
        stream = streamcast.Stream("trades", log=wide_log)
        plan = Maintain(flush_every=1.0, publish_every=0.25, compact_every=60.0)
        server = await streamcast.serve(stream, "127.0.0.1", 0, maintain=plan)
        try:
            await stream.send_many(rows(0, 50))
            deadline = time.monotonic() + 10
            while wide_log.published_through() < 50 and time.monotonic() < deadline:
                await asyncio.sleep(0.1)

            assert wide_log.published_through() == 50
        finally:
            server.close()
            await server.wait_closed()


class Recording(Supervisor):
    """A supervisor that runs nothing, and records when it is stopped."""

    __slots__ = ("events",)

    def __init__(self, role: str, events: list[str]) -> None:
        super().__init__([], Maintain(), role)
        self.events = events

    def terminate(self) -> None:
        self.events.append(f"terminate {self.role}")

    async def wait_closed(self) -> None:
        await asyncio.sleep(0)
        self.events.append(f"closed {self.role}")


class TestStopping:
    """The order the exit flushes need, while the server keeps taking writes."""

    async def test_seal_then_publish_then_the_rest(self):
        events: list[str] = []
        children = [Recording(role, events) for role in reversed(list(ROLES))]
        await _maintain.stop(children)

        assert events[:4] == [
            "terminate seal",
            "closed seal",
            "terminate publish",
            "closed publish",
        ]
        assert sorted(events[4:]) == sorted(
            f"{step} {role}"
            for role in ("compact", "clean", "clean-published")
            for step in ("terminate", "closed")
        )

    async def test_the_server_takes_writes_while_the_maintainers_finish(
        self, log, monkeypatch
    ):
        from streamcast import _server  # noqa: PLC0415

        gate = asyncio.Event()

        class Finishing:
            def start(self) -> None:
                pass

            def terminate(self) -> None:
                pass

            async def wait_closed(self) -> None:
                await gate.wait()

        monkeypatch.setattr(_server, "_supervisors", lambda *_: [Finishing()])
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0)
        uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/trades"
        server.close()
        try:
            async with streamcast.publish(uri) as producer:
                offset = await asyncio.wait_for(producer.send(rows(0, 1)[0]), 5)

            assert offset == 1
        finally:
            gate.set()
            await server.wait_closed()
