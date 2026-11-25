"""Broker plans are permission to open logs, scoped to one interpreter lifetime."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import cast

import litelink
import pyarrow as pa
import pytest
from msgspec.structs import replace

from streamcast import _maintain, _plan, _spill
from streamcast._broker.model import ExternalMaintain
from streamcast._external import External
from streamcast._stream import StreamHandle


class Reexec(BaseException):
    pass


class Deadline:
    """Fire explicitly from a blocked exit hook, without scheduler timing."""

    def __init__(self, interval, function, args):
        self.interval = interval
        self.function = function
        self.args = args
        self.daemon = False
        self.cancelled = False

    def start(self):
        pass

    def cancel(self):
        self.cancelled = True


@pytest.fixture
def run_dir(tmp_path):
    directory = tmp_path / "run"
    directory.mkdir()
    return directory


def plan(*, boot="boot-a", generation=1, state="serving", roles=None):
    streamcast, litelink_version = _plan.versions()
    return _plan.Plan(
        boot,
        generation,
        state,
        streamcast,
        litelink_version,
        roles
        if roles is not None
        else {"seal.default": _plan.RolePlan(0.25, logs=(("/data", "events"),))},
    )


def args(run_dir, role="seal", group="default"):
    return ["--follow", str(run_dir), "--role", role, "--group", group]


def terminate():
    os.kill(os.getpid(), signal.SIGTERM)


def status(run_dir, role="seal", group="default"):
    return _plan.read_status(run_dir, role, group)


@pytest.fixture
def fake_log(monkeypatch):
    opened = []
    closed = []
    flushed = []

    class Log:
        root = Path("/data")
        name = "events"

        def seal(self, *, flush):
            flushed.append(flush)
            return None

    @contextlib.contextmanager
    def open_log(root, name):
        opened.append((root, name))
        try:
            yield Log()
        finally:
            closed.append(name)

    monkeypatch.setattr(litelink, "open", open_log)
    return opened, closed, flushed


def test_role_acts_only_on_serving_plan_of_current_boot(run_dir, monkeypatch, fake_log):
    opened, closed, flushed = fake_log
    old = plan(state="fenced")
    _plan.write_plan(run_dir, old)

    def next_plan(_seconds):
        assert opened == []
        assert status(run_dir).applied_generation == 1
        _plan.write_plan(run_dir, replace(old, boot_id="boot-b", state="serving"))

    def sweep(*_args, follow):
        assert follow.plan.boot_id == "boot-b"
        assert status(run_dir).boot_id == "boot-b"
        assert status(run_dir).logs_open == 1
        terminate()

    monkeypatch.setattr(_maintain.time, "sleep", next_plan)
    monkeypatch.setattr(_maintain, "sweep", sweep)
    assert _maintain.main(args(run_dir)) == 0
    assert opened == [(Path("/data"), "events")]
    assert closed == ["events"]
    assert flushed == [True]


def test_role_refuses_version_mismatch_opens_nothing(run_dir, monkeypatch, fake_log):
    _plan.write_plan(run_dir, replace(plan(), litelink="incompatible"))

    def waiting(_seconds):
        observed = status(run_dir)
        assert observed.error == "version_mismatch"
        assert observed.applied_generation is None
        assert observed.logs_open == 0
        terminate()

    monkeypatch.setattr(_maintain.time, "sleep", waiting)
    assert _maintain.main(args(run_dir)) == 0
    assert fake_log[0] == []


def test_generation_change_runs_seal_exit_flush_then_reexecs(
    run_dir, monkeypatch, fake_log
):
    current = plan()
    _plan.write_plan(run_dir, current)
    opened, closed, flushed = fake_log
    invoked = []

    def sweep(*_args, follow):
        _plan.write_plan(run_dir, replace(current, generation=2))
        follow.checkpoint()
        pytest.fail("work continued after a new generation")

    def reexec(argv):
        assert opened and closed == ["events"] and flushed == [True]
        # The new generation cannot be acknowledged while old handles exist.
        assert status(run_dir).applied_generation == 1
        invoked.append(argv)
        raise Reexec

    monkeypatch.setattr(_maintain, "Timer", Deadline)
    monkeypatch.setattr(_maintain, "sweep", sweep)
    monkeypatch.setattr(_maintain, "_reexec", reexec)
    with pytest.raises(Reexec):
        _maintain.main(args(run_dir))

    assert invoked == [args(run_dir)]


DEADLINE_PROCESS = '''
import contextlib
import fcntl
import os
import sys
from pathlib import Path
from streamcast import _maintain

deadlines = []
class Deadline:
    def __init__(self, interval, function, args):
        self.function, self.args = function, args
    def start(self):
        deadlines.append(self)
    def cancel(self):
        pass

@contextlib.contextmanager
def opened(*args):
    try:
        yield object()
    finally:
        raise AssertionError("blocked exit unexpectedly returned")

def sweep(*args, follow):
    follow.change()
    raise KeyboardInterrupt

real_exec = os.execv

def checked_exec(executable, argv):
    assert executable == sys.executable
    assert argv[1:4] == ["-m", "streamcast", "maintain"]
    probe = """import fcntl,sys
from pathlib import Path
with Path(sys.argv[1]).open('a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print('released', flush=True)
"""
    real_exec(executable, [executable, "-c", probe,
        str(Path(sys.argv[1]) / "roles/seal.default.lock")])

def stuck_exit(*args):
    # Fire the armed deadline before any old handles or locks are closed.
    deadline = deadlines[0]
    deadline.function(*deadline.args)
    raise AssertionError("deadline did not exec")

_maintain.os.execv = checked_exec
_maintain.Timer = Deadline
_maintain.litelink.open = opened
_maintain.sweep = sweep
_maintain.on_exit = stuck_exit
raise SystemExit(_maintain.main([
    "--follow", sys.argv[1], "--role", "seal", "--group", "default"]))
'''


def test_deadline_reexec_releases_role_lock(run_dir):
    _plan.write_plan(run_dir, plan())
    result = subprocess.run(
        [sys.executable, "-c", DEADLINE_PROCESS, str(run_dir)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "released"
    with _plan.role_lock(run_dir, "seal", "default"):
        pass


def test_second_role_process_same_group_refused(run_dir):
    # A real independent interpreter must refuse even before a plan exists.
    with _plan.role_lock(run_dir, "seal", "default"):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from streamcast import _maintain, _plan\n"
                "import sys\n"
                "def read(directory):\n    raise SystemExit(7)\n"
                "_plan.read_plan = read\n"
                "raise SystemExit(_maintain.main(sys.argv[1:]))\n",
                *args(run_dir),
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    assert result.returncode == 1, result.stderr
    assert "already running" in result.stderr
    assert not (run_dir / "status").exists()


def test_role_lock_taken_before_plan_read(run_dir, monkeypatch):
    def read(_directory):
        assert (run_dir / "roles/seal.default.lock").exists()
        with (run_dir / "roles/seal.default.lock").open("a") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

        raise KeyboardInterrupt

    monkeypatch.setattr(_plan, "read_plan", read)
    assert _maintain.main(args(run_dir)) == 0


def test_role_clears_only_its_own_spill_at_start(
    run_dir, tmp_path, monkeypatch, fake_log
):
    spill_root = tmp_path / "scratch"
    own = spill_root / _spill.MAINTAINERS / "seal.default"
    neighbor = spill_root / _spill.MAINTAINERS / "publish.default"
    other_group = spill_root / _spill.MAINTAINERS / "seal.other"
    for directory in (own, neighbor, other_group):
        directory.mkdir(parents=True)
        (directory / "spill").write_text("scratch")

    spec = replace(plan().roles["seal.default"], duckdb_spill_root=str(spill_root))
    _plan.write_plan(run_dir, plan(roles={"seal.default": spec}))

    def sweep(*_args, follow):
        assert own.is_dir() and list(own.iterdir()) == []
        assert (neighbor / "spill").read_text() == "scratch"
        assert (other_group / "spill").read_text() == "scratch"
        terminate()

    monkeypatch.setattr(_maintain, "sweep", sweep)
    assert _maintain.main(args(run_dir)) == 0


def test_atomic_plan_replacement_never_exposes_torn_document(run_dir, monkeypatch):
    old, new = plan(), plan(generation=2)
    _plan.write_plan(run_dir, old)
    # A killed writer can leave one temporary document, reused on restart.
    (run_dir / "plan.json.tmp").write_text("interrupted")
    replace_file = os.replace
    seen = []

    def before_replace(source, destination):
        seen.append(_plan.read_plan(run_dir))
        assert json.loads(Path(source).read_bytes())["generation"] == 2
        replace_file(source, destination)

    monkeypatch.setattr(_plan.os, "replace", before_replace)
    _plan.write_plan(run_dir, new)
    assert seen == [old]
    assert _plan.read_plan(run_dir) == new
    assert sorted(p.name for p in run_dir.iterdir()) == ["plan.json"]


@pytest.mark.parametrize("kind", ["missing", "unknown", "invalid"])
def test_waiting_role_reports_heartbeat_and_opens_nothing(
    run_dir, monkeypatch, fake_log, kind
):
    if kind == "unknown":
        _plan.write_plan(run_dir, plan(roles={}))
    elif kind == "invalid":
        (run_dir / "plan.json").write_text('{"format":')

    def waiting(_seconds):
        observed = status(run_dir)
        assert observed.heartbeat_at > 0
        assert observed.logs_open == 0
        assert observed.applied_generation is None
        assert (
            observed.error
            == {
                "missing": "missing_plan",
                "unknown": "unknown_group",
                "invalid": "invalid_plan",
            }[kind]
        )
        terminate()

    monkeypatch.setattr(_maintain.time, "sleep", waiting)
    assert _maintain.main(args(run_dir)) == 0
    assert fake_log[0] == []


def test_watch_interrupts_once_and_keeps_old_identity_until_exec(run_dir, monkeypatch):
    current = plan()
    _plan.write_plan(run_dir, replace(current, boot_id="boot-b"))
    follow = _maintain._Follow(
        run_dir, "publish", "default", current, [], _plan.Status("boot-a", 1)
    )
    ticks = iter([False, False, True])
    calls = []
    monkeypatch.setattr(Event, "wait", lambda self, timeout=None: next(ticks))
    monkeypatch.setattr(_maintain, "Timer", Deadline)
    monkeypatch.setattr(
        _maintain._thread, "interrupt_main", lambda: calls.append("interrupt")
    )
    follow.watch()
    assert calls == ["interrupt"]
    assert follow.deadline is not None
    assert follow.deadline.interval == _maintain._PUBLISH_GRACE
    assert status(run_dir, "publish").boot_id == "boot-a"
    assert status(run_dir, "publish").applied_generation == 1


def test_real_log_seal_flushes_buffer_before_exec(run_dir, tmp_path, monkeypatch):
    root = tmp_path / "logs"
    with litelink.new(root, "events", schema=pa.schema([("value", pa.int64())])) as log:
        log.append({"value": 7})

    spec = _plan.RolePlan(0.25, logs=((str(root), "events"),))
    current = plan(roles={"seal.default": spec})
    _plan.write_plan(run_dir, current)

    def sweep(*_args, follow):
        _plan.write_plan(run_dir, replace(current, generation=2))
        follow.checkpoint()

    def reexec(_argv):
        with litelink.open(root, "events") as log:
            assert log.buffered_rows() == 0
            assert log.staging_files() == 1

        raise Reexec

    monkeypatch.setattr(_maintain, "Timer", Deadline)
    monkeypatch.setattr(_maintain, "sweep", sweep)
    monkeypatch.setattr(_maintain, "_reexec", reexec)
    with pytest.raises(Reexec):
        _maintain.main(args(run_dir))


def test_plan_change_between_logs_stops_the_next_operation(run_dir, monkeypatch):
    current = plan()
    _plan.write_plan(run_dir, current)
    follow = _maintain._Follow(
        run_dir, "seal", "default", current, [], _plan.Status("boot-a", 1)
    )
    passed = []
    first = cast(litelink.WriteHandle, object())
    second = cast(litelink.WriteHandle, object())

    def work(log, _flush):
        passed.append(log)
        _plan.write_plan(run_dir, replace(current, generation=2))

    monkeypatch.setitem(_maintain.ROLES, "seal", work)
    monkeypatch.setattr(_maintain, "Timer", Deadline)
    with pytest.raises(KeyboardInterrupt):
        _maintain.sweep(
            [("/one/events", first), ("/two/events", second)],
            "seal",
            0.25,
            follow=follow,
        )

    assert passed == [first]
    assert follow.status.last_pass["/one/events"].ok_at is not None
    assert "/two/events" not in follow.status.last_pass


def test_plan_settings_reach_role_work(run_dir, tmp_path, monkeypatch, fake_log):
    spill_root = tmp_path / "spill"
    spec = _plan.RolePlan(
        every=0.75,
        flush_every=30,
        logs=(("/data", "events"),),
        retire=(("/data", "old"),),
        streams=(("/data", "stream"),),
        duckdb_default_memory_mib=64,
        duckdb_default_threads=2,
        duckdb_retention_memory_mib=128,
        duckdb_retention_threads=3,
        duckdb_retention_spill_mib=256,
        duckdb_spill_root=str(spill_root),
    )
    _plan.write_plan(run_dir, plan(roles={"seal.default": spec}))
    defaults = []
    calls = []
    monkeypatch.setattr(litelink, "bound_default_connection", defaults.append)

    def sweep(opened, role, every, retiring, flush, streams, duckdb, *, follow):
        calls.append(role)
        assert opened[0][0] == "/data/events"
        assert (every, flush) == (0.75, 30)
        assert retiring == [(Path("/data"), "old")]
        assert streams == [(Path("/data"), "stream")]
        assert defaults == [litelink.DuckDBPolicy(64, 2)]
        assert duckdb == litelink.DuckDBPolicy(
            128,
            3,
            memory_cache=False,
            spill=litelink.Spill(spill_root / _spill.MAINTAINERS / "seal.default", 256),
        )
        terminate()

    monkeypatch.setattr(_maintain, "sweep", sweep)
    assert _maintain.main(args(run_dir)) == 0
    assert calls == ["seal"]


def test_follow_retain_finds_log_by_identical_path_string(
    run_dir, monkeypatch, tmp_path
):
    home = tmp_path / "data" / "events"
    handle = SimpleNamespace(
        _s3=None,
        log=SimpleNamespace(root=home, name="events"),
        retirement=None,
        retired=(),
    )
    external = External(
        ExternalMaintain(_maintain.Maintain(), run_dir, {"default": None}), "boot"
    )
    roles = external.plan_roles({"events": cast(StreamHandle, handle)}, {})
    _plan.write_plan(run_dir, plan(roles=roles))
    opened = object()
    calls = []

    @contextlib.contextmanager
    def opening(root, name):
        assert (root, name) == (home, "events")
        yield opened

    def retention(root, name, live, **kwargs):
        calls.append((root, name, live))
        terminate()

    def forbidden(*args, **kwargs):
        pytest.fail("retention did not find the followed live log")

    monkeypatch.setattr(litelink, "open", opening)
    monkeypatch.setattr(
        _maintain._metadata,
        "load",
        lambda *args: SimpleNamespace(live_log=SimpleNamespace(name="events")),
    )
    monkeypatch.setattr(_maintain._retention, "run", retention)
    monkeypatch.setitem(_maintain.ROLES, "clean-published", forbidden)
    assert _maintain.main(args(run_dir, role="clean-published")) == 0
    assert calls == [(home, "events", opened)]


def test_watcher_does_not_interrupt_twice_when_checkpoint_wins(run_dir, monkeypatch):
    current = plan()
    _plan.write_plan(run_dir, replace(current, generation=2))
    follow = _maintain._Follow(run_dir, "seal", "default", current, [], _plan.Status())
    interrupts = []
    armed = []

    class Changed(Event):
        def __init__(self):
            self.value = False
            self.checks = 0

        def is_set(self):
            self.checks += 1
            if self.checks == 1:
                # The watcher has observed false, but the checkpoint wins
                # before change() takes its mutex.
                with pytest.raises(KeyboardInterrupt):
                    follow.checkpoint()

                return False

            return self.value

        def set(self):
            self.value = True

    class Finished(Event):
        def wait(self, timeout=None):
            if armed:
                return True

            armed.append(True)
            return False

    changed = Changed()
    follow.changed = changed
    follow.finished = Finished()
    monkeypatch.setattr(_maintain, "Timer", Deadline)
    monkeypatch.setattr(
        _maintain._thread, "interrupt_main", lambda: interrupts.append(True)
    )
    follow.watch()
    assert changed.value
    assert follow.deadline is not None
    assert interrupts == []
