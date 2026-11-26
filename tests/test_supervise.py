"""One role restarts without asking the kubelet to restart its container."""

import json
import os
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest

import streamcast_supervise as supervise


class Child:
    def __init__(self, pid):
        self.pid = pid
        self.code = None
        self.signals = []

    def poll(self):
        return self.code

    def send_signal(self, signum):
        self.signals.append(signum)
        self.code = -signum

    def kill(self):
        self.send_signal(signal.SIGKILL)

    def wait(self):
        return self.code


@pytest.fixture
def runner(tmp_path, monkeypatch):
    (tmp_path / "memory.events").write_text("oom 0\noom_kill 0\n")
    (tmp_path / "memory.oom.group").write_text("0\n")
    children = []

    def spawn(command, **kwargs):
        child = Child(20 + len(children))
        children.append((child, command, kwargs))
        return child

    monkeypatch.setattr(supervise.subprocess, "Popen", spawn)
    instance = supervise.Supervisor(
        ["role"], tmp_path / "status.json", tmp_path, ["probe"], 10, 2, 3
    )
    return instance, children


def test_supervisor_imports_only_stdlib():
    result = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "import sys; import streamcast_supervise; "
            "assert 'streamcast' not in sys.modules; "
            "assert all(name.split('.')[0] in sys.stdlib_module_names "
            "or name == 'streamcast_supervise' or name == '__main__' for name in sys.modules)",
        ],
        env={"PYTHONPATH": str(Path(supervise.__file__).parents[1])},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_supervisor_restarts_child_and_records_exit(runner, monkeypatch):
    instance, children = runner
    monkeypatch.setattr(supervise.time, "time", lambda: 100)
    instance.tick(0)
    first = instance.child
    assert children[0][1] == ["role"]
    first.code = 7
    instance.tick(2)
    assert instance.child is None
    assert instance.state["last_exit"] == {
        "code": 7,
        "signal": None,
        "oom": False,
        "liveness": False,
    }
    assert instance.state["restarts"] == 1
    instance.tick(2.9)
    assert instance.child is None
    monkeypatch.setattr(supervise.time, "time", lambda: 104)
    instance.tick(3)
    state = json.loads(instance.status.read_text())
    assert state["pid"] == instance.child.pid != first.pid
    assert state["child_started_at"] == state["heartbeat_at"] == 104
    assert state["oom_group"] == 0 and state["oom_kills"] == 0
    (instance.cgroup / "memory.events").write_text("oom 1\noom_kill 1\n")
    instance.child.code = -signal.SIGKILL
    instance.tick(4)
    assert instance.state["last_exit"] == {
        "code": None,
        "signal": 9,
        "oom": True,
        "liveness": False,
    }
    assert instance.state["oom_kills"] == 1 and instance.state["restarts"] == 2
    assert list(instance.status.parent.glob("*.json*")) == [instance.status]


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_supervisor_forwards_sigterm_without_restart(runner, signum):
    instance, children = runner
    instance.tick(0)
    child = instance.child
    instance.forward(signum)
    assert child.signals == [signum]
    assert instance.tick(1) == 128 + signum
    assert len(children) == 1 and instance.state["restarts"] == 0


def test_supervisor_signal_during_backoff_never_starts_child(runner):
    instance, children = runner
    instance.tick(0)
    instance.child.code = 1
    instance.tick(1)
    instance.forward(signal.SIGTERM)
    assert instance.tick(3) == 143
    assert len(children) == 1


def test_supervisor_backoff_caps_and_resets(runner):
    instance, _ = runner
    instance.tick(0)
    now = 1
    delays = []
    for _ in range(9):
        instance.child.code = 1
        instance.tick(now)
        delays.append(instance.delay)
        now += instance.delay
        instance.tick(now)
        now += 1

    assert delays == [1, 2, 4, 8, 16, 32, 60, 60, 60]
    instance.child.code = 1
    instance.tick(now + 600)
    assert instance.delay == 1


@pytest.mark.parametrize("hung", [False, True])
def test_supervisor_restarts_child_failing_liveness(runner, hung):
    instance, children = runner
    instance.tick(0)
    first = instance.child
    for start in (10, 20):
        instance.tick(start)
        assert children[-1][1] == ["probe"]
        if not hung:
            instance.check.code = 1

        instance.tick(start + 3)

    assert first.signals == [signal.SIGKILL]
    instance.tick(24)
    assert instance.state["last_exit"]["liveness"] is True
    assert instance.state["last_exit"]["oom"] is False
    instance.tick(25)
    assert instance.child.pid != first.pid


def test_successful_probe_resets_consecutive_failures(runner):
    instance, _ = runner
    instance.tick(0)
    for start, result in [(10, 1), (20, 0), (30, 1)]:
        instance.tick(start)
        instance.check.code = result
        instance.tick(start + 1)

    assert instance.failures == 1 and instance.child.signals == []


def test_supervisor_sets_child_oom_score_adj(tmp_path):
    # Exercise the actual fork hook, not a helper called directly by the test.
    command = [
        sys.executable,
        "-S",
        "-c",
        "from pathlib import Path; print(Path('/proc/self/oom_score_adj').read_text())",
    ]
    with subprocess.Popen(
        command, preexec_fn=supervise.prefer_child, stdout=subprocess.PIPE, text=True
    ) as child:
        stdout, _ = child.communicate(timeout=5)

    assert child.returncode == 0 and stdout.strip() == "1000"


def test_supervisor_wires_oom_score_before_exec(runner):
    instance, children = runner
    instance.tick(0)
    assert children[0][2]["preexec_fn"] is supervise.prefer_child


def test_supervisor_rss_under_20_mib(tmp_path):
    # -S excludes test-runner/site instrumentation; measure the running supervisor.
    script = (
        "import sys; from pathlib import Path; import streamcast_supervise as s; "
        "root = Path(sys.argv[1]); "
        "runner = s.Supervisor([sys.executable, '-S', '-c', 'import time; time.sleep(60)'], "
        "root / 'status', root, ['true'], 10, 6, 15); runner.tick(0); "
        "print(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmRSS:'))); "
        "runner.forward(15); runner.child.wait()"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", script, str(tmp_path)],
        env={"PYTHONPATH": str(Path(supervise.__file__).parents[1])},
        text=True,
        capture_output=True,
        timeout=5,
        check=True,
    )
    assert 0 < int(result.stdout.strip()) < 20 * 1024


def test_unwritable_status_keeps_supervising_and_probe_fails(runner, capsys):
    instance, _ = runner
    instance.status = instance.status / "absent-parent" / "status"
    instance.tick(0)
    assert instance.child.poll() is None
    assert "Cannot write supervisor status" in capsys.readouterr().err
    assert supervise.probe_status(instance.status, 75) == 1


@pytest.mark.parametrize("heartbeat", [None, True, "100", 0, 110, float("nan")])
def test_probe_refuses_missing_stale_future_or_invalid_heartbeat(
    tmp_path, monkeypatch, heartbeat
):
    status = tmp_path / "status.json"
    monkeypatch.setattr(supervise.time, "time", lambda: 100)
    assert supervise.probe_status(status, 10) == 1
    status.write_text(json.dumps({"heartbeat_at": heartbeat}))
    assert supervise.probe_status(status, 10) == 1
    status.write_text(json.dumps({"heartbeat_at": 99}))
    assert supervise.probe_status(status, 10) == 0


def test_group_kill_warning(runner, monkeypatch, capsys):
    instance, _ = runner
    (instance.cgroup / "memory.oom.group").write_text("1")
    monkeypatch.setattr(instance, "tick", lambda now: 0)
    monkeypatch.setattr(supervise.signal, "signal", lambda *args: None)
    assert instance.run() == 0
    assert "singleProcessOOMKill: true" in capsys.readouterr().err


def test_cli_passes_liveness_settings_and_command(tmp_path, monkeypatch):
    instances = []
    monkeypatch.setattr(
        supervise.Supervisor, "run", lambda self: instances.append(self) or 0
    )
    assert (
        supervise.main(
            [
                "--status",
                str(tmp_path / "status"),
                "--run-dir",
                "/run/roles",
                "--role",
                "seal",
                "--group",
                "archive",
                "--probe-period",
                "2",
                "--probe-threshold",
                "3",
                "--probe-timeout",
                "4",
                "--",
                "custom",
                "--role",
                "seal",
            ]
        )
        == 0
    )
    instance = instances[0]
    assert instance.command == ["custom", "--role", "seal"]
    assert instance.probe == [
        "streamcast",
        "probe",
        "role",
        "--run-dir",
        "/run/roles",
        "--role",
        "seal",
        "--group",
        "archive",
    ]
    assert (instance.period, instance.threshold, instance.timeout) == (2, 3, 4)
    assert instance.status == tmp_path / "status"


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_supervisor_cli_forwards_shutdown_and_exits_child_status(tmp_path, signum):
    child_script = (
        "import signal, sys; "
        "signal.signal(signal.SIGTERM, lambda *_: sys.exit(7)); "
        "signal.signal(signal.SIGINT, lambda *_: sys.exit(7)); "
        "print('ready', flush=True); signal.pause()"
    )
    command = [
        sys.executable,
        "-S",
        "-c",
        "import streamcast_supervise as s; raise SystemExit(s.main())",
        "--status",
        str(tmp_path / "status"),
        "--run-dir",
        str(tmp_path),
        "--role",
        "seal",
        "--group",
        "default",
        "--probe-period",
        "60",
        "--",
        sys.executable,
        "-S",
        "-c",
        child_script,
    ]
    with subprocess.Popen(
        command,
        env={"PYTHONPATH": str(Path(supervise.__file__).parents[1])},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    ) as process:
        try:
            assert process.stdout is not None
            assert select.select([process.stdout], [], [], 5)[0], "child never started"
            assert process.stdout.readline().strip() == "ready"
            process.send_signal(signum)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 7, stderr
            assert stdout == ""
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
