"""Small, standard-library-only supervisor for one maintenance container."""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import TypedDict


class ExitStatus(TypedDict):
    code: int | None
    signal: int | None
    oom: bool
    liveness: bool


class Status(TypedDict):
    pid: int | None
    restarts: int
    last_exit: ExitStatus | None
    oom_kills: int | None
    child_started_at: float | None
    heartbeat_at: float | None
    oom_group: int | None


def prefer_child() -> None:
    # Raising our own score needs no capability. Do this before exec so the
    # interpreter cannot allocate under the supervisor's lower OOM score.
    Path("/proc/self/oom_score_adj").write_text("1000")


def memory_state(root: Path) -> tuple[int | None, int | None]:
    try:
        events = dict(
            line.split() for line in (root / "memory.events").read_text().splitlines()
        )
        return int(events["oom_kill"]), int((root / "memory.oom.group").read_text())
    except (OSError, ValueError, KeyError):
        return None, None


def backoff(previous: float, runtime: float) -> float:
    return 1.0 if runtime >= 600 else min(60.0, max(1.0, previous * 2))


def probe_status(path: Path, max_age: float) -> int:
    try:
        heartbeat = json.loads(path.read_text())["heartbeat_at"]
        if isinstance(heartbeat, bool) or not isinstance(heartbeat, (int, float)):
            return 1

        age = time.time() - heartbeat
        return 0 if math.isfinite(age) and 0 <= age <= max_age else 1
    except (OSError, ValueError, KeyError, TypeError):
        return 1


class Supervisor:
    def __init__(self, command, status, cgroup, probe, period, threshold, timeout):
        self.command = command
        self.status = status
        self.cgroup = cgroup
        self.probe = probe
        self.period = period
        self.threshold = threshold
        self.timeout = timeout
        self.child = None
        self.check = None
        self.stopping = None
        self.delay = 0.0
        self.next_start = 0.0
        self.next_probe = 0.0
        self.started = 0.0
        self.failures = 0
        self.liveness_failed = False
        self.write_failed = False
        self.state: Status = {
            "pid": None,
            "restarts": 0,
            "last_exit": None,
            "oom_kills": None,
            "child_started_at": None,
            "heartbeat_at": None,
            "oom_group": None,
        }

    def forward(self, signum, _frame=None):
        self.stopping = signum
        if self.child is not None and self.child.poll() is None:
            self.child.send_signal(signum)

    def write_status(self):
        self.state["heartbeat_at"] = time.time()
        temporary = self.status.with_suffix(self.status.suffix + ".tmp")
        try:
            temporary.write_text(json.dumps(self.state) + "\n")
            os.replace(temporary, self.status)
            self.write_failed = False
        except OSError as exc:
            if not self.write_failed:
                print(
                    f"Cannot write supervisor status: {exc.strerror}", file=sys.stderr
                )

            self.write_failed = True

    def stop_check(self):
        if self.check is not None:
            if self.check.poll() is None:
                self.check.kill()

            self.check.wait()
            self.check = None

    def tick(self, now):
        kills, group = memory_state(self.cgroup)
        self.state["oom_kills"], self.state["oom_group"] = kills, group
        if self.child is not None:
            code = self.child.poll()
            if code is not None:
                self.stop_check()
                if self.stopping is not None:
                    return code if code >= 0 else 128 - code

                self.state["last_exit"] = {
                    "code": code if code >= 0 else None,
                    "signal": -code if code < 0 else None,
                    "oom": code == -signal.SIGKILL
                    and kills is not None
                    and self.initial_kills is not None
                    and kills > self.initial_kills,
                    "liveness": self.liveness_failed,
                }
                self.state["restarts"] += 1
                self.state["pid"] = None
                self.delay = backoff(self.delay, now - self.started)
                self.next_start = now + self.delay
                self.child = None

        if self.stopping is not None and self.child is None:
            return 128 + self.stopping

        if self.child is None and now >= self.next_start:
            self.child = subprocess.Popen(self.command, preexec_fn=prefer_child)
            self.initial_kills = kills
            self.started = now
            self.next_probe = now + self.period
            self.failures = 0
            self.liveness_failed = False
            if self.stopping is not None:
                self.child.send_signal(self.stopping)

            self.state["pid"] = self.child.pid
            self.state["child_started_at"] = time.time()

        if self.child is not None and self.stopping is None:
            if self.check is not None:
                result = self.check.poll()
                if result is not None or now >= self.check_deadline:
                    self.failures = 0 if result == 0 else self.failures + 1
                    self.stop_check()
                    if self.failures >= self.threshold:
                        self.liveness_failed = True
                        self.child.kill()

            if (
                self.check is None
                and not self.liveness_failed
                and now >= self.next_probe
            ):
                self.check = subprocess.Popen(
                    self.probe, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                self.check_deadline = now + self.timeout
                self.next_probe = now + self.period

        self.write_status()
        return None

    def run(self):
        _, group = memory_state(self.cgroup)
        if group != 0:
            print(
                "WARNING: memory.oom.group is not 0; singleProcessOOMKill: true "
                "is required to keep the container running on a role OOM.",
                file=sys.stderr,
            )

        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, self.forward)

        while True:
            result = self.tick(time.monotonic())
            if result is not None:
                return result

            time.sleep(0.2)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", type=Path, required=True)
    if argv[:1] == ["probe"]:
        parser.add_argument("--max-age", type=float, required=True)
        args = parser.parse_args(argv[1:])
        if not math.isfinite(args.max_age) or args.max_age <= 0:
            parser.error("--max-age must be positive and finite")

        return probe_status(args.status, args.max_age)

    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--probe-period", type=float, default=10)
    parser.add_argument("--probe-threshold", type=int, default=6)
    parser.add_argument("--probe-timeout", type=float, default=15)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if (
        not all(
            math.isfinite(v) and v > 0
            for v in (args.probe_period, args.probe_threshold, args.probe_timeout)
        )
        or args.command[:1] != ["--"]
        or len(args.command) < 2
    ):
        parser.error("positive probe settings and a command after -- are required")

    probe = [
        "streamcast",
        "probe",
        "role",
        "--run-dir",
        args.run_dir,
        "--role",
        args.role,
        "--group",
        args.group,
    ]
    return Supervisor(
        args.command[1:],
        args.status,
        Path("/sys/fs/cgroup"),
        probe,
        args.probe_period,
        args.probe_threshold,
        args.probe_timeout,
    ).run()
