"""A server's children die with it, SIGKILL included.

An orderly shutdown stops the maintainer and the sidecar itself; these tests
cover the case it cannot, a server killed outright. Each runs a real server in
its own process — the thing being tested is what the kernel does when that
process vanishes, which nothing in-process can stand in for.

Linux only: `PR_SET_PDEATHSIG` has no portable equivalent (see `_process`).
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import os
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux only"
)

# The server, as its own process. It prints once it is serving, so the
# children exist by the time the test looks for them.
SERVER = """
import asyncio, sys
import litelink, pyarrow as pa, streamcast

async def main(root, bucket):
    replicated = bucket != "-"
    handle = litelink.new(
        root, "trades",
        schema=pa.schema([pa.field("event_ts", pa.int64(), nullable=False)]),
        sort_by=("event_ts",),
        config=litelink.LogConfig(wal_replication=replicated),
        **({"published": bucket, "s3_options": litelink.S3Options().resolved()} if replicated else {}),
    )
    stream = streamcast.Stream("trades", log=handle)
    async with streamcast.serve(stream, "127.0.0.1", 0):
        print("ready", flush=True)
        await asyncio.Event().wait()

asyncio.run(main(sys.argv[1], sys.argv[2]))
"""


def children(pid: int) -> set[int]:
    """`pid`'s direct children, from every one of its threads."""
    found: set[int] = set()
    for task in Path(f"/proc/{pid}/task").iterdir():
        # A thread can exit between the listing and the read.
        with contextlib.suppress(FileNotFoundError, ProcessLookupError):
            found.update(int(c) for c in (task / "children").read_text().split())

    return found


@dataclass(frozen=True)
class Child:
    """A process by identity: a pid alone can be reused once it exits."""

    pid: int
    started: int  # clock ticks after boot, `/proc/<pid>/stat` field 22
    command: str


def stat(pid: int) -> list[str] | None:
    """The fields after `comm`, from `state` on; None once the pid is gone."""
    # Reaped before the open, or between the open and the read: the second
    # is ESRCH, which Python raises as ProcessLookupError.
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except (FileNotFoundError, ProcessLookupError):
        return None


def identify(pid: int) -> Child | None:
    fields = stat(pid)
    try:
        command = (
            Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
        )
    except (FileNotFoundError, ProcessLookupError):
        return None

    return None if fields is None else Child(pid, int(fields[19]), command)


# **Death by event, not by sampling.** A pidfd is a handle on one process —
# never another that later reuses its pid — and it becomes readable when that
# process has exited, every thread of it. Waiting on pidfds makes "the child
# died" a fact the kernel reports, where reading `/proc/<pid>/stat` until a
# deadline had to judge state letters: it once read litestream's leader in `X`
# (dead, its threads still exiting) as a survivor. The deadline now bounds
# only a child that really does not die.
#
# Through `syscall` rather than `os.pidfd_open`, which some Python builds
# leave out; both numbers are the same on every Linux architecture (5.3+).
_PIDFD_OPEN = 434
_PIDFD_SEND_SIGNAL = 424
_LIBC = ctypes.CDLL(None, use_errno=True)


def pidfd(child: Child) -> int | None:
    """A pidfd on `child`, or None if it is already gone."""
    fd = _LIBC.syscall(_PIDFD_OPEN, ctypes.c_int(child.pid), ctypes.c_uint(0))
    if fd < 0:
        err = ctypes.get_errno()
        if err == errno.ESRCH:
            return None

        raise OSError(err, os.strerror(err))

    # Opened by pid, so check it is still the process `identify` named: a
    # different start time is a different process, and that one is gone.
    fields = stat(child.pid)
    if fields is None or int(fields[19]) != child.started:
        os.close(fd)
        return None

    return fd


def kill(fd: int) -> None:
    """SIGKILL through the pidfd: never another process that reused the pid."""
    _LIBC.syscall(_PIDFD_SEND_SIGNAL, fd, signal.SIGKILL, None, 0)


def kill_the_server(root: Path, bucket: str = "-") -> tuple[list[Child], list[str]]:
    """Start a server, SIGKILL it; return its children, and any still running."""
    server = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", SERVER, str(root), bucket],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert server.stdout is not None
        assert server.stdout.readline().strip() == "ready"
        # The sidecar may start a beat after `serve` returns: it takes its lock
        # first. Waiting for the count to settle keeps a late child from
        # being missed rather than tested.
        deadline = time.monotonic() + 10
        spawned = children(server.pid)
        while time.monotonic() < deadline:
            time.sleep(0.5)
            now = children(server.pid)
            if now == spawned and spawned:
                break

            spawned = now

        # Identified, and a pidfd opened on each, while the server lives: a
        # child is then named by what it was and watched as that process,
        # even if it exits before the wait below.
        found = [identify(pid) for pid in spawned]
        known = [child for child in found if child is not None]
        handles = {child: pidfd(child) for child in known}
        server.send_signal(signal.SIGKILL)
        server.wait()
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()

    # Generous: SIGKILL is immediate, but a child in uninterruptible I/O on a
    # loaded runner finishes that first. Only a child that never dies waits it out.
    alive = {fd: child for child, fd in handles.items() if fd is not None}
    poller = select.poll()
    for fd in alive:
        poller.register(fd, select.POLLIN)

    deadline = time.monotonic() + 10
    while alive and (remaining := deadline - time.monotonic()) > 0:
        for fd, _event in poller.poll(remaining * 1_000):
            poller.unregister(fd)
            os.close(fd)
            del alive[fd]

    running = [child.command for child in alive.values()]
    for fd in alive:  # leave nothing behind, whatever is asserted
        kill(fd)
        os.close(fd)

    return known, running


def test_the_maintainer_dies_with_a_killed_server(tmp_path):
    spawned, survivors = kill_the_server(tmp_path)

    assert len(spawned) == 1, "expected exactly the maintainer"
    assert survivors == []


@pytest.mark.replication
def test_the_sidecar_dies_with_a_killed_server(tmp_path, s3, bucket):  # noqa: ARG001 — s3 exports the credentials the child reads
    spawned, survivors = kill_the_server(tmp_path, bucket)

    # The maintainer and litestream.
    assert len(spawned) == 2
    assert survivors == []


def test_a_terminals_ctrl_c_reaches_the_server_not_its_children(tmp_path):
    """Ctrl-C goes to a terminal's whole foreground process group.

    A child in the server's group is interrupted at the same moment the
    server starts stopping it: the maintainer, already in its last seal pass
    when the server's SIGTERM arrived, abandoned that pass mid-transaction.
    The server owns its children's lifetimes, so they are in sessions of
    their own and only the server hears the terminal.
    """
    # Its own session, as a shell starts a job: the group a terminal signals.
    server = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", SERVER, str(tmp_path), "-"],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        assert server.stdout is not None
        assert server.stdout.readline().strip() == "ready"
        spawned = children(server.pid)
        assert spawned, "expected the maintainer"
        group = os.getpgid(server.pid)
        assert all(os.getpgid(child) != group for child in spawned)
    finally:
        server.kill()
        server.wait()
