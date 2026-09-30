"""A server's children die with it, SIGKILL included.

An orderly shutdown stops the maintainer and the sidecar itself; these tests
cover the case it cannot, a server killed outright. Each runs a real server in
its own process — the thing being tested is what the kernel does when that
process vanishes, which nothing in-process can stand in for.

Linux only: `PR_SET_PDEATHSIG` has no portable equivalent (see `_process`).
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
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
        **({"archive": bucket, "s3": litelink.S3Options().resolved()} if replicated else {}),
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


def gone(pid: int) -> bool:
    """Exited, or a zombie awaiting a reaper: either way, doing nothing."""
    # Reaped before the open, or between the open and the read: the second
    # is ESRCH, which Python raises as ProcessLookupError.
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return True

    return stat.rsplit(")", 1)[1].split()[0] == "Z"


def kill_the_server(root: Path, bucket: str = "-") -> tuple[set[int], list[str]]:
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

        server.send_signal(signal.SIGKILL)
        server.wait()
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not all(gone(pid) for pid in spawned):
        time.sleep(0.1)

    survivors = [pid for pid in spawned if not gone(pid)]
    running = [command(pid) for pid in survivors]
    for pid in survivors:  # leave nothing behind, whatever the assertion says
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)

    return spawned, running


def command(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
    except (FileNotFoundError, ProcessLookupError):
        return f"pid {pid}, exited while being named"


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
