"""Children that die with the server: the maintainer and the litestream sidecar.

An orderly shutdown stops both — `terminate`, then a grace period, then a kill.
What it cannot cover is the server being SIGKILLed, or dying any other way
that runs no Python: a signal handler cannot catch SIGKILL, so only the kernel
can act on it. Without this, each child outlives the server reparented to PID
1 — the sidecar still replicating a database the next server is about to
replicate, the maintainer ~207 MB of interpreter still contending for the
logs' leases, and both holding whatever file descriptors they inherited, so a
parent reading the server's output never sees EOF.

`PR_SET_PDEATHSIG` asks the kernel for SIGKILL when the parent dies. It is
Linux only and there is no portable equivalent: elsewhere a child is stopped
by an orderly shutdown and nothing else.

**The parent is the THREAD that forked, not the process** (`prctl(2)`): the
signal fires when that thread exits. Both supervisors spawn from the event
loop's thread, which lives as long as the server. Spawning from a pool
worker would kill the child whenever that worker was retired.
"""

from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Sequence

_PR_SET_PDEATHSIG: Final = 1


def _die_with(parent: int) -> None:  # pragma: no cover — runs between fork and exec
    """In the child: ask the kernel for SIGKILL when the parent dies."""
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    # A parent that died between the fork and the `prctl` sends no signal:
    # the child was already reparented. Seen from here, that is a changed ppid.
    if os.getppid() != parent:
        os._exit(1)


def popen(argv: Sequence[str]) -> subprocess.Popen[bytes]:
    """Start `argv` as a child the kernel kills when this process dies.

    **In a session of its own**, so a terminal's signals reach the server and
    not its children. Ctrl-C goes to the whole foreground process group: a
    child in it is interrupted at the same moment the server starts stopping
    it, and the maintainer, already in its last seal pass when the server's
    SIGTERM arrived, abandoned that pass mid-transaction. The server owns its
    children's lifetimes, and stops them itself.
    """
    parent = os.getpid()
    return subprocess.Popen(  # noqa: S603
        argv,
        preexec_fn=(lambda: _die_with(parent)) if sys.platform == "linux" else None,  # noqa: PLW1509
        start_new_session=True,
    )
