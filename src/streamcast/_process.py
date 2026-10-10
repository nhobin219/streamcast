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

**A stream's `S3Options` reach its children through their environment**
(`environment`): the variables litelink's `S3Options.resolved()` and
litestream both read. Not argv, which every user on the box can read in `ps`
— a process's environment is its owner's alone. And not the litestream config
either, which litelink keeps free of credentials so it can be copied around.
One environment holds one set, so the server starts one child per set.
"""

from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litelink import S3Options

_PR_SET_PDEATHSIG: Final = 1


def _die_with(parent: int) -> None:  # pragma: no cover — runs between fork and exec
    """In the child: ask the kernel for SIGKILL when the parent dies."""
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    # A parent that died between the fork and the `prctl` sends no signal:
    # the child was already reparented. Seen from here, that is a changed ppid.
    if os.getppid() != parent:
        os._exit(1)


_ENVIRONMENT: Final = {
    "endpoint": "AWS_ENDPOINT_URL",
    "access_key": "AWS_ACCESS_KEY_ID",
    "secret_key": "AWS_SECRET_ACCESS_KEY",
    "region": "AWS_REGION",
}
"""Where `S3Options.resolved()` reads each field it was not given."""


def environment(s3_options: S3Options | None) -> dict[str, str | None]:
    """The variables that give a child `s3_options`: each field set in them.

    A field left unset is left to what the child inherits, as `resolved()`
    leaves it to this process's own environment. Explicit keys are static
    keys, so an inherited `AWS_SESSION_TOKEN` is removed (None) beside them:
    paired with keys it was not issued for, it would fail every call.
    """
    if s3_options is None:
        return {}

    found: dict[str, str | None] = {
        variable: value
        for field, variable in _ENVIRONMENT.items()
        if (value := getattr(s3_options, field)) is not None
    }
    if s3_options.access_key is not None:
        found["AWS_SESSION_TOKEN"] = None
        # litestream's own pair, which it reads before the AWS one: an
        # inherited pair would otherwise win over these keys.
        found["LITESTREAM_ACCESS_KEY_ID"] = s3_options.access_key

    if s3_options.secret_key is not None:
        found["LITESTREAM_SECRET_ACCESS_KEY"] = s3_options.secret_key

    return found


def popen(
    argv: Sequence[str], env: Mapping[str, str | None] | None = None
) -> subprocess.Popen[bytes]:
    """Start `argv` as a child the kernel kills when this process dies.

    **In a session of its own**, so a terminal's signals reach the server and
    not its children. Ctrl-C goes to the whole foreground process group: a
    child in it is interrupted at the same moment the server starts stopping
    it, and the maintainer, already in its last seal pass when the server's
    SIGTERM arrived, abandoned that pass mid-transaction. The server owns its
    children's lifetimes, and stops them itself.

    `env` is laid over this process's environment (`environment`): a value
    replaces the variable, and None removes it.
    """
    parent = os.getpid()
    return subprocess.Popen(  # noqa: S603
        argv,
        env=None if not env else _overlaid(env),
        preexec_fn=(lambda: _die_with(parent)) if sys.platform == "linux" else None,  # noqa: PLW1509
        start_new_session=True,
    )


def _overlaid(env: Mapping[str, str | None]) -> dict[str, str]:
    merged = dict(os.environ)
    for variable, value in env.items():
        if value is None:
            merged.pop(variable, None)
        else:
            merged[variable] = value

    return merged
