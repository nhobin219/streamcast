"""The litestream sidecar: continuous WAL replication, supervised by the server.

A log with `wal_replication` on needs litestream shipping its SQLite WAL to
object storage, and that is what makes the log survive losing its machine. It
is a separate process on purpose — that is what keeps the network out of the
write path — but "separate process" is not the same as "your problem", and an
earlier version of this library made it the operator's by refusing to serve
such a log at all. `serve` starts everything the stream needs.

**Two litestream instances on one database is the thing litestream forbids**,
and the whole design here is about not doing it:

* An `flock` on a file beside the log, taken non-blocking and held for this
  process's whole life. The kernel releases it when the process dies however
  it dies, which a lock FILE cannot promise — a SIGKILL would leave that as a
  stale lock nothing could clear.
* The lock lives beside the log, not beside the ROOT. litelink's own example
  uses `log.root / "litestream.lock"`, which is correct for the single log it
  runs and wrong here: `log.root` is the PARENT directory, so two streams
  under one root would contend for one lock and only one would ever
  replicate. It is derived from the config path instead, which litelink
  writes into the log's own directory.
* `PR_SET_PDEATHSIG` on the child, so a SIGKILL of the server does not leave
  litestream running against a database the next server is about to start
  replicating. A signal handler cannot cover SIGKILL; only the kernel can.
* A process that cannot take the lock stands by and retries rather than
  giving up, so a server started beside a dying one takes over when the
  kernel frees it.

**Replication lives exactly as long as the writer**, which is the property
litelink's example could not offer — it hangs the sidecar off the maintainer,
and notes that "a deployment that runs a writer with no maintainer has
`wal_replication=True` and no replication". Here the server holds the log, so
tying the sidecar to the server ties it to the rows it protects. It is also
why `replicate` is its own argument rather than part of `maintain`.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import fcntl
import itertools
import signal
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Final

from litelink._replication import litestream_binary

if TYPE_CHECKING:
    from litelink import WriteHandle

# How often a supervisor looks at its child, and how often a standby retries
# the lock. Polled rather than waited on: `asyncio.to_thread(process.wait)`
# would park a pool thread for the child's whole life, and that pool is
# `min(32, cpu + 4)` — six on a two-core box — shared with every replay scan.
_POLL: Final = 0.5
_CLAIM_EVERY: Final = 5.0

_STOP_GRACE: Final = 10.0
_BACKOFF: Final = (0.5, 1.0, 2.0, 5.0, 10.0)

# `PR_SET_PDEATHSIG`. Linux only; there is no portable equivalent, and on
# anything else the child is reaped by `terminate` alone — which covers every
# ordinary shutdown and not a SIGKILL of the server.
_PR_SET_PDEATHSIG: Final = 1


def _die_with_parent() -> None:  # pragma: no cover — runs between fork and exec
    """In the child: ask the kernel for SIGKILL when the parent dies."""
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)


class SidecarUnavailable(RuntimeError):
    """`wal_replication` is on and litestream cannot be run.

    Raised at `serve`, not at the first missed push. A server that came up and
    quietly replicated nothing would leave an operator believing they had
    continuous RPO protection when they had none, which is the failure this
    whole module exists to prevent.
    """


class Sidecar:
    """One litestream process for one log, flock-guarded and restarted."""

    __slots__ = (
        "_binary",
        "_config",
        "_lock",
        "_name",
        "_owner",
        "_process",
        "_stopping",
        "_watch",
    )

    def __init__(self, log: WriteHandle, binary: str | None = None) -> None:
        # Written now, at `serve` time, while this process indisputably owns
        # the log — before any maintainer is spawned against it.
        self._config = Path(log.write_replication_config())
        self._name = log.name
        self._binary = litestream_binary(binary)
        self._lock: object | None = None
        self._owner = False
        self._process: subprocess.Popen[bytes] | None = None
        self._stopping = False
        self._watch: asyncio.Task[None] | None = None

        if not Path(self._binary).is_absolute() or not Path(self._binary).exists():
            # `litestream_binary` falls through to the bare name for a
            # PATH install, so resolve it the way `Popen` would.
            from shutil import which

            if which(self._binary) is None:
                msg = (
                    f"{log.name!r} has wal_replication on, but litestream was not "
                    f"found (tried {self._binary!r}). Install it, or pass "
                    f"replicate=False and run your own."
                )
                raise SidecarUnavailable(msg)

    @property
    def config(self) -> Path:
        return self._config

    @property
    def owner(self) -> bool:
        """Whether this process holds the lock and is the one replicating."""
        return self._owner

    def _claim(self) -> bool:
        """Try for the lock. Retried, not answered once.

        Answered once, a standby never becomes the owner: the process holding
        it exits cleanly, the kernel frees the lock, and the standby goes on
        believing somebody else is replicating while nobody is.
        """
        if self._lock is None:
            # Beside the log, which is the config's directory — NOT
            # `log.root`, which is the parent and shared between streams.
            self._lock = (self._config.parent / "litestream.lock").open("w")

        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # ty: ignore[invalid-argument-type]
        except OSError:
            return False

        self._owner = True

        return True

    def _spawn(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen(  # noqa: S603
            [self._binary, "replicate", "-config", str(self._config)],
            preexec_fn=_die_with_parent if sys.platform == "linux" else None,  # noqa: PLW1509
        )

    def start(self) -> None:
        self._watch = asyncio.create_task(self._supervise())

    async def _supervise(self) -> None:
        """Take the lock, run litestream, restart it, stand by if refused."""
        backoff = itertools.chain(_BACKOFF, itertools.repeat(_BACKOFF[-1]))
        while not self._stopping:
            if not self._owner and not self._claim():
                # Someone else is replicating this database. Standing by is
                # the correct answer, not an error: starting a second is the
                # one thing litestream says never to do.
                await asyncio.sleep(_CLAIM_EVERY)
                continue

            self._process = self._spawn()
            while not self._stopping and self._process.poll() is None:
                await asyncio.sleep(_POLL)

            if self._stopping:
                return

            code = self._process.returncode
            delay = next(backoff)
            print(
                f"[streamcast] litestream for {self._name!r} exited ({code}); "
                f"restarting in {delay:g}s",
                file=sys.stderr,
                flush=True,
            )
            await asyncio.sleep(delay)

    def terminate(self) -> None:
        """Ask it to stop. An orphan would keep replicating a database the
        next server is about to start replicating."""
        self._stopping = True
        if self._watch is not None:
            self._watch.cancel()

        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()

    async def wait_closed(self) -> None:
        if self._watch is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch

        process, self._process = self._process, None
        if process is not None:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(process.wait), timeout=_STOP_GRACE
                )
            except TimeoutError:
                process.kill()
                await asyncio.to_thread(process.wait)

        # Released explicitly as well as by exit, so a server restarted in the
        # same process hands the lock over rather than holding it.
        if self._lock is not None:
            with contextlib.suppress(OSError):
                self._lock.close()  # ty: ignore[unresolved-attribute]

            self._lock = None
            self._owner = False


__all__ = ["Sidecar", "SidecarUnavailable"]
