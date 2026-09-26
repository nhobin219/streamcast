"""The litestream sidecar: continuous WAL replication, supervised by the server.

A log with `wal_replication` on needs litestream shipping its SQLite WAL to
object storage, and that is what makes the log survive losing its machine. It
is a separate process on purpose — that is what keeps the network out of the
write path — but "separate process" is not the same as "your problem", and an
earlier version of this library made it the operator's by refusing to serve
such a log at all. `serve` starts everything the stream needs.

**One process for every log this server replicates**, not one per log.
litestream takes a `dbs` list, so N logs is N entries in one config rather
than N processes — measured at 40-170 MB each, which on a producer serving
four small streams was most of a gigabyte spent on sidecars for a producer of
~460 MB. The marginal cost was the worse half: a stream taking a few rows a
minute cost the same as the busiest one.

**The lock stays per log, and that is what makes sharing safe.** The flock is
what prevents two litestream instances on one database, and it is a property
of the DATABASE, not of whoever is replicating it. So this takes one lock per
log and replicates exactly the logs whose locks it holds — a log already being
replicated by another process is left out of the config and retried, rather
than making this process stand by for all of them or, far worse, replicate it
anyway.

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
* A log whose lock cannot be taken is left out and retried rather than given
  up on, so a server started beside a dying one takes each database over as
  the kernel frees it. Acquiring one mid-run means rewriting the config and
  restarting litestream, because it reads its `dbs` once at startup.

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
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Final

from litelink._replication import litestream_binary

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    """One litestream process for every log this server replicates.

    Holds one flock per log and replicates exactly the logs it holds. A log
    locked by somebody else is left out of the config and retried, so two
    servers under one root divide the databases between them rather than one
    of them replicating nothing.
    """

    __slots__ = (
        "_binary",
        "_config",
        "_locks",
        "_logs",
        "_owned",
        "_process",
        "_stopping",
        "_watch",
        "_workdir",
    )

    def __init__(
        self, *, logs: Sequence[tuple[str, Path]], binary: str, workdir: Path
    ) -> None:
        """Takes built values and does no I/O. Construct through `new`.

        litelink's own rule, and this class had been breaking it: writing a
        config file and probing PATH from an initialiser means a `Sidecar`
        cannot be made without a real log and a real filesystem.
        """
        self._logs = list(logs)
        self._binary = binary
        self._workdir = workdir
        self._config = workdir / "litestream.yml"
        self._locks: dict[str, object] = {}
        self._owned: set[str] = set()
        self._process: subprocess.Popen[bytes] | None = None
        self._stopping = False
        self._watch: asyncio.Task[None] | None = None

    @classmethod
    def new(cls, logs: Sequence[WriteHandle], binary: str | None = None) -> Sidecar:
        """Write each log's config, resolve litestream, and build the sidecar.

        **Both ordering guarantees the initialiser used to carry are kept
        here, and they are the reason this is a factory rather than a
        lazily-initialised field.** Every config is written NOW, at `serve`
        time, while this process indisputably owns the logs and before any
        maintainer is spawned against them. And a missing binary raises HERE,
        which `_sidecars` reaches before the listener binds — so a server that
        cannot replicate never starts accepting subscribers who would believe
        it was.

        litelink's per-log `litestream.yml` files are still written and still
        the thing to hand your own litestream. The merged config this process
        runs is derived from them and lives in a temporary directory, because
        it belongs to this process rather than to any one log — and two
        servers dividing the logs under one root would otherwise write it to
        the same path.
        """
        written = [(log.name, Path(log.write_replication_config())) for log in logs]
        resolved = litestream_binary(binary)

        if not Path(resolved).is_absolute() or not Path(resolved).exists():
            # `litestream_binary` falls through to the bare name for a
            # PATH install, so resolve it the way `Popen` would.
            if shutil.which(resolved) is None:
                names = ", ".join(repr(name) for name, _ in written)
                msg = (
                    f"{names} have wal_replication on, but litestream was not "
                    f"found (tried {resolved!r}). Install it, or pass "
                    f"replicate=False and run your own."
                )
                raise SidecarUnavailable(msg)

        return cls(
            logs=written,
            binary=resolved,
            workdir=Path(tempfile.mkdtemp(prefix="streamcast-litestream-")),
        )

    @property
    def config(self) -> Path:
        """The merged config this process runs, once it has claimed anything."""
        return self._config

    @property
    def owner(self) -> bool:
        """Whether this process replicates anything at all."""
        return bool(self._owned)

    @property
    def owned(self) -> set[str]:
        """The logs whose locks this process holds, and so is replicating."""
        return set(self._owned)

    def _lock_path(self, config: Path) -> Path:
        # Beside the log, which is the config's directory — NOT `log.root`,
        # which is the parent and shared between streams. Per LOG even though
        # the process is shared, because the thing the lock protects is the
        # database, not the replicator.
        return config.parent / "litestream.lock"

    def _claim(self) -> bool:
        """Try for every lock not yet held. True if the owned set grew.

        Retried, not answered once. Answered once, a standby never becomes the
        owner: the process holding a lock exits cleanly, the kernel frees it,
        and this one goes on believing somebody else is replicating that
        database while nobody is.
        """
        gained = False
        for name, config in self._logs:
            if name in self._owned:
                continue

            handle = self._locks.get(name)
            if handle is None:
                handle = self._lock_path(config).open("w")
                self._locks[name] = handle

            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)  # ty: ignore[invalid-argument-type]
            except OSError:
                continue

            self._owned.add(name)
            gained = True

        return gained

    def _write_config(self) -> None:
        """Merge the owned logs' configs into the one this process runs.

        litelink writes one `dbs:` list per log — three databases each, with
        absolute paths — so merging is concatenating the entries under a
        single header. The shape is asserted rather than assumed: if litelink
        ever writes something else, this fails here with the file named,
        instead of handing litestream a config that silently replicates less
        than it should.
        """
        lines = ["dbs:"]
        for name, config in self._logs:
            if name not in self._owned:
                continue

            body = config.read_text().splitlines()
            if not body or body[0].strip() != "dbs:":
                msg = (
                    f"{config} does not start with 'dbs:'; litelink's "
                    f"replication config format changed and the merge in "
                    f"streamcast._replicate must change with it"
                )
                raise SidecarUnavailable(msg)

            lines.extend(body[1:])

        self._config.write_text("\n".join(lines) + "\n")

    def _spawn(self) -> subprocess.Popen[bytes]:
        return subprocess.Popen(  # noqa: S603
            [self._binary, "replicate", "-config", str(self._config)],
            preexec_fn=_die_with_parent if sys.platform == "linux" else None,  # noqa: PLW1509
        )

    def start(self) -> None:
        self._watch = asyncio.create_task(self._supervise())

    async def _supervise(self) -> None:
        """Claim what is free, replicate it, and widen when more frees up."""
        backoff = itertools.chain(_BACKOFF, itertools.repeat(_BACKOFF[-1]))
        while not self._stopping:
            self._claim()
            if not self._owned:
                # Every database is being replicated by somebody else.
                # Standing by is the correct answer, not an error: starting a
                # second on any of them is the one thing litestream says
                # never to do.
                await asyncio.sleep(_CLAIM_EVERY)
                continue

            self._write_config()
            process = self._spawn()
            self._process = process

            widened = await self._run(process)
            if self._stopping:
                return

            if widened:
                # A log this process did not own became free. litestream reads
                # its `dbs` once at startup, so the only way to add a database
                # is to restart it — which costs a replication gap measured in
                # the poll interval, against leaving that database unreplicated
                # for the life of the server.
                print(
                    f"[streamcast] litestream restarting to add "
                    f"{', '.join(sorted(self._owned))}",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            code = process.returncode
            delay = next(backoff)
            print(
                f"[streamcast] litestream for {', '.join(sorted(self._owned))} "
                f"exited ({code}); restarting in {delay:g}s",
                file=sys.stderr,
                flush=True,
            )
            await asyncio.sleep(delay)

    async def _run(self, process: subprocess.Popen[bytes]) -> bool:
        """Watch one litestream. True if it was stopped to widen the config.

        POLLED, not `to_thread(process.wait)`: waiting in a thread parks a
        default-executor worker for the child's whole life, and that pool is
        `min(32, cpu + 4)` — six on a two-core box — shared with every replay
        scan.
        """
        next_claim = _CLAIM_EVERY
        while not self._stopping and process.poll() is None:
            await asyncio.sleep(_POLL)
            if len(self._owned) == len(self._logs):
                continue

            next_claim -= _POLL
            if next_claim > 0:
                continue

            next_claim = _CLAIM_EVERY
            if self._claim():
                process.terminate()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        asyncio.to_thread(process.wait), timeout=_STOP_GRACE
                    )

                return True

        return False

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
        # same process hands its locks over rather than holding them.
        for handle in self._locks.values():
            with contextlib.suppress(OSError):
                handle.close()  # ty: ignore[unresolved-attribute]

        self._locks.clear()
        self._owned.clear()
        shutil.rmtree(self._workdir, ignore_errors=True)


__all__ = ["Sidecar", "SidecarUnavailable"]
