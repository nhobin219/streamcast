"""The maintainer: a subprocess that keeps a stream's log from growing forever.

**Nothing in this library sealed before this existed, and that was a defect
rather than a missing convenience.** litelink is explicit — *"A maintainer is
not optional. Nothing seals unless something calls `seal_due()`"* — and a
streamcast server calls neither it nor `maintain()`. Measured on 120,000 rows
through a server: every row still in the SQLite buffer, zero Parquet files, a
2.6 MB `buffer.db` still growing, and a DuckDB read cache mirroring the
unsealed tail at roughly 1.1x the payload. One maintainer pass turned that
into one 0.7 MB Parquet file and an empty buffer.

**A subprocess, always, and not a thread.** A seal is CPU-bound pure Python —
most of its commit is pyiceberg copying table metadata — so it starves a thread
sharing its interpreter even while holding no lock. litelink measured appends
running 45.2 ms behind an in-process seal. On a fan-out server that is 45 ms in
which no message is fanned out, no subscriber is served and no keepalive is
answered, surfacing as latency spikes that look like a network problem. There
is deliberately no `thread=True` to get that wrong with.

**Two processes on one log is litelink's own shape**, not a liberty taken here:
its `examples/adsb/` runs the writer and the maintainer separately, and the
claim table coordinates them. Each pass claims the offset range it works on, so
the server's appends and this process's seals never contend for the same rows.

**It dies with the server, on purpose.** litelink allows one writer, so when
the server is gone nothing is appending and an unsealed buffer is not growing
— there is nothing for an orphaned maintainer to do. The interesting direction
is the other one: a maintainer that dies while the server lives puts the
library straight back into the state above, silently. So `Supervisor` restarts
it, which litelink's leases make safe — they lapse, and the next process takes
them.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final

import litelink

if TYPE_CHECKING:
    from collections.abc import Sequence

# Cadences, and they differ by 40x because the costs do. `seal_due` is an
# indexed read of one row when there is nothing to seal, so calling it four
# times a second is close to free and keeps the buffer shallow. `maintain`
# reads table metadata to compact, evict and expire, so it runs rarely.
# Both are litelink's own demo defaults.
SEAL_EVERY: Final = 0.25
MAINTAIN_EVERY: Final = 10.0

# How long a maintainer gets to exit on its own before it is killed.
_STOP_GRACE: Final = 5.0

# How often a supervisor looks at its child. Polled rather than waited on;
# see `_supervise` for why a thread is the wrong tool here.
_POLL: Final = 0.5

# Restart backoff after an unexpected exit. Capped, because a maintainer that
# cannot start is usually a permanent condition — a missing root, a corrupt
# log — and retrying it every 100 ms would bury the reason in its own noise.
_BACKOFF: Final = (0.5, 1.0, 2.0, 5.0, 10.0)


@dataclass(frozen=True, slots=True)
class Maintain:
    """How often a stream's log is swept. `serve(maintain=Maintain(...))`.

    Exists so the cadences have a home that is not `serve`'s signature, and so
    that a later need — a memory cap on the subprocess, a niced priority — has
    somewhere to go without another two keyword arguments.
    """

    seal_every: float = SEAL_EVERY
    maintain_every: float = MAINTAIN_EVERY

    dedicated: tuple[str, ...] = field(default=())
    """Streams that get a maintainer to themselves rather than sharing one.

    The shared loop sweeps its logs in series, so one log whose `maintain()`
    takes seconds delays every other log's `seal_due()` by that much. That is
    a fine trade for the small streams this sharing exists for and a bad one
    for a very large or very hot log, which is what this names.
    """


# -- the child ---------------------------------------------------------------


def sweep(logs: Sequence[tuple[str, litelink.WriteHandle]], plan: Maintain) -> None:
    """The loop over every log this process maintains, until SIGTERM.

    Both calls, at their own cadences, because **`maintain()` does not seal**
    — litelink's docstring calls `seal_due` "the maintainer's frequent call,
    and the counterpart to `maintain`". A loop that ran only `maintain()`
    would compact and expire an empty table for ever while the buffer grew,
    which is the failure this file exists to prevent wearing a disguise.

    **Every log is isolated from every other one.** The `try` is INSIDE the
    per-log loop rather than around it, so a log whose recovery fails, whose
    archive is unreachable, or whose claim is held elsewhere costs that log a
    pass and costs the others nothing. Around the loop it would cost every
    log after it in the iteration order its whole pass, which is the failure
    mode sharing a process introduces and the one thing that must not happen.

    **`maintain()` is staggered across logs.** All of them coming due in the
    same pass would put N table-metadata reads back to back, which is the
    latency spike this file moved to a subprocess to avoid — reintroduced at
    1/N the frequency and N times the size.
    """
    # Spread the first `maintain()` across the interval rather than firing
    # them together on the first pass.
    span = plan.maintain_every / max(len(logs), 1)
    due = {
        name: time.monotonic() + index * span for index, (name, _) in enumerate(logs)
    }
    while True:
        for name, log in logs:
            try:
                log.seal_due()
                if time.monotonic() >= due[name]:
                    log.maintain()
                    if log.archive:
                        log.sync()

                    due[name] = time.monotonic() + plan.maintain_every

            except RuntimeError as exc:
                # Another owner holds a claim over the range this pass wanted.
                # Not worth dying over: it means the work is already being done.
                print(f"[maintain] {name}: skipped: {exc}", file=sys.stderr, flush=True)

            except Exception as exc:  # noqa: BLE001, PERF203
                # Anything else — a lost commit race, a transient archive
                # error. A maintainer that died here would trade a delay for
                # an outage: nothing has landed, and the work is still there
                # next pass. Named, because with one process for many logs
                # "something failed" no longer says which.
                print(
                    f"[maintain] {name}: retrying next pass: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        # Once per pass, not once per log: `seal_every` is the cadence each
        # log is checked at, and N logs share one sleep rather than each
        # waiting behind the others.
        time.sleep(plan.seal_every)


def main(argv: list[str] | None = None) -> int:
    """`python -m streamcast maintain --log ROOT NAME [--log ROOT NAME ...]`

    `--log` takes its two values separately rather than one `root:name`
    string, because a root is a filesystem path and a path may contain a
    colon — a separator would have to be escaped, and the escape would be
    wrong exactly once, on somebody's machine, in a subprocess whose failure
    is a log that silently stops sealing.
    """
    parser = argparse.ArgumentParser(description="Sweep streamcast logs.")
    parser.add_argument(
        "--log",
        action="append",
        nargs=2,
        metavar=("ROOT", "NAME"),
        required=True,
        dest="logs",
    )
    parser.add_argument("--seal-every", type=float, default=SEAL_EVERY)
    parser.add_argument("--maintain-every", type=float, default=MAINTAIN_EVERY)
    args = parser.parse_args(argv)

    def stop(*_: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    plan = Maintain(seal_every=args.seal_every, maintain_every=args.maintain_every)

    # `open`, never `new`: the server created these logs and is writing to
    # them. Opened one at a time and kept only if it works, so one unopenable
    # log — a corrupt buffer, a root that moved — costs itself and not every
    # other log this process was given.
    with contextlib.ExitStack() as stack:
        opened: list[tuple[str, litelink.WriteHandle]] = []
        for root, name in args.logs:
            try:
                opened.append(
                    (name, stack.enter_context(litelink.open(Path(root), name)))
                )

            except Exception as exc:  # noqa: BLE001, PERF203
                print(
                    f"[maintain] {name}: cannot open, not maintained: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        if not opened:
            # Every one failed, which is a condition the supervisor's backoff
            # should see rather than a loop that sweeps nothing for ever.
            print("[maintain] no logs could be opened", file=sys.stderr, flush=True)

            return 1

        with contextlib.suppress(KeyboardInterrupt):
            sweep(opened, plan)

        # One last pass each on the way out, so a short-lived server does not
        # leave its whole run in the buffer. `seal()` rather than `seal_due()`:
        # an orderly shutdown is the one moment closing the open group is
        # right. Guarded per log, for the reason `sweep` is.
        for _name, log in opened:
            with contextlib.suppress(Exception):
                while log.seal() is not None:
                    pass

    return 0


# -- the parent --------------------------------------------------------------


class Supervisor:
    """One maintainer subprocess for one or more logs, restarted if it dies.

    **One per serving process, not one per log**, which is what it was. Each
    maintainer is a full interpreter with litelink, pyarrow, pyiceberg and
    duckdb loaded: measured at ~207 MB RSS each, so a producer serving four
    small streams spent ~830 MB on maintenance for a producer of ~460 MB.
    Worse than the total was the marginal cost — adding a stream that takes a
    few rows a minute cost the same ~207 MB as the busiest one, which turns
    "should this be its own stream" into a resource question it should not be.

    The work itself was never per-process: `seal_due()` on an idle log is an
    indexed read of one row, so N of them in a loop is the same shape as one.
    `Maintain.dedicated` names the logs that should still get their own.
    """

    __slots__ = ("_names", "_plan", "_process", "_stopped", "_targets", "_watch")

    def __init__(self, targets: Sequence[tuple[Path, str]], plan: Maintain) -> None:
        self._targets = list(targets)
        self._names = ", ".join(sorted(name for _root, name in targets))
        self._plan = plan
        self._process: subprocess.Popen[bytes] | None = None
        self._stopped: subprocess.Popen[bytes] | None = None
        self._watch: asyncio.Task[None] | None = None

    @property
    def targets(self) -> list[tuple[Path, str]]:
        """The logs this process sweeps. Read by the tests, and by nothing else."""
        return list(self._targets)

    def _spawn(self) -> subprocess.Popen[bytes]:
        # A fresh interpreter rather than a fork. A forked child would inherit
        # this process's event loop and its open SQLite connections, and
        # litelink's handles are not built to be used across a fork.
        argv = [sys.executable, "-m", "streamcast", "maintain"]
        for root, name in self._targets:
            argv += ["--log", str(root), name]

        argv += [
            "--seal-every",
            str(self._plan.seal_every),
            "--maintain-every",
            str(self._plan.maintain_every),
        ]

        return subprocess.Popen(argv)  # noqa: S603

    def start(self) -> None:
        self._process = self._spawn()
        self._watch = asyncio.create_task(self._supervise())

    async def _supervise(self) -> None:
        """Restart it if it exits while we are still serving.

        The direction that matters. A maintainer that dies unnoticed puts the
        server back to never sealing, which is invisible until the box runs
        out of disk — so this is not tidiness, it is the defect reappearing.
        litelink's leases make the restart safe: they lapse, and the next
        process takes them.
        """
        # The last delay repeats for ever. An `itertools` chain rather than a
        # padded tuple, which the first version of this line was — and which
        # allocated a million floats to express "and then keep trying".
        forever = itertools.chain(_BACKOFF, itertools.repeat(_BACKOFF[-1]))
        for delay in forever:
            process = self._process
            if process is None:
                return

            # POLLED, not `to_thread(process.wait)`. Waiting in a thread
            # parks a default-executor worker for the child's whole life —
            # the entire server run — and that pool is `min(32, cpu + 4)`,
            # six on a two-core box, shared with every replay scan. Three
            # maintained streams would have taken half of it permanently.
            while process.poll() is None:
                if self._process is not process:
                    # Stopped deliberately; `terminate` swapped it out.
                    return

                await asyncio.sleep(_POLL)

            if self._process is not process:
                return

            code = process.returncode

            print(
                f"[streamcast] maintainer for {self._names} exited ({code}); "
                f"restarting in {delay:g}s",
                file=sys.stderr,
                flush=True,
            )
            await asyncio.sleep(delay)
            if self._process is not process:
                return

            self._process = self._spawn()

    def terminate(self) -> None:
        """Ask it to stop. Synchronous, to match `websockets.Server.close`."""
        process, self._process = self._process, None
        # Held so `wait_closed` can REAP it. Dropping the reference here left
        # a zombie for the life of the parent, which on a server that restarts
        # its listener is one per restart.
        self._stopped = process
        if self._watch is not None:
            self._watch.cancel()

        if process is not None and process.poll() is None:
            process.terminate()

    async def wait_closed(self) -> None:
        """Wait for it to go, and kill it if it will not."""
        if self._watch is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch

        process, self._stopped = self._stopped, None
        if process is None:
            return

        try:
            await asyncio.wait_for(asyncio.to_thread(process.wait), timeout=_STOP_GRACE)
        except TimeoutError:
            # It had its grace period. A maintainer wedged mid-commit would
            # otherwise hold the server's shutdown open indefinitely, and
            # litelink's claims lapse on their own, so killing it costs a
            # lease expiry rather than consistency.
            process.kill()
            await asyncio.to_thread(process.wait)
