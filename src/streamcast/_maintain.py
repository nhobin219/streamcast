"""The maintainer: a subprocess that keeps a stream's log from growing forever.

**Nothing in this library sealed before this existed, and that was a defect
rather than a missing convenience.** litelink is explicit — *"A maintainer is
not optional. Nothing seals unless something calls `seal()`"* — and a
streamcast server calls neither it nor `advance()`. Measured on 120,000 rows
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

**Five processes, one per role — litelink's recommended split.** A step gets
its own process when it is heavy on CPU or the network:

| role | runs | why it stands alone |
|---|---|---|
| `seal` | `seal()`, flushed per `flush_every` | pure-Python CPU work, and all that bounds the buffer |
| `compact` | `compact()` | the heaviest CPU work; beside `seal` it would delay it |
| `publish` | `publish()`, flushed per `flush_every` | the network: one push to a slow bucket can take a minute |
| `clean` | `evict()`, `reclaim("buffer")`, `reclaim("staging")`, `sweep("staging")` | local disk, freed promptly |
| `clean-published` | `reclaim("published")`, `sweep("published")` | deletes and listing on the published table |

Local cleanup is one process, in that order: eviction queues the files
`reclaim` deletes, and both are metadata commits that finish in milliseconds.
The published table's cleanup is apart because on object storage it is
network calls, and a slow listing of a bucket must not delay freeing local
disk. **`evict()` is what deletes buffer rows already in staging** — `seal()`
and `publish()` move data and delete nothing — so a split without `clean`
grows `buffer.db` without bound.

The cost is memory: each process is a full interpreter with litelink, pyarrow,
pyiceberg and duckdb loaded, ~150-200 MB RSS, so five per server where there
was one. That buys isolation between the steps — a minute-long push never
delays a seal — which is the trade litelink's split makes.

**Two processes on one log is litelink's own shape**, not a liberty taken here:
its `examples/adsb/` runs the writer and the maintainer separately, and the
claim table coordinates them. Each pass claims the offset range it works on, so
the server's appends and this process's seals never contend for the same rows.

**It dies with the server, on purpose** — however the server dies, SIGKILL
included (`_process`). litelink allows one writer, so when the server is gone
nothing is appending and an unsealed buffer is not growing — there is nothing
for an orphaned maintainer to do but hold ~207 MB and contend for the next
server's leases. The interesting direction
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
import math
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final

import litelink

from streamcast._process import popen

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

# Cadences per role, litelink's own (`examples/adsb/maintainer.py`). They
# differ by orders of magnitude because the costs do: `seal` is an indexed
# read of one row when there is nothing to seal, compaction reads and rewrites
# whole files, local cleanup is metadata commits, and the published table's
# cleanup waits on a network — and its sweep lists at most every four hours
# however often it is called.
SEAL_EVERY: Final = 0.25
COMPACT_EVERY: Final = 10.0
PUBLISH_EVERY: Final = 10.0
CLEAN_EVERY: Final = 10.0
CLEAN_PUBLISHED_EVERY: Final = 60.0

# **What bounds RPO without WAL replication, and how far the published table
# trails the writer.** Since litelink 0.11 a plain `seal()` cuts only at
# `target_seal_size` and a plain `publish()` pushes only finished files —
# `target_compact_size`, 512 MiB on disk — so on a stream of 100 rows a second
# the published table trailed by days, and every row in that gap existed on
# this machine only. `seal(flush=True)` cuts whatever is buffered and
# `publish(flush=True)` pushes whatever is sealed; both are needed, which is
# why this is one interval and not one per role. What a flushed publish pushes
# early is swapped for the finished file later, at the cost of uploading
# those rows twice and one published commit per log per flush.
#
# A minute, because the published table is what a catch-up, a snapshot and a
# `Live` view's base read: a `Live` view holds every unpublished row in
# memory up to `max_tail`, and a published table hours behind is a view that
# stops on a busy stream. litelink's own example flushes every 15 minutes,
# for a table nothing reads live.
FLUSH_EVERY: Final = 60.0

# How far past each flush boundary the publish role flushes: after the seal
# role has, so a boundary's publish pushes that boundary's seal rather than
# leaving it for the next one. A seal is bounded by `target_seal_size`, and
# seconds is generous for it.
_PUBLISH_FLUSH_LAG: Final = 5.0

FLUSHED: Final = frozenset({"seal", "publish"})
"""The roles a flush applies to: `compact(flush=True)` rewrites the in-progress
file every call, which litelink keeps for shutdown and tests."""

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
    compact_every: float = COMPACT_EVERY
    publish_every: float = PUBLISH_EVERY
    clean_every: float = CLEAN_EVERY
    clean_published_every: float = CLEAN_PUBLISHED_EVERY

    flush_every: float | None = FLUSH_EVERY
    """Seal and publish with `flush=True` this often, in seconds: the RPO without
    WAL replication, and how far the published table trails the writer.

    Every other pass is unflushed, so files still come out at litelink's
    targets. Flushes fall on wall-clock multiples of this, so the seal and
    publish processes agree on when, across restarts; the publish role
    flushes a few seconds after the seal role, to push what it cut. A row
    reaches the published table about `flush_every + publish_every` after it
    is written, plus the upload. None never flushes: the published table then
    takes only finished files, and RPO is WAL replication's alone.
    """

    dedicated: tuple[str, ...] = field(default=())
    """Logs that get their own five processes rather than sharing them.

    Each role's process sweeps its logs in series, so one log whose
    compaction takes seconds delays every other log's compaction by that
    much. That is
    a fine trade for the small streams this sharing exists for and a bad one
    for a very large or very hot log, which is what this names.

    By the LOG's name, which need not be the route it is served at. A name
    this server does not serve with a log is a `ValueError` at `serve` rather
    than a no-op: ignored, a typo puts the log straight back into the shared
    loop — the one thing it was named to avoid — and the symptom is a latency
    problem the caller believes they already fixed.

    Naming every log starts no shared maintainer at all, rather than five
    interpreters sweeping nothing.
    """

    def __post_init__(self) -> None:
        if self.flush_every is not None and self.flush_every <= 0:
            msg = f"flush_every={self.flush_every}: a positive interval, or None"
            raise ValueError(msg)

    def every(self, role: str) -> float:
        """The cadence of `role`'s passes, in seconds."""
        return getattr(self, f"{role.replace('-', '_')}_every")


# -- the child ---------------------------------------------------------------


def _seal(log: litelink.WriteHandle, flush: bool) -> None:
    log.seal(flush=flush)


def _compact(log: litelink.WriteHandle, _flush: bool) -> None:
    log.compact()


def _publish(log: litelink.WriteHandle, flush: bool) -> None:
    # Every log publishes since litelink 0.6 — to an `s3://` prefix, or by
    # default a directory beside it — and must: eviction never deletes an
    # unpublished file, so a log that never published never frees its disk.
    log.publish(flush=flush)


def _clean(log: litelink.WriteHandle, _flush: bool) -> None:
    # In this order: eviction queues the files `reclaim` deletes. `evict()`
    # is both tables — the buffer rows staging now holds, and the staging
    # files the published table holds. `reclaim("buffer")` is a VACUUM only
    # when the log's `vacuum_free_ratio` is set.
    log.evict()
    log.reclaim("buffer")
    log.reclaim("staging")
    log.sweep("staging")


def _clean_published(log: litelink.WriteHandle, _flush: bool) -> None:
    log.reclaim("published")
    log.sweep("published")


ROLES: Final[dict[str, Callable[[litelink.WriteHandle, bool], None]]] = {
    "seal": _seal,
    "compact": _compact,
    "publish": _publish,
    "clean": _clean,
    "clean-published": _clean_published,
}
"""litelink's process split: one process per role, in pipeline order."""


def finish_retiring(pending: Sequence[tuple[Path, str]]) -> list[tuple[Path, str]]:
    """Retire, through litelink, the retired logs litelink does not know are.

    **A stream migrated before streamcast 0.10 left its old logs sealed but
    not retired**: streamcast sealed them its own way, and with no archive
    they were never published. litelink 0.7 opens one as an ordinary log that
    has published nothing, and since retired logs take no maintainer passes,
    nothing else would ever publish it — a snapshot, a live view or a
    catch-up across its seam would fail for good. `retire()` is what
    `Stream.migrate` does today: publish everything, evict staging, sweep both
    tables, and mark the log retired, after which it is skipped like any
    other.

    One that litelink already reports retired is done. Returns the ones that
    failed, to try again next pass.
    """
    from streamcast import _replicate  # noqa: PLC0415 — only the publish role needs it

    left = []
    for root, name in pending:
        try:
            with litelink.open(root, name) as log:
                if log.config.wal_replication:
                    with _replicate.retiring(log):
                        log.retire()
                else:
                    log.retire()

            print(f"[publish] {name}: retired, published in full", flush=True)

        except litelink.RetiredError:
            pass  # already retired: nothing left to do

        except Exception as exc:  # noqa: BLE001
            print(
                f"[publish] {name}: could not retire, retrying: {exc}",
                file=sys.stderr,
                flush=True,
            )
            left.append((root, name))

    return left


def _flush_slot(role: str, flush_every: float) -> int:
    """Which flush interval the wall clock is in, for `role`.

    Wall clock rather than monotonic, so that two processes started apart —
    or one restarted — agree on the boundaries.
    """
    # Capped at a quarter of the interval, so a short one keeps its meaning.
    lag = min(_PUBLISH_FLUSH_LAG, flush_every / 4) if role == "publish" else 0.0
    return math.floor((time.time() - lag) / flush_every)


def sweep(
    logs: Sequence[tuple[str, litelink.WriteHandle]],
    role: str,
    every: float,
    retiring: Sequence[tuple[Path, str]] = (),
    flush_every: float | None = None,
) -> None:
    """One role's passes over every log this process maintains, until SIGTERM.

    **Every log is isolated from every other one.** The `try` is INSIDE the
    per-log loop rather than around it, so a log whose recovery fails, whose
    published location is unreachable, or whose claim is held elsewhere costs
    that log a pass and costs the others nothing. Around the loop it would
    cost every log after it in the iteration order its whole pass, which is
    the failure mode sharing a process introduces and the one thing that must
    not happen.

    **The slow roles are staggered across logs.** All of them coming due in
    the same pass would put N table-metadata reads back to back, which is the
    latency spike this file moved to a subprocess to avoid — reintroduced at
    1/N the frequency and N times the size. `seal` is not: an idle seal is an
    indexed read, and every log wants one every pass.

    **A log's pass is flushed once per `flush_every` boundary** (`seal` and
    `publish` only): the first pass after the boundary, and every pass after
    that until one succeeds.
    """
    work = ROLES[role]
    flushing = flush_every if role in FLUSHED else None
    flushed = {name: _flush_slot(role, flushing) if flushing else 0 for name, _ in logs}
    pending = list(retiring)
    retry_at = time.monotonic()
    span = 0.0 if role == "seal" else every / max(len(logs), 1)
    due = {
        name: time.monotonic() + index * span for index, (name, _) in enumerate(logs)
    }
    while True:
        if pending and time.monotonic() >= retry_at:
            retry_at = time.monotonic() + every
            pending = finish_retiring(pending)

        for name, log in logs:
            if time.monotonic() < due[name]:
                continue

            # Scheduled before the work, so a pass that raises waits its full
            # interval rather than retrying on the next tick.
            due[name] = time.monotonic() + every
            slot = _flush_slot(role, flushing) if flushing else 0
            try:
                work(log, slot > flushed[name])
                flushed[name] = slot

            except RuntimeError as exc:
                # Another owner holds a claim over the range this pass wanted.
                # Not worth dying over: it means the work is already being done.
                print(f"[{role}] {name}: skipped: {exc}", file=sys.stderr, flush=True)

            except Exception as exc:  # noqa: BLE001, PERF203
                # Anything else — a lost commit race, a transient object-storage
                # error. A maintainer that died here would trade a delay for
                # an outage: nothing has landed, and the work is still there
                # next pass. Named, because with one process for many logs
                # "something failed" no longer says which.
                print(
                    f"[{role}] {name}: retrying next pass: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        # Once per loop, not once per log: N logs share one sleep rather than
        # each waiting behind the others.
        time.sleep(min(every, SEAL_EVERY))


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
    parser.add_argument(
        "--retire",
        action="append",
        nargs=2,
        metavar=("ROOT", "NAME"),
        default=[],
        help="a stream's retired log to retire through litelink if it is not",
    )
    parser.add_argument("--role", choices=list(ROLES), required=True)
    parser.add_argument("--every", type=float, required=True)
    parser.add_argument("--flush-every", type=float, default=None)
    args = parser.parse_args(argv)

    def stop(*_: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)

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
                    f"[{args.role}] {name}: cannot open, not maintained: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        if not opened:
            # Every one failed, which is a condition the supervisor's backoff
            # should see rather than a loop that sweeps nothing for ever.
            print(f"[{args.role}] no logs could be opened", file=sys.stderr, flush=True)

            return 1

        with contextlib.suppress(KeyboardInterrupt):
            sweep(
                opened,
                args.role,
                args.every,
                [(Path(root), name) for root, name in args.retire],
                args.flush_every,
            )

        # The seal role's last pass each on the way out, so a short-lived
        # server does not leave its whole run in the buffer. `seal(flush=True)`
        # rather than `seal()`: an orderly shutdown is the one moment closing
        # the open group is right. Guarded per log, for the reason `sweep` is.
        for _name, log in opened if args.role == "seal" else ():
            with contextlib.suppress(Exception):
                while log.seal(flush=True) is not None:
                    pass

    return 0


# -- the parent --------------------------------------------------------------


class Supervisor:
    """One role's maintainer subprocess for one or more logs, restarted if it dies.

    **One per role per serving process, not per log**, which is what it was. Each
    maintainer process is a full interpreter with litelink, pyarrow, pyiceberg and
    duckdb loaded: measured at ~207 MB RSS each, so a producer serving four
    small streams spent ~830 MB on maintenance for a producer of ~460 MB.
    Worse than the total was the marginal cost — adding a stream that takes a
    few rows a minute cost the same ~207 MB as the busiest one, which turns
    "should this be its own stream" into a resource question it should not be.

    The work itself was never per-process: `seal()` on an idle log is an
    indexed read of one row, so N of them in a loop is the same shape as one.
    `Maintain.dedicated` names the logs that should still get their own.
    """

    __slots__ = (
        "_names",
        "_plan",
        "_process",
        "_retiring",
        "_role",
        "_stopped",
        "_targets",
        "_watch",
    )

    def __init__(
        self,
        targets: Sequence[tuple[Path, str]],
        plan: Maintain,
        role: str,
        retiring: Sequence[tuple[Path, str]] = (),
    ) -> None:
        self._targets = list(targets)
        self._retiring = list(retiring)
        self._names = f"{role}: " + ", ".join(sorted(name for _root, name in targets))
        self._plan = plan
        self._role = role
        self._process: subprocess.Popen[bytes] | None = None
        self._stopped: subprocess.Popen[bytes] | None = None
        self._watch: asyncio.Task[None] | None = None

    @property
    def role(self) -> str:
        """Which of the five roles this process runs."""
        return self._role

    @property
    def retiring(self) -> list[tuple[Path, str]]:
        """Retired logs this process retires through litelink if they are not."""
        return list(self._retiring)

    @property
    def targets(self) -> list[tuple[Path, str]]:
        """The logs this process sweeps. Read by the tests, and by nothing else."""
        return list(self._targets)

    def _spawn(self) -> subprocess.Popen[bytes]:
        # A fresh interpreter rather than a fork. A forked child would inherit
        # this process's event loop and its open SQLite connections, and
        # litelink's handles are not built to be used across a fork.
        return popen(self._spawn_argv())

    def _spawn_argv(self) -> list[str]:
        argv = [sys.executable, "-m", "streamcast", "maintain"]
        for root, name in self._targets:
            argv += ["--log", str(root), name]

        for root, name in self._retiring:
            argv += ["--retire", str(root), name]

        argv += ["--role", self._role, "--every", str(self._plan.every(self._role))]
        if self._plan.flush_every is not None and self._role in FLUSHED:
            argv += ["--flush-every", str(self._plan.flush_every)]

        return argv

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
