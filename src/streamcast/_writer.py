"""The durable append, off the event loop: one writer thread per stream.

**Why.** A durable append is a SQLite commit at `synchronous=FULL` — about
2 ms with its fsync. Run on the event loop, as it was, every stream a broker
serves waits on every other one's disk: measured with one publisher sending
flat out on stream A, a send on stream B went from a 5.5 ms p99 to 33.7 ms,
and the whole loop stalled for up to 71 ms — no other publisher read, no frame
sent, no keepalive answered. A broker's fsync budget was one disk's, shared by
every stream.

Here each durable stream commits on a thread of its own. SQLite releases the
GIL while it waits on the disk, so two streams' commits overlap, and the loop
only ever does what is cheap: validate, queue, and later fan out.

**What must not change, and why it does not.**

* **Order within a stream.** One thread per stream takes jobs first in, first
  out, so commits happen in the order `send` queued them. Each commit is
  handed back with `call_soon_threadsafe`, which runs callbacks from one
  thread in the order they were scheduled — so `Stream._deliver` sees commits
  in commit order, assigns `_end_offset` and fans out in that order (I1, I4).
* **Durable before broadcast (I3).** Nothing is fanned out until the commit
  that holds it has returned.
* **The subscribe partition (I2).** `_end_offset` moves only in `_deliver`, on
  the loop, in the same step as the fan-out. A row committed but not yet
  delivered is at or above the frontier a new subscriber reads, so it reaches
  that subscriber live, never by replay; one below it was fanned out before
  the subscriber joined, and is replayed. No gap, no duplicate.

**Group commit, on by default.** While one commit is in flight, more jobs
queue behind it; the next commit takes all of them in one transaction, one
fsync, each job's rows still adjacent and in queue order. Under concurrent
publishers on one stream, or one publisher pipelining its sends, that is
throughput the per-call fsync never had. A stream created with
`group_commit=False` commits each job on its own. A job's rows are
validated on the loop before they are queued, so one bad row is refused alone
rather than failing every publisher's rows that shared its transaction.

**Exactly-once publishers are checked here, in the commit.** A stream served
with `exactly_once=True` hands this thread a `Publishers` cache (`_dedupe`).
A commit that holds a claimed job runs in litelink's buffer transaction: each
claimed job is judged against its publisher's mark -- appended, answered as a
duplicate, or refused as a gap -- and the marks of the publishers that
appended are written in the same transaction as their rows. The judgement
runs on this thread, not on the loop, because only here is the mark exactly
what the log holds: every job queued before this one has committed or failed.
A failed commit moves no mark, so the jobs queued behind it from the same
publisher are refused as gaps rather than committed out of order.
"""

from __future__ import annotations

import asyncio
import queue
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from litelink import WriteHandle

    from streamcast._dedupe import Publishers

GROUP_ROWS: Final = 10_000
"""The most rows one commit takes, however many are queued.

A bound on one transaction's size and on how long the jobs behind it wait —
not a target: a commit takes what is queued, up to this.
"""


@dataclass(eq=False, slots=True)
class Job:
    """One `send` or `send_many`: its rows, as sent and as stored."""

    rows: Sequence[Mapping[str, object]]
    """As the publisher sent them: what `where=` and the wire read."""
    stored: Sequence[Mapping[str, object]]
    """As the log stores them, stamped."""
    now: int
    """The wall-clock reading they were stamped with, in nanoseconds."""
    done: asyncio.Future[list[int | None]] = field(repr=False)
    publisher: str | None = None
    """The exactly-once publisher that sent them, or None."""
    seq: int = 0
    """The first row's seq, for a claimed job; the rest follow it."""
    skip: int = 0
    """Leading rows already in the log, set by the writer: not appended."""
    prior: list[int | None] = field(default_factory=list, repr=False)
    """The original offsets of those `skip` rows, where they are known."""
    gap: BaseException | None = None
    """Set by the writer when the job's first seq is past the next one."""


@dataclass(eq=False, slots=True)
class Probe:
    """A question for the writer: this publisher's mark, after every job
    queued before it has committed or failed. What a publisher's greeting
    reports."""

    publisher: str
    done: asyncio.Future[tuple[int, int | None]] = field(repr=False)


_STOP: Final = object()


class Writer:
    """The thread that commits one durable stream's rows, in order.

    Started on the first job, so a `Stream` still does no I/O and starts
    nothing when constructed. `deliver` and `fail` run on the event loop the
    first job came from.
    """

    __slots__ = (
        "_deliver",
        "_fail",
        "_group",
        "_log",
        "_loop",
        "_publishers",
        "_queue",
        "_thread",
    )

    def __init__(
        self,
        log: WriteHandle,
        deliver: Callable[[list[Job], list[int]], None],
        fail: Callable[[list[Job], BaseException], None],
        *,
        group: bool = False,
        publishers: Publishers | None = None,
    ) -> None:
        self._log = log
        self._group = group
        self._publishers = publishers
        self._deliver = deliver
        self._fail = fail
        self._queue: queue.SimpleQueue[Job | Probe | object] = queue.SimpleQueue()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def submit(self, job: Job) -> None:
        """Queue `job`. No await, and never blocks: this queue has no bound
        of its own. `Stream._room` bounds it, at `max_inbound` rows, before
        a job gets here."""
        self._start()
        self._queue.put(job)

    def probe(self, publisher: str) -> asyncio.Future[tuple[int, int | None]]:
        """`(last_seq, last_offset)` for `publisher`, once everything queued
        before this call has committed or failed. No await: what the caller
        did before calling is ordered before the answer."""
        if self._publishers is None:
            msg = "this stream does not take exactly-once publishers"
            raise RuntimeError(msg)

        self._start()
        done: asyncio.Future[tuple[int, int | None]] = (
            asyncio.get_running_loop().create_future()
        )
        self._queue.put(Probe(publisher, done))
        return done

    def _start(self) -> None:
        if self._thread is None:
            self._loop = asyncio.get_running_loop()
            self._thread = threading.Thread(
                target=self._run,
                name=f"streamcast-writer-{self._log.name}",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        loop = self._loop
        assert loop is not None  # set before the thread starts
        while True:
            first = self._queue.get()
            if first is _STOP:
                return

            if isinstance(first, Probe):
                self._answer(loop, first)
                continue

            jobs: list[Job] = [first]  # ty: ignore[invalid-assignment]
            count = len(first.stored)  # ty: ignore[unresolved-attribute]
            stopping = False
            held: Probe | None = None
            # Group commit, unless the stream opted out: whatever queued while
            # the last commit ran. Off, each job is its own transaction.
            while self._group and count < GROUP_ROWS:
                try:
                    more = self._queue.get_nowait()
                except queue.Empty:
                    break

                if more is _STOP:
                    stopping = True
                    break

                if isinstance(more, Probe):
                    # Answered after this commit, never inside it: the mark
                    # it reports must include every job queued before it.
                    held = more
                    break

                jobs.append(more)  # ty: ignore[invalid-argument-type]
                count += len(more.stored)  # ty: ignore[unresolved-attribute]

            if self._publishers is not None and any(
                job.publisher is not None for job in jobs
            ):
                self._commit_claimed(loop, jobs, self._publishers)
            else:
                rows = [row for job in jobs for row in job.stored]
                try:
                    offsets = list(self._log.extend(rows))
                except BaseException as exc:  # noqa: BLE001 — handed to every caller in the group
                    loop.call_soon_threadsafe(self._fail, jobs, exc)
                else:
                    loop.call_soon_threadsafe(self._deliver, jobs, offsets)

            if held is not None:
                self._answer(loop, held)

            if stopping:
                return

    def _commit_claimed(
        self,
        loop: asyncio.AbstractEventLoop,
        jobs: list[Job],
        publishers: Publishers,
    ) -> None:
        """One commit holding claimed jobs: judge, append, mark, together."""
        try:
            with self._log.transaction() as tx:
                publishers.plan(tx, jobs)
                rows = [
                    row
                    for job in jobs
                    if job.gap is None
                    for row in job.stored[job.skip :]
                ]
                offsets = list(self._log.extend(rows, tx=tx)) if rows else []
                publishers.write(tx, jobs, offsets)

        except BaseException as exc:  # noqa: BLE001 — handed to every caller in the group
            # Nothing committed, and no mark moved: each job is failed, and
            # the judgements `plan` left on them are moot.
            loop.call_soon_threadsafe(self._fail, jobs, exc)
            return

        publishers.committed()
        publishers.settle(jobs, offsets)
        loop.call_soon_threadsafe(self._deliver, jobs, offsets)

    def _answer(self, loop: asyncio.AbstractEventLoop, probe: Probe) -> None:
        publishers = self._publishers
        assert publishers is not None  # `probe` refuses without
        try:
            with self._log.transaction() as tx:
                entry = publishers.entry(tx, probe.publisher)
                answer = (entry.last_seq, entry.last_offset)

        except BaseException as exc:  # noqa: BLE001 — the prober's to raise
            loop.call_soon_threadsafe(_settle, probe.done, None, exc)
            return

        publishers.committed()
        loop.call_soon_threadsafe(_settle, probe.done, answer, None)

    def close(self) -> None:
        """Commit what is queued, then stop. Blocking: call it in a thread."""
        if self._thread is None:
            return

        self._queue.put(_STOP)
        self._thread.join()
        self._thread = None


def _settle(
    done: asyncio.Future[tuple[int, int | None]],
    answer: tuple[int, int | None] | None,
    exc: BaseException | None,
) -> None:
    if done.done():
        return

    if exc is not None:
        done.set_exception(exc)
    else:
        assert answer is not None
        done.set_result(answer)


__all__ = ["GROUP_ROWS", "Job", "Probe", "Writer"]
