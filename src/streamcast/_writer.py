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
fsync, each job's rows still adjacent and in queue order. A stream created
with `group_commit=False` commits each job on its own. Under concurrent publishers on one
stream that is throughput the per-call fsync never had. A job's rows are
validated on the loop before they are queued, so one bad row is refused alone
rather than failing every publisher's rows that shared its transaction.
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
    done: asyncio.Future[list[int]] = field(repr=False)


_STOP: Final = object()


class Writer:
    """The thread that commits one durable stream's rows, in order.

    Started on the first job, so a `Stream` still does no I/O and starts
    nothing when constructed. `deliver` and `fail` run on the event loop the
    first job came from.
    """

    __slots__ = ("_deliver", "_fail", "_group", "_log", "_loop", "_queue", "_thread")

    def __init__(
        self,
        log: WriteHandle,
        deliver: Callable[[list[Job], list[int]], None],
        fail: Callable[[list[Job], BaseException], None],
        *,
        group: bool = False,
    ) -> None:
        self._log = log
        self._group = group
        self._deliver = deliver
        self._fail = fail
        self._queue: queue.SimpleQueue[Job | object] = queue.SimpleQueue()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def submit(self, job: Job) -> None:
        """Queue `job`. No await, and never blocks: the queue is unbounded,
        and what bounds it is each publisher awaiting its own commit."""
        if self._thread is None:
            self._loop = asyncio.get_running_loop()
            self._thread = threading.Thread(
                target=self._run,
                name=f"streamcast-writer-{self._log.name}",
                daemon=True,
            )
            self._thread.start()

        self._queue.put(job)

    def _run(self) -> None:
        loop = self._loop
        assert loop is not None  # set before the thread starts
        while True:
            first = self._queue.get()
            if first is _STOP:
                return

            jobs: list[Job] = [first]  # ty: ignore[invalid-assignment]
            count = len(first.stored)  # ty: ignore[unresolved-attribute]
            stopping = False
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

                jobs.append(more)  # ty: ignore[invalid-argument-type]
                count += len(more.stored)  # ty: ignore[unresolved-attribute]

            rows = [row for job in jobs for row in job.stored]
            try:
                offsets = list(self._log.extend(rows))
            except BaseException as exc:  # noqa: BLE001 — handed to every caller in the group
                loop.call_soon_threadsafe(self._fail, jobs, exc)
            else:
                loop.call_soon_threadsafe(self._deliver, jobs, offsets)

            if stopping:
                return

    def close(self) -> None:
        """Commit what is queued, then stop. Blocking: call it in a thread."""
        if self._thread is None:
            return

        self._queue.put(_STOP)
        self._thread.join()
        self._thread = None


__all__ = ["GROUP_ROWS", "Job", "Writer"]
