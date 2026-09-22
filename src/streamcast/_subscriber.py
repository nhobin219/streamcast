"""One attached subscriber: a bounded queue and the task that drains it.

**This is the file that keeps a slow consumer from becoming everyone's
problem.** A broadcast written the obvious way — `for s in subscribers: await
s.send(frame)` — makes the slowest consumer the rate of the whole stream,
because every other subscriber's frame is queued behind an `await` on a TCP
window that is not opening. The fix is not a faster loop; it is that the
broadcast must not `await` a consumer at all.

So `offer` is synchronous, never blocks, never raises, and is the ONLY thing
`Stream.send` calls. Each subscriber owns a queue and a task, and a consumer
that stops reading fills its own queue and nobody else's.

A full queue is the case with no free answer, and the one taken here is to
**drop the subscriber**. The two alternatives both fail the goal in the note
this library came from — that every client receives the same data:

* Drop the oldest and keep going, and the subscriber silently has a hole in
  the middle of its stream with nothing marking it.
* Grow without bound, and the server's memory is set by its worst consumer.
  This is the OOM the design exists to avoid.

Dropping is the only one that leaves the stream a contiguous prefix. With a
log attached that is not even data loss: the subscriber reconnects one above
its last received offset and the replay fills the gap, which is exactly what
the durable tier is for. Without one it IS data loss, said out loud with a
4429 rather than discovered later.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Final

from streamcast._errors import Close
from streamcast._protocol import refusal

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from websockets.asyncio.server import ServerConnection

_OVERFLOW: Final = None
"""Queued in place of the message that would not fit.

A sentinel IN the queue rather than a flag beside it, because the pump is
parked in `queue.get()` or in `connection.send()` and a flag is only read
between the two. Travelling in the queue means the drop is delivered in order:
the subscriber receives every frame that was accepted before the overflow,
then the close — a contiguous prefix, which is the property that makes
`last_seen + 1` a correct resume point.

`None` rather than a fresh `object()`, and that is a typing decision as much
as a taste one: `bytes | None` narrows on `is None` in every type checker,
where `bytes | object` narrows in none of them and needs an `assert` on the
send path to say what the queue already guarantees.
"""


class Subscriber:
    """A connection, its backlog, and the offset it has actually been sent."""

    __slots__ = ("_backlog", "_connection", "_dropped", "_queue")

    def __init__(self, connection: ServerConnection, *, max_backlog: int) -> None:
        self._connection = connection
        self._backlog = max_backlog
        # One over, and the extra slot is reserved for `_OVERFLOW`. The
        # alternative — a queue of exactly `max_backlog` that evicts one
        # message to make room for the sentinel — was written first and is
        # wrong: the eviction takes the OLDEST queued frame, the one the
        # subscriber has not seen, so what it receives is a prefix with a hole
        # punched in it and nothing says where.
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue(
            maxsize=max_backlog + 1
        )
        self._dropped = False

    def offer(self, frame: bytes) -> None:
        """Queue one frame. Never blocks, never awaits, never raises.

        Every one of those three matters and `Stream.send` depends on all of
        them: it calls this in a loop over every subscriber, and the loop has
        to be atomic against the event loop for messages to reach every
        subscriber in the same order they were assigned offsets.
        """
        if self._dropped:
            return

        if self._queue.qsize() >= self._backlog:
            self._dropped = True
            self._queue.put_nowait(_OVERFLOW)
            return

        self._queue.put_nowait(frame)

    async def close(self, code: int, reason: str) -> None:
        """End this subscription. The pump unwinds on its own once it does.

        No flag and no cancellation: closing the connection makes the pending
        `send` or the next one raise `ConnectionClosed`, which is the same
        path a subscriber that walked away takes. One way out of the pump is
        easier to reason about than two.
        """
        await self._connection.close(code, reason)

    async def run(self, replay: AsyncIterator[tuple[int, bytes]] | None) -> None:
        """Pump until the connection ends, whichever end ends it.

        **The pump alone is not enough, and the reason is asymmetry.** A
        subscriber that walks away is noticed by `send` raising
        `ConnectionClosed` — but only if there is something to send. On a
        stream that is quiet, or between bursts, the pump is parked in
        `queue.get()` and nothing ever wakes it: the task lives for ever, the
        `Subscriber` stays in the fan-out set, and every disconnect leaks one
        of each. Measured before this existed: a connect/disconnect loop left
        the server's subscriber count climbing and its handler tasks never
        returning, and neither is visible until the box runs out of something.

        So the pump races the connection's own closed future. Two tasks per
        SUBSCRIBER, created once at attach — not two per message, which is
        what racing inside the pump's loop would have cost.
        """
        pump = asyncio.create_task(self.pump(replay))
        gone = asyncio.create_task(self._connection.wait_closed())
        try:
            done, _pending = await asyncio.wait(
                {pump, gone}, return_when=asyncio.FIRST_COMPLETED
            )
            if pump in done:
                # Re-raised, not swallowed: `ConnectionClosed` is how `serve`
                # tells an ordinary disconnect from a handler that failed.
                pump.result()

        finally:
            pump.cancel()
            gone.cancel()
            # Awaited, or a cancelled task that had already failed logs
            # "Task exception was never retrieved" on the next collection —
            # a traceback per disconnect, about nothing.
            await asyncio.gather(pump, gone, return_exceptions=True)

    async def pump(self, replay: AsyncIterator[tuple[int, bytes]] | None) -> None:
        """Send the replay, then the live queue, until the connection ends.

        The replay goes first and the live queue fills behind it — which is
        the whole reason `max_backlog` and `max_replay` are sized against each
        other rather than independently. A replay of `max_replay` messages has
        to finish within `max_backlog` messages of live traffic or the
        subscriber is dropped the moment it catches up, having done all the
        work. See `Stream` for the arithmetic the defaults are chosen by.
        """
        if replay is not None:
            async for _offset, frame in replay:
                await self._connection.send(frame)

        while True:
            frame = await self._queue.get()
            if frame is _OVERFLOW:
                await self._connection.close(
                    Close.TOO_SLOW, refusal("too_slow", backlog=self._backlog)
                )
                return

            await self._connection.send(frame)


__all__ = ["Subscriber"]
