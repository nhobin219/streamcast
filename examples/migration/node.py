"""One stage of a pipeline: subscribe to a stream, publish to another.

A node reads its source in offset order and hands each row to a `step`,
which returns the rows to publish downstream. That is all a stage is, and it
is why a stage can be run twice: a second copy of a node is one more
subscriber on the same source, and the source cannot tell it is there.

**Dropped is not lost.** A node that falls too far behind is dropped by the
server (`TooSlow`) rather than allowed to hold the stream back, which is what
keeps a slow shadow from touching production. It reconnects after the last
row it finished, and the durable source replays the gap.

**At least once.** A node that dies after publishing a row's output and
before it records the row as done publishes that output again on restart.
Every output row carries the source offset it came from, so a duplicate is
visible downstream and can be dropped there.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import streamcast

if TYPE_CHECKING:
    from collections.abc import Callable

    Step = Callable[[int, dict[str, Any]], list[dict[str, Any]]]


class Node:
    """Rows from `source`, through `step`, to `sink` (or nowhere, to watch)."""

    def __init__(
        self,
        name: str,
        source: str,
        step: Step,
        sink: streamcast.Publication | None,
        *,
        delay: float = 0.0,
    ) -> None:
        self.name = name
        self.source = source
        self.step = step
        self.sink = sink
        self.delay = delay  # seconds per row, to play a slow deployment
        self.done: int | None = None  # the last source offset fully handled
        self.published: int | None = None  # the last offset written downstream
        self.drops = 0
        self._changed = asyncio.Condition()

    async def run(self) -> None:
        """From the start of the source, then live, until cancelled."""
        while True:
            start = streamcast.EARLIEST if self.done is None else self.done + 1
            try:
                async with streamcast.connect(self.source, offset=start) as sub:
                    await self._handle(sub)

            except streamcast.TooSlow:
                self.drops += 1  # and resume after `done`: the log has the rest

            except streamcast.NotReplayable as refused:
                if refused.why != "empty":
                    raise

                # Nothing written yet: from the start and from now are the same.
                async with streamcast.connect(self.source) as sub:
                    await self._handle(sub)

    async def _handle(self, sub: streamcast.Subscription) -> None:
        async for offset, row in sub:
            assert offset is not None  # a pipeline's sources are durable
            if self.delay:
                await asyncio.sleep(self.delay)

            out = self.step(offset, row)
            if out and self.sink is not None:
                self.published = (await self.sink.send_many(out))[-1]

            self.done = offset
            async with self._changed:
                self._changed.notify_all()

    async def reached(self, offset: int, timeout: float = 30.0) -> None:
        """Wait until this node has handled source `offset`."""
        async with asyncio.timeout(timeout), self._changed:
            await self._changed.wait_for(
                lambda: self.done is not None and self.done >= offset
            )
