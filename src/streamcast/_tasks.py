"""Blocking work whose lifetime survives cancellation of its waiter."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


async def join(task: asyncio.Task) -> bool:
    """Join an owned task despite repeated waiter cancellation.

    Return whether the waiter was cancelled; the owner can then dispose of
    a successfully opened resource before propagating that cancellation.
    """
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True

    return cancelled


async def thread(call: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Join the actual worker before releasing its caller's resource lease.

    Cancelling `to_thread` only cancels the future. A replay must keep its
    handle (and its Arrow reader) until the underlying scan has returned.
    Repeated cancellation does not shorten that lifetime.
    """
    task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
    cancelled = await join(task)
    if cancelled:
        # Retrieve a worker exception too: it must not become an unobserved
        # task error when cancellation is what the caller receives.
        if not task.cancelled():
            task.exception()

        raise asyncio.CancelledError

    return task.result()


@asynccontextmanager
async def reader(call, **kwargs):
    """Join a scan acquisition and close its result even if acquisition cancels."""
    task = asyncio.create_task(asyncio.to_thread(call, **kwargs))
    cancelled = await join(task)
    opened = task.result()
    try:
        if cancelled:
            raise asyncio.CancelledError

        yield opened
    finally:
        await thread(opened.close)
