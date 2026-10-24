"""A ready transport cannot drain a whole burst before another task runs."""

import asyncio

import pytest

from streamcast._subscriber import Subscriber


@pytest.mark.parametrize("kind", ["live", "replay"])
async def test_ready_sends_yield_to_other_tasks_without_losing_order(kind):
    observer = asyncio.get_running_loop().create_future()
    finished = asyncio.Event()
    sent = []
    frames = [str(offset).encode() for offset in range(512)]

    class Peer:
        def __aiter__(self):
            raise AssertionError("the subscriber pump must not read")

        async def close(self, code=1000, reason=""):
            raise AssertionError("a healthy subscriber must not be dropped")

        async def wait_closed(self):
            raise AssertionError("this probe runs the pump directly")

        async def send(self, frame, *, text=None):
            assert text is True
            sent.append(frame)
            if len(sent) == 1:
                # ASGI's send may return without suspending while the socket
                # is writable. The first send schedules a ready competitor;
                # no clock, socket size or scheduler timing sets this race.
                asyncio.get_running_loop().call_soon(
                    lambda: observer.set_result(len(sent))
                )

            if len(sent) == len(frames):
                finished.set()

    subscriber = Subscriber(Peer(), max_backlog=len(frames))

    async def replay():
        for offset, frame in enumerate(frames):
            yield offset, frame

    if kind == "live":
        for frame in frames:
            subscriber.offer({}, frame)

    task = asyncio.create_task(subscriber.pump(replay() if kind == "replay" else None))
    try:
        before_competitor = await observer
        await finished.wait()
        assert 0 < before_competitor < len(frames)
        assert sent == frames
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
