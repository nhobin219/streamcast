"""The examples' broker, run in-process under one uvicorn worker.

The app owns the broker; this owns only the socket. It enters the app's
lifespan itself, with uvicorn's turned off, rather than leaving it to uvicorn:
uvicorn logs a failed startup and exits, where an example (and a test) wants
the exception raised where it awaited the start. `uvicorn.run(app,
lifespan="on")` is the same broker for a process that only serves.
"""

from __future__ import annotations

import asyncio

import uvicorn


class Listening:
    def __init__(self, app, host, port, *, sock=None, **transport):
        self.app, self.sock = app, sock
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=host or "127.0.0.1",
                port=port or 0,
                lifespan="off",
                log_level="critical",
                ws_per_message_deflate=transport.pop("ws_per_message_deflate", False),
                **transport,
            )
        )
        self.task = self.stopping = self.lifespan = None

    async def start(self):
        if self.task is not None:
            return self

        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()
        self.task = asyncio.create_task(
            self.server.serve(sockets=[self.sock] if self.sock else None)
        )
        async with asyncio.timeout(5):
            while not self.server.started:
                if self.task.done():
                    await self.task
                    raise RuntimeError("listener failed")

                await asyncio.sleep(0.001)

        return self

    def __await__(self):
        return self.start().__await__()

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()

    @property
    def sockets(self):
        return self.server.servers[0].sockets

    def is_serving(self):
        return self.server.started and not self.server.should_exit

    async def serve_forever(self):
        assert self.task is not None
        await self.task

    def close(self, **unused):
        if self.stopping is None:
            self.stopping = asyncio.create_task(self.stop())

    async def stop(self):
        # Runtime fences and drains before the transport's graceful wait.
        assert self.lifespan is not None and self.task is not None
        await self.lifespan.__aexit__(None, None, None)
        self.server.should_exit = True
        await self.task

    async def wait_closed(self):
        if self.stopping is not None:
            await self.stopping
