"""ASGI WebSocket transport; routing and resource ownership live in the broker."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Final

from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from websockets.frames import Close as _CloseFrame

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import Scope

MAX_QUEUE: Final = 16
"""Frames from one peer that may sit unread before the reader stops reading.

`websockets` bounds its own receive queue at this and streamcast never raised
it, so matching keeps a publisher's flow control the same on both transports.
The bound is what makes a peer that floods the socket fill its own window
rather than this process's memory — the receive-side twin of `max_backlog`.

One extra slot is reserved on top of it for the end-of-stream sentinel, the
same arithmetic and the same reason as `_subscriber`'s overflow slot.
"""

_EOF: Final = None
"""Queued when the peer is gone, so `async for` ends rather than parking.

In the queue rather than beside it, because `serve_publisher` is suspended in
`get()` and a flag is only read between iterations — the identical argument
`_subscriber` makes for putting its overflow marker in the queue. `None` for
the same typing reason too: `str | bytes | None` narrows on `is None`.
"""


class _Peer:
    """Starlette's `WebSocket` behind `_transport.Peer`.

    **The reader task is the part that is not a rename.** `websockets` runs its
    own reader, so `wait_closed()` resolves and pongs are answered whether or
    not the application ever calls `recv`. ASGI has no such thing: a disconnect
    is a message on the receive channel and nothing learns of it until someone
    calls `receive()`. Since a subscription never reads, this reads for it —
    one task per connection, feeding `__aiter__` for the publish path and
    resolving `wait_closed()` for both.

    Without it a subscriber that walked away from a quiet stream would never be
    noticed: the pump parks in `queue.get()`, the `Subscriber` stays in the
    fan-out set, and every disconnect leaks one of each. That is the same
    defect `_subscriber.run` was written to close, arriving by a different
    route.
    """

    __slots__ = ("_closed", "_incoming", "_rcvd", "_reader", "_sent_close", "_ws")

    def __init__(self, websocket: WebSocket, *, max_queue: int = MAX_QUEUE) -> None:
        self._ws = websocket
        self._incoming: asyncio.Queue[str | bytes | None] = asyncio.Queue(
            maxsize=max_queue + 1
        )
        self._closed = asyncio.Event()
        self._rcvd: _CloseFrame | None = None
        self._sent_close = False
        self._reader: asyncio.Task[None] | None = None

    async def __aenter__(self) -> _Peer:
        self._reader = asyncio.create_task(self._read())

        return self

    async def __aexit__(self, *_exc: object) -> None:
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _read(self) -> None:
        """Drain the receive channel until the peer goes away.

        The close CODE is kept, not just the fact: the broker distinguishes an
        ordinary disconnect from a failure by it, and a client that closed
        cleanly should not read as one that vanished.
        """
        try:
            while True:
                message = await self._ws.receive()
                if message["type"] == "websocket.disconnect":
                    self._rcvd = _CloseFrame(
                        int(message.get("code", 1005)), message.get("reason") or ""
                    )
                    return

                frame = message.get("text")
                if frame is None:
                    frame = message.get("bytes")

                if frame is not None:
                    # `put`, not `put_nowait`: a full queue must stop this task
                    # reading, which is what pushes back on the peer. Dropping
                    # a publisher's frame silently would acknowledge nothing
                    # and lose a row.
                    await self._incoming.put(frame)

        except (WebSocketDisconnect, RuntimeError, OSError):
            # The server tore the connection down under us. Same end as a
            # disconnect message, with no code to report.
            if self._rcvd is None:
                self._rcvd = _CloseFrame(1006, "")

        finally:
            self._closed.set()
            with contextlib.suppress(asyncio.QueueFull):
                self._incoming.put_nowait(_EOF)

    def _gone(self) -> ConnectionClosed:
        """The exception `websockets` would have raised for this disconnect.

        Reused rather than invented so `_stream`, `_subscriber` and the broker
        keep catching one type. A new exception here would mean a transport
        the stream layer has to know the name of, which is the coupling this
        module exists to avoid.
        """
        return ConnectionClosedError(self._rcvd, None)

    async def send(self, message: str | bytes, /, *, text: bool | None = None) -> None:
        """One frame, with `websockets`' rule for the opcode — see `Peer.send`.

        A `str` is TEXT, and so are `bytes` with `text=True`, which is how
        every data frame arrives. ASGI's `"text"` has to be a `str`, so this
        transport decodes the shared bytes once per subscriber — ~50 ns for a
        typical frame, the one cost of TEXT that `websockets` does not pay.
        """
        if self._closed.is_set():
            raise self._gone()

        try:
            if isinstance(message, str):
                await self._ws.send_text(message)

            elif text:
                await self._ws.send_text(message.decode())

            else:
                await self._ws.send_bytes(message)

        except (WebSocketDisconnect, RuntimeError, OSError) as exc:
            raise self._gone() from exc

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Send the close. Idempotent, because two paths reach it.

        `Subscriber.pump` closes on overflow and `StreamHandle.aclose` closes every
        subscriber; a shutdown that races a drop arrives twice, and Starlette
        raises on the second.
        """
        if self._sent_close:
            return

        self._sent_close = True
        with contextlib.suppress(WebSocketDisconnect, RuntimeError, OSError):
            await self._ws.close(code, reason)

    async def wait_closed(self) -> None:
        await self._closed.wait()

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self._frames()

    async def _frames(self) -> AsyncIterator[str | bytes]:
        while True:
            frame = await self._incoming.get()
            if frame is _EOF:
                return

            yield frame


def _relative(scope: Scope) -> str:
    """The request path relative to wherever this app was mounted.

    **`scope["path"]` is the WHOLE path, not the remainder.** Measured under
    Starlette 1.7 with `Mount("/streams", ...)`: `path` is `/streams/trades`
    and `root_path` is `/streams`. Routing on `path` alone would look up a
    stream called `streams/trades` and refuse every mounted subscribe with a
    4404 — which is what the first run of `tests/test_asgi.py` did.

    ASGI defines the app-relative path as `path` with `root_path` removed, so
    that is what this computes. A path equal to the prefix becomes `/`, which
    is where an unnamed stream is served.
    """
    root = scope.get("root_path", "")
    path = scope["path"]
    if root and path.startswith(root):
        return path[len(root) :] or "/"

    return path
