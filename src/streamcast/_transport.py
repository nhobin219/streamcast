"""The connection surface the stream layer actually uses.

`_stream` and `_subscriber` were written against `websockets`' own
`ServerConnection`, but they never used more than four things from it. Naming
those four is what lets a second transport — `streamcast.asgi`, mounted in
someone else's ASGI app — hand rows to the same `Stream` without `_stream`
learning that more than one kind of connection exists.

**A Protocol rather than a base class**, because neither implementation is
ours to subclass: one is `websockets`', the other wraps Starlette's. Structural
typing is the only kind available here, and it buys the thing that matters —
the type checker, not a test, is what says an adapter is complete.

The surface is deliberately the smallest that compiles. Everything else a
transport offers (`ping`, `request`, addresses, TLS details) is reached by the
code that knows which transport it has: `_server` narrows to `ServerConnection`
for `connection.request`, and `asgi` reads the ASGI scope for the same thing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@runtime_checkable
class Peer(Protocol):
    """One connected client, from the stream layer's point of view.

    `runtime_checkable` so `tests/test_asgi.py` can assert the adapter
    satisfies this without importing a type checker — which catches the case
    where a method is present but spelled wrong, the one failure a Protocol
    cannot catch if nothing ever passes the adapter to an annotated parameter.
    """

    async def send(self, message: str | bytes, /) -> None:
        """One frame. `str` goes out as TEXT, `bytes` as BINARY.

        **The split is load-bearing and not a detail of the encoder.** The
        greeting is a `str` and every data frame is `bytes`, so a transport
        that sent both the same way would change the wire for every existing
        client. `tests/test_asgi.py` asserts the opcodes rather than the
        payloads for exactly this reason.

        Raises `ConnectionClosed` once the peer is gone, because that is the
        signal `Subscriber.pump` unwinds on and `_server` treats as an
        ordinary disconnect rather than a handler failure.
        """
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """End the connection, carrying a refusal if there is one.

        Must be idempotent: `Subscriber.pump` closes on overflow and
        `Stream.aclose` closes every subscriber, and a shutdown that races a
        drop reaches both.
        """
        ...

    async def wait_closed(self) -> None:
        """Resolve once the connection has ended.

        What `Subscriber.run` races the pump against, so that a subscriber on
        a quiet stream is still noticed when it walks away — the pump is
        parked in `queue.get()` and nothing else would ever wake it.
        """
        ...

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        """Frames from the peer, ending when the connection does.

        Only the publish path reads: `serve_publisher` iterates this. A
        subscription is write-only and never touches it.
        """
        ...


__all__ = ["Peer"]
