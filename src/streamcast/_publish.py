"""`publish` — the producer end, for a publisher that is not the server.

    async with streamcast.publish("ws://localhost:8765/trades") as producer:
        await producer.send({"event_ts": 1790038800123456, "price": 85565.0})

**A sibling of `Subscription`, not a method on it.** A connection is one or
the other: a subscriber has no `send` and a publisher has no `recv`, and
neither carries a method that raises. That is the same argument `_client`
makes for a read-only subscription, applied the other way round.

**The server stays the only writer**, which is the whole reason this exists
rather than opening the log from another box. litelink allows one writer per
log, refuses neither a second nor detects one, and has no lease that spans
machines — so two `WriteHandle`s on one log is a corruption path with no
guard (`docs/SPEC.md` §8b). Handing rows to the process that already owns the
handle resolves the concurrency where it can actually be resolved. Any number
of publishers, one writer.

**Granularity is the publisher's, exactly as it is locally.** `send` is one
row and one fsync; `send_many` is a group in one transaction. The choice and
its consequences are identical to `Stream.send` versus `Stream.send_many`,
because they ARE those calls — the server makes them on the publisher's
behalf. See `Stream.send` for what a publisher that never yields does to the
subscribers sharing that loop; a remote publisher can do it too, and nothing
here prevents it any more than the local path does.

**What a lost acknowledgement means.** A row is durable when `send` returns,
the way a local `await send(...)` is. If the connection drops before the reply
arrives, the publisher cannot tell whether the append happened: retrying may
duplicate a row and not retrying may lose one. Delivery here is therefore
AT LEAST ONCE under retry, and the library does not resolve it — a publisher
that cannot tolerate a duplicate carries its own key in the row and
deduplicates downstream, which is the only place the ambiguity is decidable.
`docs/SPEC.md` §6b has the reasoning, and the publisher-key pattern that
makes recovery a query rather than a guess.
"""

from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed

from streamcast._client import _refusal
from streamcast._protocol import (
    Greeting,
    encode_publish,
    parse_greeting,
    parse_publish_reply,
    publish_path,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from websockets.asyncio.client import ClientConnection

    # litelink's own row type, spelled here so the annotations read the same
    # as `Stream.send`'s without importing litelink at runtime.
    Row = Mapping[str, object]


class Publication:
    """A live publish connection. Write-only, and offset-aware."""

    __slots__ = ("_connection", "_info", "_stream")

    def __init__(
        self, connection: ClientConnection, info: Greeting, stream: str
    ) -> None:
        self._connection = connection
        self._info = info
        self._stream = stream

    def __repr__(self) -> str:
        return f"<streamcast.Publication {self._stream!r} durable={self._info.durable}>"

    @property
    def info(self) -> Greeting:
        """What the server said at connect: its frontier, the stream's schema,
        whether its offsets survive a restart, and where its log is.

        The same greeting a subscriber gets, and the schema is the useful part
        here: a publisher can check the shape it is about to send against the
        shape the server will accept, before sending anything.
        """
        return self._info

    @property
    def connection(self) -> ClientConnection:
        """The underlying `websockets` connection, for `ping`, addresses, TLS
        details, and anything else this class deliberately does not wrap."""
        return self._connection

    async def send(self, row: Row) -> int | None:
        """Publish one row. Returns its offset once it is durable.

        `None` on a stream with no log, for the same reason `Stream.send`
        returns it: nothing assigned an offset.

        Raises `Rejected` if the server would not take the row — a column the
        schema does not have, a value of the wrong type — with litelink's own
        message naming the column. Nothing was committed, and the connection
        stays open, so a corrected row can be sent next.
        """
        offsets = await self._round_trip(dict(row))

        return offsets[0] if offsets else None

    async def send_many(self, rows: Iterable[Row]) -> list[int | None]:
        """Publish a group in ONE transaction. Returns their offsets.

        One fsync for the group rather than one each, which is the write
        throughput lever — the same one `Stream.send_many` is, for the same
        reason, because it is that call.

        All or nothing: a row the schema refuses rejects the whole group and
        commits none of it. That is what one transaction means, and it is the
        behaviour worth having — a partially committed batch would leave the
        publisher unable to say which rows to send again.
        """
        batch = [dict(row) for row in rows]
        if not batch:
            return []

        return await self._round_trip(batch)

    async def _round_trip(
        self, payload: dict[str, object] | list[dict[str, object]]
    ) -> list[int | None]:
        """One publish, one reply.

        Strictly serial per connection, which is what makes the reply
        unambiguous without a correlation id: one frame is outstanding at a
        time, so the next reply is this one's. A publisher that wants more in
        flight opens another connection — and gets another position in the
        offset order, which is the honest representation of what it asked for.
        """
        try:
            await self._connection.send(encode_publish(payload))

            return parse_publish_reply(await self._connection.recv())

        except ConnectionClosed as exc:
            refusal = _refusal(exc, stream=self._stream, offset=None)
            if refusal is None:
                raise

            raise refusal from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """End the connection. Idempotent, and awaits the close handshake."""
        await self._connection.close(code, reason)


class publish:  # noqa: N801 — a sibling of `connect`, which mirrors `websockets`
    """Open a publish connection. Awaitable, and an async context manager.

        async with streamcast.publish(uri) as producer: ...
        producer = await streamcast.publish(uri)

    The URI is the stream's, exactly as a subscriber's is — `?publish` is
    added here rather than asked of the caller, so one address serves both
    ends and neither has to know the query string.

    **The server must allow it**: `serve(..., publish=True)`. A server that
    silently became writable on an upgrade would be a security change nobody
    asked for, so the default refuses with `publish_disabled` and says which
    setting to change.

    Every other keyword goes to `websockets.connect` unchanged. `compression`
    defaults to None for the same reason it does in `serve` and `connect`.
    """

    __slots__ = ("_kwargs", "_publication", "_stream", "_uri")

    def __init__(
        self,
        uri: str,
        *,
        compression: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._uri = uri
        self._stream = urlsplit(uri).path.lstrip("/")
        self._kwargs: dict[str, Any] = {"compression": compression, **kwargs}
        self._publication: Publication | None = None

    async def _open(self) -> Publication:
        """Connect, then read the greeting before handing anything back.

        The greeting is awaited here rather than lazily, so entering the block
        MEANS the server accepted this publisher — a refusal surfaces where
        `publish` was called rather than at whatever `send` reached first.
        """
        connection = await _ws_connect(publish_path(self._uri), **self._kwargs)
        try:
            info = parse_greeting(await connection.recv())

        except ConnectionClosed as closed:
            refusal = _refusal(closed, stream=self._stream, offset=None)
            if refusal is None:
                raise

            raise refusal from None

        except BaseException:
            await connection.close()
            raise

        self._publication = Publication(connection, info, self._stream)

        return self._publication

    def __await__(self):  # noqa: ANN204 — an awaitable's own protocol
        return self._open().__await__()

    async def __aenter__(self) -> Publication:
        return await self._open()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._publication is not None:
            await self._publication.close()


__all__ = ["Publication", "publish"]
