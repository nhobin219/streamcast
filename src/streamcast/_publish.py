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
row; `send_many` is a group in one transaction. The choice and
its consequences are identical to `Stream.send` versus `Stream.send_many`,
because they ARE those calls — the server makes them on the publisher's
behalf. See `Stream.send` for what a publisher that never yields does to a
stream with no log; a remote publisher can do it too, and nothing here
prevents it any more than the local path does.

**What a lost acknowledgement means.** A row is durable when `send` returns,
the way a local `await send(...)` is. If the connection drops before the reply
arrives, the publisher cannot tell whether the append happened: retrying may
duplicate a row and not retrying may lose one. With `submit`, up to
`max_in_flight` frames can be in that state at once, each possibly a batch. A plain publisher's
delivery is therefore AT LEAST ONCE under retry.

**`publisher=` makes it exactly once**, on a stream served with
`exactly_once=True`. The publisher names one ordered sequence and numbers its
rows 1, 2, 3, … with no holes (`send(row, seq=n)`); the broker keeps the last
seq it appended for that publisher, committed with the rows, and answers a
resent row without appending it again, returning its original offset if known
or None otherwise. The
greeting says where the broker has the publisher (`info.publisher.last_seq`),
so after a reconnect the publisher drops what it holds at or below that and
sends from the next. A seq past the next raises `SequenceGap`. Best effort
across a lost broker buffer: `docs/SPEC.md` §6b.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
from types import TracebackType
from typing import TYPE_CHECKING, Any

from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from streamcast._client import _refusal
from streamcast._codec import from_greeting
from streamcast._dedupe import check_publisher_id
from streamcast._errors import ExactlyOnceUnavailable, ProtocolError
from streamcast._limits import MAX_IN_FLIGHT
from streamcast._protocol import (
    Greeting,
    encode_publish,
    encode_publish_seq,
    parse_greeting,
    parse_publish_reply,
    publish_path,
    stream_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from websockets.asyncio.client import ClientConnection

    # litelink's own row type, spelled here so the annotations read the same
    # as `Stream.send`'s without importing litelink at runtime.
    Row = Mapping[str, object]


class Publication:
    """A live publish connection. Write-only, and offset-aware."""

    __slots__ = (
        "_connection",
        "_info",
        "_outbound",
        "_pending",
        "_publisher",
        "_reader",
        "_stream",
        "_window",
        "_writing",
    )

    def __init__(
        self,
        connection: ClientConnection,
        info: Greeting,
        stream: str,
        max_in_flight: int = MAX_IN_FLIGHT,
        publisher: str | None = None,
    ) -> None:
        if max_in_flight < 1:
            msg = f"max_in_flight={max_in_flight}: at least one send must be allowed"
            raise ValueError(msg)

        self._connection = connection
        # The replies still owed, in the order their frames went out. The
        # server answers in that order, so the next reply is the oldest
        # entry's — no correlation id.
        self._pending: collections.deque[asyncio.Future[list[int | None]]] = (
            collections.deque()
        )
        self._window = asyncio.Semaphore(max_in_flight)
        # Queueing a reply's future and writing its frame are one step under
        # this lock, so two tasks submitting at once cannot cross them.
        self._writing = asyncio.Lock()
        self._reader: asyncio.Task[None] | None = None
        self._info = info
        # Bytes to hex for a `base16` column, before encoding — the server
        # decodes each binary column with its declared encoding, and msgspec
        # would write bytes as base64 regardless. None for a stream with no
        # hex column, which keeps its `send` exactly as it was.
        self._outbound = from_greeting(info.schema).outbound
        self._stream = stream
        self._publisher = publisher

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

    async def send(self, row: Row, *, seq: int | None = None) -> int | None:
        """Publish one row. Returns its offset once it is durable.

        Waits for the acknowledgement, so a loop of `await send(row)` has one
        row in flight at a time: one round trip per row. To keep several in
        flight — and let the server commit them together — use `submit`.

        `None` on a stream with no log, for the same reason `Stream.send`
        returns it: nothing assigned an offset.

        Raises `Rejected` if the server would not take the row — a column the
        schema does not have, a value of the wrong type — with litelink's own
        message naming the column. Nothing was committed, and the connection
        stays open, so a corrected row can be sent next.

        **`seq` is required on an exactly-once publication** (`publisher=`)
        and refused on any other: the row's place in the publisher's sequence.
        A row already in the log returns its original offset, or None where
        the broker no longer remembers it; a seq past the next raises
        `SequenceGap`, with nothing appended.
        """
        [offset] = await (await self.submit_many([row], single=True, seq=seq))
        return offset

    async def submit(
        self, row: Row, *, seq: int | None = None
    ) -> asyncio.Future[int | None]:
        """Publish one row without waiting for it to be durable.

        Returns once the frame is written — waiting first if `max_in_flight`
        sends are already unacknowledged — with a future of the row's offset,
        which resolves once the row is durable, or raises what `send` would.

            futures = [await producer.submit(row) for row in rows]
            offsets = await asyncio.gather(*futures)   # whenever you need them

        **The lever for a single publisher.** A loop of `await send(row)` is
        one round trip per row. A loop of `await submit(row)` keeps up to
        `max_in_flight` frames on the wire, the server commits what arrived
        while a commit was in flight together (`group_commit`), and the
        acknowledgements come back in the order the rows were sent.
        """
        many = await self.submit_many([row], single=True, seq=seq)
        return asyncio.ensure_future(_first(many))

    async def send_many(
        self, rows: Iterable[Row], *, seq: int | None = None
    ) -> list[int | None]:
        """Publish a group in ONE transaction. Returns their offsets.

        One fsync for the group rather than one each, which is a write
        throughput lever beside `submit` — the same one `Stream.send_many`
        is, for the same reason, because it is that call.

        All or nothing: a row the schema refuses rejects the whole group and
        commits none of it. That is what one transaction means, and it is the
        behaviour worth having — a partially committed batch would leave the
        publisher unable to say which rows to send again.

        On an exactly-once publication `seq` is the first row's, and the rest
        take `seq + 1`, … in order.
        """
        rows = list(rows)
        if not rows:
            return []

        return await (await self.submit_many(rows, seq=seq))

    async def submit_many(
        self, rows: Iterable[Row], *, single: bool = False, seq: int | None = None
    ) -> asyncio.Future[list[int | None]]:
        """`send_many` without waiting for it: a future of the group's offsets.

        One transaction, all or nothing, exactly as `send_many`; see `submit`
        for the window and the order. `single` sends one row as a row rather
        than a list of one — the shape `send` uses on the wire.
        """
        if self._publisher is None and seq is not None:
            msg = "seq= is for an exactly-once publication; open it with publisher="
            raise ValueError(msg)

        if self._publisher is not None and (seq is None or seq < 1):
            msg = (
                f"an exactly-once publication needs seq= on every send, from 1; "
                f"got {seq!r}"
            )
            raise ValueError(msg)

        outbound = self._outbound
        batch = [dict(row) if outbound is None else outbound(row) for row in rows]
        payload: dict[str, object] | list[dict[str, object]] = (
            batch[0] if single else batch
        )
        frame = (
            encode_publish(payload) if seq is None else encode_publish_seq(seq, payload)
        )
        await self._window.acquire()
        try:
            done: asyncio.Future[list[int | None]] = (
                asyncio.get_running_loop().create_future()
            )
            async with self._writing:
                self._pending.append(done)
                try:
                    # A TEXT frame, as every frame is: the payload is JSON.
                    await self._connection.send(frame, text=True)
                except ConnectionClosed as closed:
                    self._unowed(done)
                    # The server's reason, where it gave one: a publisher told
                    # it was superseded should not have to read a close code.
                    refusal = _refusal(closed, stream=self._stream, offset=None)
                    if refusal is None:
                        raise

                    raise refusal from closed
                except Exception as exc:
                    self._unowed(done)
                    if self._connection.protocol.state is State.OPEN:
                        raise

                    # Anything else from a connection that is no longer open
                    # is the connection being gone, raised as websockets'
                    # own exception for it: a publisher reconnects on that,
                    # and dies on anything else. websockets' `send_context`
                    # aborts the transport before raising it, and on an
                    # asyncio transport whose `connection_lost` already ran
                    # the abort raises AttributeError instead (#106).
                    closed = self._connection.protocol.close_exc
                    refusal = _refusal(closed, stream=self._stream, offset=None)
                    raise (closed if refusal is None else refusal) from exc
                except BaseException:
                    self._unowed(done)
                    raise

            if self._reader is None:
                self._reader = asyncio.create_task(self._read())

        except BaseException:
            self._window.release()
            raise

        done.add_done_callback(self._settled)
        return done

    def _unowed(self, done: asyncio.Future[list[int | None]]) -> None:
        """`done`'s frame was never written, so no reply is owed for it.

        Removed by identity, not popped: when the connection closes, the
        reader may already have failed and removed every pending future,
        this one included, before the write raises here. Its failure is then
        retrieved: the send raises the connection's end itself, and `done` is
        never handed out, so asyncio would log it as never retrieved.
        """
        with contextlib.suppress(ValueError):
            self._pending.remove(done)

        if done.done() and not done.cancelled():
            done.exception()

    def _settled(self, _done: asyncio.Future[list[int | None]]) -> None:
        """An acknowledgement, or its failure: free the window."""
        self._window.release()

    async def _read(self) -> None:
        """Match each reply to the oldest send still owed one, until the end.

        The only reader of the connection. Replies arrive in the order frames
        went out — the server queues and commits them in that order — so the
        oldest pending future is always the one a reply answers.
        """
        failure: BaseException
        try:
            async for frame in self._connection:
                done = self._pending.popleft()
                try:
                    offsets = parse_publish_reply(frame)
                except Exception as exc:  # noqa: BLE001 — that send's to raise
                    if not done.done():
                        done.set_exception(exc)
                else:
                    if not done.done():
                        done.set_result(offsets)

            # An orderly close ends the iteration rather than raising: the
            # exception it would have raised is the one to hand on.
            failure = self._connection.protocol.close_exc
        except ConnectionClosed as exc:
            failure = _refusal(exc, stream=self._stream, offset=None) or exc

        # Every send still owed a reply learns the connection is gone.
        while self._pending:
            done = self._pending.popleft()
            if not done.done():
                done.set_exception(failure)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """End the connection, once every send in flight is acknowledged.

        Idempotent, and awaits the close handshake. A send still unanswered
        when the connection ends raises from its own future.
        """
        if self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)

        await self._connection.close(code, reason)
        if self._reader is not None:
            with contextlib.suppress(Exception):
                await self._reader


class publish:  # noqa: N801 — a sibling of `connect`, which mirrors `websockets`
    """Open a publish connection. Awaitable, and an async context manager.

        async with streamcast.publish(uri) as producer: ...
        producer = await streamcast.publish(uri)

    The URI is the stream's, exactly as a subscriber's is — `?publish` is
    added here rather than asked of the caller, so one address serves both
    ends and neither has to know the query string.

    Every served stream takes publishers. One that cannot be written — a
    retired stream — refuses with 4410 (`StreamRetired`).

    **`publisher` opens an exactly-once publication**: 1 to 128 bytes of
    UTF-8 naming one ordered sequence, stable across restarts, and distinct
    per parallel producer. Every `send` then carries `seq=`, and
    `info.publisher` says where the broker has this publisher. A stream not
    served with `exactly_once=True` refuses it (`ExactlyOnceUnavailable`,
    `why="stream"`), and so does a server from before the feature
    (`why="broker"`), each before anything is sent: it never falls back to
    at least once by itself. A newer connection with the same id ends this
    one (`Superseded`).

    `max_in_flight` (64) is how many frames `submit` keeps unacknowledged
    at once. A server that owes one connection fewer replies than that
    (`serve(max_in_flight=)`) stops reading it, and the rest wait at the
    socket.

    Every other keyword goes to `websockets.connect` unchanged. `compression`
    defaults to None for the same reason it does in `serve` and `connect`.
    """

    __slots__ = (
        "_kwargs",
        "_max_in_flight",
        "_publication",
        "_publisher",
        "_stream",
        "_uri",
    )

    def __init__(
        self,
        uri: str,
        *,
        publisher: str | None = None,
        max_in_flight: int = MAX_IN_FLIGHT,
        compression: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._uri = uri
        self._publisher = None if publisher is None else check_publisher_id(publisher)
        self._max_in_flight = max_in_flight
        self._stream = stream_name(uri)
        self._kwargs: dict[str, Any] = {"compression": compression, **kwargs}
        self._publication: Publication | None = None

    async def _open(self) -> Publication:
        """Connect, then read the greeting before handing anything back.

        The greeting is awaited here rather than lazily, so entering the block
        MEANS the server accepted this publisher — a refusal surfaces where
        `publish` was called rather than at whatever `send` reached first.
        """
        connection = await _ws_connect(
            publish_path(self._uri, self._publisher), **self._kwargs
        )
        try:
            info = parse_greeting(await connection.recv())

        except ConnectionClosed as closed:
            refusal = _refusal(closed, stream=self._stream, offset=None)
            if refusal is None:
                raise

            if (
                self._publisher is not None
                and isinstance(refusal, ProtocolError)
                and "publisher" in str(refusal)
            ):
                # A server from before exactly-once refuses the parameter it
                # does not know, at the open, with nothing appended.
                raise ExactlyOnceUnavailable(self._stream, "broker") from None

            raise refusal from None

        except BaseException:
            await connection.close()
            raise

        if self._publisher is not None and info.publisher is None:
            # A server that took the parameter and did not answer it is not
            # deduping this publisher; sending would be at least once.
            await connection.close()
            raise ExactlyOnceUnavailable(self._stream, "broker")

        self._publication = Publication(
            connection,
            info,
            self._stream,
            max_in_flight=self._max_in_flight,
            publisher=self._publisher,
        )
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


async def _first(many: asyncio.Future[list[int | None]]) -> int | None:
    [offset] = await many
    return offset


__all__ = ["Publication", "publish"]
