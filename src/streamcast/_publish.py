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
behalf. See `Stream.send` for what a publisher that never yields does to a
stream with no log; a remote publisher can do it too, and nothing here
prevents it any more than the local path does.

**What a lost acknowledgement means.** A row is durable when `send` returns,
the way a local `await send(...)` is. If the connection drops before the reply
arrives, the publisher cannot tell whether the append happened: retrying may
duplicate a row and not retrying may lose one. With `submit`, up to
`max_in_flight` rows can be in that state at once. Delivery here is therefore
AT LEAST ONCE under retry, and the library does not resolve it because it
cannot: the ambiguity is in the publisher's knowledge, not in the log.

Resolving it costs two columns and a replay. Carry a publisher key and a
per-publisher sequence, and on reconnect subscribe from the offset `send` last
returned — the offset bounds the window, the key picks your rows out of it,
and `info.replay` says how many to read. `docs/SPEC.md` §6b has it, including
the two ways the loop goes wrong silently.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
from types import TracebackType
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from litelink import S3Options
from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed

from streamcast._client import _refusal
from streamcast._codec import from_greeting
from streamcast._cursor import Cursor
from streamcast._limits import MAX_IN_FLIGHT
from streamcast._protocol import (
    Greeting,
    encode_publish,
    parse_greeting,
    parse_publish_reply,
    publish_path,
)
from streamcast._remote import UPLOAD_EVERY, RemoteCursor

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from os import PathLike

    from websockets.asyncio.client import ClientConnection

    # litelink's own row type, spelled here so the annotations read the same
    # as `Stream.send`'s without importing litelink at runtime.
    Row = Mapping[str, object]


class Publication:
    """A live publish connection. Write-only, and offset-aware."""

    __slots__ = (
        "_acked",
        "_connection",
        "_cursor",
        "_info",
        "_outbound",
        "_pending",
        "_reader",
        "_resumed",
        "_stream",
        "_window",
        "_writing",
    )

    def __init__(
        self,
        connection: ClientConnection,
        info: Greeting,
        stream: str,
        cursor: Cursor | None = None,
        resumed: int | None = None,
        max_in_flight: int = MAX_IN_FLIGHT,
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
        self._cursor = cursor
        self._resumed = resumed
        self._acked: int | None = resumed

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

    @property
    def resumed_from(self) -> int | None:
        """The offset this publisher was last acknowledged for, or None.

        What `cursor=` loaded, and the starting point for a recovery replay —
        subscribe from HERE, inclusive, and the first row is this publisher's
        own last acknowledged one, so the sequence it carried comes back out
        of the log. `docs/SPEC.md` §6b.

        **Nothing resumes automatically, and that is the asymmetry with a
        consumer.** A consumer cursor is enough to resume by itself: the
        server replays from it. A producer cursor says where this publisher
        got to, not what it should send next — that is its own outbox, or a
        position in whatever it reads from, and the library cannot know
        either. So this is reported and acted on by the caller.
        """
        return self._resumed

    async def send(self, row: Row) -> int | None:
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
        """
        [offset] = await (await self.submit_many([row], single=True))
        return offset

    async def submit(self, row: Row) -> asyncio.Future[int | None]:
        """Publish one row without waiting for it to be durable.

        Returns once the frame is written — waiting first if `max_in_flight`
        sends are already unacknowledged — with a future of the row's offset,
        which resolves once the row is durable, or raises what `send` would.

            futures = [await producer.submit(row) for row in rows]
            offsets = await asyncio.gather(*futures)   # whenever you need them

        **The lever for a single publisher.** A loop of `await send(row)` is
        one round trip per row. A loop of `await submit(row)` keeps up to
        `max_in_flight` rows on the wire, the server commits what arrived
        while a commit was in flight together (`group_commit`), and the
        acknowledgements come back in the order the rows were sent.
        """
        many = await self.submit_many([row], single=True)
        return asyncio.ensure_future(_first(many))

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
        rows = list(rows)
        if not rows:
            return []

        return await (await self.submit_many(rows))

    async def submit_many(
        self, rows: Iterable[Row], *, single: bool = False
    ) -> asyncio.Future[list[int | None]]:
        """`send_many` without waiting for it: a future of the group's offsets.

        One transaction, all or nothing, exactly as `send_many`; see `submit`
        for the window and the order. `single` sends one row as a row rather
        than a list of one — the shape `send` uses on the wire.
        """
        outbound = self._outbound
        batch = [dict(row) if outbound is None else outbound(row) for row in rows]
        payload: dict[str, object] | list[dict[str, object]] = (
            batch[0] if single else batch
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
                    await self._connection.send(encode_publish(payload), text=True)
                except BaseException:
                    self._pending.pop()  # never written: no reply is owed
                    raise

            if self._reader is None:
                self._reader = asyncio.create_task(self._read())

        except BaseException:
            self._window.release()
            raise

        done.add_done_callback(self._settled)
        return done

    def _settled(self, done: asyncio.Future[list[int | None]]) -> None:
        """An acknowledgement, or its failure: free the window, save the cursor."""
        self._window.release()
        if done.cancelled() or done.exception() is not None:
            return

        offsets = done.result()
        if self._cursor is not None:
            # **After the acknowledgement, never before**, which is the same
            # rule a consumer cursor follows for the mirrored reason. A
            # producer cursor that LAGS makes the recovery window larger —
            # more to replay, still correct. One that LEADS names a row that
            # may never have landed, so the window misses it and the
            # publisher resends what it already wrote. `Cursor.save` is
            # forward-only and throttled, so a burst of sends costs one
            # write a second rather than one each.
            highest = [offset for offset in offsets if offset is not None]
            if highest and (self._acked is None or max(highest) > self._acked):
                self._acked = max(highest)
                self._cursor.save(self._acked)

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

    def commit(self, offset: int | None = None) -> None:
        """Save the cursor now, rather than waiting for the throttle.

        The automatic save is throttled to once a second, because an
        `os.replace` per row on a fast publisher is three syscalls to record
        something allowed to be stale. That staleness is safe — a lagging
        producer cursor only widens the recovery replay — but a publisher
        that is about to exit, or that has just trimmed its outbox, can say
        so rather than wait.

        `offset` states what YOU consider settled, and is allowed to move the
        cursor backwards for the same reason `Subscription.commit` is: a
        publisher correcting an optimistic value downward is the point, and
        the automatic saves never do it. A no-op without `cursor=`.
        """
        if self._cursor is None:
            return

        if offset is None:
            if self._acked is None:
                return

            self._cursor.save(self._acked, force=True)
            return

        self._acked = offset
        self._cursor.save(offset, force=True, rewind=True)

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

    **The server must allow it**: `serve(..., publish=True)`. A server that
    silently became writable on an upgrade would be a security change nobody
    asked for, so the default refuses with `publish_disabled` and says which
    setting to change.

    Every other keyword goes to `websockets.connect` unchanged. `compression`
    defaults to None for the same reason it does in `serve` and `connect`.
    """

    __slots__ = (
        "_cursor",
        "_kwargs",
        "_max_in_flight",
        "_publication",
        "_remote",
        "_stream",
        "_uri",
    )

    def __init__(
        self,
        uri: str,
        *,
        cursor: str | PathLike[str] | None = None,
        cursor_uri: str | None = None,
        s3_options: S3Options | None = None,
        upload_every: float = UPLOAD_EVERY,
        max_in_flight: int = MAX_IN_FLIGHT,
        compression: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._uri = uri
        self._max_in_flight = max_in_flight
        self._stream = urlsplit(uri).path.lstrip("/")
        self._kwargs: dict[str, Any] = {"compression": compression, **kwargs}
        self._publication: Publication | None = None
        self._cursor = Cursor(cursor) if cursor is not None else None

        self._remote: RemoteCursor | None = None
        if cursor_uri is not None:
            if self._cursor is None:
                msg = "cursor_uri needs a cursor= to ship; it is a copy of one"
                raise ValueError(msg)

            self._remote = RemoteCursor(
                self._cursor,
                cursor_uri,
                s3_options=s3_options,
                upload_every=upload_every,
            )

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

        # **Local first, remote only when there is no local**, the same order
        # a consumer resolves them in and for the same reason: the remote
        # copy lags by up to `upload_every`, so it should decide only when
        # there is nothing better — which is the box-is-gone case.
        resumed = self._cursor.load() if self._cursor is not None else None
        if resumed is None and self._remote is not None and self._cursor is not None:
            resumed = self._remote.load()
            if resumed is not None:
                self._cursor.save(resumed, force=True, rewind=True)

        self._publication = Publication(
            connection,
            info,
            self._stream,
            self._cursor,
            resumed,
            max_in_flight=self._max_in_flight,
        )
        if self._remote is not None:
            self._remote.start()

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
        if self._publication is not None and exc_type is None:
            # A clean exit settles what was acknowledged, so a publisher that
            # finishes its work does not leave the last second of it to be
            # replayed on the next start. An exception leaves it alone: the
            # throttled value is the safe one, because a wider replay window
            # costs a scan and a narrower one costs a duplicate.
            self._publication.commit()

        if self._remote is not None:
            # Before the socket closes, so the final upload carries whatever
            # the last acknowledgement saved.
            self._remote.stop()

        if self._publication is not None:
            await self._publication.close()


async def _first(many: asyncio.Future[list[int | None]]) -> int | None:
    [offset] = await many
    return offset


__all__ = ["Publication", "publish"]
