"""`connect` — the subscriber end, and where a close code becomes an exception.

    async with streamcast.connect("ws://localhost:8765/trades", offset=123) as stream:
        async for offset, message in stream:
            ...

**The one deviation from `websockets` is the pair.** Iterating a
`websockets` connection yields a message; iterating this yields
`(offset, row)`, because the offset is the only thing that makes a reconnect a
resume rather than a restart, and a subscriber that has to ask for it
separately will forget to.

`row` is a `dict` over the stream's declared columns — the same row the
publisher sent and the same row the log holds, with `litelink_offset` among
its keys. It is not a blob to parse: the server's table is typed, so the
parsing happened once at the publisher rather than once per subscriber.

**The second deviation is that there is no `send`.** A subscription is
read-only, and rather than carrying a `send` that raises, it does not have
one — the same reason litelink's read handles are not writable handles with
thirteen methods that refuse. Publishing is `Stream.send` in the server's own
process; a remote publisher is an open question, not an omission (`docs/SPEC.md`
§9).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed

from streamcast._cursor import Cursor
from streamcast._errors import (
    Close,
    NotReplayable,
    ProtocolError,
    StreamcastError,
    StreamNotFound,
    TooSlow,
)
from streamcast._protocol import decode, parse_greeting, parse_refusal

if TYPE_CHECKING:
    from os import PathLike
    from types import TracebackType
    from typing import Self

    from websockets.asyncio.client import ClientConnection

    from streamcast._protocol import Greeting

_UNSET: Final = object()
"""Tells `offset=None` apart from "offset not given".

Both are meaningful once `cursor=` exists: not given means "use the file",
and `None` means "ignore the file and take the live stream". A default of
`None` could not express the difference, and the two are one keystroke apart
at the call site.
"""


# The close codes that end a subscription rather than break it. Normal closure
# and going-away both mean the server finished with this connection on
# purpose, which is what `async for` should stop on and not raise for.
_ENDED = frozenset({1000, 1001})


def _refusal(
    exc: ConnectionClosed, *, stream: str, offset: int | None
) -> StreamcastError | None:
    """The exception this close means, or None if it is not a refusal.

    A function rather than a method because a refusal can arrive in place of
    the greeting — before any `Subscription` exists to ask.

    **The close CODE is what this dispatches on; the reason only fills in the
    numbers.** That ordering matters because the reason can be trimmed to fit
    the frame or rewritten by an intermediary, and a refusal that lost its
    detail is still a refusal — reporting it as a clean end would turn a
    rejected subscribe into an empty stream.
    """
    close = exc.rcvd
    if close is None or close.code in _ENDED:
        return None

    _error, fields = parse_refusal(close.reason)
    if close.code == Close.NO_SUCH_STREAM:
        serves = fields.get("serves")
        listed = tuple(str(name) for name in serves) if isinstance(serves, list) else ()

        return StreamNotFound(stream, listed)

    if close.code == Close.NOT_REPLAYABLE:
        why = fields.pop("why", "")

        return NotReplayable(str(why), **fields)

    if close.code == Close.TOO_SLOW:
        backlog = fields.get("backlog")

        return TooSlow(backlog if isinstance(backlog, int) else None, offset=offset)

    if close.code == Close.BAD_REQUEST:
        detail = fields.get("detail", "the server could not parse this subscribe")

        return ProtocolError(str(detail))

    # A code this build does not know, or one `websockets` itself sent. Left
    # to surface as the `ConnectionClosed` it already is, rather than being
    # relabelled as a streamcast refusal it is not.
    return None


def _with_offset(uri: str, offset: int | None) -> str:
    """The URI a subscribe actually opens.

    An offset may be given as the argument or written into the URI — the
    second is what makes `wscat ws://localhost:8765/trades?offset=0` a working
    subscriber, so it cannot be forbidden — but never both. Given both, this
    raises rather than picking, because the two disagreeing is a resume from
    the wrong place and neither value is more likely to be the intended one.
    """
    if offset is None:
        return uri

    split = urlsplit(uri)
    if any(
        key == "offset" for key, _ in parse_qsl(split.query, keep_blank_values=True)
    ):
        msg = (
            f"offset is given twice: as offset={offset} and in the URI "
            f"({split.query!r}). Pass it once."
        )
        raise ValueError(msg)

    query = (
        f"{split.query}&offset={int(offset)}"
        if split.query
        else f"offset={int(offset)}"
    )

    return urlunsplit(split._replace(query=query))


class Subscription:
    """A live subscription. Async-iterable, read-only, and offset-aware."""

    __slots__ = ("_connection", "_cursor", "_info", "_offset", "_stream", "_unsaved")

    def __init__(
        self,
        connection: ClientConnection,
        info: Greeting,
        stream: str,
        cursor: Cursor | None = None,
    ) -> None:
        self._connection = connection
        self._info = info
        self._stream = stream
        self._offset: int | None = None
        self._cursor = cursor
        # The offset received but not yet known to be handled. It becomes the
        # saved value when the caller comes back for another message, which is
        # the only evidence this library has that the last one was finished.
        self._unsaved: int | None = None

    def __repr__(self) -> str:
        return (
            f"<streamcast.Subscription {self._info.stream!r} "
            f"offset={self._offset} durable={self._info.durable}>"
        )

    @property
    def info(self) -> Greeting:
        """What the server said at subscribe: its frontier, the replay range,
        and whether its offsets survive a restart."""
        return self._info

    @property
    def pending(self) -> bool:
        """Whether a received offset is still unaccounted for.

        False once `commit` has been called, which is what keeps the clean
        exit from overwriting an explicit commit.
        """
        return self._unsaved is not None

    @property
    def offset(self) -> int | None:
        """The last offset received, or None before the first message.

        **This is the resume cursor.** Reconnect with `offset=stream.offset + 1`
        and the server replays exactly what was missed:

            offset = None
            while True:
                try:
                    async with streamcast.connect(uri, offset=offset) as stream:
                        async for offset, message in stream:
                            handle(message)

                except (ConnectionClosed, OSError, streamcast.TooSlow):
                    offset = None if offset is None else offset + 1

        The `+ 1` is the whole of it, and it belongs at the call site rather
        than inside a helper, because only the caller knows whether the last
        message was actually *processed* — a subscriber that persists its
        work resumes from what it committed, not from what it received.
        """
        return self._offset

    @property
    def connection(self) -> ClientConnection:
        """The underlying `websockets` connection, for `ping`, addresses, TLS
        details, and anything else this class deliberately does not wrap."""
        return self._connection

    async def recv(self) -> tuple[int | None, dict[str, object]]:
        """The next `(offset, row)`.

        Raises the refusal the server closed with, or `ConnectionClosed` as
        `websockets` raised it when the close carries no refusal.
        """
        # Asking for another message is what says the last one is done. Saved
        # here rather than on delivery, so a consumer that crashes inside its
        # handler re-reads that message instead of skipping it.
        if self._cursor is not None:
            self._cursor.save(self._unsaved)

        try:
            frame = await self._connection.recv()
        except ConnectionClosed as exc:
            refusal = _refusal(exc, stream=self._stream, offset=self._offset)
            if refusal is None:
                raise

            raise refusal from None

        offset, row = decode(frame)
        self._offset = offset
        self._unsaved = offset

        return offset, row

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[int | None, dict[str, object]]:
        """Stops on an ordinary close; raises on anything else.

        Same contract as iterating a `websockets` connection — a normal
        closure ends the loop and an abnormal one raises — with the refusals
        in `_errors` filling in for what a bare code cannot say.
        """
        try:
            return await self.recv()
        except ConnectionClosed as exc:
            if exc.rcvd is not None and exc.rcvd.code in _ENDED:
                raise StopAsyncIteration from None

            raise

    def commit(self, offset: int | None = None) -> None:
        """Save the cursor now, rather than at the next message.

        For a consumer whose work is not idempotent, or that batches: call it
        once the batch is durable, and pass the offset you actually committed
        if it is not simply the last one received. A no-op without `cursor=`.

        **Passing an offset is allowed to move the cursor backwards**, because
        it is you stating what is durable, and correcting an optimistic value
        downward is the point. The automatic saves never do — see `_cursor`.

        A consumer that batches should not rely on the automatic save at all:
        it advances as you read, which for a batch is ahead of what you have
        flushed. Drive `streamcast.Cursor` yourself there.
        """
        if self._cursor is None:
            return

        if offset is None:
            # The automatic value, so the forward-only guard applies.
            self._cursor.save(self._offset, force=True)
        else:
            # The caller's word about what they actually committed, which they
            # are entitled to state lower than what was received.
            self._cursor.save(offset, force=True, rewind=True)

        # Nothing outstanding now, which is what stops the clean-exit commit
        # from overwriting an explicit one. Without this, `commit(8)` followed
        # by leaving the block wrote back the last offset RECEIVED — undoing
        # the caller's statement about what was durable.
        self._unsaved = None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """End the subscription. Idempotent, and awaits the close handshake."""
        await self._connection.close(code, reason)


class connect:  # noqa: N801 — `websockets.connect` is lowercase and this mirrors it
    """Open a subscription. Awaitable, and an async context manager.

        async with streamcast.connect(uri, offset=123) as stream: ...
        stream = await streamcast.connect(uri)

    `offset` is the resume point: absent for live-only, `streamcast.EARLIEST`
    for everything the stream still holds, or an offset to resume from
    inclusive. It is refused rather than ignored when the server cannot serve
    it — see `NotReplayable`, which says which of the five reasons it is.

    **`cursor=path` keeps the resume point on disk**, which is the loop every
    consumer otherwise writes by hand. The file holds the last offset finished
    with; the subscription resumes one above it and saves as it goes.

        async with streamcast.connect(uri, cursor=".trades.offset") as sub:
            async for offset, msg in sub:
                handle(msg)

    That is the whole recovery story — stop the consumer, start it again, and
    it picks up where it stopped. `offset` still wins if given, including
    `offset=None` for "ignore the file, take the live stream", which is why
    its default is a sentinel rather than None.

    **The cursor lags on purpose.** It advances when you ask for the NEXT
    message, is throttled to once a second, and is not saved at all if the
    block exits with an exception — so a crash re-delivers rather than skips.
    `Subscription.commit()` forces it for a consumer that batches or whose
    work is not idempotent. See `_cursor`.

    Every other keyword goes to `websockets.connect` unchanged. `compression`
    defaults to None for the same reason it does in `serve`.
    """

    __slots__ = ("_connect", "_cursor", "_stream", "_subscription")

    def __init__(
        self,
        uri: str,
        *,
        offset: int | None | object = _UNSET,
        cursor: str | PathLike[str] | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._stream = urlsplit(uri).path.lstrip("/")
        self._cursor = Cursor(cursor) if cursor is not None else None

        resolved: int | None
        # ALWAYS read, even when an explicit offset makes the value unused:
        # `load` is what seeds the cursor's high-water mark, and without it
        # the forward-only guard is inert. A one-off `offset=1` beside a
        # production cursor then wrote 1, 2, 3 over a file that said 400.
        saved = self._cursor.load() if self._cursor is not None else None

        if offset is _UNSET:
            # Not given: the file decides. `+ 1` because the file holds the
            # last offset FINISHED with, and a resume asks for the next one.
            resolved = None if saved is None else saved + 1
        elif offset is None or isinstance(offset, int):
            # Given: it wins, including when it is None. Overriding a stale
            # file has to be expressible, and so does "ignore the file, take
            # the live stream" — which is why `None` is not the default.
            resolved = offset
        else:
            msg = f"offset is an int, None, or omitted — not {type(offset).__name__}"
            raise TypeError(msg)

        self._connect = _ws_connect(
            _with_offset(uri, resolved), compression=compression, **kwargs
        )
        self._subscription: Subscription | None = None

    async def _open(self) -> Subscription:
        """Connect, then read the greeting before handing anything back.

        The greeting is awaited here rather than lazily on the first message,
        so that entering the block MEANS the server accepted this subscribe.
        The alternative surfaces a refused offset as a failure of whatever
        `recv` the application happened to reach first, which on a stream that
        is quiet out of hours is minutes later and somewhere else.
        """
        connection = await self._connect
        try:
            frame = await connection.recv()
            info = parse_greeting(frame)
        except ConnectionClosed as exc:
            refusal = _refusal(exc, stream=self._stream, offset=None)
            if refusal is None:
                raise

            raise refusal from None
        except BaseException:
            # A greeting that will not parse leaves an open connection nobody
            # holds — `__aexit__` never runs, because `__aenter__` did not
            # return. Closing here is what keeps a server-side handler from
            # surviving every malformed handshake until its keepalive fires.
            await connection.close()
            raise

        self._subscription = Subscription(connection, info, self._stream, self._cursor)

        return self._subscription

    def __await__(self):  # noqa: ANN204 — an awaitable's own protocol
        return self._open().__await__()

    async def __aenter__(self) -> Subscription:
        return await self._open()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._subscription is None:
            return

        # Only on a clean exit, and only if something is outstanding. Leaving
        # the block because a handler raised means the last message was NOT
        # finished with, and saving it there would skip it on the next run —
        # the one thing a cursor must never do. It is re-delivered instead.
        if exc_type is None and self._subscription.pending:
            self._subscription.commit()

        await self._subscription.close()


__all__ = ["Subscription", "connect"]
