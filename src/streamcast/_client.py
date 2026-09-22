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
publisher sent and the same row the log holds, and **nothing else**: the
offset is the other half of the pair, not a key in it, so the row can be
logged, forwarded or appended to another stream whole. It is not a blob to
parse either: the server's table is typed, so the parsing happened once at the
publisher rather than once per subscriber.

**Resuming is three keywords, because otherwise every consumer writes the same
loop.** `cursor=` keeps the last handled offset on disk and resumes one above
it; `cursor_uri=` ships that integer to object storage, so a consumer whose
machine is gone can resume on another; `catch_up=True` reads the gap from the
log's archive when the consumer has fallen past what the server will replay,
then picks the socket up where the archive ended. `_cursor`, `_remote` and
`_catchup` are where each is argued.

**The second deviation is that there is no `send`.** A subscription is
read-only, and rather than carrying a `send` that raises, it does not have
one — the same reason litelink's read handles are not writable handles with
thirteen methods that refuse. Publishing is `Stream.send` in the server's own
process; a remote publisher is an open question, not an omission (`docs/SPEC.md`
§9).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from litelink import S3Options
from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed

from streamcast._catchup import (
    CATCH_UP_RETRIES,
    RECOVERABLE,
    Catcher,
    from_refusal,
    nowhere_to_read,
)
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
from streamcast._remote import UPLOAD_EVERY, RemoteCursor

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

    __slots__ = (
        "_catcher",
        "_connection",
        "_cursor",
        "_info",
        "_offset",
        "_prelude",
        "_stream",
        "_unsaved",
    )

    def __init__(
        self,
        connection: ClientConnection | None,
        info: Greeting,
        stream: str,
        cursor: Cursor | None = None,
        catcher: Catcher | None = None,
    ) -> None:
        self._connection = connection
        self._info = info
        self._stream = stream
        self._offset: int | None = None
        self._cursor = cursor
        # Rows from the archive, drained BEFORE anything is connected — see
        # `_catchup`, where holding a socket through a long catch-up is what
        # gets the subscriber dropped for falling behind. `_catcher` opens the
        # connection itself once the gap is closed.
        self._catcher = catcher
        self._prelude = None if catcher is None else catcher.stream()
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

    def _live(self) -> ClientConnection:
        """The socket, once there is one.

        None only while a catch-up is draining the archive — `Catcher` opens
        it when the gap closes, and `recv` swaps it in before reaching here.
        """
        if self._connection is None:  # pragma: no cover — recv swaps it in
            msg = "still reading the archive; there is no connection yet"
            raise RuntimeError(msg)

        return self._connection

    @property
    def connection(self) -> ClientConnection:
        """The underlying `websockets` connection, for `ping`, addresses, TLS
        details, and anything else this class deliberately does not wrap."""
        return self._live()

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

        if self._prelude is not None:
            caught = await anext(self._prelude, None)
            if caught is not None:
                offset, row = caught
                self._offset = offset
                self._unsaved = offset

                return offset, row

            # Exhausted, which means `Catcher.stream` returned — and it does
            # not return until it has a live connection. Everything below the
            # socket came from object storage; everything from here comes from
            # the server, starting where the archive stopped.
            self._prelude = None
            catcher, self._catcher = self._catcher, None
            if catcher is not None and catcher.connection is not None:
                self._connection = catcher.connection
                self._info = catcher.info

        try:
            frame = await self._live().recv()
        except ConnectionClosed as exc:
            refusal = _refusal(exc, stream=self._stream, offset=self._offset)
            if refusal is None:
                raise

            raise refusal from None

        offset, row = decode(frame)
        # **This is not a guard against the network.** TCP delivers a byte
        # stream in order, so frames on ONE connection cannot overtake each
        # other, and this comparison will never fire because of reordering
        # between two hosts. An earlier version of this comment led with that
        # hazard, which oversold a check that cannot see it.
        #
        # What it actually guards is the two places the offsets come from
        # somewhere TCP says nothing about:
        #
        # * **The catch-up join.** `self._offset` above is set by rows read
        #   out of OBJECT STORAGE, and the first frame off the socket is
        #   compared against it. Those are two different sources spliced into
        #   one stream, and the splice is computed by `Catcher.start` and the
        #   server's replay window agreeing about an inclusive/exclusive
        #   boundary. This is the line where an off-by-one there stops being
        #   silent. It is the only case reachable today.
        # * **Our own replay/live partition (I2).** The server writes the
        #   replay and then the live queue onto one connection; TCP preserves
        #   the order they were WRITTEN, not whether `_attach` picked the
        #   frontier correctly. An overlap would repeat an offset, and a
        #   repeat is what this sees.
        #
        # One integer compare per message, and what it buys is that both of
        # those fail loudly. Silent out-of-order processing means, with a
        # cursor, silently skipping data — the one thing a resume must never
        # do — so the cost is worth paying even though the hazard the check
        # is usually assumed to cover cannot happen.
        #
        # It does NOT span a reconnect: a new `Subscription` starts with
        # `_offset = None`, so nothing is compared across the gap. The log is
        # what makes that safe, not this.
        #
        # `<=` rather than `!= previous + 1`: litelink's offset space has
        # legitimate GAPS (a `restore` fences 2**20 of them), so a jump
        # forward is ordinary and only a step backwards is wrong.
        if offset is not None and self._offset is not None and offset <= self._offset:
            msg = (
                f"offsets must increase within a subscription; received "
                f"{offset} after {self._offset}"
            )
            raise ProtocolError(msg)

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
        prelude, self._prelude = self._prelude, None
        catcher, self._catcher = self._catcher, None
        if prelude is not None:
            # A consumer that walks away mid-catch-up leaves this generator
            # suspended inside `Catcher.stream`, holding a DuckDB connection
            # and the snapshot's scratch DIRECTORY — so abandoning it leaks
            # disk as well as memory. `aclose` runs the `finally` that
            # releases both. The server has the same guard for the same
            # reason; this one was missing until it was looked for.
            await prelude.aclose()

        if catcher is not None:
            # `aclose` above is not enough on its own. A subscription closed
            # before its first `recv` never STARTED that generator, so its
            # `finally` never ran — and `prepare` has an open reader waiting
            # for a round that will not happen.
            await catcher.close()

        if self._connection is not None:
            await _close(self._connection, code, reason)


async def _close(connection: ClientConnection, code: int, reason: str) -> None:
    """Close, draining what is still in flight so the handshake can finish.

    **Measured: 10.01s without the drain, 0.00s with it.** `websockets`
    applies flow control at `max_queue` (16 by default): once that many
    unread messages are buffered, the client STOPS READING THE SOCKET. A
    consumer that closes mid-replay has far more than 16 frames in flight, so
    its reader is already paused — and the server's Close echo, which is just
    another frame on that socket, is never consumed. `close()` then waits out
    `close_timeout` (10s) and gives up.

    That is the ordinary case, not an exotic one: a consumer killed while
    catching up, a `break` out of the loop, or any `async with` exited early.
    Every one of them paid ten seconds, and the shutdown looked like a hang.

    So a task reads and discards until the socket is done, which un-pauses
    the reader and lets the echo through. Discarding is correct rather than
    merely expedient — the caller has said it wants no more messages, and the
    cursor was committed before this ran. Nothing here touches `_offset`, so
    a drained frame can never be mistaken for one the consumer handled.
    """
    drain = asyncio.create_task(_discard(connection))
    try:
        await connection.close(code, reason)

    finally:
        drain.cancel()
        # Awaited, or a cancelled task that had already failed logs
        # "Task exception was never retrieved" on the next collection.
        await asyncio.gather(drain, return_exceptions=True)


async def _discard(connection: ClientConnection) -> None:
    """Read and throw away until the connection ends."""
    try:
        while True:
            await connection.recv()

    except (ConnectionClosed, RuntimeError):
        # RuntimeError covers `websockets` refusing a read on a connection
        # that is already finished, which is a race this does not need to win.
        return


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

    **`cursor_uri` ships that cursor to object storage**, so a consumer can
    resume on a different box after losing this one:

        streamcast.connect(uri, cursor=".trades.offset",
                           cursor_uri="s3://streamcast/consumer1/")

    A daemon thread uploads it every `upload_every` seconds; credentials
    resolve from the environment and `s3=S3Options(...)` overrides them, the
    same way litelink does it. On connect the LOCAL cursor wins and the remote
    is read only when there is no local one — the disaster-recovery case, and
    the only one where a copy that lags should decide. See `_remote` for what
    this deliberately does not solve.

    **`catch_up=True` handles having been down too long.** A consumer that
    falls past the server's `max_replay` is refused — the rows are in the
    log's archive, not gone — and this reads the gap from there, then picks
    the socket up where the archive ended:

        streamcast.connect(uri, cursor=".trades.offset", catch_up=True)

    Nothing is connected while the archive is read, because a subscriber that
    holds a socket through a long catch-up is dropped for falling behind. It
    loops, bounded by `catch_up_retries` (3), for a server that moves on while
    the gap is being read. `archive=` overrides where to read — otherwise it
    comes from the refusal, or from the greeting — and `s3=` the credentials.
    An archive that cannot be read raises `CatchUpUnavailable` HERE, at
    `connect`, with a message naming what was tried and what to change. See
    `_catchup`.

    Every other keyword goes to `websockets.connect` unchanged. `compression`
    defaults to None for the same reason it does in `serve`.
    """

    __slots__ = (
        "_archive",
        "_catch_up",
        "_catch_up_retries",
        "_cursor",
        "_kwargs",
        "_offset",
        "_remote",
        "_resolved",
        "_s3",
        "_stream",
        "_subscription",
        "_uri",
    )

    def __init__(
        self,
        uri: str,
        *,
        offset: int | None | object = _UNSET,
        cursor: str | PathLike[str] | None = None,
        cursor_uri: str | None = None,
        s3: S3Options | None = None,
        upload_every: float = UPLOAD_EVERY,
        catch_up: bool = False,
        catch_up_retries: int = CATCH_UP_RETRIES,
        archive: str | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._uri = uri
        # `Any`, so the checker resolves `**self._kwargs` against
        # `websockets.connect`'s very precise signature rather than against a
        # value type inferred from `compression`.
        self._kwargs: dict[str, Any] = {"compression": compression, **kwargs}
        self._catch_up = catch_up
        self._catch_up_retries = catch_up_retries
        self._archive = archive
        self._s3 = s3
        self._stream = urlsplit(uri).path.lstrip("/")
        self._cursor = Cursor(cursor) if cursor is not None else None

        self._remote: RemoteCursor | None = None
        if cursor_uri is not None:
            if self._cursor is None:
                msg = "cursor_uri needs a cursor= to ship; it is a copy of one"
                raise ValueError(msg)

            self._remote = RemoteCursor(
                self._cursor, cursor_uri, s3=s3, upload_every=upload_every
            )

        if not (offset is _UNSET or offset is None or isinstance(offset, int)):
            msg = f"offset is an int, None, or omitted — not {type(offset).__name__}"
            raise TypeError(msg)

        if isinstance(offset, int):
            # Called for its validation — it raises when an offset is given
            # BOTH as an argument and in the URI — and discarded, because
            # `_handshake` builds each connection as it needs one. Only the
            # explicit case is checkable without reading the cursor; a
            # file-resolved offset that collides with the URI is caught by
            # the same function in `_handshake`.
            _with_offset(uri, offset)

        self._offset = offset
        self._resolved: int | None = None
        self._subscription: Subscription | None = None

    async def _handshake(self, offset: int | None) -> tuple[ClientConnection, Greeting]:
        """One connection, opened and greeted. Raises the refusal it was given."""
        connection = await _ws_connect(_with_offset(self._uri, offset), **self._kwargs)
        try:
            return connection, parse_greeting(await connection.recv())

        except ConnectionClosed as closed:
            refusal = _refusal(closed, stream=self._stream, offset=None)
            if refusal is None:
                raise

            raise refusal from None

        except BaseException:
            await connection.close()
            raise

    async def _recover(self, refused: NotReplayable) -> Subscription:
        """Hand back a subscription that reads the gap before it connects.

        **Nothing is connected while the archive is read.** Holding a socket
        through a catch-up that streams millions of rows makes the server
        queue for a subscriber that is not reading, and `max_backlog` drops it
        — a recovery that guaranteed its own failure. `Catcher` opens the
        connection itself, after the gap is closed and at the offset the
        archive actually reached, looping if the server has moved on since.
        """
        name = self._stream
        # One throwaway live connection, always. Something has to describe
        # the stream until the real connection exists — `durable`, `schema`
        # and `log` are properties of the STREAM rather than of a connection
        # — and the greeting is the only place the log's NAME is published.
        # `end_offset` and `replay` are replaced when the socket opens.
        connection, probe = await self._handshake(None)
        await connection.close()

        # **The LOG's name, which is not always the stream's.** A stream
        # serves at its own name and its log has its own; `Stream.new` feeds
        # one through and `Stream(log=handle)` does not. Asking the archive
        # for a table named after the stream found nothing and reported it as
        # a credentials failure — the server knows the answer, so it says it.
        log_name = name if probe.log is None else probe.log.name

        where = from_refusal(refused, self._archive)
        if where is None:
            # The refusal did not carry it — a bucket URI and the numbers that
            # diagnose the refusal do not both fit in 123 bytes, and the
            # numbers are ordered first. The greeting has no such limit.
            where = None if probe.log is None else probe.log.archive
            if where is None:
                raise nowhere_to_read(name)

        start = self._resolved if isinstance(self._resolved, int) else 1
        catcher = Catcher(
            where, log_name, self._s3, start, self._catch_up_retries, self._handshake
        )
        # BEFORE handing anything back, so an unreadable archive raises here
        # rather than from whatever line first calls `recv`. Entering the
        # block has to keep meaning that the subscription works.
        await catcher.prepare()

        self._subscription = Subscription(None, probe, name, self._cursor, catcher)

        return self._subscription

    def _resolve(self) -> int | None:
        """The offset to subscribe at, reading the cursor if there is one.

        **Called from `_open`, not from `__init__`.** It reads a file and can
        reach S3 — a remote cursor is fetched when there is no local one —
        and neither belongs in a constructor: `streamcast.connect(...)` that
        nobody has awaited yet would do network I/O, on the event loop,
        before the object it returns is used for anything. litelink's rule
        (`__init__` takes built collaborators and does no I/O) is the same
        rule, and this is where it was being broken.

        `Cursor` and `RemoteCursor` are still BUILT in `__init__`, because
        building them is pure: one holds a path, the other validates a URI.
        """
        # ALWAYS read, even when an explicit offset makes the value unused:
        # `load` is what seeds the cursor's high-water mark, and without it
        # the forward-only guard is inert. A one-off `offset=1` beside a
        # production cursor then wrote 1, 2, 3 over a file that said 400.
        saved = self._cursor.load() if self._cursor is not None else None

        if saved is None and self._remote is not None and self._cursor is not None:
            # **Local first, remote only when there is no local.** This is
            # disaster recovery: the box is gone, so there is no local file.
            # Preferring the remote when a local one exists would mean a
            # consumer's own position losing to a copy that lags it by up to
            # `upload_every` — or to another box's, which is a configuration
            # this deliberately does not support.
            saved = self._remote.load()
            if saved is not None:
                # Written down locally too, so a second restart on this box
                # does not need the bucket at all.
                self._cursor.save(saved, force=True, rewind=True)

        if self._offset is _UNSET:
            # Not given: the file decides. `+ 1` because the file holds the
            # last offset FINISHED with, and a resume asks for the next one.
            return None if saved is None else saved + 1

        # Given: it wins, including when it is None. Overriding a stale file
        # has to be expressible, and so does "ignore the file, take the live
        # stream" — which is why `None` is not the default.
        return self._offset if isinstance(self._offset, int) else None

    async def _open(self) -> Subscription:
        """Connect, then read the greeting before handing anything back.

        The greeting is awaited here rather than lazily on the first message,
        so that entering the block MEANS the server accepted this subscribe.
        The alternative surfaces a refused offset as a failure of whatever
        `recv` the application happened to reach first, which on a stream that
        is quiet out of hours is minutes later and somewhere else.
        """
        self._resolved = self._resolve()
        try:
            connection, info = await self._handshake(self._resolved)

        except NotReplayable as refusal:
            if not (self._catch_up and refusal.why in RECOVERABLE):
                raise

            # Too far behind for the server to replay, which is exactly what
            # the archive is for.
            subscription = await self._recover(refusal)

        else:
            subscription = Subscription(connection, info, self._stream, self._cursor)
            self._subscription = subscription

        if self._remote is not None:
            self._remote.start()

        return subscription

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

        # Before the socket closes, so its final upload sees the committed
        # value rather than racing the commit above.
        if self._remote is not None:
            self._remote.stop()

        await self._subscription.close()


__all__ = ["Subscription", "connect"]
