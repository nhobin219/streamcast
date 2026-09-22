"""Reading the gap out of the archive when a consumer has fallen too far behind.

A server refuses `?offset=` that is further back than `max_replay`, and until
this existed the answer was "read the log yourself" — which means the consumer
has to know where the archive is, open litelink, scan it in batches without
running out of memory, convert rows, and then work out where to resume the
socket. That is orchestration nobody wants to write twice, so `catch_up=True`
does it:

    async with streamcast.connect(uri, cursor=path, catch_up=True) as stream:
        async for offset, msg in stream:
            ...

The consumer sees one stream. Underneath, the rows below the server's window
come from object storage and the rest come from the socket.

**No socket is held while the archive is read.** The first version of this
opened the live connection at the archive's frontier first, reasoning that it
closed the gap by construction. It does — and it also makes the server queue
for a subscriber that will not read a message until it has streamed millions
of rows out of object storage. `max_backlog` is 8,192, so the connection would
be dropped with `TooSlow` before the catch-up finished: a recovery that
guaranteed its own failure on exactly the consumers that needed it.

So the archive is read with nothing connected, and the socket is opened after,
at the offset the archive actually reached. That leaves a window — rows
published while the gap was being read — and the server still holds those,
because they are inside its replay window. If they are not, the archive has
grown in the meantime, so the whole thing is a LOOP: read, try to connect,
and if the server still says too old, read the newly archived rows and try
again. It converges once the archive gets within the server's window, and
`catch_up_retries` bounds it for when that never happens.

**Memory is one batch.** The scan is a `RecordBatchReader` and every blocking
call crosses into a thread, exactly as the server's replay does — a catch-up
of ten million rows holds one batch, not ten million.

**What it cannot fix.** If the archive's frontier is itself below the server's
window, there is a range nothing holds: the server has forgotten it and the
archive never received it. That is reported with both numbers rather than
half-served, because a consumer that silently resumed above the gap would have
lost data and been told it recovered.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Final

import litelink

from streamcast import _log
from streamcast._errors import NotReplayable, StreamcastError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from litelink import RemoteReadHandle, S3Options

# The `why` values a catch-up can answer. `not_durable`, `empty` and `ahead`
# are not gaps in an archive — they are a stream with no log, a log with
# nothing in it, and a cursor from the future — and no amount of reading
# object storage fixes any of them.
RECOVERABLE: Final = frozenset({"too_old", "evicted"})

CATCH_UP_RETRIES: Final = 3
"""Rounds of read-the-archive-then-connect before giving up.

Each round narrows the gap, because the archive grows while the last one was
read. Three is enough for a server syncing on any ordinary interval and small
enough that a stream published faster than it is archived fails quickly with
a message saying so, rather than reading object storage for ever.
"""


class CatchUpUnavailable(StreamcastError):
    """The gap cannot be read from object storage, and why.

    Its own type because the caller's next move is specific and usually
    administrative — credentials, a bucket policy, an endpoint — rather than
    anything the retry loop can do.
    """


def _credentials_help(
    archive: str, name: str, s3: S3Options | None, exc: object
) -> str:
    """What to actually do about a failed archive read.

    Long on purpose. This fires on a box that is behind, at the moment its
    operator most needs to know whether the problem is a typo, a missing
    role, or a genuinely unreadable bucket — and "AccessDenied" on its own
    answers none of those.
    """
    where = (
        f"endpoint {s3.endpoint}"
        if s3 is not None and s3.endpoint
        else "the AWS default endpoint"
    )
    region = f", region {s3.region}" if s3 is not None and s3.region else ""
    keyed = (
        "an explicit access key"
        if s3 is not None and s3.access_key
        else "the ambient credential chain (profile, instance metadata, SSO)"
    )

    return (
        f"cannot read stream {name!r} from {archive} to catch up.\n"
        f"\n"
        f"  tried:       {where}{region}\n"
        f"  credentials: {keyed}\n"
        f"  underlying:  {type(exc).__name__}: {str(exc)[:200]}\n"
        f"\n"
        f"This consumer has fallen further behind than the server will replay, "
        f"so the missing rows can only come from the archive — and reading it "
        f"needs credentials this process does not appear to have.\n"
        f"\n"
        f"  * On AWS, the usual fix is an instance role or profile that can "
        f"GET and LIST under {archive}.\n"
        f"  * Elsewhere, set AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, "
        f"AWS_SECRET_ACCESS_KEY and AWS_REGION, or pass "
        f"streamcast.S3Options(...) as `s3=`.\n"
        f"  * `catch_up=False` turns this back into the plain NotReplayable "
        f"refusal, if you would rather handle the gap yourself.\n"
        f"  * To skip the gap and accept the loss, reconnect with "
        f"offset=streamcast.EARLIEST, or with no offset at all for live only."
    )


class CatchUp:
    """A bounded read of one stream's archive, and where to resume after it."""

    __slots__ = ("_archive", "_name", "_reader", "_s3")

    def __init__(self, archive: str, name: str, s3: S3Options | None) -> None:
        self._archive = archive
        self._name = name
        self._s3 = s3
        self._reader: RemoteReadHandle | None = None

    async def open(self) -> int:
        """Assemble the reader and return the offset after its last row.

        In a thread: `litelink.snapshot` resolves a catalog and reads table
        metadata over the network, which is seconds, and on the event loop
        that is the consumer's whole process stopped.

        The credential failure is caught HERE rather than at the first batch,
        because this is the call that touches the bucket first and the caller
        should learn it cannot read before it has been told it is recovering.
        """
        try:
            self._reader = await asyncio.to_thread(
                litelink.snapshot, self._name, archive=self._archive, s3=self._s3
            )
            return await asyncio.to_thread(self._reader.end_offset)

        except Exception as exc:
            await self.close()
            raise CatchUpUnavailable(
                _credentials_help(self._archive, self._name, self._s3, exc)
            ) from exc

    def rows(self, start: int, stop: int) -> AsyncGenerator[tuple[int, dict], None]:
        """`[start, stop)` from the archive, one batch in memory at a time.

        The server's own batch reader, reused — so a caught-up row is built
        exactly the way a replayed one is, from the same projection in the
        same order.
        """
        if self._reader is None:  # pragma: no cover — `open` comes first
            msg = "open() before rows()"
            raise RuntimeError(msg)

        return _log.rows(self._reader, start, stop)

    async def close(self) -> None:
        reader, self._reader = self._reader, None
        if reader is not None:
            # A snapshot owns a scratch directory it removes on close, so
            # leaking one leaks disk as well as a DuckDB connection.
            await asyncio.to_thread(reader.close)


class Catcher:
    """Read the archive, connect, and go round again if still too far behind.

    The loop is the whole design. Each round reads whatever the archive holds
    above where the last one stopped, then asks the server to take over from
    there. A round that fails has not wasted its work: the rows it yielded are
    already delivered, and the next round starts above them.
    """

    __slots__ = (
        "_archive",
        "_first",
        "_frontier",
        "_handshake",
        "_name",
        "_retries",
        "_s3",
        "connection",
        "info",
        "start",
    )

    def __init__(
        self,
        archive: str,
        name: str,
        s3: S3Options | None,
        start: int,
        retries: int,
        handshake: Callable[[int], Awaitable[tuple[Any, Any]]],
    ) -> None:
        self._archive = archive
        self._name = name
        self._s3 = s3
        self._retries = retries
        self._handshake = handshake
        self.start = start
        self.connection: Any = None
        self.info: Any = None
        # The first round's reader, opened by `prepare` rather than inside
        # the loop — see there for why.
        self._first: CatchUp | None = None
        self._frontier = 0

    async def prepare(self) -> None:
        """Open the first reader NOW, before any rows are asked for.

        **So that an unreadable archive raises at `connect`.** The rows stream
        lazily, which puts everything inside `stream` on the caller's first
        `recv` — and a consumer told its subscription was open, then handed an
        S3 credentials error minutes later from whatever line happened to read
        next, is exactly the failure the eager greeting exists to prevent.
        Observed doing precisely that before this existed.

        It settles the first round's frontier too, so "the archive does not
        reach far enough" also lands at `connect`.
        """
        self._first = CatchUp(self._archive, self._name, self._s3)
        self._frontier = await self._first.open()
        if self._frontier <= self.start:
            await self._first.close()
            self._first = None
            raise _nothing_above(self._name, self._archive, self._frontier, self.start)

    async def close(self) -> None:
        """Release a reader `prepare` opened that `stream` never took.

        `prepare` opens the first round's reader eagerly, so that a
        credentials failure lands at `connect`. If the caller then closes the
        subscription without ever calling `recv`, the generator below is never
        STARTED — `aclose` on an unstarted generator runs no code, so the
        `finally` that closes the reader never runs either, and a DuckDB
        connection and the snapshot's scratch directory are left behind.

        Idempotent, and a no-op in the ordinary case: `stream` clears `_first`
        the moment it takes it, so only the never-read path has anything here.
        """
        first, self._first = self._first, None
        if first is not None:
            await first.close()

    async def stream(self) -> AsyncGenerator[tuple[int, dict], None]:
        """Yield the gap, and leave `connection` set when it returns.

        Nothing is connected while rows are being yielded. That is the point.
        """
        refused: NotReplayable | None = None
        for _attempt in range(self._retries):
            # Round one uses what `prepare` already opened, so the credential
            # check and the first read are not two round trips.
            reader = self._first or CatchUp(self._archive, self._name, self._s3)
            frontier = self._frontier if self._first is not None else 0
            self._first = None
            try:
                if frontier == 0:
                    frontier = await reader.open()

                if frontier > self.start:
                    async for offset, row in reader.rows(self.start, frontier):
                        yield offset, row
                        # Tracked per ROW, so a round that fails partway still
                        # leaves the next one starting where this one stopped.
                        self.start = offset + 1

            finally:
                await reader.close()

            try:
                self.connection, self.info = await self._handshake(self.start)
                return

            except NotReplayable as exc:
                if exc.why not in RECOVERABLE:
                    raise

                # Still behind: the server moved on while the gap was being
                # read. The archive will have moved with it, so go again.
                refused = exc

        msg = (
            f"{self._name!r} could not be caught up in {self._retries} rounds: "
            f"after reading the archive at {self._archive} up to offset "
            f"{self.start}, the server still will not replay from there "
            f"({refused}). The stream is being published faster than its "
            f"archive is synced — raise the server's `max_replay`, sync more "
            f"often, or pass a larger `catch_up_retries`."
        )
        raise CatchUpUnavailable(msg)


def _nothing_above(
    name: str, archive: str, frontier: int, start: int
) -> CatchUpUnavailable:
    """The archive does not reach the offset being asked for.

    A range exists that the server has forgotten and the archive never
    received. Reported with both numbers rather than half-served, because a
    consumer that silently resumed above it would have lost data and been told
    it recovered.
    """
    return CatchUpUnavailable(
        f"{name!r} is behind the server's replay window and the archive at "
        f"{archive} ends at offset {frontier}, which is not above the {start} "
        f"being asked for. The rows between are gone from both — neither "
        f"holds them: the server has forgotten them and the archive never "
        f"received them. Reconnect with offset=streamcast.EARLIEST to take "
        f"what is left and accept the loss."
    )


def from_refusal(exc: NotReplayable, configured: str | None) -> str | None:
    """Where to read the gap, from the caller or from the refusal.

    Explicit wins: a caller that named an archive meant that one, and it also
    covers a server whose own is unreachable from here. Otherwise the refusal
    may carry it — but only when it fitted, since `refusal` trims to 123 bytes
    and the numbers are ordered ahead of it. `None` means ask the greeting.
    """
    if configured:
        return configured

    found = exc.fields.get("archive")

    return found if isinstance(found, str) and found else None


def nowhere_to_read(name: str) -> CatchUpUnavailable:
    """Neither the caller, the refusal, nor the greeting named an archive."""
    return CatchUpUnavailable(
        f"stream {name!r} is further behind than the server will replay, and "
        f"there is no archive to read the gap from — the server has none "
        f"configured. Either give the server an archive (litelink's "
        f"`archive=`), raise its `max_replay`, or reconnect with "
        f"offset=streamcast.EARLIEST to take what it still holds and accept "
        f"the loss. `catch_up=False` turns this back into the plain refusal."
    )


__all__ = [
    "RECOVERABLE",
    "CatchUp",
    "CATCH_UP_RETRIES",
    "CatchUpUnavailable",
    "Catcher",
    "from_refusal",
    "nowhere_to_read",
]
