"""Reading the gap from the published tables when a consumer has fallen too far behind.

A server refuses `?offset=` that is further back than `max_replay`, and
`catch_up=True` reads what it will not serve from the stream's published
tables, then picks the socket up where they end:

    async with streamcast.connect(uri, cursor=path, catch_up=True) as stream:
        async for offset, msg in stream:
            ...

The consumer sees one stream. Underneath, the rows below the server's window
come from object storage and the rest come from the socket.

**Built on `Stream.snapshot`.** A snapshot is the layer below: be correct or
fail, reading to a fixed point. Catch-up is one user of it, and owns only what
a snapshot does not — the retry loop and the live connection. The greeting
names the stream's metadata file and id, and a snapshot of it is everything
published, from the cursor up.

**No socket is held while the tables are read.** A connection opened first
would close the gap by construction — and make the server queue for a
subscriber that will not read a message until it has streamed millions of
rows out of object storage, so `max_backlog` would drop it before the catch-up
finished (invariant 7). So the snapshot is read with nothing connected, and
the socket opens after, at the offset the snapshot ended at. Rows published
meanwhile are inside the server's replay window; if they are not, more has
been published since, so the whole thing is a LOOP — read, connect, and if
the server still says too old, read again from where this round stopped.
`catch_up_retries` bounds it.

**Memory is one batch.** `Snapshot.rows` streams through the same batch
reader a server replays with, and every blocking call crosses into a thread.

**What it cannot fix.** If the published tables end below the server's
window, a range exists that neither holds. That is reported with both
numbers rather than half-served, because a consumer that silently resumed
above the gap would have lost data and been told it recovered.
"""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any, Final

from streamcast import _snapshot
from streamcast._errors import NotReplayable, StreamcastError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from litelink import S3Options

# The `why` values a catch-up can answer. `not_durable`, `empty` and `ahead`
# are not gaps in the published tables — they are a stream with no log, a log
# with nothing in it, and a cursor from the future — and no amount of reading
# object storage fixes any of them.
RECOVERABLE: Final = frozenset({"too_old", "evicted"})

CATCH_UP_RETRIES: Final = 3
"""Rounds of read-then-connect before giving up.

Each round narrows the gap, because more is published while the last one was
read. Three is enough for a stream publishing on any ordinary interval and
small enough that one published faster than it is read fails quickly, with a
message saying so, rather than reading object storage for ever.
"""


class CatchUpUnavailable(StreamcastError):
    """The gap cannot be read from the published tables, and why.

    Its own type because the caller's next move is specific and usually
    administrative — credentials, a bucket policy, an endpoint — rather than
    anything the retry loop can do.
    """


def _credentials_help(
    location: str, name: str, s3: S3Options | None, exc: object
) -> str:
    """What to actually do about a failed read.

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
        f"cannot read stream {name!r} from {location} to catch up.\n"
        f"\n"
        f"  tried:       {where}{region}\n"
        f"  credentials: {keyed}\n"
        f"  underlying:  {type(exc).__name__}: {str(exc)[:200]}\n"
        f"\n"
        f"This consumer has fallen further behind than the server will replay, "
        f"so the missing rows can only come from the stream's published "
        f"tables — and reading them needs credentials this process does not "
        f"appear to have.\n"
        f"\n"
        f"  * On AWS, the usual fix is an instance role or profile that can "
        f"GET and LIST where the stream publishes.\n"
        f"  * Elsewhere, set AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, "
        f"AWS_SECRET_ACCESS_KEY and AWS_REGION, or pass "
        f"streamcast.S3Options(...) as `s3=`.\n"
        f"  * `catch_up=False` turns this back into the plain NotReplayable "
        f"refusal, if you would rather handle the gap yourself.\n"
        f"  * To skip the gap and accept the loss, reconnect with "
        f"offset=streamcast.EARLIEST, or with no offset at all for live only."
    )


class Catcher:
    """Snapshot, connect, and go round again if still too far behind.

    Each round reads whatever is published above where the last one stopped,
    then asks the server to take over from there. A round that fails has not
    wasted its work: the rows it yielded are already delivered, and the next
    round starts above them.
    """

    __slots__ = (
        "_first",
        "_handshake",
        "_name",
        "_retries",
        "_s3",
        "_stream_id",
        "_uri",
        "connection",
        "info",
        "start",
    )

    def __init__(
        self,
        uri: str,
        stream_id: str | None,
        name: str,
        s3: S3Options | None,
        start: int,
        retries: int,
        handshake: Callable[[int], Awaitable[tuple[Any, Any]]],
    ) -> None:
        self._uri = uri
        self._stream_id = stream_id
        self._name = name
        self._s3 = s3
        self._retries = retries
        self._handshake = handshake
        self.start = start
        self.connection: Any = None
        self.info: Any = None
        # The first round's snapshot, opened by `prepare` rather than inside
        # the loop — see there for why.
        self._first: _snapshot.Snapshot | None = None

    async def _open(self) -> _snapshot.Snapshot:
        try:
            return await _snapshot.snapshot(
                self._uri, s3=self._s3, stream_id=self._stream_id
            )
        except _snapshot.SnapshotUnavailable as exc:
            raise CatchUpUnavailable(str(exc)) from exc
        except Exception as exc:
            raise CatchUpUnavailable(
                _credentials_help(self._uri, self._name, self._s3, exc)
            ) from exc

    async def prepare(self) -> None:
        """Open the first snapshot NOW, before any rows are asked for.

        **So that an unreadable stream raises at `connect`.** The rows stream
        lazily, which puts everything inside `stream` on the caller's first
        `recv` — and a consumer told its subscription was open, then handed an
        S3 credentials error minutes later from whatever line happened to read
        next, is exactly the failure the eager greeting exists to prevent.

        It settles the first round's extent too, so "the published tables do
        not reach far enough", at either end, also lands at `connect`.
        """
        first = await self._open()
        try:
            if first.end_offset <= self.start:
                raise _nothing_above(
                    self._name, self._uri, first.end_offset, self.start
                )

            # And the other end: tables that end above the request and still do
            # not go back far enough to cover it, which used to be served
            # silently from wherever they did start — rows missing, and a
            # cursor advanced past them.
            floor = await _refusing(first.floor())
            if floor is not None and floor > self.start:
                raise _gap_below(self._name, self._uri, floor, self.start)
        except BaseException:
            await first.close()
            raise

        self._first = first

    async def close(self) -> None:
        """Release a snapshot `prepare` opened that `stream` never took.

        A subscription closed before its first `recv` never STARTS the
        generator below, and `aclose` on an unstarted generator runs no code —
        so its `finally` never runs, and the snapshot's DuckDB connection is
        left behind. Idempotent, and a no-op once `stream` has taken it.
        """
        first, self._first = self._first, None
        if first is not None:
            await first.close()

    async def stream(self) -> AsyncGenerator[tuple[int, dict], None]:
        """Yield the gap, and leave `connection` set when it returns.

        Nothing is connected while rows are being yielded. That is the point.
        """
        refused: NotReplayable | None = None
        # What the consumer actually asked for, kept because `self.start`
        # advances as rows are delivered. The first row is checked against it.
        requested = self.start
        checked = False
        for _attempt in range(self._retries):
            # Round one uses what `prepare` opened, so the credential check
            # and the first read are not two round trips.
            snap = self._first or await self._open()
            self._first = None
            try:
                async for offset, row in _rows(snap, self.start):
                    if not checked:
                        checked = True
                        if offset > requested:
                            # **The hole at the join, caught at the other end.**
                            # `prepare` checks the tables' own extent; this is
                            # the backstop for retention moving the floor up
                            # between `prepare` and the read.
                            raise _gap_below(self._name, self._uri, offset, requested)

                    yield offset, row
                    # Tracked per ROW, so a round that fails partway still
                    # leaves the next one starting where this one stopped.
                    self.start = offset + 1

                # Past every row the snapshot held, which may be above the
                # last row read: offsets are not dense across a restore fence.
                self.start = max(self.start, snap.end_offset)
            finally:
                await snap.close()

            try:
                self.connection, self.info = await self._handshake(self.start)
                return

            except NotReplayable as exc:
                if exc.why not in RECOVERABLE:
                    raise

                # Still behind: the server moved on while the tables were
                # read. More will have been published since, so go again.
                refused = exc

        msg = (
            f"{self._name!r} could not be caught up in {self._retries} rounds: "
            f"everything published was delivered, up to offset {self.start}, and "
            f"the server still will not replay from there ({refused}).\n"
            f"\n"
            f"Two things look like this and the fix differs:\n"
            f"\n"
            f"  * The stream is written faster than it is published, so the gap "
            f"keeps moving. Publish more often, raise the server's "
            f"`max_replay`, or pass a larger `catch_up_retries`.\n"
            f"  * The server was RESTORED onto another machine. litelink "
            f"fences offsets on a restore — 2**20 of them — so the range "
            f"below its window was never issued and no table will ever hold "
            f"it. Only raising `max_replay` (or `None`) helps. Restore a "
            f"failed-over producer with `max_replay=None` if existing "
            f"consumers must resume.\n"
            f"\n"
            f"Either way the rows that DO exist were delivered, so a consumer "
            f"that has committed them can reconnect at offset={self.start} "
            f"once the server will serve it."
        )
        raise CatchUpUnavailable(msg)


async def _refusing(read: Awaitable[Any]) -> Any:
    """`read`, with a snapshot's refusal reported as catch-up's.

    A table is opened when it is first read, not when the snapshot is taken,
    so a refusal — a retired log published short — can surface from any read.
    """
    try:
        return await read
    except _snapshot.SnapshotUnavailable as exc:
        raise CatchUpUnavailable(str(exc)) from exc


async def _rows(
    snap: _snapshot.Snapshot, start: int
) -> AsyncGenerator[tuple[int, dict], None]:
    """`snap.rows(start)`, refusing as `_refusing` does."""
    try:
        async with aclosing(snap.rows(start)) as rows:
            async for item in rows:
                yield item

    except _snapshot.SnapshotUnavailable as exc:
        raise CatchUpUnavailable(str(exc)) from exc


def _nothing_above(name: str, uri: str, end: int, start: int) -> CatchUpUnavailable:
    """The published tables do not reach the offset being asked for.

    A range exists that the server has forgotten and nothing has published.
    Reported with both numbers rather than half-served, because a consumer that
    silently resumed above it would have lost data and been told it recovered.
    """
    return CatchUpUnavailable(
        f"{name!r} is behind the server's replay window, and its published "
        f"tables ({uri}) end at offset {end}, which is not above the {start} "
        f"being asked for. The rows between are gone from both — neither "
        f"holds them: the server has forgotten them and they were never "
        f"published. Reconnect with offset=streamcast.EARLIEST to take what "
        f"is left and accept the loss."
    )


def _gap_below(
    name: str, uri: str, earliest: int, requested: int
) -> CatchUpUnavailable:
    """The published tables do not go back as far as the offset being asked for.

    The server refused because its own tier had already dropped the rows, and
    the published tables turn out not to hold them either — so they are gone.
    Raised rather than served from wherever the tables do start, because a
    consumer handed a stream that silently begins above where it asked has
    lost data and been told it recovered.
    """
    return CatchUpUnavailable(
        f"{name!r} asked to catch up from offset {requested}, but its published "
        f"tables ({uri}) start at {earliest} — the {earliest - requested} rows "
        f"between are in neither the server nor the published tables. They "
        f"are gone. Reconnect with offset=streamcast.EARLIEST to take what is "
        f"left and accept the loss, or with offset={earliest} to state that you "
        f"know what is missing."
    )


def nowhere_to_read(name: str) -> CatchUpUnavailable:
    """The server names no metadata file: the stream has no log to read."""
    return CatchUpUnavailable(
        f"stream {name!r} is further behind than the server will replay, and "
        f"it has no log, so there is nothing published to read the gap from. "
        f"Raise the server's `max_replay`, or reconnect with "
        f"offset=streamcast.EARLIEST to take what it still holds and accept "
        f"the loss. `catch_up=False` turns this back into the plain refusal."
    )


__all__ = [
    "CATCH_UP_RETRIES",
    "RECOVERABLE",
    "CatchUpUnavailable",
    "Catcher",
    "nowhere_to_read",
]
