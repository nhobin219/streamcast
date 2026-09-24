"""What a subscription can fail with, and the close codes that carry it.

Every one of these is raised at the *client*, because every one of them is the
server's answer to a subscribe. The server does not raise them at itself — it
closes the connection with a code and a reason, and `_client` turns that pair
back into the exception below.

**The reason on the wire is a code and some numbers; the sentence is built
here.** RFC 6455 gives a close reason 123 bytes, which is not a sentence, and
a server that spent them on prose would hand the subscriber something only a
regex could act on. So `NotReplayable` travels as `{"why":"too_old",
"earliest":5000}` and this module turns it back into English — which means the
English has exactly one home and can be reworded without a protocol change.

The codes sit in the WebSocket private-use range (4000-4999) and deliberately
echo their HTTP cousins, so a reason line in a log is readable without this
file open: 4400 bad request, 4404 no such stream, 4416 range not satisfiable,
4429 you could not keep up.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Final


class Close(IntEnum):
    """The close codes a server uses to refuse or end a subscription.

    1000 and 1001 are not here: those are WebSocket's own normal-closure and
    going-away, they mean what they mean everywhere else, and a subscription
    that ends on either is not an error.
    """

    BAD_REQUEST = 4400
    """The path or query string is not a subscribe this server can parse."""

    NO_SUCH_STREAM = 4404
    """Nothing is served at that path."""

    NOT_REPLAYABLE = 4416
    """The requested offset cannot be served. `why` says which of the five
    reasons it is — see `NotReplayable`."""

    TOO_SLOW = 4429
    """The subscriber fell `max_backlog` messages behind and was dropped.

    Not data loss when the stream is durable: what it received is a contiguous
    prefix, so reconnecting one above its last offset replays the gap. It IS
    data loss on a stream with no log, which is the plainest argument for
    attaching one."""


class StreamcastError(Exception):
    """Base for everything this library raises. Catch this to catch all."""


class ProtocolError(StreamcastError):
    """The peer is not speaking streamcast.

    A greeting that will not parse, a version this build does not know, or a
    data frame too short to hold a header. Distinct from a refusal: a refusal
    is a server that understood the request and said no, and a different
    request would work. Nothing about this one will.
    """


class StreamNotFound(StreamcastError):
    """No stream is served at that path.

    Carries what the server does serve, because the overwhelmingly common
    cause is a typo in a name and the answer travels in the refusal. The list
    can be empty — a server serving many streams trims it to fit the close
    frame — which is why it is not part of the message when it is.
    """

    def __init__(self, requested: str, serves: tuple[str, ...] = ()) -> None:
        self.requested = requested
        self.serves = serves
        known = (
            f"; this server serves {', '.join(repr(n) for n in serves)}"
            if serves
            else ""
        )
        super().__init__(f"no stream at {requested!r}{known}")


class _Missing(dict):
    """A format mapping that answers `?` for a key the wire did not carry.

    A refusal is trimmed to 123 bytes by dropping fields, so a template here
    can legitimately be asked to render a number that never arrived. `?` in
    the sentence is a worse message; a `KeyError` inside exception
    construction is a crash while reporting a refusal, which loses the refusal
    entirely.
    """

    def __missing__(self, key: str) -> str:
        return "?"


_WHY: Final = {
    "not_durable": (
        "this stream has no log attached, so it assigns no offsets and there "
        "is nothing to replay from. Subscribe without `offset=`, or give the "
        "server a log."
    ),
    "empty": (
        "this stream's log holds no rows yet, so there is nothing to replay. "
        "Subscribe without `offset=`."
    ),
    "ahead": (
        "offset {offset} is above {end_offset}, the next offset this stream "
        "will assign — nothing has been issued there. A resume cursor this "
        "high usually means the server was restored or rebuilt."
    ),
    # These two are the pair `catch_up=True` exists for: the rows are not
    # gone, they are in the archive. Naming the flag in the sentence is the
    # point — this text is what an operator reads at 3am, and "read the log
    # directly" was an instruction to write the orchestration `_catchup`
    # now contains.
    "too_old": (
        "offset {offset} is {behind} messages behind and this server replays "
        "at most {max_replay}. Reconnect with catch_up=True to read the gap "
        "from the log's archive, or read the log directly."
    ),
    # **Conditional on purpose.** `earliest` here is the first offset the
    # SCAN returned, and litelink picks the tier per scan: local files while
    # the table still holds any, the archive only once it does not. So the
    # rows below it may still be in the archive — in which case catch_up
    # reads them — or may be below the archive too, in which case they are
    # gone. The server does not know which without a metadata GET it would
    # have to pay on every refusal, so the message says "if", and
    # `catch_up` reports the difference from the archive's own extent.
    "evicted": (
        "offset {offset} is below {earliest}, the earliest offset this "
        "stream's log still serves. Reconnect with catch_up=True to read the "
        "rows between from the archive if it still holds them — it will say "
        "so if it does not — or with offset=streamcast.EARLIEST to take what "
        "is left and accept the gap."
    ),
}
"""One home for every refusal sentence, keyed by what travels on the wire.

Rewording one of these is a documentation change. Adding a key is a protocol
change in the direction that degrades safely: an older subscriber renders the
fallback below and still gets the code, the numbers and a correct diagnosis of
"the server refused this offset".
"""


class NotReplayable(StreamcastError):
    """The requested offset cannot be served, and `why` says which reason.

    Five distinct states arrive here and the caller's next move differs for
    each: drop the offset and take the live stream, ask again from a different
    one, or stop asking the server and read the log directly. Collapsing them
    into one message was the first design and made every one of those a guess.
    """

    def __init__(self, why: str, **fields: object) -> None:
        self.why = why
        self.fields = fields
        template = _WHY.get(why)
        if template is None:
            detail = f"the server cannot replay from this offset ({why})"
        else:
            detail = template.format_map(_Missing(fields))

        super().__init__(detail)


class Rejected(StreamcastError):
    """The server would not take a published row, and why.

    The remote counterpart of `Stream.send` raising: litelink validates a row
    against the declared schema and names the column it objects to, and that
    message arrives here whole rather than trimmed, because a publisher
    debugging a schema mismatch needs the column name.

    **The connection survives it.** A local publisher catches this and sends
    the next row; a remote one is no worse off. What did NOT happen is the
    append — nothing was committed and no offset was issued, so the row can be
    corrected and sent again.
    """


class TooSlow(StreamcastError):
    """The server dropped this subscriber for falling too far behind.

    `offset` is the last one it received — tracked locally by the subscription
    rather than reported by the server, because the close frame has no room
    for it and the subscriber already knows. On a durable stream, reconnecting
    at `offset + 1` misses nothing. It is None only when the drop happened
    before a single message arrived.
    """

    def __init__(
        self, backlog: int | None = None, *, offset: int | None = None
    ) -> None:
        self.offset = offset
        self.backlog = backlog
        behind = f" more than {backlog} messages" if backlog is not None else ""
        resume = f"; resume at offset {offset + 1}" if offset is not None else ""
        super().__init__(
            f"the server dropped this subscriber for falling{behind} behind{resume}"
        )


__all__ = [
    "Close",
    "NotReplayable",
    "ProtocolError",
    "Rejected",
    "StreamNotFound",
    "StreamcastError",
    "TooSlow",
]
