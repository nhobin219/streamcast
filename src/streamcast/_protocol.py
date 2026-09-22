"""Everything that crosses the wire, and nothing that does not.

Two frame kinds, told apart by WebSocket's own text/binary bit rather than by
a discriminator this library would have to define:

    TEXT    exactly one frame, first, the greeting. JSON.
    BINARY  every frame after it, one per message. `>QB` header, then payload.

That split is the whole protocol. It costs nothing — the bit is already in
every WebSocket frame — and it means a data frame needs no room for "am I
control", so the header is the offset and the payload's own kind and stops.

**A subscribe is a URL, not a handshake.** The stream is the path and the
resume point is `?offset=`, so subscribing is the WebSocket open and there is
no round trip in front of the data. The price is that a refusal has to be a
close code (see `_errors.Close`) rather than a reply; the return is that
`wscat ws://broker:8765/trades?offset=0` is a working subscriber, which is
worth more than symmetry on a path nobody debugs when it works.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Final
from urllib.parse import parse_qsl, quote, urlsplit

from streamcast._errors import ProtocolError

VERSION: Final = 1
"""The protocol this build speaks. A greeting naming any other is refused.

One number for the whole protocol rather than a feature list, because there is
nothing yet to negotiate: a broker and a subscriber that disagree about the
frame layout disagree about all of it.
"""

EARLIEST: Final = 0
"""`offset=EARLIEST` asks for everything the stream can still serve.

0 rather than -1 or a string, because litelink's own offsets start at 1 by
default and nothing is ever assigned 0 — so it is a value the offset space
already reserves, and it orders correctly against every real offset. A
negative offset is refused rather than being given a second meaning.
"""

TEXT: Final = 0
BINARY: Final = 1
"""Whether a message's payload is `str` or `bytes`.

Carried per message rather than declared per stream, because WebSocket lets a
feed mix them and a subscriber that receives `str` where the publisher sent
`bytes` has been handed different data, not a different encoding.
"""

_HEADER: Final = struct.Struct(">QB")
"""offset, then kind. 9 bytes, big-endian, unsigned.

Unsigned because an offset is never negative, and 8 bytes because litelink's
are int64. Fixed-width rather than varint: the saving would be ~6 bytes on a
payload that is rarely under a hundred, and a fixed header can be sliced
without being parsed.
"""


def kind_of(message: str | bytes) -> int:
    """`TEXT` or `BINARY`, and the one place a message's type is decided.

    Also the library's input validation, which is why it raises rather than
    guessing. Everything downstream — the row, the frame, the replay — trusts
    that a message is one of these two, and a `dict` that reached `encode`
    would fail there with a `struct` error naming nothing the caller passed.

    **`bytearray` and `memoryview` are refused rather than accepted**, which
    is narrower than `websockets`. Accepting them would buy nothing: `encode`
    concatenates the header onto the payload and `bytes.__add__` takes neither
    — so a buffer would be copied to `bytes` here anyway, and the caller doing
    `bytes(view)` at least sees the copy it is paying for.
    """
    if isinstance(message, str):
        return TEXT

    if isinstance(message, bytes):
        return BINARY

    msg = f"a message is str or bytes, not {type(message).__name__}"
    raise TypeError(msg)


def encode(offset: int, kind: int, payload: str | bytes) -> bytes:
    """One message, as the bytes every subscriber gets.

    Encoded ONCE per message by the broker and handed to every subscriber's
    queue, which is the reason this takes a payload rather than a connection:
    fan-out is a queue insert, not a serialisation. At 200 subscribers that is
    one UTF-8 encode instead of 200.
    """
    body = payload.encode() if isinstance(payload, str) else payload

    return _HEADER.pack(offset, kind) + body


def decode(frame: bytes) -> tuple[int, str | bytes]:
    """The inverse, and the only place a subscriber's `(offset, message)` is built."""
    if len(frame) < _HEADER.size:
        msg = f"data frame is {len(frame)} bytes, too short for a {_HEADER.size}-byte header"
        raise ProtocolError(msg)

    offset, kind = _HEADER.unpack_from(frame)
    body = frame[_HEADER.size :]
    if kind == TEXT:
        try:
            return offset, body.decode()
        except UnicodeDecodeError as exc:
            msg = f"offset {offset} is marked text and is not valid UTF-8"
            raise ProtocolError(msg) from exc

    if kind != BINARY:
        msg = f"offset {offset} has unknown payload kind {kind}"
        raise ProtocolError(msg)

    return offset, body


@dataclass(frozen=True, slots=True)
class Greeting:
    """What the broker says before the first message, and the only reply there is.

    It exists so that "the connection opened" means something on a stream that
    is silent — which, for market data outside a session, is most of them. A
    subscriber that gets this knows the broker understood its offset, knows
    whether the offsets it is about to see survive a broker restart, and knows
    what is about to be replayed before any of it arrives.
    """

    version: int
    stream: str
    end_offset: int
    """The offset the next message will be assigned, as of this subscribe.

    Also the exclusive upper bound of `replay`: everything below it comes from
    the log, everything from it up arrives live. That partition is what makes
    a resume exactly-once, and publishing the number is what lets a subscriber
    check it.
    """

    replay: tuple[int, int] | None
    """The `[start, end)` about to be replayed, or None for a live-only subscribe."""

    durable: bool
    """Whether a log is attached.

    False means these offsets are a counter in the broker's memory: they order
    correctly for as long as it runs and mean nothing across a restart. A
    subscriber that intends to resume should refuse to depend on them, and
    `?offset=` is refused outright on such a stream rather than appearing to
    work until the day it matters.
    """


def greeting(
    *,
    stream: str,
    end_offset: int,
    replay: tuple[int, int] | None,
    durable: bool,
) -> str:
    """The greeting, as the JSON that goes on the wire."""
    return json.dumps(
        {
            "streamcast": VERSION,
            "stream": stream,
            "end_offset": end_offset,
            "replay": list(replay) if replay is not None else None,
            "durable": durable,
        }
    )


def parse_greeting(frame: str | bytes) -> Greeting:
    """Read a greeting, or say precisely how the peer is not one.

    Every failure here is `ProtocolError` rather than a refusal, because all
    of them mean the thing on the other end is not a streamcast broker — an
    HTTP proxy's error page, a different service on a reused port, a build
    from before the frame layout changed. A subscriber cannot recover from any
    of that by asking differently.
    """
    if isinstance(frame, bytes):
        msg = "the broker's first frame was binary; a greeting is text"
        raise ProtocolError(msg)

    try:
        fields = json.loads(frame)
    except ValueError as exc:
        msg = f"the broker's first frame is not JSON: {frame[:120]!r}"
        raise ProtocolError(msg) from exc

    if not isinstance(fields, dict) or "streamcast" not in fields:
        msg = f"the broker's first frame is not a streamcast greeting: {frame[:120]!r}"
        raise ProtocolError(msg)

    version = fields["streamcast"]
    if version != VERSION:
        msg = f"broker speaks streamcast {version}; this build speaks {VERSION}"
        raise ProtocolError(msg)

    replay = fields.get("replay")

    return Greeting(
        version=version,
        stream=fields.get("stream", ""),
        end_offset=int(fields["end_offset"]),
        replay=(int(replay[0]), int(replay[1])) if replay is not None else None,
        durable=bool(fields.get("durable", False)),
    )


CLOSE_REASON_LIMIT: Final = 123
"""What RFC 6455 allows a close reason to be, in bytes of UTF-8.

The limit is the reason refusals are JSON and not prose. A sentence that says
"offset 100 is below the earliest this broker can still serve, which is 5000"
is 74 bytes of text a subscriber would have to parse with a regex to act on;
`{"error":"not_replayable","earliest":5000}` is 42 bytes it can act on with
`json.loads`. Both fit; only one is still readable after the next reword.
"""


def refusal(error: str, **fields: object) -> str:
    """A close reason as compact JSON, trimmed to fit `CLOSE_REASON_LIMIT`.

    Trimming drops the LAST field first and keeps going, so the caller passes
    fields in decreasing order of usefulness and `{"error": ...}` is the
    irreducible core — the code is already on the close frame, so even a fully
    trimmed reason tells the subscriber nothing it did not have. The case that
    forces this is a broker serving many streams: the name list in a 4404 is
    unbounded and everything else here is not.
    """
    carried = dict(fields)
    while True:
        reason = json.dumps({"error": error, **carried}, separators=(",", ":"))
        if len(reason.encode()) <= CLOSE_REASON_LIMIT or not carried:
            return reason

        carried.popitem()


def parse_refusal(reason: str) -> tuple[str, dict[str, object]]:
    """A close reason, as `(error, fields)`. Never raises.

    A reason this cannot read comes back as `("", {})` rather than an
    exception, because the caller is already handling a closed connection and
    the close CODE is what it dispatches on. An unparseable reason costs the
    detail, not the diagnosis — and it is exactly what arrives from a proxy or
    a load balancer that closed the connection itself.
    """
    try:
        fields = json.loads(reason)
    except ValueError:
        return "", {}

    if not isinstance(fields, dict):
        return "", {}

    error = fields.pop("error", "")

    return (error if isinstance(error, str) else ""), fields


def parse_subscribe(path: str) -> tuple[str, int | None]:
    """A request path, as the stream name and the offset asked for.

    `/trades?offset=1200` is the whole of a subscribe. The name is the path
    with its leading slash removed, so a broker serving one unnamed stream
    serves it at `/` and the name is `""` — which is the same empty name
    `Stream()` carries, rather than a second spelling of "no name".

    Raises `ValueError` for anything malformed; the caller turns that into a
    4400, because a path this cannot read is a client bug and no amount of
    retrying changes it.
    """
    split = urlsplit(path)
    name = split.path.lstrip("/")
    query = dict(parse_qsl(split.query, keep_blank_values=True))

    unknown = set(query) - {"offset"}
    if unknown:
        msg = f"unknown query parameter(s): {', '.join(sorted(unknown))}"
        raise ValueError(msg)

    raw = query.get("offset")
    if raw is None:
        return name, None

    try:
        offset = int(raw)
    except ValueError:
        msg = f"offset={raw!r} is not an integer"
        raise ValueError(msg) from None

    if offset < EARLIEST:
        # Refused rather than folded into EARLIEST. A negative offset is
        # almost always `last_seen - 1` arithmetic gone wrong on an empty
        # stream, and silently serving the whole log for it is the loudest
        # possible wrong answer.
        msg = f"offset={offset} is negative; {EARLIEST} means everything"
        raise ValueError(msg)

    return name, offset


def subscribe_path(name: str, offset: int | None) -> str:
    """The inverse, for the client. Kept beside the parser so they cannot drift."""
    path = "/" + quote(name)
    if offset is None:
        return path

    return f"{path}?offset={int(offset)}"


def frame_offset(frame: bytes) -> int:
    """The offset out of an already-encoded frame, without decoding the payload.

    Nine bytes unpacked, on the send path, once per subscriber per message.
    The alternative was queueing `(offset, frame)` tuples instead of the
    frame — and a queue entry is a POINTER to a frame every subscriber shares,
    so a tuple per entry would be ~56 bytes against 8 on the one term that
    grows with both backlog depth and subscriber count. Encoding once and
    re-reading the header is what keeps fan-out memory proportional to
    messages rather than to messages times subscribers.
    """
    return int(_HEADER.unpack_from(frame)[0])


__all__ = [
    "BINARY",
    "CLOSE_REASON_LIMIT",
    "EARLIEST",
    "TEXT",
    "VERSION",
    "Greeting",
    "decode",
    "encode",
    "frame_offset",
    "kind_of",
    "greeting",
    "parse_greeting",
    "parse_refusal",
    "parse_subscribe",
    "refusal",
    "subscribe_path",
]
