"""Everything that crosses the wire, and nothing that does not.

**Every frame is JSON text.** The greeting is one, and every message after it
is a two-element pair — the offset, then one row of the stream's table:

    {"streamcast":2,"stream":"trades","end_offset":1861,"log":{...},...}
    [1861,{"event_ts":1790038800123456,"price":85565.0,"amount":0.015}]

**The offset is POSITIONAL, and the row is untouched.** It is element 0 of
the pair rather than a key in the object, so `msg` is exactly what the
publisher sent: no offset column, no injected metadata, nothing to strip
before forwarding it or appending it to another stream. litelink's own column
is called `litelink_offset` and that name never reaches a subscriber — it is
projected off in `_log` and re-attached here by position. A subscriber reads
`offset, msg = frame`; it does not read a name it would then have to know
belongs to a library it is not importing.

There is no binary framing, no length header and no payload kind, and an
earlier version of this file had all three. They existed to carry an opaque
blob — a whole upstream frame stored verbatim — which is the design litelink's
own example warns against in as many words: *"the reason to declare a schema
rather than store the frame whole"*. Once the log is a typed table per stream,
a message IS a row, and a row is a JSON object. The header held the offset the
pair now carries positionally, and nothing read it (see `_subscriber`).

What that buys is not just simplicity. `wscat ws://localhost:8765/trades?offset=0`
now prints the stream, readably, with no client library at all — and a
subscriber in any language needs a JSON parser rather than this file.

**msgspec, not `json`.** Serialisation is on the hot path in both directions
now — every publish encodes a row, every replayed row re-encodes one — and
msgspec is measured at 20.4x stdlib for encode and 12.9x for decode on a
six-column trade row (0.285 us against 5.815). At stdlib speed the encode
would cost more than the Parquet read it rides on.

**A subscribe is a URL, not a handshake.** The stream is the path and the
resume point is `?offset=`, so subscribing is the WebSocket open and there is
no round trip in front of the data. The price is that a refusal has to be a
close code (see `_errors.Close`) rather than a reply; the return is the
`wscat` line above.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final
from urllib.parse import parse_qsl, quote, urlsplit

import msgspec

from streamcast._errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Mapping

_ENCODER: Final = msgspec.json.Encoder()
_DECODER: Final = msgspec.json.Decoder()

# A data frame is a PAIR, `[offset, msg]`, and the two halves are different
# kinds of thing: the offset is the server's framing, and `msg` is the
# publisher's row, untouched.
#
# Two earlier versions put the offset INSIDE the object — first as
# `litelink_offset`, then as `offset` — and both were wrong for the same
# reason. A subscriber consumes the offset positionally (`offset, msg = ...`
# in Python, `const [offset, msg] = JSON.parse(f)` in JS), so the key name was
# a contract nobody wanted, argued about twice; and injecting it meant `msg`
# was never quite the row that was published. It is now, exactly.
#
# There is no key name here on purpose. That is the point.

VERSION: Final = 2
"""The protocol this build speaks. A greeting naming any other is refused.

One number for the whole protocol rather than a feature list, because there is
nothing yet to negotiate: a server and a subscriber that disagree about the
frame layout disagree about all of it.
"""

EARLIEST: Final = 0
"""`offset=EARLIEST` asks for everything the stream can still serve.

0 rather than -1 or a string, because litelink's own offsets start at 1 by
default and nothing is ever assigned 0 — so it is a value the offset space
already reserves, and it orders correctly against every real offset. A
negative offset is refused rather than being given a second meaning.
"""


def encode(
    offset: int | None,
    row: Mapping[str, object],
    columns: tuple[str, ...] | None,
) -> bytes:
    """One message, as the `[offset, msg]` bytes every subscriber gets.

    Encoded ONCE per message by the server and handed to every subscriber's
    queue, which is why this takes a row rather than a connection: fan-out is
    a queue insert, not a serialisation. At 200 subscribers that is one encode
    instead of 200 (I6).

    **`columns` fixes the key order, and that is what makes a replayed message
    byte-identical to the live one it repeats.** A live row arrives as a dict
    in whatever order the caller built it; a replayed row arrives from Arrow
    in schema order. Projecting both through the log's declared columns makes
    them the same bytes, so a subscriber that resumes across the join cannot
    tell where it happened. `None` — a stream with no log, which has no
    declared schema — takes the row's own order instead.

    `.get` rather than `[...]`, so a nullable column the caller omitted
    becomes JSON `null`. That is exactly what the table stores for it, and
    therefore exactly what a replay of the same row will send.

    **`msg` is the publisher's row and nothing else.** No offset key, no
    injected metadata — what a subscriber receives is what was sent, so it can
    be logged, forwarded or appended to another stream whole.

    **`offset` is `null` on a stream with no log.** A live-only stream assigns
    nothing: there is no log, so there is no offset, and a per-process counter
    would hand a subscriber an integer that looks exactly like a resume cursor
    and is not one. `null` cannot be mistaken for that — arithmetic on it
    fails where `7 + 1` quietly succeeds against a server that has restarted.
    """
    if columns is None:
        message: dict[str, object] = dict(row)
    else:
        message = {name: row.get(name) for name in columns}

    return _ENCODER.encode((offset, message))


def encode_projected(offset: int | None, message: Mapping[str, object]) -> bytes:
    """The same frame, for a message whose keys are ALREADY in wire order.

    The replay path only. A scan projected into `(litelink_offset, *columns)`
    hands back batches in that order, so Arrow's `to_pylist()` builds each
    dict in C and popping the offset off the front leaves exactly the message
    the live path would have built — 1.00 us a row against 2.58 for rebuilding
    each dict in Python.

    A second entry point into one encoder rather than a second encoder, so
    there is still one answer to "what does a frame look like". `_log.replay`
    checks the batch's column order against what it projected before using
    this, because the saving is sound only while that holds.
    """
    return _ENCODER.encode((offset, message))


def decode(frame: str | bytes) -> tuple[int | None, dict[str, object]]:
    """The inverse: `(offset, msg)`.

    `None` is a legitimate offset — a stream with no log. Everything else here
    is a peer that is not a streamcast server, which is why each shape gets
    its own message rather than one "malformed frame".
    """
    try:
        pair = _DECODER.decode(frame)
    except msgspec.DecodeError as exc:
        msg = f"a data frame is not JSON: {frame[:120]!r}"
        raise ProtocolError(msg) from exc

    if not isinstance(pair, list) or len(pair) != 2:
        msg = f"a data frame is not an [offset, msg] pair: {frame[:120]!r}"
        raise ProtocolError(msg)

    offset, message = pair
    if offset is not None and not isinstance(offset, int):
        msg = f"a frame's offset is {type(offset).__name__}, not an integer or null"
        raise ProtocolError(msg)

    if not isinstance(message, dict):
        msg = f"a frame's message is {type(message).__name__}, not an object"
        raise ProtocolError(msg)

    return offset, message


@dataclass(frozen=True, slots=True)
class LogInfo:
    """Enough to open the stream's log without asking anyone.

        reader = litelink.snapshot(info.log.name, archive=info.log.archive)

    The NAME is the part that cannot be guessed. A stream serves at its own
    name and its log has its own, and nothing makes them equal: `Stream.new`
    feeds one through, but `Stream(log=handle)` takes a log the caller opened
    and named. A subscriber that assumed `stream` was the log name asked the
    archive for a table that was not there, and `catch_up` reported it as a
    credentials problem.

    Credentials are deliberately absent. They are the reader's own, resolved
    from its environment the way litelink resolves them, and a server that
    sent them would be handing every subscriber its own keys.
    """

    name: str
    archive: str | None
    """Where the log is archived, or None if it has none.

    Published so a subscriber that falls behind the server's replay window
    knows where to read the gap — see `_catchup`. It is here rather than only
    in the refusal because a close frame has 123 bytes and a bucket URI plus
    the numbers that diagnose the refusal do not both fit; the refusal carries
    it too, last, so the numbers win when something has to go.
    """


@dataclass(frozen=True, slots=True)
class Greeting:
    """What the server says before the first message, and the only reply there is.

    It exists so that "the connection opened" means something on a stream that
    is silent — which, for market data outside a session, is most of them. A
    subscriber that gets this knows the server understood its offset, knows
    whether the offsets it is about to see survive a server restart, and knows
    what is about to be replayed before any of it arrives.
    """

    version: int
    stream: str
    end_offset: int | None
    """The offset the next message will be assigned, as of this subscribe.

    Also the exclusive upper bound of `replay`: everything below it comes from
    the log, everything from it up arrives live. That partition is what makes
    a resume exactly-once, and publishing the number is what lets a subscriber
    check it.
    """

    replay: tuple[int, int] | None
    """The `[start, end)` about to be replayed, or None for a live-only subscribe."""

    log: LogInfo | None
    """The log behind this stream, or None if it has none.

    `durable` answers whether there is one; this says which, so a subscriber
    can read it directly rather than through the socket — the whole history
    with `litelink.snapshot`, or any Iceberg engine pointed at the archive.
    """

    schema: dict[str, object] | None
    """The stream's columns as JSON Schema, or None without a log.

    Published so a subscriber in another language can read the shape without
    this repo — which is most of the reason the schema is declared in JSON
    terms rather than as a `pa.schema`. It is the greeting's only unbounded
    field, and a schema large enough to matter is a stream with hundreds of
    columns, which litelink would be the wrong store for anyway.
    """

    durable: bool
    """Whether a log is attached.

    False means the stream assigns no offsets: every frame carries `null`
    where an offset would be, and `?offset=` is refused outright rather than
    appearing to work until the day it matters. A subscriber that intends to
    resume can check this once, at subscribe, instead of discovering it from a
    cursor that was never real.
    """


def greeting(
    *,
    stream: str,
    end_offset: int | None,
    replay: tuple[int, int] | None,
    durable: bool,
    schema: dict[str, object] | None = None,
    log: tuple[str, str | None] | None = None,
) -> str:
    """The greeting, as the JSON that goes on the wire.

    `log` is the log's `(name, archive)`, or None for a stream with none. A
    nested object rather than flat keys, so the things a subscriber needs to
    open the log arrive together and `null` says plainly that there is
    nothing to open.
    """
    return _ENCODER.encode(
        {
            "streamcast": VERSION,
            "stream": stream,
            "end_offset": end_offset,
            "replay": list(replay) if replay is not None else None,
            "durable": durable,
            "schema": schema,
            "log": None if log is None else {"name": log[0], "archive": log[1]},
        }
    ).decode()


def parse_greeting(frame: str | bytes) -> Greeting:
    """Read a greeting, or say precisely how the peer is not one.

    Every failure here is `ProtocolError` rather than a refusal, because all
    of them mean the thing on the other end is not a streamcast server — an
    HTTP proxy's error page, a different service on a reused port, a build
    from before the frame layout changed. A subscriber cannot recover from any
    of that by asking differently.
    """
    if isinstance(frame, bytes):
        msg = "the server's first frame was binary; a greeting is text"
        raise ProtocolError(msg)

    try:
        fields = _DECODER.decode(frame)
    except msgspec.DecodeError as exc:
        msg = f"the server's first frame is not JSON: {frame[:120]!r}"
        raise ProtocolError(msg) from exc

    if not isinstance(fields, dict) or "streamcast" not in fields:
        msg = f"the server's first frame is not a streamcast greeting: {frame[:120]!r}"
        raise ProtocolError(msg)

    version = fields["streamcast"]
    if version != VERSION:
        msg = f"server speaks streamcast {version}; this build speaks {VERSION}"
        raise ProtocolError(msg)

    replay = fields.get("replay")
    schema = fields.get("schema")

    # A log with no name is not a log this can open, so it is not one worth
    # reporting: `None` says "nothing to read directly", which is also what a
    # stream without a log says.
    raw = fields.get("log")
    log = None
    if isinstance(raw, dict) and isinstance(raw.get("name"), str):
        archive = raw.get("archive")
        log = LogInfo(
            name=raw["name"],
            archive=archive if isinstance(archive, str) else None,
        )

    return Greeting(
        version=version,
        stream=fields.get("stream", ""),
        end_offset=None if fields["end_offset"] is None else int(fields["end_offset"]),
        replay=(int(replay[0]), int(replay[1])) if replay is not None else None,
        schema=schema if isinstance(schema, dict) else None,
        log=log,
        durable=bool(fields.get("durable", False)),
    )


CLOSE_REASON_LIMIT: Final = 123
"""What RFC 6455 allows a close reason to be, in bytes of UTF-8.

The limit is the reason refusals are JSON and not prose. A sentence that says
"offset 100 is below the earliest this server can still serve, which is 5000"
is 74 bytes of text a subscriber would have to parse with a regex to act on;
`{"error":"not_replayable","earliest":5000}` is 42 bytes it can act on with
`msgspec.json.decode`. Both fit; only one is readable after the next reword.
"""


def refusal(error: str, **fields: object) -> str:
    """A close reason as compact JSON, trimmed to fit `CLOSE_REASON_LIMIT`.

    Trimming drops the LAST field first and keeps going, so the caller passes
    fields in decreasing order of usefulness and `{"error": ...}` is the
    irreducible core — the code is already on the close frame, so even a fully
    trimmed reason tells the subscriber nothing it did not have. The case that
    forces this is a server serving many streams: the name list in a 4404 is
    unbounded and everything else here is not.
    """
    carried = dict(fields)
    while True:
        reason = _ENCODER.encode({"error": error, **carried}).decode()
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
        fields = _DECODER.decode(reason)
    except msgspec.DecodeError:
        return "", {}

    if not isinstance(fields, dict):
        return "", {}

    error = fields.pop("error", "")

    return (error if isinstance(error, str) else ""), fields


def parse_subscribe(path: str) -> tuple[str, int | None]:
    """A request path, as the stream name and the offset asked for.

    `/trades?offset=1200` is the whole of a subscribe. The name is the path
    with its leading slash removed, so a server serving one unnamed stream
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


__all__ = [
    "CLOSE_REASON_LIMIT",
    "EARLIEST",
    "VERSION",
    "Greeting",
    "LogInfo",
    "decode",
    "encode",
    "encode_projected",
    "greeting",
    "parse_greeting",
    "parse_refusal",
    "parse_subscribe",
    "refusal",
    "subscribe_path",
]
