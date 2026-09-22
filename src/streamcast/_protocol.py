"""Everything that crosses the wire, and nothing that does not.

**Every frame is JSON text.** The greeting is one, and every message after it
is one row of the stream's table with `litelink_offset` in it:

    {"streamcast":1,"stream":"trades","end_offset":1861,...}
    {"litelink_offset":1861,"event_ts":1790038800123456,"price":85565.0,...}

There is no binary framing, no length header and no payload kind, and an
earlier version of this file had all three. They existed to carry an opaque
blob — a whole upstream frame stored verbatim — which is the design litelink's
own example warns against in as many words: *"the reason to declare a schema
rather than store the frame whole"*. Once the log is a typed table per stream,
a message IS a row, and a row is a JSON object. The header held an offset that
the object now carries itself, and nothing read it (see `_subscriber`).

What that buys is not just simplicity. `wscat ws://broker:8765/trades?offset=0`
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

# The one column litelink owns, imported rather than spelled again. It is the
# key a subscriber resumes from, so a drift between the name on the wire and
# the name in the table is a resume that reads a column that is not there.
from litelink.log import OFFSET

from streamcast._errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Mapping

_ENCODER: Final = msgspec.json.Encoder()
_DECODER: Final = msgspec.json.Decoder()

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


def encode(
    offset: int, row: Mapping[str, object], columns: tuple[str, ...] | None
) -> bytes:
    """One row, as the JSON text bytes every subscriber gets.

    Encoded ONCE per message by the broker and handed to every subscriber's
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
    """
    payload: dict[str, object] = {OFFSET: offset}
    if columns is None:
        payload.update(row)
    else:
        for name in columns:
            payload[name] = row.get(name)

    return _ENCODER.encode(payload)


def encode_projected(row: Mapping[str, object]) -> bytes:
    """The same frame, for a row whose keys are ALREADY in wire order.

    The replay path only: a scan projected into `(litelink_offset, *columns)`
    hands back batches in that order, so Arrow's `to_pylist()` builds each
    dict already correct and re-projecting it in Python would be work done
    twice. Measured at 1.55 us a row against 2.38 for the rebuild, on a path
    that is ~86% encode.

    It is a second entry point into one encoder rather than a second encoder,
    so there is still one answer to "what does a frame look like" — and
    `_log.replay` checks the batch's column order against what it projected
    before using this, because the saving is only sound while that holds.
    """
    return _ENCODER.encode(row)


def decode(frame: str | bytes) -> tuple[int, dict[str, object]]:
    """The inverse: `(offset, row)`, with the offset still in the row.

    Left in rather than popped, because the row is the log's row and
    `litelink_offset` is one of its columns — a subscriber that writes what it
    receives into its own litelink log wants the column, and one that does not
    can ignore a key.
    """
    try:
        row = _DECODER.decode(frame)
    except msgspec.DecodeError as exc:
        msg = f"a data frame is not JSON: {frame[:120]!r}"
        raise ProtocolError(msg) from exc

    if not isinstance(row, dict):
        msg = f"a data frame is not a JSON object: {frame[:120]!r}"
        raise ProtocolError(msg)

    offset = row.get(OFFSET)
    if not isinstance(offset, int):
        msg = f"a data frame carries no {OFFSET!r}: {frame[:120]!r}"
        raise ProtocolError(msg)

    return offset, row


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
    return _ENCODER.encode(
        {
            "streamcast": VERSION,
            "stream": stream,
            "end_offset": end_offset,
            "replay": list(replay) if replay is not None else None,
            "durable": durable,
        }
    ).decode()


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
        fields = _DECODER.decode(frame)
    except msgspec.DecodeError as exc:
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
`msgspec.json.decode`. Both fit; only one is readable after the next reword.
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


__all__ = [
    "CLOSE_REASON_LIMIT",
    "EARLIEST",
    "OFFSET",
    "VERSION",
    "Greeting",
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
