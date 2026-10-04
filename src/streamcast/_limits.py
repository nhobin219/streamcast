"""The queue bounds: their defaults, and `serve`'s per-stream form of each.

Neither end may be run out of memory by the other, or by a stall — SPEC §4
lists each queue and what bounds it. The bounds are deployment settings, not
properties of a stream: they belong to the process that serves or reads, so
they change on a restart without touching the code that builds the streams.
So each is a keyword of the call that owns the queue:

    streamcast.serve(streams, host, port, max_backlog=16_384)        # every stream
    streamcast.serve(streams, host, port,
                     max_inbound={"trades": 262_144, "quotes": 65_536})  # per stream
    streamcast.publish(uri, max_in_flight=128)
    await streamcast.Stream.live(uri, max_tail=5_000_000)

`serve` and `asgi` take each of theirs as one int for every stream or a map
from stream name to int. A map must name exactly the streams being served — a
missing name would serve that stream on a default nobody chose, and an
unknown one is a typo — so either raises at the call, naming them.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from streamcast._stream import Stream

MAX_BACKLOG: Final = 8_192
"""Messages a subscriber may fall behind before it is dropped.

Counted in MESSAGES, not bytes, because that is what the queue holds — a
pointer to a frame every subscriber shares. A backlog is ~8 bytes per
subscriber per queued message plus one copy of each frame, so 8,192 across 200
subscribers is ~13 MB of pointers over whatever the frames themselves weigh.
Size it in bytes by multiplying by your own message size; there is no setting
that does it for you, because the library never sees a typical message until
it is running.
"""

MAX_INBOUND: Final = 65_536
"""Rows a durable stream may have queued for commit before a send waits.

The inbound twin of `max_backlog`. Every publisher on a stream feeds one
writer thread; if the disk falls behind, rows queue in memory, and without a
bound they queue until the broker is out of it. At the bound a `send` waits
for room — a remote publisher's connection stops being read, and TCP holds
the publisher back — rather than anything being refused. Per stream, across
all its publishers.
"""

MAX_IN_FLIGHT: Final = 64
"""Publish frames a connection may have unanswered — both ends' default.

The client's window: enough that a publisher calling `submit` keeps the
server's writer fed, so its rows group into one commit rather than one round
trip each; few enough that a dropped connection leaves a short tail to check
on reconnect (`docs/SPEC.md` §6b). The server's bound: how many replies it
will owe one connection before it stops reading it — backpressure at the
socket, not an error. One default, so a publisher on defaults is never held.
"""

MAX_TAIL: Final = 1_000_000
"""Rows a snapshot or live view will hold from the broker, in memory.

What the published tables do not hold yet is read off the socket and kept; it
stays small while publishing keeps up. Past this, a snapshot is refused and a
live view stops, each saying the published tables are too far behind, rather
than the reader running out of memory waiting on a publisher that has
stalled. Counted in rows read, not offset distance, which a restore fence
stretches by 2**20, or 2**40 with no WAL replica.
"""


def per_stream(
    keyword: str, value: int | Mapping[str, int], names: list[str]
) -> dict[str, int]:
    """Each served stream's `keyword` bound, from one int or a map naming every
    stream. `names` are the streams' names as served (`""` is the one at `/`).
    """
    by_stream = isinstance(value, Mapping)
    chosen = dict(value) if by_stream else dict.fromkeys(names, value)
    missing = sorted(set(names) - set(chosen))
    unknown = sorted(set(chosen) - set(names))
    if missing or unknown:
        parts = []
        if missing:
            parts.append(f"nothing for {', '.join(map(repr, missing))}")

        if unknown:
            parts.append(f"{', '.join(map(repr, unknown))}, which is not served")

        msg = (
            f"{keyword}= by stream must name exactly the streams served: "
            f"{'; '.join(parts)}. Pass one int to apply it to every stream."
        )
        raise ValueError(msg)

    for name in names:
        bound = chosen[name]
        # At the call, where a bad config is found, rather than at the first
        # subscriber or send — and a zero would mean "never deliver anything".
        if not isinstance(bound, int) or isinstance(bound, bool) or bound < 1:
            where = f"[{name!r}]" if by_stream else ""
            msg = f"{keyword}{where}={bound!r}: a bound is an int of at least 1"
            raise ValueError(msg)

    return {name: chosen[name] for name in names}


def _bound(
    routes: dict[str, Stream],
    max_backlog: int | Mapping[str, int],
    max_inbound: int | Mapping[str, int],
    max_in_flight: int | Mapping[str, int],
) -> None:
    """Resolve `serve`'s three bounds per stream and set them on each.

    Every keyword is checked before any stream is touched, so a bad one
    leaves every stream as it was.
    """
    names = list(routes)
    backlog = per_stream("max_backlog", max_backlog, names)
    inbound = per_stream("max_inbound", max_inbound, names)
    in_flight = per_stream("max_in_flight", max_in_flight, names)
    for name, stream in routes.items():
        stream._bound(  # noqa: SLF001 — the serving process's settings
            max_backlog=backlog[name],
            max_inbound=inbound[name],
            max_in_flight=in_flight[name],
        )


__all__ = ["MAX_BACKLOG", "MAX_INBOUND", "MAX_IN_FLIGHT", "MAX_TAIL", "per_stream"]
