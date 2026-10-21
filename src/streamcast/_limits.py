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

from litelink import LogConfig

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
socket, not an error. `serve` and `asgi` take one int or a map choosing the
window for connections to each served stream; it is not a stream-wide total.
The two ends share a default.
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

PAGE_BYTES: Final = LogConfig().target_row_group_size
"""Arrow bytes a catch-up reads from a published table in one query, at most.

What `page_bytes=` bounds, on `connect(catch_up=True)`, `Stream.live` and
`Snapshot.rows`: a page is one `ORDER BY` query over a range of offsets,
which DuckDB sorts and hands back whole before its first row, so a page is
what a catch-up holds at once. The rows it is turned into weigh a multiple of
it as Python objects, which is the caller's to allow for.

litelink's default row group (`LogConfig.target_row_group_size`, 64 MiB of
Arrow), and taken from litelink rather than chosen here. It is the unit a
published file is sorted and written in, so an offset range of this size
reads one or two row groups rather than a sliver of many. It is also what a
log's own compaction holds at a time (about 2.5x it at the peak, by
litelink's measurement), so a process already sized for writing a stream is
sized for reading one back. litelink's default rather than the table's own
setting, because the reader is not the writer and does not know it.
"""

PAGE_ROWS: Final = None
"""Rows in one catch-up query, at most, besides `PAGE_BYTES`: none by default.

What `page_rows=` adds: a page ends at whichever budget it reaches first. A
row count alone cannot bound memory, because rows have no bounded width (a
million 10 KB rows is 10 GB), so bytes are the budget and this is a ceiling
for a caller whose cost is per row rather than per byte. With both None a
page is the whole range, one query, and nothing bounds it.
"""


def pages(page_bytes: int | None, page_rows: int | None) -> None:
    """Refuse a page budget that is not a positive int or None, at the call."""
    for keyword, value in (("page_bytes", page_bytes), ("page_rows", page_rows)):
        if value is None:
            continue

        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            msg = f"{keyword}={value!r}: a budget is an int of at least 1, or None"
            raise ValueError(msg)


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


DEDUPE_WINDOW: Final = 64
"""Committed ranges an exactly-once stream remembers per publisher, in memory.

What answers a resent duplicate below the publisher's last seq with the
offset it was first appended at (`_dedupe`). Matches the default
`MAX_IN_FLIGHT`; a larger window or cache eviction can leave a duplicate
without a remembered offset, but never causes it to be appended again.
About 1.5 KiB per cached publisher. Not stored: after a restart a duplicate below the last seq
is answered with `null`.
"""

MAX_PUBLISHERS: Final = 4_096
"""Publishers per exactly-once stream whose marks the broker caches.

Past it the least recently active is evicted and re-read from the table by
key at its next row, so the bound costs a lookup, never correctness: the
table in the log's buffer is the truth. About 2 KiB each with its ranges, so
about 8 MiB per stream at the default.
"""

INBOX_MAX_BYTES: Final = 64 * 2**20
"""Bytes of received, unprocessed rows a subscriber's inbox holds before the
library stops receiving (`streamcast.inbox(..., inbox_max_bytes=)`).

The inbox is a bounded receive window, not a backlog store: the backlog stays
in the log, durable and published, and is read again from the received
position. 64 MiB is 64 frames at the broker's 1 MiB `max_size`, so a stream of
the largest rows still has a working window, and about 65,000 rows at 1 KB,
enough to keep a processor fed across a reconnect. Counted as the stored JSON
text's UTF-8 length.
"""

INBOX_RESUME_AT: Final = 0.5
"""The low-water mark: receiving resumes once the inbox is below this
fraction of each of its bounds. Half, so a subscriber held at its bound does
not reconnect once per processed row."""

SEND_BATCH: Final = 100
"""Pending outbox rows sent as one frame, `[first_seq, [rows]]`."""

POLL_EVERY: Final = 1.0
"""Seconds between an outbox's or an inbox's looks at its own tables, for
rows another process's transaction committed or deleted."""

BACKOFF: Final = (0.5, 1.0, 2.0, 5.0, 10.0)
"""Seconds before each reconnect of an outbox's sender or an inbox's
receiver; the last repeats."""

__all__ = [
    "BACKOFF",
    "DEDUPE_WINDOW",
    "INBOX_MAX_BYTES",
    "INBOX_RESUME_AT",
    "MAX_BACKLOG",
    "MAX_INBOUND",
    "MAX_IN_FLIGHT",
    "MAX_PUBLISHERS",
    "MAX_TAIL",
    "PAGE_BYTES",
    "PAGE_ROWS",
    "POLL_EVERY",
    "SEND_BATCH",
    "pages",
    "per_stream",
]
