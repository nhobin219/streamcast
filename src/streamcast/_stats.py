"""What a stream can say about itself, and the endpoint that serves it.

**Facts, not a verdict.** Freshness is domain knowledge — a 30-second poller
and a 5-second socket disagree about what "stale" means, and a stream that
publishes once a day at 00:20 UTC is healthy while silent for 23 hours. A
threshold chosen here would be wrong for someone and, worse, would look
authoritative. So this reports numbers and the application decides; an
application-specific health check is a route or a script that reads these and
applies its own rules.

That is also why there is no `health=` callback. An application that can
classify can classify at its own endpoint — asking the library to call back
into it and then serve the answer on its behalf is indirection with no payoff.

**Ages are computed here; timestamps are for you.** `last_send_age_s` comes
off `time.monotonic`, so it survives an NTP step and means the same thing on a
machine whose clock disagrees with the caller's. The wall-clock fields are
what a human reads and what correlates with your own logs. A caller that
subtracted `last_send_ts` from its own `time.time()` would be measuring clock
skew as much as staleness, which is why both are published rather than one.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from msgspec import json

if TYPE_CHECKING:
    from collections.abc import Iterable

    from streamcast._stream import Stream

INFO_PATH: Final = "/info"
"""Where `serve(info=True)` answers.

`/info` rather than `/health`, because the payload carries no verdict and a
name that implied one would be read as though it did.
"""


@dataclass(frozen=True, slots=True)
class Stats:
    """One stream, as of now.

    Reachable as `Stream.stats` without a socket, which is the point: the
    facts are the API and HTTP is one way to publish them. A mounted ASGI app
    writes its own route over this; `serve` exposes `/info` because a
    standalone server has no app to add a route to.
    """

    name: str
    durable: bool

    end_offset: int | None
    """The offset the next message will be assigned, or None with no log.

    **Rate lives here, not in a field of its own.** Read it twice and you know
    the throughput over whatever window you actually care about. A `rows_1m`
    computed here would impose a window, which is the same mistake as imposing
    a freshness threshold.
    """

    subscribers: int

    started_ts: float
    """Wall clock when this `Stream` was constructed."""

    uptime_s: float
    """Seconds since construction, from the monotonic clock.

    **Published because `last_send_*` is null until the first send.** A server
    that has just restarted reports a large `end_offset` and no send, which on
    its own is indistinguishable from a stream that died — and on a quiet
    stream that ambiguity can last hours, which is the false alarm this whole
    endpoint exists to remove. Nothing sent four seconds into a restart is
    ordinary; nothing sent six hours in is not. Neither fact says that alone.
    """

    last_send_ts: float | None
    """Wall clock of the most recent `send`, or None if there has been none.

    **None means "not in this process"**, not "never" — the log may hold
    millions of rows written before the last restart. Nothing reads a
    timestamp back out of the log today, because no column carries one; see
    the `streamcast_send_ts` proposal for the change that would let a restart
    report the true last ingest time instead of null.
    """

    last_send_age_s: float | None
    """Seconds since the most recent `send`, or None if there has been none.

    From the monotonic clock, so a clock step does not turn a live stream into
    an apparently stale one.
    """


def payload(streams: Iterable[Stream]) -> bytes:
    """Every stream's stats as a JSON object, for the `/info` response.

    `served_at` is here so a caller can measure its own skew against this
    server rather than assume the two clocks agree — the ages are already
    immune to that, and this makes the wall-clock fields usable too.
    """
    return json.encode(
        {"served_at": time.time(), "streams": [stream.stats for stream in streams]}
    )


__all__ = ["INFO_PATH", "Stats", "payload"]
