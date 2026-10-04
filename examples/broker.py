"""A broker: streams declared by their schemas, served, and nothing else.

    uv run python -m examples.broker --stream trades=examples/trades/schema.json

Every example has the same three roles, and this is the middle one. A
**producer** is a client that publishes rows (`streamcast.publish`); a
**subscriber** is a client that reads them (`streamcast.connect`); the broker
between them holds each stream's log and fans rows out. It has no application
code: what a row means is the producer's business, and what to do with it is
the subscriber's.

`--stream NAME=SCHEMA` declares one stream. SCHEMA is a JSON Schema file, or
`module:ATTRIBUTE` for one built in Python. The broker creates each stream's
log under `--root` the first time and opens it every time after, so a
restarted broker carries on from the offset it stopped at. `--no-log` serves
live-only streams: no durable tier, and `?offset=` is refused.

`serve` starts the maintainer that seals each log, and litestream if a log
replicates its WAL. There is nothing else to run.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import json
import signal
from pathlib import Path
from typing import Any

import streamcast


def schema(source: str) -> dict[str, Any]:
    """A JSON Schema file, or `module:ATTRIBUTE` naming a dict in Python."""
    if source.endswith(".json"):
        return json.loads(Path(source).read_text())

    module, _, attribute = source.partition(":")
    return getattr(importlib.import_module(module), attribute)


async def report(streams: list[streamcast.Stream]) -> None:
    """A line every five seconds per stream that moved, and nothing otherwise."""
    last = {stream.name: stream.end_offset for stream in streams}
    while True:
        await asyncio.sleep(5)
        for stream in streams:
            now = stream.end_offset
            if now is None:  # live-only: nothing assigns offsets
                print(f"{stream.name}: live-only, {stream.subscribers} subscriber(s)")
            elif now != last[stream.name]:
                moved = now - (last[stream.name] or now)
                print(
                    f"{stream.name}: offset {now - 1:,} (+{moved} in 5s), "
                    f"{stream.subscribers} subscriber(s)"
                )

            last[stream.name] = now


async def main(
    declared: list[str],
    sort_by: dict[str, str],
    root: Path,
    host: str,
    port: int,
    *,
    live: bool,
) -> None:
    streams = []
    for declaration in declared:
        name, _, source = declaration.partition("=")
        if live:
            streams.append(streamcast.Stream(name, schema=schema(source)))
        else:
            # `sort_by` on a leading time column is what lets a bounded query
            # on it prune whole files.
            order = (sort_by[name],) if name in sort_by else None
            streams.append(
                streamcast.Stream.new(
                    name, root=root, schema=schema(source), sort_by=order
                )
            )

    # Producers are clients, and publish over the socket.
    async with streamcast.serve(streams, host, port):
        for stream in streams:
            where = "live-only" if live else f"durable, next offset {stream.end_offset}"
            print(f"ws://{host}:{port}/{stream.name}  ({where})", flush=True)

        await report(streams)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--stream",
        action="append",
        required=True,
        metavar="NAME=SCHEMA",
        help="a stream, and its JSON Schema file or module:ATTRIBUTE",
    )
    parser.add_argument(
        "--sort-by",
        action="append",
        default=[],
        metavar="NAME=COLUMN",
        help="the column a stream's log is sorted by",
    )
    parser.add_argument("--root", type=Path, default=Path("streamcast-data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-log", action="store_true", help="live-only streams")
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(
            main(
                arguments.stream,
                dict(entry.split("=", 1) for entry in arguments.sort_by),
                arguments.root,
                arguments.host,
                arguments.port,
                live=arguments.no_log,
            )
        )
