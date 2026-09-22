"""A consumer that resumes, and the whole of what that costs.

    just demo-consumer

Run several. Stop one with Ctrl-C, leave it stopped while trades keep
arriving, and start it again: it asks for the offset after the last one it
processed and the broker replays the gap out of the log before switching it to
live. The line it prints says which messages were replayed and which arrived
live, because that is the thing worth seeing.

The recovery loop is the four lines around `offset`:

    offset = load()
    async with streamcast.connect(uri, offset=offset) as stream:
        async for offset, message in stream:
            handle(message)

`offset` is reassigned by the loop, so the resume point is simply whatever it
holds — and `+ 1` belongs at the reconnect rather than inside the library,
because only this file knows whether the last message was actually *processed*
or merely received.

A cursor file stands in for whatever a real consumer persists. It is written
after the message is handled, never before: a cursor ahead of the work is a
message skipped for ever, where a cursor behind it is one handled twice.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import websockets

import streamcast


def load(cursor: Path) -> int | None:
    """The offset to resume from, or None for live-only.

    `+ 1` here rather than at save time, so the file always holds an offset
    that was DONE. A crash between handling and saving re-delivers one
    message; the other order loses one.
    """
    if not cursor.exists():
        return None

    return int(cursor.read_text()) + 1


async def run(uri: str, cursor: Path, label: str) -> None:
    offset = load(cursor)
    while True:
        try:
            async with streamcast.connect(uri, offset=offset) as stream:
                replay = stream.info.replay
                behind = 0 if replay is None else replay[1] - replay[0]
                print(
                    f"[{label}] connected at offset {stream.info.end_offset}"
                    + (f", replaying {behind:,} missed" if behind else ", live")
                    + ("" if stream.info.durable else "  (stream is NOT durable)")
                )

                async for offset, row in stream:
                    handle(label, offset, row, replayed=offset < stream.info.end_offset)
                    cursor.write_text(str(offset))

            print(f"[{label}] the broker closed the stream")
            return

        except streamcast.TooSlow as exc:
            # Dropped for falling behind. Not data loss on a durable stream:
            # what arrived is a contiguous prefix, so resuming one above it
            # replays the gap.
            print(f"[{label}] {exc}")
            offset = None if exc.offset is None else exc.offset + 1

        except streamcast.NotReplayable as exc:
            # The one failure a retry cannot fix. Printing `why` rather than
            # just the sentence, because it is what an operator greps for.
            print(f"[{label}] cannot resume ({exc.why}): {exc}")
            return

        except (OSError, websockets.ConnectionClosed) as exc:
            # The ordinary case: the broker restarted, or the network blipped.
            # The cursor is on disk, so the reconnect resumes rather than
            # restarts — which is the difference a log makes.
            print(f"[{label}] {type(exc).__name__}: reconnecting in 1s")
            await asyncio.sleep(1)


def handle(label: str, offset: int, row: dict, *, replayed: bool) -> None:
    """Whatever your consumer actually does. Note there is no parsing here.

    The broker's table is typed, so `row` arrives as columns — the feed
    handler parsed once, at the publisher, rather than every subscriber
    parsing the same frame independently.
    """
    mark = "replay" if replayed else " live "
    print(
        f"[{label}] {mark} {offset:>8,}  {row['price']:>12,.2f}"
        f"  {row['amount']:.8f}  {'sell' if row['side'] else 'buy '}"
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="ws://127.0.0.1:8765/trades")
    parser.add_argument("--label", default="consumer")
    parser.add_argument(
        "--cursor",
        type=Path,
        default=None,
        help="where the resume offset is kept (default: .<label>.offset)",
    )
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="ignore the cursor and replay everything the log still holds",
    )
    args = parser.parse_args()

    cursor = args.cursor or Path(f".{args.label}.offset")
    if args.from_start:
        # `load` adds one to whatever is here, so -1 resolves to EARLIEST (0),
        # which is "everything the log still holds".
        cursor.write_text(str(streamcast.EARLIEST - 1))

    await run(args.uri, cursor, args.label)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped; the cursor is saved — start again to resume")
