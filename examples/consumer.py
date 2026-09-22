"""A consumer that resumes, and the whole of what that costs.

    just demo-consumer

Run several. Stop one with Ctrl-C, leave it stopped while trades keep
arriving, and start it again: it asks for the offset after the last one it
processed and the server replays the gap out of the log before switching it to
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

`cursor=` is the whole of the recovery machinery: pass a path and the offset
is loaded at connect, resumed one above, and saved as the loop runs. Add
`--cursor-uri s3://bucket/consumer1/` and it is shipped to object storage too,
so this consumer can come back on a different machine. It
advances only when the loop comes back for another message and never when the
handler raised — a cursor ahead of the work is a message skipped for ever,
where a cursor behind it is one handled twice.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import websockets

import streamcast


async def run(uri: str, cursor: Path, label: str, cursor_uri: str | None) -> None:
    """The whole recovery story, and `cursor=` is most of it.

    The offset is loaded from the file at connect, resumed one above, and
    saved as the loop goes — so stopping this and starting it again picks up
    where it left off. The hand-rolled version of that was twenty lines here
    and got the save ORDERING subtly right by accident; `streamcast` now
    guarantees it (`_cursor`: the file advances only when the loop comes back
    for another message, and not at all if the handler raised).
    """
    while True:
        try:
            async with streamcast.connect(
                uri, cursor=cursor, cursor_uri=cursor_uri
            ) as stream:
                replay = stream.info.replay
                behind = 0 if replay is None else replay[1] - replay[0]
                print(
                    f"[{label}] connected at offset {stream.info.end_offset}"
                    + (f", replaying {behind:,} missed" if behind else ", live")
                    + ("" if stream.info.durable else "  (stream is NOT durable)")
                )

                async for offset, msg in stream:
                    replayed = (
                        offset is not None
                        and stream.info.end_offset is not None
                        and offset < stream.info.end_offset
                    )
                    handle(label, offset, msg, replayed=replayed)

            print(f"[{label}] the server closed the stream")
            return

        except streamcast.TooSlow as exc:
            # Dropped for falling behind. Not data loss on a durable stream:
            # what arrived is a contiguous prefix, and the cursor holds the
            # last one handled, so reconnecting fills the gap.
            print(f"[{label}] {exc}")

        except streamcast.NotReplayable as exc:
            # The one failure a retry cannot fix. Printing `why` as well as
            # the sentence, because it is what an operator greps for.
            print(f"[{label}] cannot resume ({exc.why}): {exc}")
            return

        except (OSError, websockets.ConnectionClosed) as exc:
            print(f"[{label}] {type(exc).__name__}: reconnecting in 1s")
            await asyncio.sleep(1)


def handle(label: str, offset: int | None, row: dict, *, replayed: bool) -> None:
    """Whatever your consumer actually does. Note there is no parsing here.

    The server's table is typed, so `row` arrives as columns — the feed
    handler parsed once, at the publisher, rather than every subscriber
    parsing the same frame independently.
    """
    mark = "replay" if replayed else " live "
    where = f"{offset:>8,}" if offset is not None else "       -"
    print(
        f"[{label}] {mark} {where}  {row['price']:>12,.2f}"
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
        "--cursor-uri",
        default=None,
        help=(
            "an s3:// prefix to ship the cursor to, so this consumer can "
            "resume on another box after losing this one"
        ),
    )
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="ignore the cursor and replay everything the log still holds",
    )
    args = parser.parse_args()

    cursor = args.cursor or Path(f".{args.label}.offset")
    if args.from_start:
        # The file holds the last offset FINISHED with and a resume asks for
        # one above it, so -1 resolves to EARLIEST (0) — everything the log
        # still holds.
        cursor.write_text(str(streamcast.EARLIEST - 1))

    await run(args.uri, cursor, args.label, args.cursor_uri)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped; the cursor is saved — start again to resume")
