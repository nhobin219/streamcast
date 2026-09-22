"""What the fan-out costs, and where each part of it stops being free.

    just bench
    just bench --subscribers 500 --messages 20000

Three numbers, because three different things could be the bottleneck and
only one of them is streamcast's:

**encode**  a row to its JSON frame, with msgspec. Done ONCE per message
however many subscribers there are, which is the reason `Stream.send` takes a
row and the queue holds a shared `bytes`. If this scaled with subscriber
count, the library's central claim would be wrong. The stdlib `json` figure is
printed beside it, because that gap is what earns the dependency.

**fan-out** the queue insert per subscriber, with no sockets — the part
`Stream.send` actually spends. Linear in subscribers by construction; the
question is the constant.

**durable** the same send with a litelink log attached. One SQLite
transaction at `synchronous=FULL`, and it dominates everything above it by
two orders of magnitude — which is the whole argument for `send_many`.

Numbers move with hardware. Measure before and after in the same session on
the same machine; a comparison across two runs on two boxes says nothing.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import tempfile
import time
from pathlib import Path

import litelink
import pyarrow as pa

import streamcast
from streamcast._protocol import encode
from streamcast._subscriber import Subscriber

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        pa.field("side", pa.int64()),
        pa.field("tag", pa.string()),
    ]
)
COLUMNS = tuple(SCHEMA.names)


class _Sink:
    """A connection that is never written to. `Subscriber` only needs these."""

    async def send(self, frame: bytes) -> None: ...

    async def close(self, code: int, reason: str) -> None: ...

    async def wait_closed(self) -> None: ...


def _rate(seconds: float, count: int) -> str:
    return f"{count / seconds:>12,.0f}/s" if seconds > 0 else f"{'inf':>12}"


def _report(label: str, seconds: float, count: int, each: str = "") -> None:
    per = seconds / count * 1e6
    print(f"  {label:<22} {_rate(seconds, count)}   {per:>8.2f} us{each}")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=50_000)
    parser.add_argument("--subscribers", type=int, default=100)
    # A realistic trade row is mostly numbers with a short tag. The default
    # is small on purpose: msgspec's advantage is largest on numeric columns
    # and shrinks as one string column comes to dominate the frame — at
    # --payload 200 the ratio is ~1.7x, at 12 it is over 3x, and on a purely
    # numeric row it is 20x. Raise it to see your own shape.
    parser.add_argument(
        "--payload", type=int, default=12, help="bytes in the tag column"
    )
    parser.add_argument("--durable", type=int, default=2_000, help="messages, logged")
    args = parser.parse_args()

    row = {
        "event_ts": 1_790_038_800_123_456,
        "price": 85_565.0,
        "amount": 0.015,
        "side": 0,
        "tag": "x" * args.payload,
    }
    print(
        f"{args.messages:,} rows, {args.payload} B in the string column, "
        f"{args.subscribers} subscribers\n"
    )

    # --- encode: once per message, whatever the subscriber count ------------
    started = time.perf_counter()
    for offset in range(args.messages):
        encode(offset, row, COLUMNS)

    _report("encode (msgspec)", time.perf_counter() - started, args.messages)

    # The same frame through the stdlib, for the ratio that justifies msgspec.
    import json

    started = time.perf_counter()
    for offset in range(args.messages):
        json.dumps({"litelink_offset": offset, **row}).encode()

    _report("encode (stdlib json)", time.perf_counter() - started, args.messages)

    # --- fan-out: the queue insert per subscriber, no sockets ---------------
    for count in sorted({1, 10, args.subscribers}):
        stream = streamcast.Stream("bench", max_backlog=args.messages + 1)
        subscribers = [
            Subscriber(_Sink(), max_backlog=args.messages + 1)  # ty: ignore[invalid-argument-type]
            for _ in range(count)
        ]
        for subscriber in subscribers:
            stream._subscribers.add(subscriber)  # noqa: SLF001 — a benchmark, not a caller

        started = time.perf_counter()
        for _ in range(args.messages):
            await stream.send(row)

        elapsed = time.perf_counter() - started
        _report(f"send, {count} subscriber(s)", elapsed, args.messages)

    # --- durable: the same send, with the log in it -------------------------
    root = Path(tempfile.mkdtemp())
    with litelink.new(root, "bench", schema=SCHEMA, sort_by=("event_ts",)) as log:
        stream = streamcast.Stream("bench", log=log)
        started = time.perf_counter()
        for _ in range(args.durable):
            await stream.send(row)

        one_at_a_time = time.perf_counter() - started
        _report("send, durable", one_at_a_time, args.durable)

        # Every size commits the SAME number of messages, so the column is a
        # comparison rather than three differently-shaped runs. An earlier
        # version divided a fixed budget by the batch size, which gave the
        # largest batch a single round and reported its cold-start as its
        # steady state — 211 us per message against 10 for a smaller batch,
        # which is backwards and was the measurement, not the library.
        for size in (100, 500):
            batch = [row] * size
            rounds = max(1, args.durable // size)
            started = time.perf_counter()
            for _ in range(rounds):
                await stream.send_many(batch)

            elapsed = time.perf_counter() - started
            _report(f"send_many({size}), durable", elapsed, rounds * size)

    print(
        f"\n  the durable path is {one_at_a_time / args.durable * 1e6:,.0f} us per "
        f"message, one fsync each.\n"
        f"  everything above it is memory. that gap is why `send_many` exists."
    )

    # --- end to end, over a real socket -------------------------------------
    stream = streamcast.Stream("bench")
    server = await streamcast.serve(stream, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    uri = f"ws://127.0.0.1:{port}/bench"
    count = min(args.subscribers, 50)
    subs = [await streamcast.connect(uri) for _ in range(count)]
    try:
        total = min(args.messages, 5_000)

        async def read(subscription):
            latencies = []
            for _ in range(total):
                _offset, received = await subscription.recv()
                latencies.append(time.perf_counter_ns() - int(received["tag"]))

            return latencies

        readers = [asyncio.create_task(read(sub)) for sub in subs]
        await asyncio.sleep(0.05)

        started = time.perf_counter()
        for _ in range(total):
            # The offset is not the clock, so a column carries the send time
            # instead: what this measures is queue insert to consumer `recv`,
            # across a real loopback socket.
            await stream.send({**row, "tag": str(time.perf_counter_ns())})
            await asyncio.sleep(0)

        gathered = await asyncio.gather(*readers)
        elapsed = time.perf_counter() - started

    finally:
        await asyncio.gather(*(sub.close() for sub in subs))
        server.close()
        await server.wait_closed()

    print(f"\n{total:,} messages to {count} live subscribers over loopback\n")
    _report("delivered", elapsed, total * count, "  per delivery")
    _latency(gathered)


def _latency(gathered: list[list[int]]) -> None:
    """Send to `recv`, over loopback, across every subscriber.

    p99 rather than a mean, because the interesting failure is a tail: a
    fan-out that is fast on average and occasionally parks is a fan-out that
    is awaiting something it should not be.
    """
    flat = [value / 1e6 for run in gathered for value in run]
    print(
        f"  latency p50 {statistics.median(flat):.2f} ms  "
        f"p99 {statistics.quantiles(flat, n=100)[98]:.2f} ms"
    )


if __name__ == "__main__":
    asyncio.run(main())
