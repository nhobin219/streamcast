"""One end of a cross-version pair, run by `tests/test_interop.py`.

The same file runs under this checkout and under an older streamcast installed
from PyPI into a virtualenv of its own. The factory dispatches to that version's
API; `serve`, `publish`, `connect` and `Stream.snapshot` are shared. It imports nothing from `tests`, and is run with `python -I`
so the interpreter's own streamcast is the one it imports, never this tree's.

    python -I interop_peer.py server ROOT     prints the URI, serves until stdin closes
    python -I interop_peer.py client URI N    publishes, subscribes, snapshots; prints JSON

Every failure in the client is caught and printed as its type and message, so
the test can say which step a pair broke at and how.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import sys

import streamcast

SEALED = 2
"""Rows the server has sealed and published before any client connects."""

SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": ["number", "null"]},
    },
    "required": ["event_ts"],
}


def row(i: int) -> dict[str, object]:
    return {"event_ts": 1_790_038_800_000_000 + i, "price": float(i)}


async def server(root: str) -> None:
    # SEALED rows in the published table before serving, so a snapshot reads
    # this version's metadata, manifest and Parquet, not only the broker's tail.
    # The helper also runs under releases with the pre-0.24 factory. This is
    # test-only version dispatch; the library exposes just `open`.
    factory = getattr(streamcast.Stream, "open", None)
    if factory is None:
        old_factory = getattr(streamcast.Stream, "new", None)
        assert old_factory is not None
        stream = old_factory("trades", root=root, schema=SCHEMA)
    else:
        stream = factory("trades", create=True, root=root, schema=SCHEMA)

    await stream.send_many([row(-i) for i in range(SEALED, 0, -1)])
    assert stream.log is not None
    while stream.log.seal(flush=True) is not None:
        pass

    stream.log.publish(flush=True)
    # No maintainer: nothing here outgrows a seal, and its five subprocesses
    # would cost seconds per pair for nothing the pair is about.
    async with streamcast.serve(stream, "127.0.0.1", 0, maintain=False) as served:
        port = served.sockets[0].getsockname()[1]
        print(f"ws://127.0.0.1:{port}/trades", flush=True)
        # Until the test closes our stdin: no signal handling to get wrong.
        await asyncio.get_running_loop().run_in_executor(None, sys.stdin.read)


async def client(uri: str, n: int) -> dict[str, object]:
    result: dict[str, object] = {"version": importlib.metadata.version("streamcast")}
    step = "publish"
    try:
        async with streamcast.publish(uri) as producer:
            published = [await producer.send(row(i)) for i in range(n)]

        result["published"] = published

        step = "subscribe"
        async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
            result["greeting_version"] = sub.info.version
            result["metadata"] = sub.info.metadata
            # Everything up to the last row this client published, whatever
            # the stream held before it.
            last = published[-1]
            assert last is not None
            received: list[tuple[int, object]] = []
            while not received or received[-1][0] < last:
                offset, _ts, message = await asyncio.wait_for(sub.recv(), 10)
                # Every row of a durable stream has one; None would hang here.
                assert offset is not None
                received.append((offset, message["event_ts"]))

        result["received"] = received

        # The metadata URI the greeting gave, read as this version reads it:
        # first the published table alone, then with the broker's tail.
        step = "snapshot"
        metadata = sub.info.metadata
        assert metadata is not None
        async with await streamcast.Stream.snapshot(metadata) as snap:
            table = await snap.scan().read_all()

        result["snapshot"] = table.column("event_ts").to_pylist()

        step = "snapshot through the broker"
        async with await streamcast.Stream.snapshot(
            metadata, as_of_offset=streamcast.LATEST, broker=uri
        ) as snap:
            table = await snap.scan().read_all()

        result["snapshot_latest"] = table.column("event_ts").to_pylist()
    except Exception as exc:  # noqa: BLE001 — reported to the test, which judges it
        result["failed"] = {"step": step, "type": type(exc).__name__, "error": str(exc)}

    return result


def main() -> None:
    match sys.argv[1:]:
        case ["server", root]:
            asyncio.run(server(root))
        case ["client", uri, n]:
            print(json.dumps(asyncio.run(client(uri, int(n)))), flush=True)
        case _:
            sys.exit(__doc__)


if __name__ == "__main__":
    main()
