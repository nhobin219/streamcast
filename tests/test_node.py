"""A JavaScript client — the browser's `WebSocket` API — reads and writes a stream.

Node's built-in `WebSocket` is the WHATWG API every browser exposes, so this is
what front-end code does: open a socket, `JSON.parse(event.data)`, and read
each row with the reader docs/SPEC.md §2 prints — taken from the SPEC
verbatim, so the documented code cannot drift from what works.

It checks the three things a JS consumer depends on and Python tests cannot
see: every frame is a TEXT frame (a browser hands a binary one over as a
`Blob`, which `JSON.parse` cannot read), binary columns decode from their
text encodings at every depth, and a JSON publisher's text round-trips into
stored bytes.

Needs Node 22 or later, where `WebSocket` is global. Skips without it, which
`STREAMCAST_REQUIRE_NODE` turns into a failure — CI sets it, as it sets
`STREAMCAST_REQUIRE_S3`, so a missing runtime cannot pass by skipping.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import streamcast
from tests.test_types import SCHEMA, TRACE, row

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "tests" / "js" / "client.mjs"
SPEC = ROOT / "docs" / "SPEC.md"


@pytest.fixture(scope="module")
def node() -> str:
    """The `node` binary, 22 or later, or a skip — a failure in CI."""
    found = os.environ.get("STREAMCAST_NODE") or shutil.which("node")
    reason = None
    if found is None:
        reason = "node is not installed"
    else:
        version = subprocess.run(
            [found, "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
        if int(version.lstrip("v").split(".")[0]) < 22:
            reason = f"node {version} has no global WebSocket; 22 or later is needed"

    if reason is not None:
        if os.environ.get("STREAMCAST_REQUIRE_NODE"):
            pytest.fail(f"{reason}, and STREAMCAST_REQUIRE_NODE is set")

        pytest.skip(reason)

    assert found is not None
    return found


def plain(value: object) -> object:
    """A row as the JS client prints it: bytes as lists of integers."""
    if isinstance(value, bytes):
        return list(value)

    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}

    if isinstance(value, list):
        return [plain(item) for item in value]

    return value


async def test_a_browser_websocket_reads_and_writes_a_stream(tmp_path, serve, node):
    stream = streamcast.Stream.new("otel", root=tmp_path, schema=SCHEMA)
    await stream.send_many([row(1), row(2)])

    published = row(3)
    # What a JSON publisher sends: binary as text in each column's encoding.
    wire = {
        **published,
        "trace_id": TRACE.hex(),
        "blob": "AP8D",  # base64 of 00 ff 03
        "nested": {"m": {"b": "0102"}},
    }

    async with serve(stream, publish=True, maintain=False) as uri:
        process = await asyncio.create_subprocess_exec(
            node,
            str(CLIENT),
            uri,
            str(SPEC),
            json.dumps(wire),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(process.communicate(), timeout=60)

    assert process.returncode == 0, f"node failed: {out.decode()} {err.decode()}"
    seen = json.loads(out)

    assert seen["textFrames"] is True, "a browser gets a Blob for a binary frame"
    assert seen["ack"] == {"ok": [3]}
    # `[offset, ts, msg]`: the stamp is the server's, beside the row.
    stamps = [ts for _offset, ts, _msg in seen["rows"]]
    assert all(isinstance(ts, int) for ts in stamps), stamps
    assert stamps == sorted(stamps)
    assert [[offset, msg] for offset, _ts, msg in seen["rows"]] == [
        [1, plain(row(1))],
        [2, plain(row(2))],
        [3, plain(published)],
    ]
