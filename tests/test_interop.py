"""This build against the last releases, client to server and server to client.

Each older end is the real release from PyPI, installed into a virtualenv of
its own beside this checkout, not a fixture of its frames: what a deployment
that upgrades one process at a time runs is a mix of real releases, and a
fixture would only ever agree with this tree's idea of them. The litelink
under every end is this checkout's, so streamcast is the only thing that
differs within a pair.

Every pair runs `tests/interop_peer.py` on both ends, the server in one
process and the client in another: the client publishes, subscribes from
`EARLIEST`, and opens `Stream.snapshot` on the metadata URI its greeting
named, once alone and once with the broker's tail. The server has sealed and
published rows before the client connects, so the snapshot reads the files
the other version wrote, not only rows over the socket.

Installing a release needs uv and its cache, or the network. Without either
the tier SKIPS, which `STREAMCAST_REQUIRE_INTEROP` turns into a failure and
lets the install reach PyPI: CI sets it, as it sets `STREAMCAST_REQUIRE_S3`.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from streamcast import _protocol

PEER = Path(__file__).resolve().parent / "interop_peer.py"

RELEASES = ("0.19.0", "0.20.1")
"""Every release a 0.21 process may meet during an upgrade (CHANGELOG, 0.20.1).

0.20.0 is not here: it reads only protocol 5, so it refuses this build's
server as it refuses a 0.19 one, and it was yanked for that (#135).
"""

SENT = 3
SEALED = 2  # interop_peer.SEALED; the peer is not importable under `-I`.

pytestmark = [pytest.mark.interop, pytest.mark.timeout(180)]


def _install(version: str, venv: Path) -> str | None:
    """A virtualenv with streamcast `version` and this checkout's litelink, or
    why there is none."""
    uv = shutil.which("uv")
    if uv is None:
        return "uv is not installed"

    python = venv / "bin" / "python"
    subprocess.run(
        [uv, "venv", "-q", "--python", sys.executable, str(venv)],
        check=True,
        capture_output=True,
    )
    pins = [
        f"streamcast=={version}",
        f"litelink=={importlib.metadata.version('litelink')}",
    ]
    offline = not os.environ.get("STREAMCAST_REQUIRE_INTEROP")
    done = subprocess.run(
        [uv, "pip", "install", "-q", "--python", str(python), *pins]
        + (["--offline"] if offline else []),
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        return f"cannot install streamcast {version}: {done.stderr.strip()[-400:]}"

    return None


@pytest.fixture(scope="session", params=RELEASES)
def release(request, tmp_path_factory) -> tuple[str, str]:
    """`(version, python)` for one released streamcast, or a skip — a failure
    in CI."""
    version = request.param
    venv = tmp_path_factory.mktemp(f"streamcast-{version}")
    reason = _install(version, venv)
    if reason is not None:
        if os.environ.get("STREAMCAST_REQUIRE_INTEROP"):
            pytest.fail(f"{reason}, and STREAMCAST_REQUIRE_INTEROP is set")

        pytest.skip(f"{reason} (set STREAMCAST_REQUIRE_INTEROP to fetch it)")

    return version, str(venv / "bin" / "python")


def pair(server: str, client: str, root: Path) -> dict:
    """Serve with one interpreter, run the client with the other, and return
    the client's report."""
    process = subprocess.Popen(
        [server, "-I", str(PEER), "server", str(root)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        uri = process.stdout.readline().strip()
        if not uri:
            process.kill()
            _, err = process.communicate()
            pytest.fail(f"the server did not start:\n{err[-2000:]}")

        done = subprocess.run(
            [client, "-I", str(PEER), "client", uri, str(SENT)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert done.returncode == 0, done.stderr[-2000:]
        report = json.loads(done.stdout)
    finally:
        # Closing stdin is the server's signal to stop.
        _, err = process.communicate(timeout=60)

    assert process.returncode == 0, err[-2000:]
    return report


def assert_works(report: dict, *, client: str) -> None:
    """Publish, subscribe and both snapshots succeeded, and each read every row."""
    assert "failed" not in report, report["failed"]
    assert report["version"] == client
    sealed = [1_790_038_800_000_000 - i for i in range(SEALED, 0, -1)]
    sent = [1_790_038_800_000_000 + i for i in range(SENT)]
    assert report["published"] == list(range(SEALED + 1, SEALED + SENT + 1))
    assert report["received"] == [
        [offset, ts] for offset, ts in enumerate(sealed + sent, start=1)
    ]
    assert report["snapshot"] == sealed
    assert report["snapshot_latest"] == sealed + sent


THIS = importlib.metadata.version("streamcast")


class TestThisClient:
    def test_it_reads_an_older_server(self, release, tmp_path):
        version, python = release
        report = pair(python, sys.executable, tmp_path / "data")

        assert_works(report, client=THIS)
        # Both releases serve protocol 4 with the hint beside the plain copy,
        # and this client opens the hint.
        assert report["greeting_version"] == 4
        assert report["metadata"].endswith("/trades.metadata/version-hint.text")


class TestThisServer:
    def test_an_older_client_reads_it(self, release, tmp_path):
        version, python = release
        report = pair(sys.executable, python, tmp_path / "data")

        assert_works(report, client=version)
        assert report["greeting_version"] == _protocol.VERSION == 4
        # 0.19 reads the plain `metadata`, so this server still writes that
        # copy (#135); 0.20.1 reads the hint from `metadata_hint`.
        if version == "0.19.0":
            assert report["metadata"].endswith("/trades/trades.metadata.json")
        else:
            assert report["metadata"].endswith("/trades.metadata/version-hint.text")
