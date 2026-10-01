"""otel-gui, the dashboard `just demo otel` exports to.

    just demo otel    # this, a broker, the services, and the OTLP exporter

[otel-gui](https://github.com/metafab/otel-gui) is a local OTLP receiver
with a dashboard for logs, traces and the service map. The first run
downloads its release for this platform, checks its SHA-256 against the
published one, and caches it. It listens on 127.0.0.1, and Ctrl-C stops it.

It is not one of the example's roles: it is where the subscriber,
`export.py`, sends what it reads, as any OTLP receiver would be.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import os
import platform
import signal
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

VERSION = "2.1.0"
ASSETS = {
    ("Linux", "x86_64"): "otel-gui-linux-x64",
    ("Linux", "aarch64"): "otel-gui-linux-arm64",
    ("Linux", "arm64"): "otel-gui-linux-arm64",
    ("Darwin", "x86_64"): "otel-gui-macos-x64",
    ("Darwin", "arm64"): "otel-gui-macos-arm64",
}


def otel_gui() -> Path:
    """otel-gui's executable, downloaded and checked on first use."""
    asset = ASSETS.get((platform.system(), platform.machine()))
    if asset is None:
        msg = (
            f"no otel-gui build for {platform.system()} {platform.machine()}; "
            "see https://github.com/metafab/otel-gui"
        )
        raise SystemExit(msg)

    cache = (
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
        / "streamcast"
        / f"otel-gui-{VERSION}"
    )
    executable = cache / asset / "otel-gui"
    if os.access(executable, os.X_OK):
        return executable

    print(f"downloading otel-gui {VERSION} ({asset})", flush=True)
    cache.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/metafab/otel-gui/releases/download/v{VERSION}/{asset}.tar.gz"
    tarball = cache / f"{asset}.tar.gz"
    urllib.request.urlretrieve(url, tarball)  # noqa: S310 — a fixed https URL
    with urllib.request.urlopen(f"{url}.sha256") as published:  # noqa: S310
        expected = published.read().split()[0].decode()

    actual = hashlib.sha256(tarball.read_bytes()).hexdigest()
    if actual != expected:
        tarball.unlink()
        msg = f"otel-gui {asset}.tar.gz: SHA-256 {actual}, published {expected}"
        raise SystemExit(msg)

    with tarfile.open(tarball) as archive:
        archive.extractall(cache, filter="data")

    return executable


def ready(port: int, gui: subprocess.Popen[bytes], log: Path) -> None:
    """Wait until otel-gui answers, or say why it never will."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if gui.poll() is not None:
            msg = f"otel-gui exited ({gui.returncode}):\n{log.read_text()}"
            raise SystemExit(msg)

        with contextlib.suppress(urllib.error.URLError, ConnectionError):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1).close()  # noqa: S310
            return

        time.sleep(0.1)

    msg = f"otel-gui did not answer on port {port} within 10s; see {log}"
    raise SystemExit(msg)


def warm(port: int) -> None:
    """Have otel-gui load its trace and logs decoders, one after the other.

    otel-gui (2.1.0, and 3.0.0 unchanged) loads its trace and logs `.proto`
    files lazily into one shared protobufjs Root, on the first request to
    each. The exporter sends both at once, the two loads interleave, one
    resolves before `resource.proto` is parsed, and the throw escapes into a
    callback and kills the dashboard. An empty request to each, in turn, does
    the loading before anything can race it.
    """
    for signal_name in ("traces", "logs"):
        request = urllib.request.Request(  # noqa: S310
            f"http://127.0.0.1:{port}/v1/{signal_name}",
            data=b"",
            headers={"Content-Type": "application/x-protobuf"},
            method="POST",
        )
        urllib.request.urlopen(request, timeout=5).close()  # noqa: S310


def main(host: str, port: int) -> None:
    executable = otel_gui()
    log_path = Path(tempfile.mkstemp(prefix="otel-gui-", suffix=".log")[1])
    with log_path.open("w") as log:
        # HOST and SHUTDOWN_TIMEOUT are not in otel-gui's README, but its
        # server honours both (SvelteKit's node adapter). 127.0.0.1 keeps the
        # dashboard off the network. On a signal it waits SHUTDOWN_TIMEOUT
        # seconds (30 by default) for open connections to close, and an open
        # dashboard's live stream never does: 1 lets Ctrl-C return at once.
        gui = subprocess.Popen(  # noqa: S603
            [str(executable)],
            env={
                **os.environ,
                "HOST": host,
                "PORT": str(port),
                "SHUTDOWN_TIMEOUT": "1",
            },
            stdout=log,
            stderr=subprocess.STDOUT,
            # Its own session, so a terminal's Ctrl-C reaches only this
            # process, and the `finally` below stops it.
            start_new_session=True,
        )
        try:
            ready(port, gui, log_path)
            warm(port)
            print(
                f"dashboard: http://{host}:{port}   (its log: {log_path})", flush=True
            )
            gui.wait()
        except KeyboardInterrupt:
            pass
        finally:
            gui.terminate()
            try:
                gui.wait(timeout=5)
            except subprocess.TimeoutExpired:
                gui.kill()
                gui.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--host", default="127.0.0.1", help="where the dashboard listens"
    )
    parser.add_argument(
        "--port", type=int, default=4318, help="OTLP/HTTP's standard port"
    )
    arguments = parser.parse_args()
    # SIGTERM stops it as Ctrl-C does, so every exit stops otel-gui too.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    main(arguments.host, arguments.port)
