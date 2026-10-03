"""Every demo, by name, with all of its processes: `just demo NAME`.

    just demo                        # every demo, and what it shows
    just demo trades                 # a broker, the trades producer, a consumer
    just demo consumer --label b     # one more consumer, in another terminal
    just demo otel                   # OpenTelemetry, live in a dashboard

Every example has the same three roles, each its own process: a **producer**
client publishes rows, a **broker** serves the streams, and a **subscriber**
client reads them. A demo here is the list of those processes. This runs
them in order, waiting for each one that listens to answer before starting
the next, prints their output in one terminal with each line labelled by its
role, and stops them all, last started first, on Ctrl-C.

Each process is a module with its own `--help`, so any one of them runs on its
own with `uv run python -m`. `just demo NAME ARGS` passes ARGS to the demo's
last process, its subscriber. With no NAME, or `--help`, it lists the demos.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Process:
    role: str
    argv: list[str]
    port: int | None = None  # wait for this to answer before starting the next
    options: bool = True  # whether its own --help describes what ARGS can be


@dataclass(frozen=True)
class Demo:
    about: str
    processes: list[Process] = field(default_factory=list)


def module(name: str, *args: str) -> list[str]:
    return [sys.executable, "-u", "-m", name, *args]


TRADES = "trades=examples/trades/schema.json"


def trades(
    *broker: str, port: int = 8765, uri: str = "ws://127.0.0.1:8765/trades"
) -> list[Process]:
    """The trades demo's three processes: broker, producer, consumer."""
    return [
        Process("broker", module("examples.broker", "--stream", TRADES, *broker), port),
        Process("producer", module("examples.trades.producer", "--uri", uri)),
        Process(
            "consumer",
            module("examples.trades.consumer", "--uri", uri, "--label", "one"),
        ),
    ]


DEMOS: dict[str, Demo] = {
    "trades": Demo(
        "Bitstamp's BTC/USD trades: a producer, a broker with a log, a consumer",
        trades("--sort-by", "trades=event_ts"),
    ),
    "consumer": Demo(
        "one more consumer of the trades demo: stop it, restart it, watch it replay",
        [Process("consumer", module("examples.trades.consumer"))],
    ),
    "live": Demo(
        "the trades demo with a live-only broker: no log, nothing to replay",
        trades("--no-log"),
    ),
    "fastapi": Demo(
        "the trades demo with the broker mounted in a FastAPI app",
        [
            Process(
                "broker",
                module("uvicorn", "examples.fastapi_app:app", "--port", "8770"),
                8770,
            ),
            *trades(uri="ws://127.0.0.1:8770/streams/trades")[1:],
        ],
    ),
    "book": Demo(
        "Bitstamp's live order book as a keyed table log, kept by a browser page",
        [
            Process(
                "broker",
                module(
                    "examples.broker",
                    "--stream",
                    "orders=examples/book/schema.json",
                    "--port",
                    "8767",
                    "--root",
                    "streamcast-book",
                ),
                8767,
            ),
            Process("producer", module("examples.book.producer")),
            Process(
                "page",
                module(
                    "http.server",
                    "8768",
                    "--bind",
                    "127.0.0.1",
                    "--directory",
                    "examples/book",
                ),
                8768,
                options=False,
            ),
        ],
    ),
    "otel": Demo(
        "OpenTelemetry logs, traces and metrics through streams, in otel-gui",
        [
            Process("otel-gui", module("examples.otel.gui"), 4318),
            Process(
                "broker",
                module(
                    "examples.broker",
                    "--stream",
                    "logs=examples.otel.logs:SCHEMA",
                    "--stream",
                    "spans=examples.otel.spans:SCHEMA",
                    "--stream",
                    "metrics=examples.otel.metrics:SCHEMA",
                    "--port",
                    "8766",
                    "--root",
                    "streamcast-otel",
                ),
                8766,
            ),
            Process("producer", module("examples.otel.services")),
            # Real-time analytics over `/spans` with `Stream.live`, printed on
            # an interval. Before the exporter, which stays last: the last
            # process is the one whose options `just demo otel --help` shows.
            Process("analytics", module("examples.otel.analytics")),
            Process("exporter", module("examples.otel.export")),
        ],
    ),
}
HINTS = {
    "book": "open http://127.0.0.1:8768/",
    "otel": "open http://127.0.0.1:4318/",
}
STORED = ("streamcast-data", "streamcast-book", "streamcast-otel")


def options(name: str, demo: Demo) -> None:
    """What `just demo NAME ARGS` can pass: the subscriber's own options."""
    subscriber = demo.processes[-1]
    if not subscriber.options:
        print(f"just demo {name}: its {subscriber.role} takes no arguments")
        return

    print(f"just demo {name} [ARGS]: ARGS go to its {subscriber.role}\n", flush=True)
    subprocess.run([*subscriber.argv, "--help"], check=False)  # noqa: S603


def listing() -> str:
    width = max(len(name) for name in DEMOS) + 2
    lines = [f"  {name:<{width}}{demo.about}" for name, demo in DEMOS.items()]
    lines.append(f"  {'clean':<{width}}delete what the demos stored")
    return "just demo NAME [ARGS]   (ARGS go to the demo's subscriber)\n\n" + "\n".join(
        lines
    )


def clean() -> None:
    for root in map(Path, STORED):
        if root.exists():
            print(f"removing {root}")
            shutil.rmtree(root)


def listening(port: int) -> bool:
    """Whether something already accepts connections on `port`."""
    with socket.socket() as probe:
        probe.settimeout(0.5)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def answering(port: int, process: subprocess.Popen[str], timeout: float = 30) -> bool:
    """Whether `port` accepts a connection before `process` exits or time runs out."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and process.poll() is None:
        if listening(port):
            return True

        time.sleep(0.1)

    return False


def labelled(role: str, width: int, process: subprocess.Popen[str]) -> None:
    """Copy a process's output to ours, each line led by its role."""
    assert process.stdout is not None
    for line in process.stdout:
        print(f"[{role:>{width}}] {line}", end="", flush=True)


def run(demo: Demo, args: list[str]) -> None:
    width = max(len(step.role) for step in demo.processes)
    started: list[subprocess.Popen[str]] = []
    try:
        for index, step in enumerate(demo.processes):
            extra = args if index == len(demo.processes) - 1 else []
            if step.port is not None and listening(step.port):
                print(
                    f"port {step.port} is already in use, so the {step.role} "
                    "cannot listen there; stop whatever holds it",
                    file=sys.stderr,
                )
                return

            process = subprocess.Popen(  # noqa: S603
                [*step.argv, *extra],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                # Its own session, so a terminal's Ctrl-C reaches only this
                # runner, which stops each process in order below.
                start_new_session=True,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            started.append(process)
            threading.Thread(
                target=labelled, args=(step.role, width, process), daemon=True
            ).start()
            if step.port is not None and not answering(step.port, process):
                print(f"{step.role} did not start; stopping", file=sys.stderr)
                return

        for process in started:
            process.wait()

    except KeyboardInterrupt:
        pass

    finally:
        # Last started first: subscribers, then producers, then the broker.
        # SIGINT, as a terminal's Ctrl-C would send: `asyncio.run` turns it
        # into a clean cancellation, where SIGTERM interrupts mid-call.
        for process in reversed(started):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def main(argv: list[str]) -> None:
    if not argv or argv[0] in ("-h", "--help"):
        print(listing())
        return

    name, args = argv[0], argv[1:]
    if name == "clean":
        clean()
        return

    if name not in DEMOS:
        print(f"no demo called {name!r}; name one\n\n{listing()}", file=sys.stderr)
        raise SystemExit(2)

    if "-h" in args or "--help" in args:
        options(name, DEMOS[name])
        return

    if name in HINTS:
        print(HINTS[name], flush=True)

    # SIGTERM stops a demo as Ctrl-C does: every process, in order.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    run(DEMOS[name], args)


if __name__ == "__main__":
    main(sys.argv[1:])
