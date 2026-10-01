"""Every demo, by name.

    just demo                        # a live public feed through a server
    just demo consumer --label a     # a resuming subscriber, in another terminal
    just demo otel                   # OpenTelemetry, live in a dashboard
    just demo --list                 # every demo, and what it shows

`just demo NAME ARGS` is `uv run python -m examples NAME ARGS`. Each demo is a
module with its own `--help`; this file only maps names to them. Arguments
that start with `-` and no name go to the default, so `just demo --no-log`
still means the server.
"""

from __future__ import annotations

import runpy
import shutil
import sys
from pathlib import Path

DEFAULT = "server"

# name: (module, the arguments it always gets, what it shows)
DEMOS: dict[str, tuple[str, list[str], str]] = {
    "server": (
        "examples.server",
        [],
        "a live public feed (Bitstamp BTC/USD) through a server, with a log",
    ),
    "consumer": (
        "examples.consumer",
        [],
        "a subscriber that resumes where it stopped; run several",
    ),
    "live": (
        "examples.server",
        ["--no-log"],
        "the same server with no log: live-only, nothing to replay",
    ),
    "fastapi": (
        "uvicorn",
        ["examples.fastapi_app:app", "--port", "8000"],
        "the same stream mounted in a FastAPI app",
    ),
    "book": (
        "examples.keyed_table.book",
        [],
        "Bitstamp's live order book as a keyed table log, kept by a browser page",
    ),
    "otel": (
        "examples.otel.dashboard",
        [],
        "OpenTelemetry logs and traces through streams, in otel-gui",
    ),
}


def listing() -> str:
    width = max(len(name) for name in DEMOS) + 2
    lines = [f"  {name:<{width}}{about}" for name, (_, _, about) in DEMOS.items()]
    lines.append(f"  {'clean':<{width}}delete what the server demo captured (--root)")
    return (
        "just demo [NAME] [ARGS]   (no NAME: server; NAME --help for its options)\n\n"
        + "\n".join(lines)
    )


def clean(args: list[str]) -> None:
    root = Path(
        args[args.index("--root") + 1] if "--root" in args else "streamcast-data"
    )
    if not root.exists():
        print(f"nothing at {root}")
        return

    print(f"removing {root}")
    shutil.rmtree(root)


def main(argv: list[str]) -> None:
    if argv[:1] in (["--list"], ["-l"], ["list"]):
        print(listing())
        return

    if not argv or argv[0].startswith("-"):
        argv = [DEFAULT, *argv]

    name, args = argv[0], argv[1:]

    if name == "clean":
        clean(args)
        return

    if name not in DEMOS:
        print(f"no demo called {name!r}\n\n{listing()}", file=sys.stderr)
        raise SystemExit(2)

    module, fixed, _ = DEMOS[name]
    sys.argv = [module, *fixed, *args]
    runpy.run_module(module, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main(sys.argv[1:])
