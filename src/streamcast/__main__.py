"""The broker, explicit provisioning, probes, inspection and role processes."""

from __future__ import annotations

import sys

from streamcast import _inspect, _probe, _serve
from streamcast._maintain import main as _maintain


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        command, arguments = argv[0], argv[1:]
        if command == "maintain":
            return _maintain(arguments)

        if command in ("serve", "init-registry"):
            return _serve.main(arguments, initialize=command == "init-registry")

        if command == "probe":
            return _probe.main(arguments)

        if command == "inspect":
            return _inspect.main(arguments)

    print(
        "usage: streamcast {serve,init-registry,maintain,probe,inspect} ...",
        file=sys.stderr,
    )
    return 0 if argv == ["--help"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
