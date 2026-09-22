"""`python -m streamcast <command>` — the out-of-process half of the library.

One command today. It exists as a package entry point rather than as
`python -m streamcast._maintain` because that form re-executes a module the
parent has already imported — `__init__` reaches `_maintain` through `_server`
— and CPython warns about it:

    RuntimeWarning: 'streamcast._maintain' found in sys.modules after import
    of package 'streamcast', but prior to execution

Benign here, since that module is constants and functions, but a warning on
every maintainer start is noise in exactly the logs an operator reads when
something is wrong. `__main__` is imported once, by nothing else.
"""

from __future__ import annotations

import sys

from streamcast._maintain import main as _maintain


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "maintain":
        return _maintain(argv[1:])

    print(
        "usage: python -m streamcast maintain --root PATH --name NAME\n"
        "\n"
        "Sweeps a stream's litelink log: seals the buffer into Parquet, then\n"
        "compacts, evicts and expires. `streamcast.serve(maintain=True)`\n"
        "starts one of these per stream and stops it on close, so running it\n"
        "by hand is for a deployment that passed `maintain=False`.",
        file=sys.stderr,
    )

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
