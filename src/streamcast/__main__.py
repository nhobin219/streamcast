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
        "usage: python -m streamcast maintain --log PATH NAME [--log PATH NAME ...]\n"
        "\n"
        "Sweeps streamcast's litelink logs: seals each buffer into Parquet,\n"
        "then compacts, evicts and expires. `streamcast.serve(maintain=True)`\n"
        "starts ONE of these covering every log it serves and stops it on\n"
        "close, so running it by hand is for a deployment that passed\n"
        "`maintain=False`. Repeat --log to sweep several.",
        file=sys.stderr,
    )

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
