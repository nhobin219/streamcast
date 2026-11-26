"""File-backed control authority; requests without it retain public access."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import secrets
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from streamcast._broker.model import PublicAccess

_PUBLIC = PublicAccess()


@dataclass(frozen=True)
class TokenFileAccess:
    """A token digest read at startup, with public data and stats access."""

    _digest: bytes = field(repr=False)

    @classmethod
    def from_env(cls) -> TokenFileAccess:
        """Read the token file named by STREAMCAST_CONTROL_TOKEN_FILE."""
        path = Path(
            os.environ.get(
                "STREAMCAST_CONTROL_TOKEN_FILE", "/run/streamcast-control/token"
            )
        )
        try:
            with path.open("rb") as token_file:
                # Inspect the opened file, so permissions and bytes belong to
                # the same inode even if its directory entry changes.
                if os.fstat(token_file.fileno()).st_mode & 0o077:
                    raise ValueError("unsafe permissions")

                token = token_file.read()
                if not token:
                    raise ValueError("empty token")

        except (OSError, ValueError):
            raise ValueError("unavailable or unsafe control token file") from None

        return cls(hashlib.sha256(token).digest())

    async def authenticate(self, scope: Mapping[str, Any]) -> str:
        headers = [
            value
            for name, value in scope.get("headers", ())
            if name.lower() == b"authorization"
        ]
        if len(headers) == 1:
            scheme, separator, token = headers[0].partition(b" ")
            if (
                separator
                and scheme.lower() == b"bearer"
                and hmac.compare_digest(hashlib.sha256(token).digest(), self._digest)
            ):
                return "operator"

        return await _PUBLIC.authenticate(scope)

    async def allows(self, principal: str, action: str, resource: str) -> bool:
        return principal == "operator" or await _PUBLIC.allows(
            principal, action, resource
        )


def create(path: Path) -> None:
    """Create a private token once; an existing file belongs to its owner."""
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return

    with os.fdopen(descriptor, "wb") as token_file:
        os.fchmod(token_file.fileno(), 0o600)
        token_file.write(secrets.token_urlsafe(32).encode("ascii"))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="streamcast token-file")
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("create")
    command.add_argument("path", type=Path)
    args = parser.parse_args(argv)
    try:
        create(args.path)
    except OSError:
        print("unable to create control token file", file=sys.stderr)
        return 2

    return 0
