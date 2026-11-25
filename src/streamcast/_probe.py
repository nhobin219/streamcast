"""Small process probes: no storage opens, writes, or credential diagnostics."""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen

from streamcast._maintain import ROLES
from streamcast._spec import NAME


def role_ready(run_dir: Path, role: str, group: str) -> bool:
    if role not in ROLES or NAME.fullmatch(group) is None:
        return False

    try:
        plan = json.loads((run_dir / "plan.json").read_text())
        status = json.loads((run_dir / "status" / f"{role}.{group}.json").read_text())
        every = float(plan["roles"][f"{role}.{group}"]["every"])
        age = time.time() - float(status["heartbeat_at"])
        return (
            plan["format"] == 1
            and plan["state"] == "serving"
            and status["boot_id"] == plan["boot_id"]
            and status["applied_generation"] == plan["generation"]
            and math.isfinite(every)
            and every > 0
            and 0 <= age < max(30.0, 3 * every)
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="streamcast probe")
    commands = parser.add_subparsers(dest="kind", required=True)
    for kind in ("live", "ready"):
        command = commands.add_parser(kind)
        command.add_argument("--url", required=True)

    role = commands.add_parser("role")
    role.add_argument("--run-dir", type=Path, required=True)
    role.add_argument("--role", choices=ROLES, required=True)
    role.add_argument("--group", required=True)
    args = parser.parse_args(argv)
    if args.kind == "role":
        return 0 if role_ready(args.run_dir, args.role, args.group) else 1

    route = "/livez" if args.kind == "live" else "/readyz"
    try:
        target = urlsplit(args.url)
        if (
            target.scheme not in ("http", "https")
            or target.username is not None
            or target.query
            or target.fragment
        ):
            return 1

        with urlopen(args.url.rstrip("/") + route, timeout=2.0) as response:
            return 0 if response.status == 200 else 1

    except (OSError, URLError, ValueError, TypeError):
        return 1
