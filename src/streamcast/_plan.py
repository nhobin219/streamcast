"""The local broker/role protocol. Only the broker writes the plan.

Replacement is atomic: readers see a complete old or new document. A role's
lock is taken before its first read and held until exit or close-on-exec.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
from collections.abc import Iterator
from importlib.metadata import version
from pathlib import Path
from typing import Literal

import msgspec

from streamcast._spec import NAME


class RolePlan(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    every: float
    flush_every: float | None = None
    logs: tuple[tuple[str, str], ...] = ()
    retire: tuple[tuple[str, str], ...] = ()
    streams: tuple[tuple[str, str], ...] = ()
    duckdb_default_memory_mib: int | None = None
    duckdb_default_threads: int | None = None
    duckdb_retention_memory_mib: int | None = None
    duckdb_retention_threads: int | None = None
    duckdb_retention_spill_mib: int | None = None
    duckdb_spill_root: str | None = None


class Plan(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    boot_id: str
    generation: int
    state: Literal["fenced", "serving"]
    streamcast: str
    litelink: str
    roles: dict[str, RolePlan]
    format: int = 1


class Pass(msgspec.Struct):
    ok_at: float | None = None
    error: str | None = None


class Status(msgspec.Struct):
    boot_id: str | None = None
    applied_generation: int | None = None
    heartbeat_at: float = 0
    logs_open: int = 0
    last_pass: dict[str, Pass] = msgspec.field(default_factory=dict)
    error: str | None = None


def versions() -> tuple[str, str]:
    return version("streamcast"), version("litelink")


def key(role: str, group: str) -> str:
    if NAME.fullmatch(role) is None or NAME.fullmatch(group) is None:
        raise ValueError("role and group must be valid names")

    return f"{role}.{group}"


def read_plan(run_dir: Path) -> Plan:
    plan = msgspec.json.decode((run_dir / "plan.json").read_bytes(), type=Plan)
    if plan.format != 1 or not plan.boot_id or plan.generation < 1:
        raise ValueError("unsupported plan format or identity")

    return plan


def write_plan(run_dir: Path, plan: Plan) -> None:
    atomic_write(run_dir / "plan.json", plan)


def read_status(run_dir: Path, role: str, group: str) -> Status:
    return msgspec.json.decode(
        (run_dir / "status" / f"{key(role, group)}.json").read_bytes(), type=Status
    )


def write_status(run_dir: Path, role: str, group: str, status: Status) -> None:
    atomic_write(run_dir / "status" / f"{key(role, group)}.json", status)


def atomic_write(path: Path, value: object) -> None:
    payload = msgspec.json.encode(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Each document has one writer (broker ownership or the role lock).
    # A fixed scratch name also bounds leftovers from SIGKILL to one file.
    temporary = path.with_name(f"{path.name}.tmp")
    try:
        with temporary.open("wb") as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())

        os.replace(temporary, path)

    finally:
        temporary.unlink(missing_ok=True)


@contextlib.contextmanager
def role_lock(run_dir: Path, role: str, group: str) -> Iterator[None]:
    path = run_dir / "roles" / f"{key(role, group)}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Never unlink this inode: a contender must lock the same file even
    # while its previous holder exits. Python opens descriptors non-inheritable.
    with path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
