"""Broker-owned role plans and the barrier replacing child stop/reap.

A successful lock probe is sufficient only after the new plan is durable:
new roles take the same inode before reading their permission to open logs.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import time
from collections.abc import Mapping
from dataclasses import fields
from pathlib import Path
from typing import Any, Literal

import msgspec
from litelink import S3Options

from streamcast import _plan
from streamcast._broker.model import ExternalMaintain
from streamcast._maintain import FLUSHED, ROLES
from streamcast._spec import Stream
from streamcast._stream import StreamHandle
from streamcast._tasks import thread

logger = logging.getLogger(__name__)


class External:
    def __init__(self, config: ExternalMaintain, boot_id: str) -> None:
        self.config = config
        self.boot_id = boot_id
        self.generation = 0
        self.roles: dict[str, _plan.RolePlan] | None = None
        self.starting = True
        self.serving = False
        self.reports: dict[str, dict] = {}

    def group(self, options: S3Options | None, profile: str | None = None) -> str:
        if profile is not None:
            if profile in self.config.groups and self.config.groups[profile] == options:
                return profile

        else:
            for name, candidate in self.config.groups.items():
                if candidate == options:
                    return name

        raise ValueError("unmapped S3 options for external maintenance")

    def validate(self, definition: Stream, *, profile: str | None = None) -> None:
        if definition.storage.replicate:
            raise ValueError("external maintenance refuses replicate=True")

        if definition.spec.maintenance.dedicated:
            raise ValueError("external maintenance refuses dedicated streams")

        self.group(definition.storage.s3_options, profile)

    def empty_roles(self) -> dict[str, _plan.RolePlan]:
        policy = self.config.policy
        duckdb: dict[str, Any] = {
            field.name: str(value) if isinstance(value, Path) else value
            for field in fields(policy)
            if field.name.startswith("duckdb_")
            and (value := getattr(policy, field.name)) is not None
        }
        return {
            _plan.key(role, group): _plan.RolePlan(
                every=policy.every(role),
                flush_every=policy.flush_every if role in FLUSHED else None,
                **duckdb,
            )
            for group in self.config.groups
            for role in ROLES
        }

    def plan_roles(
        self, streams: Mapping[str, StreamHandle], profiles: Mapping[str, str]
    ) -> dict[str, _plan.RolePlan]:
        roles = self.empty_roles()
        targets: dict[str, dict[str, set[tuple[str, str]]]] = {
            group: {"logs": set(), "retire": set(), "streams": set()}
            for group in self.config.groups
        }
        for name, stream in streams.items():
            group = self.group(stream._s3, profiles.get(name))
            members = targets[group]
            if stream.log is not None and stream.retirement is None:
                # Retention and follow's opened-log map must use identical homes.
                home = str(Path(stream.log.root))
                members["logs"].add((home, stream.log.name))
                members["streams"].add((home, name))

            members["retire"].update(
                (str(Path(root)), log) for root, log in stream.retired
            )

        for group, members in targets.items():
            for role in ROLES:
                key = _plan.key(role, group)
                roles[key] = msgspec.structs.replace(
                    roles[key],
                    logs=tuple(sorted(members["logs"])),
                    retire=tuple(sorted(members["retire"]))
                    if role == "publish"
                    else (),
                    streams=tuple(sorted(members["streams"]))
                    if role == "clean-published"
                    else (),
                )

        return roles

    def write(
        self, state: Literal["fenced", "serving"], roles: dict[str, _plan.RolePlan]
    ) -> None:
        self.generation += 1
        streamcast, litelink = _plan.versions()
        plan = _plan.Plan(
            self.boot_id, self.generation, state, streamcast, litelink, roles
        )
        _plan.write_plan(self.config.run_dir, plan)
        self.roles = roles
        status = self.config.run_dir / "status"
        for path in status.glob("*.json*"):
            if path.name.removesuffix(".tmp").removesuffix(".json") not in roles:
                path.unlink(missing_ok=True)

    def broker_status(self, state: str, waiting: list[str]) -> None:
        _plan.atomic_write(
            self.config.run_dir / "broker.json",
            {"boot_id": self.boot_id, "state": state, "waiting": waiting},
        )

    def read_status(self, name: str) -> _plan.Status | None:
        try:
            return msgspec.json.decode(
                (self.config.run_dir / "status" / f"{name}.json").read_bytes(),
                type=_plan.Status,
            )
        except (OSError, ValueError):
            return None

    def waiting(self) -> list[str]:
        waiting = []
        for path in sorted((self.config.run_dir / "roles").glob("*.lock")):
            status = self.read_status(path.stem)
            if (
                status is not None
                and status.boot_id == self.boot_id
                and status.applied_generation is not None
                and status.applied_generation >= self.generation
            ):
                continue

            with path.open("a") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    waiting.append(path.stem)

        return waiting

    async def barrier(self) -> None:
        last_log = float("-inf")
        while True:
            waiting = await thread(self.waiting)
            await thread(self.broker_status, "barrier_waiting", waiting)
            if not waiting:
                return

            now = time.monotonic()
            if now - last_log >= 10:
                logger.info("waiting for role barrier: %s", ", ".join(waiting))
                last_log = now

            await asyncio.sleep(0.1)

    async def fence(self) -> None:
        await thread(self.write, "fenced", self.empty_roles())
        await self.barrier()
        await thread(self.broker_status, "opening", [])

    async def reconcile(
        self,
        streams: Mapping[str, StreamHandle],
        profiles: Mapping[str, str],
        *,
        closing: bool,
    ) -> None:
        if self.starting:
            return

        roles = self.empty_roles() if closing else self.plan_roles(streams, profiles)
        if roles == self.roles and not closing:
            return

        await thread(self.write, "fenced" if closing else "serving", roles)
        if self.serving or closing:
            await self.barrier()

        self.serving = not closing
        if not closing:
            await thread(self.broker_status, "serving", [])

    def status(self, visible: set[str] | None = None) -> dict[str, Any]:
        roles = {}
        for name, spec in (self.roles or {}).items():
            status = self.read_status(name)
            age = None if status is None else time.time() - status.heartbeat_at
            ready = (
                status is not None
                and status.boot_id == self.boot_id
                and status.applied_generation == self.generation
                and age is not None
                and 0 <= age < max(30, 3 * spec.every)
                and status.error is None
            )
            roles[name] = {
                "ready": ready,
                "applied_generation": None
                if status is None
                else status.applied_generation,
                "heartbeat_age": age,
                "last_pass": {}
                if status is None
                else {
                    path: msgspec.to_builtins(result)
                    for path, result in status.last_pass.items()
                    if visible is None
                    or path
                    in {
                        str(Path(home) / log)
                        for stream in visible
                        for home, log in self.targets_for(stream)
                    }
                },
            }

        return {
            "converged": all(role["ready"] for role in roles.values()),
            "roles": roles,
            "startup": {
                name: report
                for name, report in self.reports.items()
                if visible is None or name in visible
            },
        }

    def targets_for(self, name: str) -> tuple[tuple[str, str], ...]:
        report = self.reports.get(name)
        if report is None:
            return ()

        return ((report["home"], report["current_log"]),)
