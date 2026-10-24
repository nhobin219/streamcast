"""Host capabilities and the durable, JSON-only management declaration."""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from streamcast import _schema
from streamcast._limits import (
    MAX_BACKLOG,
    MAX_IN_FLIGHT,
    MAX_INBOUND,
    MAX_MESSAGE_BYTES,
)
from streamcast._stream import MAX_REPLAY

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
CONTROL = "/control/v1"


class Fault(Exception):
    def __init__(self, status: int, code: str) -> None:
        self.status = status
        self.code = code
        super().__init__(code)


class Authorization(Protocol):
    """Host authentication and per-resource grants; None challenges with Basic.

    The principal is a stable, nonsecret identity used to scope receipts.
    The host validates ingress trust and Origin here; forwarded headers are
    never interpreted by the broker. Actions are data.read, data.publish,
    stats.read, control.read, control.mutate, and control.migrate.
    """

    async def authenticate(self, scope: Mapping[str, Any]) -> str | None: ...

    async def allows(self, principal: str, action: str, resource: str) -> bool: ...


@dataclass(frozen=True)
class HistoryGuard:
    """An existing message stream whose complete origin must remain readable.

    `known_row` is a captured application row at `known_offset`, supplied by
    the host handoff audit. Admission verifies it through the broker's replay
    machinery and scans for a dense prefix from offset one. Neither is a
    client request nor an authorization bypass.
    """

    stream_id: str
    known_offset: int
    known_row: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True)
class StorageProfile:
    """Immutable host settings; paths and credentials never enter HTTP bodies."""

    revision: str
    root: Path
    published: str | None = None
    config: Any = field(default=None, repr=False)
    s3_options: Any = field(default=None, repr=False)
    replicate: bool = False

    def public(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "replicate": self.replicate,
            "maintenance": True,
            "replay_scope": "retained_current_log",
        }


@dataclass(frozen=True)
class BrokerConfig:
    broker_id: str
    registry: Path
    profiles: Mapping[str, StorageProfile]
    authorization: Authorization
    history_guards: Mapping[str, HistoryGuard] = field(default_factory=dict)
    transport_max_bytes: int = MAX_MESSAGE_BYTES
    control_max_bytes: int = 256 * 1024
    max_pending: int = 32
    readiness_timeout: float = 30.0
    readiness_every: float = 0.25

    def __post_init__(self) -> None:
        if not self.broker_id or not self.profiles:
            raise ValueError("broker identity and profiles required")

        if any(
            NAME.fullmatch(name) is None or not profile.revision
            for name, profile in self.profiles.items()
        ):
            raise ValueError("invalid profile")

        if any(
            NAME.fullmatch(name) is None
            or not guard.stream_id
            or guard.known_offset < 1
            for name, guard in self.history_guards.items()
        ):
            raise ValueError("invalid history guard")

        if any(
            type(value) is not int or value < 1
            for value in (
                self.transport_max_bytes,
                self.control_max_bytes,
                self.max_pending,
            )
        ):
            raise ValueError("positive integer bounds required")

        if any(
            not math.isfinite(value) or value <= 0
            for value in (self.readiness_timeout, self.readiness_every)
        ):
            raise ValueError("positive readiness bounds required")

        object.__setattr__(self, "profiles", MappingProxyType(dict(self.profiles)))
        object.__setattr__(
            self,
            "history_guards",
            MappingProxyType(copy.deepcopy(dict(self.history_guards))),
        )


def declaration(
    body: Any, profiles: Mapping[str, StorageProfile], ceiling: int
) -> dict:
    if not isinstance(body, dict) or set(body) - {
        "schema",
        "sort_by",
        "storage_profile",
        "initialization",
        "exactly_once",
        "replay_published",
        "max_replay",
        "group_commit",
        "limits",
    }:
        raise Fault(422, "invalid_declaration")

    try:
        shape = body["schema"]
        # The schema compiler is authoritative. Reject annotations/features
        # it would otherwise ignore: accepting a constraint and not enforcing
        # it would give the controller a false contract.
        _schema_keys(shape, root=True)
        arrow = _schema.to_arrow(shape)
        if "streamcast_ts" in arrow.names or "litelink_offset" in arrow.names:
            raise ValueError

        profile = body["storage_profile"]
        if not isinstance(profile, str) or profile not in profiles:
            raise ValueError

        sort = body.get("sort_by", [])
        if (
            not isinstance(sort, list)
            or any(type(k) is not str or k not in arrow.names for k in sort)
            or len(set(sort)) != len(sort)
        ):
            raise ValueError

        initialization = body.get("initialization", "open_existing")
        if initialization not in ("open_existing", "create_if_absent"):
            raise ValueError

        limits = body.get("limits", {})
        defaults = {
            "max_backlog": MAX_BACKLOG,
            "max_inbound": MAX_INBOUND,
            "max_in_flight": MAX_IN_FLIGHT,
            "max_message_bytes": ceiling,
        }
        if not isinstance(limits, dict) or set(limits) - defaults.keys():
            raise ValueError

        resolved = defaults | limits
        if (
            any(type(v) is not int or v < 1 for v in resolved.values())
            or resolved["max_message_bytes"] > ceiling
        ):
            raise ValueError

        replay = body.get("max_replay", MAX_REPLAY)
        if replay is not None and (type(replay) is not int or replay < 0):
            raise ValueError

        flags = {
            k: body.get(k, default)
            for k, default in (
                ("exactly_once", False),
                ("replay_published", False),
                ("group_commit", True),
            )
        }
        if any(type(v) is not bool for v in flags.values()):
            raise ValueError

    except (
        KeyError,
        ValueError,
        TypeError,
        AttributeError,
        RecursionError,
        OverflowError,
    ):
        raise Fault(422, "invalid_declaration") from None

    return {
        "schema": shape,
        "sort_by": sort,
        "storage_profile": profile,
        "initialization": initialization,
        **flags,
        "max_replay": replay,
        "limits": resolved,
    }


def _schema_keys(spec: Any, *, root: bool = False) -> None:
    if not isinstance(spec, dict):
        raise ValueError

    declared = spec.get("type")
    if isinstance(declared, list):
        declared = next((kind for kind in declared if kind != "null"), None)

    allowed = {"type", "description", "title"}
    if root or declared == "object":
        allowed |= {"properties", "required", "additionalProperties"}
    elif declared == "array":
        allowed.add("items")
    elif declared in ("integer", "number", "string"):
        allowed.add("format")
        if declared == "string":
            allowed.add("contentEncoding")

    if set(spec) - allowed:
        raise ValueError

    additional = spec.get("additionalProperties", False)
    if additional is not False and (root or not isinstance(additional, dict)):
        raise ValueError

    for child in spec.get("properties", {}).values():
        _schema_keys(child)

    if "items" in spec:
        _schema_keys(spec["items"])

    if isinstance(additional, dict):
        _schema_keys(additional)
