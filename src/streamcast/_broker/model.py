"""Host capabilities and authorization for the managed broker."""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from streamcast._limits import (
    MAX_MESSAGE_BYTES,
    SUBSCRIBER_YIELD_EVERY,
)
from streamcast._spec import NAME, Storage

CONTROL = "/control/v1"


class Fault(Exception):
    def __init__(self, status: int, code: str, **details: Any) -> None:
        self.status = status
        self.code = code
        self.details = details
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


class PublicAccess:
    """Default static hosting: data and statistics, no management authority."""

    async def authenticate(self, scope):
        return "public"

    async def allows(self, principal, action, resource):
        return action in ("data.read", "data.publish", "stats.read")


@dataclass(frozen=True)
class BrokerConfig:
    broker_id: str = "static"
    registry: Path | None = None
    storage_profiles: Mapping[str, Storage] = field(default_factory=dict)
    authorization: Authorization = field(default_factory=PublicAccess)
    history_guards: Mapping[str, HistoryGuard] = field(default_factory=dict)
    transport_max_bytes: int = MAX_MESSAGE_BYTES
    control_max_bytes: int = 256 * 1024
    max_pending: int = 32
    readiness_timeout: float = 30.0
    readiness_every: float = 0.25
    subscriber_yield_every: int = SUBSCRIBER_YIELD_EVERY
    maintain: Any = True
    stats: bool | str = True

    def __post_init__(self) -> None:
        if NAME.fullmatch(self.broker_id) is None:
            raise ValueError("invalid broker identity")

        profiles = dict(self.storage_profiles) or {
            "default": Storage(root=Path.home() / ".streamcast" / self.broker_id)
        }
        if any(
            NAME.fullmatch(name) is None or not isinstance(profile, Storage)
            for name, profile in profiles.items()
        ):
            raise ValueError("invalid storage profile")

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
                self.subscriber_yield_every,
            )
        ):
            raise ValueError("positive integer bounds required")

        if any(
            not math.isfinite(value) or value <= 0
            for value in (self.readiness_timeout, self.readiness_every)
        ):
            raise ValueError("positive readiness bounds required")

        if self.registry is not None and isinstance(self.authorization, PublicAccess):
            raise ValueError("registry hosting requires explicit authorization")

        if self.registry is not None and self.maintain is not True:
            raise ValueError("managed streams require maintenance")

        if not isinstance(self.stats, (bool, str)) or (
            isinstance(self.stats, str)
            and (not self.stats.startswith("/") or self.stats == "/")
        ):
            raise ValueError("stats must be a bool or an absolute non-root path")

        object.__setattr__(self, "storage_profiles", MappingProxyType(profiles))
        object.__setattr__(
            self,
            "history_guards",
            MappingProxyType(copy.deepcopy(dict(self.history_guards))),
        )
