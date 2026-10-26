"""Portable stream policy and host storage settings, validated without I/O."""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal, Self

import pyarrow as pa
from litelink import LogConfig, S3Options
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from streamcast import _schema
from streamcast._limits import MAX_BACKLOG, MAX_IN_FLIGHT, MAX_INBOUND, MAX_PUBLISHERS
from streamcast._stream import MAX_REPLAY

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
Initialization = Literal["open_existing", "create_if_absent", "restore_published"]


def _name(value: str) -> str:
    if NAME.fullmatch(value) is None:
        raise ValueError("invalid stream name")

    return value


Name = Annotated[str, AfterValidator(_name)]


class StreamSpec(BaseModel):
    """A stream's schema and policy, with the same shape in Python and JSON."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
    )

    name: Name
    schema_: dict[str, Any] = Field(alias="schema")
    sort_by: tuple[str, ...] = ()
    exactly_once: StrictBool = False
    replay_published: StrictBool = False
    group_commit: StrictBool = True
    max_replay: Annotated[StrictInt, Field(ge=0)] | None = MAX_REPLAY
    max_publishers: Annotated[StrictInt, Field(ge=1)] = MAX_PUBLISHERS
    max_backlog: Annotated[StrictInt, Field(ge=1)] = MAX_BACKLOG
    max_inbound: Annotated[StrictInt, Field(ge=1)] = MAX_INBOUND
    max_in_flight: Annotated[StrictInt, Field(ge=1)] = MAX_IN_FLIGHT
    max_message_bytes: Annotated[StrictInt, Field(ge=1)] | None = None

    @model_validator(mode="after")
    def _validate_schema(self) -> Self:
        try:
            _schema_keys(self.schema_, root=True)
            arrow = self.arrow
        except (TypeError, AttributeError, RecursionError, OverflowError) as exc:
            # Schema compilation uses TypeError; model validation must report
            # it as a ValidationError just like the other declaration rules.
            raise ValueError("invalid stream schema") from exc

        if {"streamcast_ts", "litelink_offset"} & set(arrow.names):
            raise ValueError("reserved stream column")

        if len(set(self.sort_by)) != len(self.sort_by) or any(
            name not in arrow.names for name in self.sort_by
        ):
            raise ValueError("sort_by must contain distinct column names")

        return self

    @cached_property
    def arrow(self) -> pa.Schema:
        return _schema.to_arrow(self.schema_)


class Storage(BaseModel):
    """Configured locations and host-only credentials for a durable stream."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    root: Path
    published: str | None = None
    replicate: StrictBool = False
    config: LogConfig | None = None
    s3_options: S3Options | None = Field(default=None, exclude=True, repr=False)

    @field_validator("root")
    @classmethod
    def _absolute_root(cls, value: Path) -> Path:
        root = value.expanduser()
        if not root.is_absolute():
            raise ValueError("root must be absolute after expanduser")

        return root

    @model_validator(mode="after")
    def _replication(self) -> Self:
        if self.replicate and not self.published:
            raise ValueError("replicate requires published")

        if (
            self.config is not None
            and self.config.wal_replication
            and not self.replicate
        ):
            raise ValueError("wal_replication requires replicate")

        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Storage):
            return NotImplemented

        # Rotating host credentials does not change the recorded storage.
        return type(self) is type(other) and self.model_dump() == other.model_dump()

    def __hash__(self) -> int:
        # Pydantic's generated hash includes every field, including credentials.
        return hash((self.root, self.published, self.replicate, self.config))


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
