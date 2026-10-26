"""Portable stream policy and host storage settings, validated without I/O."""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal, LiteralString, Self, cast

import pyarrow as pa
from litelink import LogConfig, S3Options
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import InitErrorDetails, PydanticCustomError

from streamcast import _schema
from streamcast._limits import (
    MAX_BACKLOG,
    MAX_IN_FLIGHT,
    MAX_INBOUND,
    MAX_PUBLISHERS,
    MAX_REPLAY,
)

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
Initialization = Literal["open_existing", "create_if_absent", "restore_published"]


def _name(value: str) -> str:
    if NAME.fullmatch(value) is None:
        raise ValueError("invalid stream name")

    return value


Name = Annotated[StrictStr, AfterValidator(_name)]


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

    @field_validator("sort_by", mode="before")
    @classmethod
    def _ordered_sort(cls, value: Any) -> Any:
        if not isinstance(value, (list, tuple)):
            raise ValueError("sort_by must be a list or tuple of column names")

        return value

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

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    root: Path
    published: str | None = None
    replicate: StrictBool = False
    config: LogConfig | None = None
    s3_options: S3Options | None = Field(default=None, exclude=True, repr=False)

    @model_validator(mode="wrap")
    @classmethod
    def _safe_errors(cls, value: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        try:
            return handler(value)
        except ValidationError as exc:
            # Missing fields report the whole input, and invalid credentials
            # report their own values. hide_input_in_errors only protects str().
            raise ValidationError.from_exception_data(
                cls.__name__,
                [
                    InitErrorDetails(
                        type=PydanticCustomError(
                            cast(LiteralString, e["type"]),
                            "{message}",
                            {"message": e["msg"]},
                        ),
                        loc=e["loc"],
                        input=None,
                    )
                    for e in exc.errors(include_input=False)
                ],
                hide_input=True,
            ) from None

    @field_validator("root")
    @classmethod
    def _absolute_root(cls, value: Path) -> Path:
        try:
            root = value.expanduser()
        except RuntimeError as exc:
            raise ValueError("root home directory could not be determined") from exc

        if not root.is_absolute():
            raise ValueError("root must be absolute after expanduser")

        return root

    @field_validator("replicate")
    @classmethod
    def _replication(cls, value: bool, info: ValidationInfo) -> bool:
        if value and not info.data.get("published"):
            raise ValueError("replicate requires published")

        return value

    @field_validator("config")
    @classmethod
    def _wal_replication(
        cls, value: LogConfig | None, info: ValidationInfo
    ) -> LogConfig | None:
        if (
            value is not None
            and value.wal_replication
            and not info.data.get("replicate")
        ):
            raise ValueError("wal_replication requires replicate")

        return value

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


class Stream(BaseModel):
    """A durable stream's portable policy and host storage."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    spec: StreamSpec
    storage: Storage
