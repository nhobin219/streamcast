"""TOML service configuration, with the same policy objects as Python hosts.

``[server]`` sets host/port. ``[broker]`` contains BrokerConfig fields;
``authorization = "package.module:object"`` names a host's authorization
object (or a no-argument factory). Storage objects live under
``[storage_profiles.NAME]``. Each ``[[streams]]`` has ``spec`` and either
``storage`` or ``storage_profile``. Static definitions only open existing state.
Credentials are never deserialized: litelink resolves them from the environment.
"""

from __future__ import annotations

import importlib
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from streamcast._broker.model import BrokerConfig, ExternalMaintain, HistoryGuard
from streamcast._maintain import Maintain
from streamcast._spec import Storage, Stream

# The deployment file is portable policy, never a credential store. Refusal
# text contains only these fixed variable names, even for malformed input.
_CREDENTIALS = {
    "access_key": "AWS_ACCESS_KEY_ID",
    "access_key_id": "AWS_ACCESS_KEY_ID",
    "aws_access_key_id": "AWS_ACCESS_KEY_ID",
    "secret_key": "AWS_SECRET_ACCESS_KEY",
    "secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "session_token": "AWS_SESSION_TOKEN",
    "aws_session_token": "AWS_SESSION_TOKEN",
    "password": "STREAMCAST_PASSWORD",
    "token": "STREAMCAST_TOKEN",
    "credentials": "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY",
}


class ConfigurationError(ValueError):
    """A refusal whose fixed text is safe for process logs."""


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    server: dict[str, Any] = Field(default_factory=dict)
    broker: dict[str, Any] = Field(default_factory=dict)
    storage_profiles: dict[str, dict[str, Any]] = Field(default_factory=dict)
    streams: list[dict[str, Any]] = Field(default_factory=list)


class _Server(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    host: str = "0.0.0.0"
    port: int = Field(default=8765, ge=1, le=65535)


class _External(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    run_dir: str
    groups: list[str] = Field(min_length=1)


@dataclass(frozen=True)
class ServiceConfig:
    broker: BrokerConfig
    streams: tuple[Stream, ...]
    host: str
    port: int


def _refuse_credentials(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "schema":
                continue

            variable = _CREDENTIALS.get(key.lower().replace("-", "_"))
            if variable is not None:
                raise ConfigurationError(f"credentials belong in {variable}, not TOML")

            _refuse_credentials(child)

    elif isinstance(value, list):
        for child in value:
            _refuse_credentials(child)

    elif isinstance(value, str) and "://" in value:
        try:
            embedded = urlsplit(value).username is not None
        except ValueError:
            embedded = True

        if embedded:
            raise ConfigurationError(
                "URL credentials belong in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, not TOML"
            )


def _authorization(reference: str):
    module, separator, attribute = reference.partition(":")
    if not separator:
        raise ValueError("authorization requires module:attribute")

    value = importlib.import_module(module)
    for name in attribute.split("."):
        value = getattr(value, name)

    if isinstance(value, type) or not hasattr(value, "authenticate"):
        value = value()

    if not callable(value.authenticate) or not callable(value.allows):
        raise ValueError("authorization must implement authenticate and allows")

    return value


def load(path: Path) -> ServiceConfig:
    try:
        raw = tomllib.loads(path.read_text())
    except (tomllib.TOMLDecodeError, UnicodeError):
        raise ConfigurationError("invalid TOML configuration") from None

    _refuse_credentials(raw)
    try:
        file = _File.model_validate(raw)
        server = _Server.model_validate(file.server)
        fields = dict(file.broker)
        if "registry" in fields:
            fields["registry"] = Path(fields["registry"])

        if "history_guards" in fields:
            fields["history_guards"] = {
                name: HistoryGuard(**guard)
                for name, guard in fields["history_guards"].items()
            }

        if "authorization" in fields:
            fields["authorization"] = _authorization(fields["authorization"])

        external = None
        if isinstance(fields.get("maintain"), dict):
            policy = dict(fields["maintain"])
            if "external" in policy:
                external = _External.model_validate(policy.pop("external"))
                if not Path(external.run_dir).is_absolute() or len(
                    set(external.groups)
                ) != len(external.groups):
                    raise ValueError("invalid external maintenance directory or groups")

            if "duckdb_spill_root" in policy:
                policy["duckdb_spill_root"] = Path(policy["duckdb_spill_root"])

            fields["maintain"] = Maintain(**policy)

        profiles = {
            name: Storage.model_validate(value)
            for name, value in file.storage_profiles.items()
        }
        broker = BrokerConfig(**fields, storage_profiles=profiles)
        if external is not None:
            broker = replace(
                broker,
                maintain=ExternalMaintain(
                    policy=fields["maintain"],
                    run_dir=Path(external.run_dir),
                    groups={
                        name: broker.storage_profiles[name].s3_options
                        for name in external.groups
                    },
                ),
            )

        streams = []
        for definition in file.streams:
            value = dict(definition)
            if "storage_profile" in value:
                profile = value.pop("storage_profile")
                if "storage" in value:
                    raise ValueError("choose storage or storage_profile")

                value["storage"] = broker.storage_profiles[profile]

            streams.append(Stream.model_validate(value))

        if bool(streams) == (broker.registry is not None):
            raise ValueError("configure static streams or a registry")

        return ServiceConfig(broker, tuple(streams), server.host, server.port)
    except (ValueError, TypeError, KeyError, AttributeError, ImportError):
        # Pydantic, constructors and host plugins may include input values in
        # their errors; the CLI must never turn rejected input into a log leak.
        raise ConfigurationError(
            "invalid service configuration; check fields and host authorization"
        ) from None
