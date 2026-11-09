"""Released registry writer, explicit upgrader, reader and recovery process.

Run under either the checkout or released 0.27, with isolated imports.
The write phase deliberately stops at durable acceptance, before a worker
can apply the definition; the next process must read and replay it.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import sys
from pathlib import Path

from streamcast import (
    BrokerConfig,
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    initialize_registry,
    upgrade_registry,
)
from streamcast._broker.model import Fault
from streamcast._broker.registry import Registry
from streamcast._broker.runtime import BrokerRuntime

SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
}
DEFAULT = {
    "dedicated": False,
    "seal_every": None,
    "compact_every": None,
    "publish_every": None,
    "clean_every": None,
    "clean_published_every": None,
    "flush_every": None,
}


class Access:
    async def authenticate(self, scope):
        return "admin"

    async def allows(self, principal, action, resource):
        return True


def declaration() -> dict:
    body = {"name": "events", "schema": SCHEMA}
    if "maintenance" in StreamSpec.model_fields:
        body["maintenance"] = DEFAULT

    return body


async def run(phase: str, root: Path) -> dict:
    config = BrokerConfig(
        broker_id="interop",
        registry=root / "registry.db",
        storage_profiles={"local": Storage(root=root / "logs")},
        authorization=Access(),
    )
    report = {"version": importlib.metadata.version("streamcast")}
    if phase == "probe":
        try:
            registry = Registry.open(root / "registry.db", config.broker_id)
        except ValueError as refused:
            report["registry_error"] = str(refused)
        else:
            registry.close()
            report["registry_opened"] = True

        return report

    if phase == "upgrade":
        upgrade_registry(config)
        report["upgraded"] = True
        return report

    if phase in ("write", "read"):
        if phase == "write":
            initialize_registry(config)

        registry = Registry.open(root / "registry.db", config.broker_id)
        owner = BrokerRuntime(config, registry)
        try:
            try:
                normalized = owner.validate_spec(
                    {"name": "events", "schema": SCHEMA, "maintenance": DEFAULT},
                    "events",
                )
            except Fault as refused:
                assert refused.code == "invalid_declaration"
                report["default_maintenance"] = False
            else:
                assert "maintenance" not in normalized
                report["default_maintenance"] = True

            owner.records, owner.collection = registry.load()
            if phase == "read":
                record = owner.records["events"]
                assert record["pending"]["kind"] == "definition"
                for spec in (
                    record["stream"]["spec"],
                    record["pending"]["target"]["spec"],
                ):
                    assert owner.validate_spec(spec, "events") == spec
                    assert "maintenance" not in spec

                owner.observe("events", "unknown")

            await owner.mutate(
                "events",
                "definition",
                declaration(),
                '"1"' if phase == "read" else None,
                None if phase == "read" else "*",
                storage_profile="local",
                initialization="create_if_absent",
            )
            records, revision = registry.load()
            assert records == owner.records
            report.update(record=records["events"], collection=revision)
        finally:
            registry.close()

        return report

    owner = await BrokerRuntime.open(config)
    try:
        record = owner.records["events"]
        assert record["pending"] is None
        assert owner.observed["events"]["state"] == "stopped"
        report.update(record=owner.registry.load()[0]["events"])
    finally:
        await owner.close()

    definition = Stream.model_validate(record["stream"])
    for offset in (1, 2):
        stream = StreamHandle.open(definition, initialization="create_if_absent")
        try:
            assert await stream.send({"value": offset}) == offset
            report["rows"] = offset
        finally:
            await stream.aclose()

    return report


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run(sys.argv[1], Path(sys.argv[2])))), flush=True)
