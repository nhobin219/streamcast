"""Registry exchange between the expanded reader and released 0.27.0."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import sys
from pathlib import Path

import streamcast
from streamcast._broker.registry import Registry, dumps
from streamcast._broker.runtime import BrokerRuntime

SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
}


class RegistryAccess:
    async def authenticate(self, scope):
        return "admin"

    async def allows(self, principal, action, resource):
        return True


async def registry_pair(mode: str, root: str, variant: str) -> dict:
    registry_path = Path(root) / "registry.db"
    config = streamcast.BrokerConfig(
        broker_id="interop",
        registry=registry_path,
        storage_profiles={"local": streamcast.Storage(root=Path(root) / "logs")},
        authorization=RegistryAccess(),
    )
    result = {"version": importlib.metadata.version("streamcast")}
    if mode == "registry-write":
        Path(root).mkdir(parents=True)
        streamcast.initialize_registry(config)
        registry = Registry.open(registry_path, config.broker_id)
        owner = BrokerRuntime(config, registry)
        try:
            body = {"name": "events", "schema": SCHEMA}
            # Only the expanded reader knows the new field. Both must persist
            # the same bytes, even when the new caller explicitly supplies it.
            if "maintenance" in streamcast.StreamSpec.model_fields:
                body["maintenance"] = {"dedicated": False}

            await owner.mutate(
                "events",
                "definition",
                body,
                None,
                "*",
                storage_profile="local",
                initialization="create_if_absent",
            )
            if variant == "replace":
                await owner.execute("events")
                await owner.mutate(
                    "events", "definition", body | {"max_backlog": 42}, '"1"', None
                )

            result["records"], result["collection"] = registry.load()
            result["spec_bytes"] = dumps(result["records"]["events"]["stream"]["spec"])
            result["target_bytes"] = dumps(
                result["records"]["events"]["pending"]["target"]
            )
        finally:
            registry.close()

    else:
        registry = Registry.open(registry_path, config.broker_id)
        try:
            before, _ = registry.load()
            result["spec_bytes"] = dumps(
                streamcast.StreamSpec.model_validate(
                    before["events"]["stream"]["spec"]
                ).model_dump(mode="json")
            )
            target = before["events"]["pending"]["target"]
            result["target_bytes"] = dumps(
                target
                | {
                    "spec": streamcast.StreamSpec.model_validate(
                        target["spec"]
                    ).model_dump(mode="json")
                }
            )
            for record in before.values():
                streamcast.Stream.model_validate(record["stream"])
                streamcast.StreamSpec.model_validate(
                    record["pending"]["target"]["spec"]
                )

        finally:
            registry.close()

        owner = await BrokerRuntime.open(config)
        try:
            result.update(
                before=before,
                after=owner.records,
                collection=owner.collection,
                observed=owner.status("events")["observed"],
            )
        finally:
            await owner.close()

    return result


if __name__ == "__main__":
    print(json.dumps(asyncio.run(registry_pair(*sys.argv[1:]))), flush=True)
