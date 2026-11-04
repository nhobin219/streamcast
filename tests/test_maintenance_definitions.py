"""Definition-driven dedication, live edits and admission without storage churn."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace

import pytest

from streamcast import (
    BrokerConfig,
    Maintain,
    Maintenance,
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    create_app,
)
from streamcast._broker import runtime
from tests.test_broker import (
    READER,
    SHAPE,
    build,
    definition,
    managed,
    put,
    serving,
)
from tests.test_declarative_control import Mutator

pytest_plugins = ["tests.test_broker"]
BASE = "/control/v1/streams/"


@pytest.mark.parametrize("value", [-1, 1.5, True, "1", None])
def test_max_dedicated_is_a_nonnegative_integer(value):
    with pytest.raises(ValueError, match="max_dedicated"):
        BrokerConfig(max_dedicated=value)


def test_managed_maintain_defaults(config):
    assert (
        replace(config, maintain=Maintain(flush_every=None)).maintain.flush_every
        is None
    )
    with pytest.raises(ValueError, match="require maintenance"):
        replace(config, maintain=False)


@pytest.mark.parametrize("state", ["serving", "stopped", "retired"])
async def test_maintenance_only_preserves_generation_and_avoids_storage(
    config, monkeypatch, state
):
    config = replace(config, max_dedicated=1)
    async with managed(config) as (owner, client, _):
        await serving(owner, client)
        if state != "serving":
            await put(
                owner, client, "events", {"state": state}, suffix="/desired", revision=2
            )

        owner.monitor.cancel()
        await asyncio.gather(owner.monitor, return_exceptions=True)
        old = owner.generations.get("events")
        writer = None if old is None else old.stream._writer
        before_generation = owner.generation
        before_id = owner.records["events"]["stream_id"]
        calls = []
        child_pass = owner.reconcile_children

        async def pass_once():
            calls.append("pass")
            await child_pass()

        def forbidden(*_):
            pytest.fail(
                "maintenance-only update touched storage or replaced a generation"
            )

        async def forbidden_async(*_):
            forbidden()

        monkeypatch.setattr(owner, "reconcile_children", pass_once)
        for name in ("claim", "offline", "storage_transition"):
            monkeypatch.setattr(owner, name, forbidden_async)

        for name in ("open_stream", "check_identity", "s3_options"):
            monkeypatch.setattr(owner, name, forbidden)

        revision = owner.records["events"]["revision"]
        value = definition(maintenance={"dedicated": True, "compact_every": 9})
        answer = await put(owner, client, "events", value, revision=revision)
        assert answer.status_code in (200, 202), answer.text
        record = owner.records["events"]
        assert record["pending"] is None
        assert record["revision"] == revision + 1
        assert owner.observed["events"]["state"] == state
        assert owner.generations.get("events") is old
        assert owner.generation == before_generation
        assert record["stream_id"] == before_id
        if old is not None:
            assert old.stream._writer is writer
            assert old.accepting

        if state == "serving":
            assert calls == ["pass"]
            assert old.maintenance == Maintenance(dedicated=True, compact_every=9)
            assert await old.stream.send({"value": 1}) == 1
        else:
            assert calls == []

        before = owner.registry.load()
        noop = await put(owner, client, "events", value, revision=revision + 1)
        assert noop.status_code in (200, 202)
        assert owner.registry.load() == before
        monkeypatch.undo()


async def test_cap_admission_exemptions_and_capabilities(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        before = owner.registry.load()
        refused = await put(
            owner, client, "events", definition(maintenance={"dedicated": True})
        )
        assert refused.status_code == 409
        assert refused.json()["error"]["code"] == "dedicated_limit"
        assert owner.registry.load() == before

    config = replace(config, max_dedicated=1)
    async with managed(config) as (owner, client, _):
        await serving(owner, client, maintenance={"dedicated": True})
        assert owner.dedicated_count() == 1
        # Lowering the host cap never evicts, and retuning is exempt.
        owner.config = replace(config, max_dedicated=0)
        retuned = await put(
            owner,
            client,
            "events",
            definition(maintenance={"dedicated": True, "seal_every": 2}),
            revision=2,
        )
        assert retuned.status_code in (200, 202)
        assert owner.records["events"]["pending"] is None
        unchanged = copy.deepcopy(owner.records)
        rejected = await put(
            owner, client, "other", definition(maintenance={"dedicated": True})
        )
        assert rejected.status_code == 409
        assert owner.records == unchanged
        # False -> true is admission too, with the same no-write guarantee.
        await put(owner, client, "other", definition())
        before = owner.registry.load()
        rejected = await put(
            owner,
            client,
            "other",
            definition(maintenance={"dedicated": True}),
            revision=1,
        )
        assert rejected.status_code == 409
        assert owner.registry.load() == before
        await put(owner, client, "events", definition(), revision=3)
        assert owner.dedicated_count() == 0
        rededicate = await put(
            owner,
            client,
            "events",
            definition(maintenance={"dedicated": True}),
            revision=4,
        )
        assert rededicate.status_code == 409
        owner.config = config
        await put(
            owner,
            client,
            "events",
            definition(maintenance={"dedicated": True}),
            revision=4,
        )
        await put(
            owner, client, "events", {"state": "retired"}, suffix="/desired", revision=5
        )
        assert owner.dedicated_count() == 0
        accepted = await put(
            owner,
            client,
            "other",
            definition(maintenance={"dedicated": True}),
            revision=1,
        )
        assert accepted.status_code in (200, 202)
        caps = (await client.get("/control/v1")).json()
        assert caps["dedicated"] == 1
        assert caps["max_dedicated"] == 1


async def test_static_dedication_counts_and_lent_handles_stay_shared(config, tmp_path):
    static = StreamHandle.open(
        Stream(
            spec=StreamSpec(
                name="static", schema=SHAPE, maintenance=Maintenance(dedicated=True)
            ),
            storage=Storage(root=tmp_path / "static"),
        ),
        initialization="create_if_absent",
    )
    lent = StreamHandle("lent")
    app = create_app(replace(config, max_dedicated=0), streams=[static, lent])
    async with app.router.lifespan_context(app):
        owner = app.state.broker
        assert owner.dedicated_count() == 1
        assert owner.generations["static"].maintenance.dedicated
        assert owner.generations["lent"].maintenance == Maintenance()
        assert await static.send({"value": 1}) == 1


async def test_live_comparison_preserves_schema_order_and_precedence(config):
    async with managed(replace(config, max_dedicated=1)) as (owner, client, _):
        shape = {
            "type": "object",
            "properties": {"value": {"type": "integer"}, "other": {"type": "integer"}},
            "required": ["value", "other"],
        }
        await serving(owner, client, schema=shape)
        maintenance = {"dedicated": True}
        reordered = copy.deepcopy(shape)
        reordered["properties"] = dict(reversed(list(shape["properties"].items())))
        before = owner.registry.load()
        value = definition(schema=reordered, maintenance=maintenance)
        # A stale ETag wins over the semantic conflict.
        stale = await put(owner, client, "events", value, revision=1)
        assert stale.status_code == 412
        refused = await put(owner, client, "events", value, revision=2)
        assert refused.json()["error"]["code"] == "must_be_stopped"
        assert owner.registry.load() == before
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        refused = await put(owner, client, "events", value, revision=3)
        assert refused.json()["error"]["code"] == "migration_required"
        invalid = await put(
            owner,
            client,
            "events",
            definition(maintenance={"seal_every": 1}),
            revision=1,
        )
        assert invalid.status_code == 422
        denied = await client.put(
            BASE + "events",
            json=definition(maintenance=maintenance),
            headers=READER | {"If-Match": '"3"'},
        )
        assert denied.status_code == 403


async def test_live_dedication_uses_real_children(config, monkeypatch):
    monkeypatch.setattr(runtime, "build", build)
    async with managed(replace(config, max_dedicated=1)) as (owner, client, _):
        await serving(owner, client)
        generation = owner.generations["events"]
        before = dict(owner.running)
        changed = await put(
            owner,
            client,
            "events",
            definition(maintenance={"dedicated": True, "compact_every": 9}),
            revision=2,
        )
        assert changed.status_code in (200, 202)
        assert owner.generations["events"] is generation
        assert len(owner.running) == 5
        assert next(spec for spec in owner.running if spec.role == "compact").every == 9
        assert all(child._process is not None for child in owner.running.values())
        assert any(child not in before.values() for child in owner.running.values())
        assert await generation.stream.send({"value": 1}) == 1


@pytest.mark.parametrize("failed", [False, True])
async def test_pending_dedication_join_and_retry_are_not_readmitted(
    config, monkeypatch, failed
):
    async with managed(replace(config, max_dedicated=1, max_pending=1)) as (
        owner,
        client,
        _,
    ):
        await serving(owner, client)
        target = definition(maintenance={"dedicated": True})
        entered, release = asyncio.Event(), asyncio.Event()
        reconcile = owner.reconcile
        calls = []

        async def held(record):
            calls.append(record["pending"]["attempt"])
            if failed and len(calls) == 1:
                raise OSError("injected")

            entered.set()
            await release.wait()
            await reconcile(record)

        monkeypatch.setattr(owner, "reconcile", held)
        first = await client.put(
            BASE + "events", json=target, headers={"If-Match": '"2"'}
        )
        assert first.status_code == 202
        if failed:
            await owner.queue.join()
            assert owner.records["events"]["pending"]["status"] == "failed"
        else:
            await entered.wait()

        owner.config = replace(owner.config, max_dedicated=0)
        try:
            before = owner.registry.load()
            stale = await client.put(
                BASE + "events", json=target, headers={"If-Match": '"2"'}
            )
            assert stale.status_code == 412
            conflict = await client.put(
                BASE + "events", json=definition(), headers={"If-Match": '"3"'}
            )
            assert conflict.json()["error"]["code"] == "transition_pending"
            assert owner.registry.load() == before
            joined = await client.put(
                BASE + "events", json=target, headers={"If-Match": '"3"'}
            )
            assert joined.status_code == 202
            await entered.wait()
            assert owner.records["events"]["pending"]["attempt"] == (2 if failed else 1)
            assert len(calls) == (2 if failed else 1)
        finally:
            release.set()
            await owner.queue.join()

        assert owner.records["events"]["pending"] is None
        assert owner.generations["events"].maintenance.dedicated


async def test_migration_preserves_maintenance(config):
    async with managed(replace(config, max_dedicated=1)) as (
        owner,
        client,
        _,
    ):
        policy = {"dedicated": True, "compact_every": 3}
        stream = await serving(owner, client, maintenance=policy)
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        schema = copy.deepcopy(SHAPE)
        schema["properties"]["extra"] = {"type": ["string", "null"]}
        migrated = await put(
            owner,
            client,
            "events",
            {"stream_id": stream._stream_id, "schema": schema, "sort_by": []},
            suffix="/schema",
            revision=3,
        )
        assert migrated.status_code == 202
        assert owner.records["events"]["pending"] is None
        assert (
            owner.records["events"]["stream"]["spec"]["maintenance"]
            == Maintenance(**policy).model_dump()
        )
        started = await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=4
        )
        assert started.status_code == 202
        assert owner.generations["events"].maintenance == Maintenance(**policy)
        edited = await put(
            owner,
            client,
            "events",
            definition(
                schema=schema, maintenance={"dedicated": True, "compact_every": 5}
            ),
            revision=5,
        )
        assert edited.status_code == 202
        assert owner.generations["events"].maintenance.compact_every == 5


async def test_maintenance_needs_only_control_mutate(config):
    async with managed(replace(config, max_dedicated=1, authorization=Mutator())) as (
        owner,
        client,
        _,
    ):
        await serving(owner, client)
        edited = await put(
            owner,
            client,
            "events",
            definition(maintenance={"dedicated": True}),
            revision=2,
        )
        assert edited.status_code == 202
        assert owner.records["events"]["pending"] is None
        assert owner.generations["events"].maintenance.dedicated
        assert (await client.get(BASE + "events")).status_code == 403
