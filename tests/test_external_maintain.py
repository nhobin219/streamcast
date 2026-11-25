"""The broker may open or remove logs only after the old roles release them."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from contextlib import closing
from dataclasses import fields
from pathlib import Path

import httpx
import litelink
import pytest

from streamcast import (
    BrokerConfig,
    Maintain,
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    _dedupe,
    _inspect,
    _plan,
)
from streamcast._application import Opening, create_app, initialize_registry
from streamcast._broker.model import ExternalMaintain
from streamcast._broker.registry import Lock
from streamcast._broker.runtime import BrokerRuntime
from streamcast._external import External
from streamcast._maintain import FLUSHED, ROLES

SHAPE = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
}


class Grants:
    async def authenticate(self, scope):
        return "operator"

    async def allows(self, principal, action, resource):
        return True


def config(tmp_path, *, policy=None, groups=None, **kwargs):
    return BrokerConfig(
        maintain=ExternalMaintain(
            policy or Maintain(),
            tmp_path / "run",
            {"default": None} if groups is None else groups,
        ),
        **kwargs,
    )


@pytest.fixture
async def definition(tmp_path):
    definition = Stream(
        spec=StreamSpec(name="events", schema=SHAPE),
        storage=Storage(root=tmp_path / "data"),
    )
    handle = StreamHandle.open(definition, initialization="create_if_absent")
    await handle.aclose()
    return definition


def observe_barrier(monkeypatch, *, after=1):
    event = asyncio.Event()
    loop = asyncio.get_running_loop()
    original = External.waiting
    observed = []

    def waiting(self):
        result = original(self)
        observed.append(result)
        if len(observed) >= after:
            loop.call_soon_threadsafe(event.set)

        return result

    monkeypatch.setattr(External, "waiting", waiting)
    return event, observed


async def at_barrier(event, task):
    waiter = asyncio.create_task(event.wait())
    try:
        await asyncio.wait((waiter, task), return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            task.result()

    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_broker_spawns_no_children(tmp_path, definition, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("external maintenance spawned a child")

    monkeypatch.setattr("streamcast._maintain.Supervisor.start", forbidden)
    monkeypatch.setattr("streamcast._broker.runtime.build", forbidden)
    app = create_app(config(tmp_path), streams=Opening(definition))
    async with app.router.lifespan_context(app):
        assert app.state.broker.running == {}
        assert len(_plan.read_plan(tmp_path / "run").roles) == len(ROLES)
        assert app.state.broker.generations["events"].writable


async def test_startup_barrier_waits_for_live_role_then_opens(
    tmp_path, definition, monkeypatch
):
    event, observed = observe_barrier(monkeypatch)
    run_dir = tmp_path / "run"
    with _plan.role_lock(run_dir, "seal", "default"):
        # Even a very large generation from a previous boot must not pass.
        _plan.write_status(run_dir, "seal", "default", _plan.Status("old-boot", 999))
        opening = asyncio.create_task(
            BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])
        )
        await at_barrier(event, opening)
        assert not opening.done()
        assert observed and all(waiting == ["seal.default"] for waiting in observed)
        plan = _plan.read_plan(run_dir)
        assert plan.state == "fenced"
        _plan.write_status(
            run_dir, "seal", "default", _plan.Status(plan.boot_id, plan.generation)
        )
        broker = await opening
        assert "events" in broker.generations

    await broker.close()


async def test_no_log_opened_before_barrier_passes(tmp_path, definition, monkeypatch):
    event, _ = observe_barrier(monkeypatch, after=2)
    calls = []

    def open_stream():
        calls.append("open")
        return StreamHandle.open(definition)

    with _plan.role_lock(tmp_path / "run", "publish", "default"):
        task = asyncio.create_task(
            BrokerRuntime.open(
                config(tmp_path), streams=[Opening(definition, open_stream)]
            )
        )
        await at_barrier(event, task)
        assert calls == []
        assert not task.done()

    broker = await task
    try:
        assert calls == ["open"]
    finally:
        await broker.close()


async def test_external_maintain_refuses_open_handles(tmp_path, definition):
    handle = StreamHandle.open(definition)
    try:
        with pytest.raises(ValueError, match="already-open stream cannot be fenced"):
            create_app(config(tmp_path), streams=handle)
    finally:
        await handle.aclose()


@pytest.mark.parametrize("mismatch", ["root", "name"])
async def test_opening_root_or_name_mismatch_closes_and_refuses(
    tmp_path, definition, monkeypatch, mismatch
):
    wrong = Stream(
        spec=definition.spec.model_copy(update={"name": "other"})
        if mismatch == "name"
        else definition.spec,
        storage=Storage(root=tmp_path / "other")
        if mismatch == "root"
        else definition.storage,
    )
    handle = StreamHandle.open(wrong, initialization="create_if_absent")
    closed = []
    original = StreamHandle.aclose

    async def close(self):
        if self is handle:
            closed.append(self.name)

        await original(self)

    monkeypatch.setattr(StreamHandle, "aclose", close)
    with pytest.raises(ValueError, match="name or storage root"):
        await BrokerRuntime.open(
            config(tmp_path), streams=[Opening(definition, lambda: handle)]
        )

    assert closed == [wrong.spec.name]
    # Startup failure releases every owner lock as well as the bad handle.
    with closing(Lock.open(definition.storage.root / ".streamcast-owners/events.lock")):
        pass


async def test_owner_lock_taken_before_open(tmp_path, definition):
    calls = []

    def opening():
        for path in (
            definition.storage.root / ".streamcast-owners/events.lock",
            definition.storage.root / "events/.streamcast-log-owners/events.lock",
        ):
            with pytest.raises(BlockingIOError):
                Lock.open(path)

        calls.append(True)
        return StreamHandle.open(definition)

    broker = await BrokerRuntime.open(
        config(tmp_path), streams=[Opening(definition, opening)]
    )
    await broker.close()
    assert calls == [True]


async def test_barrier_passes_dead_role_lock_free(tmp_path, definition):
    with _plan.role_lock(tmp_path / "run", "compact", "default"):
        pass

    (tmp_path / "run/status").mkdir()
    (tmp_path / "run/status/unknown.json").write_text("{}")
    broker = await BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])
    try:
        assert broker.generations["events"].accepting
        assert not (tmp_path / "run/status/unknown.json").exists()
        assert (tmp_path / "run/roles/compact.default.lock").exists()
    finally:
        await broker.close()


async def test_removal_not_published_before_ack(tmp_path, definition, monkeypatch):
    broker = await BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])
    event, _ = observe_barrier(monkeypatch)
    finished = []

    async def finalize(stream, record):
        finished.append(record["name"])

    # Exercise the managed final-publication branch using the same real handle.
    broker.static.clear()
    broker.records["events"] = {"revision": 1, "storage_profile": "default"}
    monkeypatch.setattr(broker, "finalize", finalize)
    try:
        with _plan.role_lock(tmp_path / "run", "publish", "default"):
            removal = asyncio.create_task(broker.offline({"name": "events"}))
            await at_barrier(event, removal)
            assert not removal.done()
            assert finished == []
            assert "events" in broker.generations
            plan = _plan.read_plan(tmp_path / "run")
            assert plan.roles["publish.default"].logs == ()
            _plan.write_status(
                tmp_path / "run",
                "publish",
                "default",
                _plan.Status(plan.boot_id, plan.generation),
            )
            await removal
            assert finished == ["events"]

    finally:
        await broker.close()


async def test_fenced_plan_lists_every_declared_role_group(
    tmp_path, definition, monkeypatch
):
    event, _ = observe_barrier(monkeypatch)
    options = litelink.S3Options(region="test-region")
    with _plan.role_lock(tmp_path / "run", "clean", "other"):
        task = asyncio.create_task(
            BrokerRuntime.open(
                config(tmp_path, groups={"default": None, "other": options}),
                streams=[Opening(definition)],
            )
        )
        await at_barrier(event, task)
        plan = _plan.read_plan(tmp_path / "run")
        assert plan.state == "fenced"
        assert set(plan.roles) == {
            f"{role}.{group}" for role in ROLES for group in ("default", "other")
        }
        assert all(
            not role.logs and not role.retire and not role.streams
            for role in plan.roles.values()
        )

    broker = await task
    await broker.close()


async def test_unmapped_s3_options_refused_at_startup(tmp_path, definition):
    with pytest.raises(ValueError, match="unmapped S3 options"):
        await BrokerRuntime.open(
            config(
                tmp_path, groups={"other": litelink.S3Options(region="test-region")}
            ),
            streams=[Opening(definition)],
        )


async def test_replicate_true_refused(tmp_path, definition):
    definition = Stream(
        spec=definition.spec,
        storage=Storage(
            root=definition.storage.root,
            published=str(tmp_path / "published"),
            replicate=True,
        ),
    )
    with pytest.raises(ValueError, match="replicate=True"):
        await BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])


async def test_dedicated_refused(tmp_path, definition):
    definition = Stream(
        spec=definition.spec.model_copy(
            update={
                "maintenance": definition.spec.maintenance.model_copy(
                    update={"dedicated": True}
                )
            }
        ),
        storage=definition.storage,
    )
    with pytest.raises(ValueError, match="dedicated"):
        await BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])


def test_plan_carries_every_maintain_field(tmp_path):
    policy = Maintain(
        seal_every=1,
        publish_every=2,
        compact_every=3,
        clean_every=4,
        clean_published_every=5,
        flush_every=6,
        duckdb_default_memory_mib=192,
        duckdb_default_threads=1,
        duckdb_retention_memory_mib=193,
        duckdb_retention_threads=2,
        duckdb_retention_spill_mib=194,
        duckdb_spill_root=tmp_path / "spill",
    )
    external = External(config(tmp_path, policy=policy).maintain, "boot")
    external.write("fenced", external.empty_roles())
    plan = _plan.read_plan(tmp_path / "run")
    for role in ROLES:
        spec = plan.roles[f"{role}.default"]
        assert spec.every == policy.every(role)
        assert spec.flush_every == (policy.flush_every if role in FLUSHED else None)
        for field in fields(policy):
            if field.name.startswith("duckdb_"):
                value = getattr(policy, field.name)
                assert getattr(spec, field.name) == (
                    str(value) if isinstance(value, Path) else value
                )


async def test_control_reports_role_status(tmp_path, definition):
    app = create_app(
        config(tmp_path, authorization=Grants()), streams=Opening(definition)
    )
    async with app.router.lifespan_context(app):
        broker = app.state.broker
        for role in ROLES:
            _plan.write_status(
                tmp_path / "run",
                role,
                "default",
                _plan.Status(
                    broker.boot_id,
                    broker.external.generation,
                    time.time(),
                    1,
                    {
                        str(
                            Path(broker.generations["events"].stream.log.root)
                            / "events"
                        ): _plan.Pass(time.time())
                    },
                ),
            )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://broker"
        ) as client:
            control = (await client.get("/control/v1")).json()
            stats = (await client.get("/stats")).json()

        assert control["children"]["converged"]
        assert len(control["children"]["roles"]) == 5
        assert all(
            role["last_pass"] and 0 <= role["heartbeat_age"] < 30
            for role in control["children"]["roles"].values()
        )
        assert broker.children_ready()
        assert stats["children"]["roles"]
        assert broker.child_status(counts=False, visible=set())["startup"] == {}
        assert not any(
            role["last_pass"]
            for role in broker.child_status(counts=False, visible=set())[
                "roles"
            ].values()
        )
        _plan.write_status(
            tmp_path / "run",
            "seal",
            "default",
            _plan.Status(
                broker.boot_id, broker.external.generation, time.time() - 1000
            ),
        )
        assert not broker.children_ready()
        assert broker.generations["events"].writable


async def test_startup_report_has_marks_digest(tmp_path, definition, monkeypatch):
    handle = StreamHandle.open(definition)
    await handle.send({"value": 1})
    assert handle.log is not None
    _dedupe.write_all(handle.log, [("z", 3, 1), ("a", 1, 1)])
    await handle.aclose()
    reports = []
    original = _inspect.startup_report

    def report(stream):
        result = original(stream)
        reports.append(result)
        return result

    monkeypatch.setattr("streamcast._broker.runtime.startup_report", report)
    broker = await BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])
    try:
        observed = broker.child_status(counts=False)["startup"]["events"]
        assert len(reports) == 1
        assert (
            observed["marks_sha256"]
            == hashlib.sha256(b'["a",1,1]\n["z",3,1]\n').hexdigest()
        )
        assert observed["marks_count"] == 2
        assert observed["end_offset"] == 2
    finally:
        await broker.close()


async def test_barrier_waiting_written_to_run_dir(
    tmp_path, definition, monkeypatch, caplog
):
    event, _ = observe_barrier(monkeypatch)
    caplog.set_level("INFO", logger="streamcast._external")
    original = External.broker_status
    written = asyncio.Event()
    loop = asyncio.get_running_loop()

    def write(self, state, waiting):
        original(self, state, waiting)
        if waiting:
            loop.call_soon_threadsafe(written.set)

    monkeypatch.setattr(External, "broker_status", write)
    with _plan.role_lock(tmp_path / "run", "seal", "default"):
        opening = asyncio.create_task(
            BrokerRuntime.open(config(tmp_path), streams=[Opening(definition)])
        )
        await at_barrier(event, opening)
        await at_barrier(written, opening)
        status = json.loads((tmp_path / "run/broker.json").read_bytes())
        assert status == {
            "boot_id": _plan.read_plan(tmp_path / "run").boot_id,
            "state": "barrier_waiting",
            "waiting": ["seal.default"],
        }
        assert not opening.done()

    broker = await opening
    assert any("seal.default" in record.message for record in caplog.records)
    assert json.loads((tmp_path / "run/broker.json").read_bytes())["state"] == "serving"
    await broker.close()


async def test_failed_open_releases_previous_handles_and_all_owner_locks(
    tmp_path, definition, monkeypatch
):
    other = Stream(
        spec=definition.spec.model_copy(update={"name": "other"}),
        storage=definition.storage,
    )
    opened, closed = [], []
    original = StreamHandle.aclose

    def first():
        handle = StreamHandle.open(definition)
        opened.append(handle)
        return handle

    def failure():
        raise OSError("unavailable storage")

    async def close(self):
        if self in opened:
            closed.append(self.name)

        await original(self)

    monkeypatch.setattr(StreamHandle, "aclose", close)
    with pytest.raises(OSError, match="unavailable storage"):
        await BrokerRuntime.open(
            config(tmp_path),
            streams=[Opening(definition, first), Opening(other, failure)],
        )

    assert closed == ["events"]
    for name in ("events", "other"):
        with closing(
            Lock.open(definition.storage.root / f".streamcast-owners/{name}.lock")
        ):
            pass


async def test_registry_groups_follow_storage_profile_names_and_recover(
    tmp_path, definition
):
    settings = config(
        tmp_path,
        groups={"unused": None, "archive": None},
        registry=tmp_path / "registry.db",
        authorization=Grants(),
        storage_profiles={"archive": definition.storage},
        control_apply_wait_s=0,
    )
    initialize_registry(settings)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://broker"
        ) as client:
            response = await client.put(
                "/control/v1/streams/events",
                headers={"If-None-Match": "*", "Idempotency-Key": "create-events"},
                params={
                    "storage_profile": "archive",
                    "initialization": "open_existing",
                },
                json=definition.spec.model_dump(mode="json"),
            )
            assert response.status_code == 202, response.text
            await app.state.broker.queue.join()
            response = await client.put(
                "/control/v1/streams/events/desired",
                headers={
                    "If-Match": f'"{app.state.broker.records["events"]["revision"]}"',
                    "Idempotency-Key": "serve-events",
                },
                json={"state": "serving"},
            )
            assert response.status_code == 202, response.text
            await app.state.broker.queue.join()
            plan = _plan.read_plan(tmp_path / "run")
            assert plan.roles["seal.archive"].logs
            assert not plan.roles["seal.unused"].logs

    recovered = await BrokerRuntime.open(settings)
    try:
        assert recovered.generations["events"].accepting
        assert recovered.external is not None
        assert recovered.external.reports["events"]["marks_count"] == 0
    finally:
        await recovered.close()
