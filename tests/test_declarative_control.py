"""Durable stream intent, independent HTTP waits, and bounded recovery."""

from __future__ import annotations

import asyncio
import copy
import threading
from dataclasses import replace

import pyarrow as pa
import pytest

from streamcast import _log, _metadata
from streamcast._broker import api
from streamcast._broker.model import Fault
from streamcast._broker.registry import Registry
from streamcast._broker.runtime import BrokerRuntime
from tests.test_broker import Grants, definition, managed, put, serving

pytest_plugins = ["tests.test_broker"]
BASE = "/control/v1/streams/"


async def request(client, name="events", body=None, revision=None, suffix=""):
    return await client.put(
        BASE + name + suffix,
        json=definition(name=name) if body is None else body,
        headers={"If-None-Match": "*"}
        if revision is None
        else {"If-Match": f'"{revision}"'},
        params={"storage_profile": "local", "initialization": "create_if_absent"}
        if not suffix
        else {},
    )


def hold(monkeypatch, owner, method="reconcile"):
    entered, release = asyncio.Event(), asyncio.Event()
    original = getattr(owner, method)
    calls = []

    async def paused(*args):
        calls.append(copy.deepcopy(args[0]))
        entered.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(owner, method, paused)
    return entered, release, calls


async def test_fast_changes_noop_and_stale_recovery(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        created = await request(client)
        assert created.status_code == 200
        assert created.json()["pending"] is None
        assert created.json()["observed"] == {"state": "stopped", "error": None}
        assert (await request(client)).status_code == 412
        serving_response = await request(
            client, body={"state": "serving"}, revision=1, suffix="/desired"
        )
        assert serving_response.status_code == 200
        assert serving_response.json()["observed"]["state"] == "serving"
        stopped = await request(
            client, body={"state": "stopped"}, revision=2, suffix="/desired"
        )
        assert stopped.status_code == 200
        assert stopped.headers["etag"] == '"3"'
        assert (
            await request(
                client, body={"state": "stopped"}, revision=2, suffix="/desired"
            )
        ).status_code == 412
        changed = definition(max_backlog=42)
        applied = await request(client, body=changed, revision=3)
        assert applied.status_code == 200
        assert (await request(client, body=changed, revision=3)).status_code == 412
        current = await client.get(BASE + "events")
        assert current.json()["stream"]["spec"]["max_backlog"] == 42
        assert current.json()["pending"] is None
        assert current.headers["etag"] == '"4"'
        before = copy.deepcopy(owner.records), owner.collection

        def forbidden(*_):
            pytest.fail("satisfied no-op wrote to registry")

        monkeypatch.setattr(owner.registry, "accept", forbidden)
        monkeypatch.setattr(owner.registry, "update", forbidden)
        noop = await request(client, body=changed, revision=4)
        assert noop.status_code == 200
        assert noop.json()["revision"] == 4
        assert (owner.records, owner.collection) == before
        monkeypatch.undo()


async def test_default_maintenance_pending_target_and_noop(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=0)) as (owner, client, _):
        entered, release, _calls = hold(monkeypatch, owner)
        try:
            created = await request(
                client, body=definition(maintenance={"dedicated": False})
            )
            assert created.status_code == 202
            await entered.wait()
            record = owner.records["events"]
            assert "maintenance" not in record["stream"]["spec"]
            assert "maintenance" not in record["pending"]["target"]["spec"]
            saved, collection = owner.registry.load()
            assert saved == owner.records
            joined = await request(client, revision=1)
            assert joined.status_code == 202
            assert joined.json()["revision"] == 1
            assert owner.registry.load() == (saved, collection)
        finally:
            release.set()

        await owner.attempt("events")
        noop = await request(client, body=definition(maintenance={}), revision=1)
        assert noop.status_code == 200
        assert noop.json()["revision"] == 1
        assert owner.registry.load()[1] == collection


async def test_wait_outside_acceptance_and_serial_lock(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5, max_pending=1)) as (
        owner,
        client,
        _,
    ):
        entered, release, calls = hold(monkeypatch, owner)
        first = asyncio.create_task(request(client))
        await entered.wait()
        accepted = asyncio.Event()
        mutate = owner.mutate

        async def watched(*args, **kwargs):
            answer = await mutate(*args, **kwargs)
            accepted.set()
            return answer

        monkeypatch.setattr(owner, "mutate", watched)
        # The first intent fills the only slot, so finish acceptance of the
        # second without waiting on the first HTTP request to return.
        second = asyncio.create_task(request(client, "other"))
        try:
            async with asyncio.timeout(2):
                answer = await second

            assert answer.status_code == 429
            assert not first.done()
            assert not owner.serial.locked()
        finally:
            release.set()
            await first

    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        entered, release, calls = hold(monkeypatch, owner)
        first = asyncio.create_task(request(client, "one"))
        await entered.wait()
        accepted = asyncio.Event()
        mutate = owner.mutate

        async def watched_second(*args, **kwargs):
            answer = await mutate(*args, **kwargs)
            accepted.set()
            return answer

        monkeypatch.setattr(owner, "mutate", watched_second)
        second = asyncio.create_task(request(client, "two"))
        try:
            async with asyncio.timeout(2):
                await accepted.wait()

            assert "two" in owner.records
            assert "two" in owner.queued
            assert not first.done()
        finally:
            release.set()
            answers = await asyncio.gather(first, second)

        assert [r.status_code for r in answers] == [200, 200]
        assert len(calls) == 2
        assert not owner.attempts


async def test_zero_timeout_and_waiter_bounds(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        entered, release, _ = hold(monkeypatch, owner)
        answer = await request(client)
        assert answer.status_code == 202
        assert answer.headers["location"] == BASE + "events"
        await entered.wait()
        release.set()
        await owner.queue.join()
        assert owner.records["events"]["pending"] is None

    async with managed(replace(config, control_apply_wait_s=0.01)) as (
        owner,
        client,
        _,
    ):
        entered, release, _ = hold(monkeypatch, owner)
        answer = await request(client, "timeout")
        assert answer.status_code == 202
        await entered.wait()
        future = next(iter(owner.attempts.values()))
        assert not future.cancelled()
        release.set()
        await owner.queue.join()
        assert owner.records["timeout"]["pending"] is None
        assert not owner.attempts
        assert not owner.worker.done()

    async with managed(replace(config, control_apply_wait_s=5, max_waiters=1)) as (
        owner,
        client,
        _,
    ):
        entered, release, _ = hold(monkeypatch, owner)
        first = asyncio.create_task(request(client, "waiter"))
        await entered.wait()
        try:
            assert owner.waiters == 1
            current = await client.get(BASE + "waiter")
            assert current.status_code == 200
            overflow = await request(
                client, "waiter", revision=current.json()["revision"]
            )
            assert overflow.status_code == 202
            assert owner.waiters == 1
        finally:
            release.set()
            assert (await first).status_code == 200

        assert owner.waiters == 0


async def test_cancel_one_joiner_keeps_other_and_worker_alive(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        entered, release, calls = hold(monkeypatch, owner)
        first = asyncio.create_task(request(client))
        await entered.wait()
        joined = asyncio.Event()
        attempt = owner.attempt

        def watched(name):
            future = attempt(name)
            joined.set()
            return future

        monkeypatch.setattr(owner, "attempt", watched)
        second = asyncio.create_task(request(client, revision=1))
        await joined.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        try:
            assert not next(iter(owner.attempts.values())).cancelled()
        finally:
            release.set()

        assert (await second).status_code == 200
        assert len(calls) == 1
        assert owner.records["events"]["pending"] is None
        assert (await request(client, "next")).status_code == 200
        assert not owner.worker.done()


@pytest.mark.parametrize("equal", [False, True])
async def test_two_old_etags_accept_only_one(config, monkeypatch, equal):
    async with managed(config) as (owner, client, _):
        await put(owner, client, "events", definition())
        entered, release, _ = hold(monkeypatch, owner)
        first, second = await asyncio.gather(
            request(client, body=definition(max_backlog=42), revision=1),
            request(
                client, body=definition(max_backlog=42 if equal else 43), revision=1
            ),
        )
        try:
            assert sorted([first.status_code, second.status_code]) == [202, 412]
            await entered.wait()
            current = await client.get(BASE + "events")
            assert current.json()["pending"]["target"]["spec"]["max_backlog"] in (
                42,
                43,
            )
            assert current.headers["etag"] == '"2"'
            joined = await request(
                client,
                body=definition(
                    max_backlog=owner.records["events"]["stream"]["spec"]["max_backlog"]
                ),
                revision=2,
            )
            assert joined.status_code == 202
            assert len(owner.attempts) == 1
        finally:
            release.set()
            await owner.queue.join()


@pytest.mark.parametrize("failed", [False, True])
async def test_pending_migration_guards_and_single_retry(config, monkeypatch, failed):
    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        identity = stream._stream_id
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        active = copy.deepcopy(owner.records["events"]["stream"]["spec"])
        shape = copy.deepcopy(active["schema"])
        shape["properties"]["extra"] = {"type": ["string", "null"]}
        target = {"stream_id": identity, "schema": shape, "sort_by": []}
        entered, release, calls = hold(monkeypatch, owner, "storage_transition")
        original = owner.storage_transition

        async def broken(*_):
            raise OSError("injected")

        if failed:
            monkeypatch.setattr(owner, "storage_transition", broken)

        accepted = await request(client, body=target, revision=3, suffix="/schema")
        assert accepted.status_code == 202
        if failed:
            await owner.queue.join()
            assert owner.records["events"]["pending"]["status"] == "failed"
            monkeypatch.setattr(owner, "storage_transition", original)
        else:
            await entered.wait()

        try:
            before = copy.deepcopy(owner.records["events"])
            for suffix, body in (
                ("/schema", target | {"schema": active["schema"]}),
                ("/desired", {"state": "stopped"}),
                ("", active),
            ):
                refused = await request(client, body=body, revision=4, suffix=suffix)
                assert refused.status_code == 409
                assert refused.json()["error"]["code"] == "transition_pending"

            malformed = await request(
                client, body=target, revision=4, suffix="/desired"
            )
            assert malformed.status_code == 422
            assert malformed.json()["error"]["code"] == "invalid_desired"
            assert owner.records["events"] == before
            joined = await asyncio.gather(
                *[
                    request(client, body=target, revision=4, suffix="/schema")
                    for _ in range(2)
                ]
            )
            assert [r.status_code for r in joined] == [202, 202]
            await entered.wait()
            pending = owner.records["events"]["pending"]
            assert pending["revision"] == 4
            assert pending["attempt"] == (2 if failed else 1)
            assert len(calls) == 1
            assert owner.records["events"]["stream"]["spec"] == active
        finally:
            release.set()
            await owner.queue.join()

        assert owner.records["events"]["pending"] is None
        assert owner.records["events"]["stream"]["spec"]["schema"] == shape


async def test_same_schema_executes_migration(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        assert (await request(client)).status_code == 200
        assert (
            await request(
                client, body={"state": "serving"}, revision=1, suffix="/desired"
            )
        ).status_code == 200
        identity = owner.records["events"]["stream_id"]
        assert (
            await request(
                client, body={"state": "stopped"}, revision=2, suffix="/desired"
            )
        ).status_code == 200
        extra = "streamcast_future"
        monkeypatch.setattr(
            _log,
            "SYSTEM",
            {**_log.SYSTEM, extra: {"type": "integer", "format": "int64"}},
        )
        monkeypatch.setattr(
            _log,
            "_SYSTEM_ARROW",
            _log._SYSTEM_ARROW.append(pa.field(extra, pa.int64(), nullable=False)),
        )
        calls = []
        original = owner.storage_transition

        async def watched(*args):
            calls.append(args)
            await original(*args)

        monkeypatch.setattr(owner, "storage_transition", watched)
        result = await request(
            client,
            body={
                "stream_id": identity,
                "schema": definition()["schema"],
                "sort_by": [],
            },
            revision=3,
            suffix="/schema",
        )
        assert result.status_code == 200
        assert result.json()["revision"] == 4
        assert len(calls) == 1
        metadata = _metadata.load(
            _metadata.home(config.storage_profiles["local"].root, "events"), "events"
        )
        assert metadata is not None
        properties = metadata.current.system_schema["properties"]
        assert isinstance(properties, dict)
        assert extra in properties
        assert metadata.current.name == "events-v2"


@pytest.mark.parametrize("fail", [False, True])
async def test_equal_intent_repairs_at_same_revision(config, monkeypatch, fail):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        await request(client)
        owner.observe("events", "error", error="recovery_failed")
        before = owner.collection
        calls = []
        original = owner.reconcile

        async def repair(record):
            calls.append(copy.deepcopy(record))
            if fail:
                raise Fault(422, "unknown_storage_profile")

            await original(record)

        monkeypatch.setattr(owner, "reconcile", repair)
        result = await request(client, revision=1)
        assert result.status_code == (500 if fail else 200)
        assert result.headers["etag"] == '"1"'
        assert calls[0]["pending"]["revision"] == 1
        assert owner.collection == before
        if fail:
            assert result.json()["error"]["code"] == "unknown_storage_profile"
            assert result.json()["error"]["retryable"] is True
        else:
            assert result.json()["pending"] is None


@pytest.mark.parametrize("pending_status", [None, "failed"])
async def test_execute_ignores_ineligible_slot(config, monkeypatch, pending_status):
    async with managed(config) as (owner, client, _):
        await put(owner, client, "events", definition())
        if pending_status:
            owner.records["events"]["pending"] = {
                "kind": "desired",
                "revision": 1,
                "attempt": 1,
                "target": {"state": "stopped"},
                "error": "failed",
                "status": pending_status,
            }

        before = copy.deepcopy(owner.records)
        future = asyncio.get_running_loop().create_future()
        owner.attempts[("events", 1, 1)] = future

        def forbidden(*_):
            pytest.fail("ineligible execute had an effect")

        async def no_transition(*_):
            forbidden()

        monkeypatch.setattr(owner.registry, "update", forbidden)
        monkeypatch.setattr(owner, "reconcile", no_transition)
        await owner.execute("events")
        assert owner.records == before
        assert not future.done()
        monkeypatch.undo()


async def test_waiter_keeps_own_result_when_next_revision_pending(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5, max_waiters=1)) as (
        owner,
        client,
        _,
    ):
        completed, resume = asyncio.Event(), asyncio.Event()
        wait_for = asyncio.wait_for

        async def delayed(*args, **kwargs):
            result = await wait_for(*args, **kwargs)
            completed.set()
            await resume.wait()
            return result

        monkeypatch.setattr(api.asyncio, "wait_for", delayed)
        first = asyncio.create_task(request(client))
        await completed.wait()
        entered, release, _ = hold(monkeypatch, owner)
        try:
            later = await request(client, body=definition(max_backlog=42), revision=1)
            assert later.status_code == 202
            await entered.wait()
            assert owner.records["events"]["pending"]["revision"] == 2
            resume.set()
            own = await first
            assert own.status_code == 200
            assert own.headers["etag"] == '"1"'
            assert own.json()["pending"] is None
        finally:
            resume.set()
            release.set()
            await owner.queue.join()


class Mutator(Grants):
    async def allows(self, principal, action, resource):
        return action == "control.mutate"


async def test_mutate_only_responses_schema_auth_and_removed_routes(
    config, monkeypatch
):
    async with managed(
        replace(config, authorization=Mutator(), control_apply_wait_s=5)
    ) as (owner, client, _):
        assert (
            await client.put(BASE + "events/schema", content=b"invalid")
        ).status_code == 403
        assert (await client.get("/control/v1/operations/anything")).status_code == 404
        assert (
            await client.post(BASE + "events/migrations", content=b"invalid")
        ).status_code == 404
        assert (await client.get(BASE + "events")).status_code == 403
        created = await request(client)
        assert created.status_code == 200
        assert set(created.json()) == {"name", "revision", "pending", "observed"}
        assert "stream" not in created.json() and "target" not in created.text
        entered, release, _ = hold(monkeypatch, owner)
        # No waiter slots: mutation remains available and returns sanitized 202.
        object.__setattr__(owner.config, "max_waiters", 0)
        queued = await request(client, "other")
        assert queued.status_code == 202
        assert set(queued.json()["pending"]) == {
            "kind",
            "revision",
            "status",
            "attempt",
            "error",
        }
        assert "target" not in queued.text and "storage" not in queued.text
        await entered.wait()
        release.set()
        await owner.queue.join()
        object.__setattr__(owner.config, "max_waiters", 256)

        async def broken(*_):
            raise OSError("sensitive storage path")

        monkeypatch.setattr(owner, "reconcile", broken)
        failed = await request(client, "failed")
        assert failed.status_code == 500
        assert failed.json()["error"]["retryable"] is True
        assert "storage" not in failed.text and "target" not in failed.text
        assert failed.json()["error"]["name"] == "failed"
        revision = queued.json()["revision"] + 1
        assert failed.json()["error"]["revision"] == revision
        assert failed.headers["etag"] == f'"{revision}"'
        # Supplied keys have no validation or durable identity semantics.
        keyed = await client.put(
            BASE + "keyed",
            json=definition(name="keyed"),
            params={"storage_profile": "local"},
            headers={"If-None-Match": "*", "Idempotency-Key": " "},
        )
        assert keyed.status_code == 500


@pytest.mark.parametrize(
    "changes",
    [
        {"control_apply_wait_s": -1},
        {"control_apply_wait_s": float("nan")},
        {"control_apply_wait_s": float("inf")},
        {"max_streams": 0},
        {"max_waiters": -1},
    ],
)
def test_config_refuses_invalid_bounds(config, changes):
    with pytest.raises(ValueError):
        replace(config, **changes)


async def test_stream_bound_preserves_existing_streams(config):
    async with managed(config) as (owner, client, _):
        await serving(owner, client)
        await put(owner, client, "other", definition())

    async with managed(replace(config, max_streams=1, control_apply_wait_s=5)) as (
        owner,
        client,
        _,
    ):
        assert await owner.generations["events"].stream.send({"value": 7}) == 1
        before = owner.collection
        result = await request(client, "overflow")
        assert result.status_code == 409
        assert result.json()["error"]["code"] == "stream_limit"
        assert result.json()["error"]["limit"] == 1
        assert "overflow" not in owner.records
        assert owner.collection == before


@pytest.mark.parametrize("kind", ["definition", "desired", "migration"])
@pytest.mark.parametrize("status", ["accepted", "running", "failed"])
async def test_startup_resumes_every_slot(config, monkeypatch, kind, status):
    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        identity = stream._stream_id
        await put(
            owner, client, "events", {"state": "stopped"}, revision=2, suffix="/desired"
        )
        record = copy.deepcopy(owner.records["events"])

    targets = {
        "definition": {
            "spec": record["stream"]["spec"],
            "storage_profile": "local",
            "initialization": "create_if_absent",
        },
        "desired": {"state": "stopped"},
        "migration": {
            "stream_id": identity,
            "schema": record["stream"]["spec"]["schema"],
            "sort_by": [],
        },
    }
    record["pending"] = {
        "kind": kind,
        "revision": 3,
        "attempt": 1,
        "status": status,
        "error": "transition_failed" if status == "failed" else None,
        "target": targets[kind],
    }
    registry = Registry.open(config.registry, config.broker_id)
    registry.update(record)
    registry.close()
    calls = []
    execute = BrokerRuntime.execute

    async def watched(self, name):
        calls.append(copy.deepcopy(self.records[name]["pending"]))
        await execute(self, name)

    monkeypatch.setattr(BrokerRuntime, "execute", watched)
    async with managed(config) as (owner, _, _):
        assert len(calls) == 1
        assert calls[0]["status"] == "accepted"
        assert owner.records["events"]["pending"] is None
        assert owner.observed["events"]["state"] == "stopped"


async def test_worker_exit_resolves_wait_unknown_then_startup_recovers(
    config, monkeypatch
):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        entered, release, _ = hold(monkeypatch, owner)
        waiting = asyncio.create_task(request(client))
        await entered.wait()
        owner.worker.cancel()
        await asyncio.gather(owner.worker, return_exceptions=True)
        result = await waiting
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "control_unavailable"
        assert result.headers["etag"] == '"1"'
        assert owner.records["events"]["pending"] is not None
        # No handles were opened. Simulate process teardown before reopening.
        owner.worker = None

    async with managed(config) as (owner, _, _):
        assert owner.records["events"]["pending"] is None
        assert owner.observed["events"]["state"] == "stopped"


async def test_stop_publication_failure_answers_500(config, monkeypatch):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        await request(client)
        await request(client, body={"state": "serving"}, revision=1, suffix="/desired")
        original = owner.finalize

        async def broken(*_):
            raise OSError("publication failed")

        monkeypatch.setattr(owner, "finalize", broken)
        result = await request(
            client, body={"state": "stopped"}, revision=2, suffix="/desired"
        )
        assert result.status_code == 500
        assert result.json()["error"]["code"] == "transition_failed"
        assert result.headers["etag"] == '"3"'
        assert owner.records["events"]["pending"]["status"] == "failed"
        monkeypatch.setattr(owner, "finalize", original)


@pytest.mark.parametrize("worker_exit", [False, True])
async def test_unavailable_resolves_acceptance_already_committing(
    config, monkeypatch, worker_exit
):
    async with managed(replace(config, control_apply_wait_s=5)) as (owner, client, _):
        committing = threading.Event()
        finish_commit = threading.Event()
        closing = asyncio.Event()
        accept = owner.registry.accept
        unavailable = owner.unavailable

        def paused(record):
            accept(record)
            committing.set()
            assert finish_commit.wait(5)

        def watched():
            unavailable()
            closing.set()

        monkeypatch.setattr(owner.registry, "accept", paused)
        monkeypatch.setattr(owner, "unavailable", watched)
        entered, release, _ = hold(monkeypatch, owner)
        writing = asyncio.create_task(request(client))
        assert await asyncio.to_thread(committing.wait, 5)
        if worker_exit:
            owner.worker.cancel()
            await asyncio.gather(owner.worker, return_exceptions=True)
            owner.worker = None
            shutdown = None
        else:
            shutdown = asyncio.create_task(owner.close())

        await closing.wait()
        finish_commit.set()
        try:
            if not worker_exit:
                await entered.wait()

            async with asyncio.timeout(2):
                response = await writing

            assert response.status_code == 503
            assert response.json()["error"]["code"] == "control_unavailable"
            assert response.headers["etag"] == '"1"'
            assert not owner.attempts
            if shutdown is not None:
                assert not shutdown.done()
            else:
                assert owner.records["events"]["pending"] is not None
        finally:
            finish_commit.set()
            release.set()
            if shutdown is not None:
                await shutdown
