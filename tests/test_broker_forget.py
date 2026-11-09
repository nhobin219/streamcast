"""Forgetting registry intent never deletes storage or reuses a name's ETags."""

from __future__ import annotations

import asyncio
import copy
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from websockets.asyncio.client import connect as raw_connect
from websockets.exceptions import ConnectionClosed

from streamcast import Stream, StreamHandle, StreamSpec, create_app
from streamcast._broker.model import Fault, PublicAccess
from streamcast._broker.registry import Lock, Registry
from tests.test_broker import ADMIN, definition, managed, put, running, serving

pytest_plugins = ["tests.test_broker"]
URL = "/control/v1/streams/events"


async def delete(client, revision, **headers):
    return await client.delete(URL, headers={"If-Match": f'"{revision}"', **headers})


async def raw_put(client, name="events", revision=None):
    return await client.put(
        f"/control/v1/streams/{name}",
        params={"storage_profile": "local", "initialization": "create_if_absent"},
        headers={"If-None-Match": "*"}
        if revision is None
        else {"If-Match": f'"{revision}"'},
        json=definition(name=name),
    )


@asynccontextmanager
async def blocked_worker(owner, monkeypatch):
    """Hold the real single worker on a different stream, after marking running."""
    entered = asyncio.Event()
    release = asyncio.Event()
    reconcile = owner.reconcile

    async def blocked(record):
        if record["name"] == "blocker":
            entered.set()
            await release.wait()

        await reconcile(record)

    monkeypatch.setattr(owner, "reconcile", blocked)
    try:
        yield entered, release
    finally:
        release.set()
        await owner.queue.join()


@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("retired", [False, True])
@pytest.mark.parametrize("pending", [None, "accepted", "running", "failed"])
async def test_forget_predicate_truth_table(config, bound, retired, pending):
    async with managed(config) as (owner, client, _):
        await put(owner, client, "events", definition())
        record = owner.records["events"]
        record["stream_id"] = "bound-identity" if bound else None
        record["desired"] = "retired" if retired else "stopped"
        record["pending"] = (
            None
            if pending is None
            else {
                "kind": "definition",
                "revision": record["revision"],
                "status": pending,
                "attempt": 1,
                "error": None,
                "target": {},
            }
        )
        owner.registry.update(record)
        answer = await delete(client, record["revision"])
        allowed = (retired and pending is None) or (not bound and pending != "running")
        if allowed:
            assert answer.status_code == 204, answer.text
            assert answer.content == b""
            assert "events" not in owner.registry.load()[0]
            assert "events" not in owner.records
        else:
            assert answer.status_code == 409
            assert answer.json()["error"]["code"] == (
                "transition_running" if pending == "running" else "not_forgettable"
            )
            assert owner.registry.load()[0]["events"] == record


async def test_accepted_unbound_forget_waiters_and_queued_worker(config, monkeypatch):
    config = replace(config, control_apply_wait_s=5)
    async with managed(config) as (owner, client, _):
        async with blocked_worker(owner, monkeypatch) as (entered, release):
            blocker = asyncio.create_task(raw_put(client, "blocker"))
            await entered.wait()
            accepted = asyncio.Event()
            joined = asyncio.Event()
            attempt = owner.attempt
            count = 0

            def watched(name):
                nonlocal count
                future = attempt(name)
                if name == "events":
                    count += 1
                    accepted.set()
                    if count == 2:
                        joined.set()

                return future

            monkeypatch.setattr(owner, "attempt", watched)
            first = asyncio.create_task(raw_put(client))
            await accepted.wait()
            revision = owner.records["events"]["revision"]
            second = asyncio.create_task(raw_put(client, revision=revision))
            await joined.wait()
            record = owner.records["events"]
            assert record["stream_id"] is None
            assert record["pending"]["status"] == "accepted"
            assert "events" in owner.queued
            shared = owner.attempts[("events", revision, 1)]
            other_attempt = asyncio.get_running_loop().create_future()
            owner.attempts[("events", revision, 99)] = other_attempt
            answer = await delete(client, revision)
            assert answer.status_code == 204
            one, two = await asyncio.gather(first, second)
            for response in (one, two):
                assert response.status_code == 410
                assert response.json()["error"] == {
                    "code": "stream_forgotten",
                    "message": "stream forgotten",
                    "retryable": False,
                    "name": "events",
                    "revision": revision,
                }
                assert "etag" not in response.headers

            assert shared.result() is other_attempt.result()
            assert shared.result() == {
                "outcome": "forgotten",
                "error": "stream_forgotten",
                "revision": revision,
                "status": {
                    "name": "events",
                    "revision": revision,
                    "pending": {
                        k: record["pending"][k]
                        for k in ("kind", "revision", "status", "attempt", "error")
                    },
                    "observed": {"state": "forgotten", "error": None},
                },
            }
            assert "events" not in owner.queued
            assert "events" not in owner.observed
            assert "events" not in owner.applied_specs
            assert not any(key[0] == "events" for key in owner.attempts)
            assert (await client.get(URL)).status_code == 404
            release.set()
            assert (await blocker).status_code == 200

        assert not owner.closing
        assert not owner.worker.done()
        assert "events" not in owner.records

    async with managed(config) as (restarted, _, _):
        assert "events" not in restarted.records


async def test_actual_running_transition_cannot_be_forgotten(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        async with blocked_worker(owner, monkeypatch) as (entered, _):
            assert (await raw_put(client, "blocker")).status_code == 202
            await entered.wait()
            record = owner.records["blocker"]
            assert record["pending"]["status"] == "running"
            answer = await client.delete(
                "/control/v1/streams/blocker",
                headers={"If-Match": f'"{record["revision"]}"'},
            )
            assert answer.status_code == 409
            assert answer.json()["error"]["code"] == "transition_running"
            assert "blocker" in owner.records


async def test_timed_out_waiter_keeps_202_then_location_is_404(config, monkeypatch):
    config = replace(config, control_apply_wait_s=0.01)
    async with managed(config) as (owner, client, _):
        async with blocked_worker(owner, monkeypatch) as (entered, _):
            await raw_put(client, "blocker")
            await entered.wait()
            response = await raw_put(client)
            assert response.status_code == 202
            revision = response.json()["revision"]
            location = response.headers["Location"]
            assert response.headers["ETag"] == f'"{revision}"'
            assert (await delete(client, revision)).status_code == 204
            assert response.status_code == 202
            missing = await client.get(location)
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "unknown_stream"


async def test_forget_preconditions_and_distinct_grant(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        await put(owner, client, "events", definition())
        record = copy.deepcopy(owner.records["events"])
        for headers, code in (
            ({}, 428),
            ({"If-Match": '"0"'}, 412),
            ({"If-Match": '"1"', "If-None-Match": "*"}, 412),
        ):
            assert (await client.delete(URL, headers=headers)).status_code == code
            assert owner.records["events"] == record

        calls = []

        async def denied(principal, action, resource):
            calls.append((principal, action, resource))
            return False

        monkeypatch.setattr(config.authorization, "allows", denied)
        denied_response = await delete(client, record["revision"])
        assert denied_response.status_code == 403
        assert calls == [("admin", "control.forget", "events")]
        assert owner.records["events"] == record
        assert not await PublicAccess().allows("public", "control.forget", "events")
        monkeypatch.undo()
        assert (await delete(client, record["revision"])).status_code == 204
        missing = await delete(client, record["revision"])
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "unknown_stream"


async def test_retired_forget_closes_subscribers_preserves_storage_and_claims(
    config, tmp_path
):
    async with managed(config) as (owner, client, app):
        stream = await serving(owner, client)
        await stream.send({"value": 7})
        await put(
            owner,
            client,
            "events",
            {"state": "retired"},
            suffix="/desired",
            revision=owner.records["events"]["revision"],
        )
        stream = owner.generations["events"].stream
        assert stream.retirement is not None
        await put(owner, client, "other", definition())
        shared_path = next(
            path for path, names in owner.claims.items() if names == {"events"}
        )
        owner.claim_path(shared_path, "other")
        shared_lock = owner.locks[shared_path]
        exclusive_paths = set(owner.locks) - {shared_path}
        root = config.storage_profiles["local"].root
        before = {
            path.relative_to(root): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }
        mark = owner.registry.high_water()
        async with (
            running(app) as uri,
            raw_connect(uri + "/events", additional_headers=ADMIN) as socket,
        ):
            await socket.recv()  # greeting proves the retired generation serves
            assert (
                await delete(client, owner.records["events"]["revision"])
            ).status_code == 204
            async with asyncio.timeout(1):
                with pytest.raises(ConnectionClosed) as closed:
                    await socket.recv()

            assert closed.value.rcvd is not None
            assert closed.value.rcvd.code == 1001

        assert "events" not in owner.generations
        assert owner.registry.high_water() == mark
        assert {
            path.relative_to(root): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        } == before
        assert owner.locks[shared_path] is shared_lock
        with pytest.raises(BlockingIOError):
            Lock.open(shared_path)

        for path in exclusive_paths:
            if path not in owner.locks:
                lock = Lock.open(path)
                lock.close()

        assert all("events" not in names for names in owner.claims.values())
        assert (await client.get(URL)).status_code == 404


async def test_static_claim_survives_forget(config, tmp_path):
    static = StreamHandle.open(
        Stream(
            spec=StreamSpec(name="static", schema=definition()["schema"]),
            storage=config.storage_profiles["local"],
        ),
        initialization="create_if_absent",
    )
    app = create_app(config, streams=static)
    async with app.router.lifespan_context(app):
        owner = app.state.broker
        accepted = await owner.mutate(
            "events", "definition", definition(), None, "*", storage_profile="local"
        )
        await owner.queue.join()
        path = next(path for path, names in owner.claims.items() if names == {"static"})
        owner.claim_path(path, "events")
        await owner.forget("events", f'"{accepted["status"]["revision"]}"', None)
        assert owner.claims[path] == {"static"}
        with pytest.raises(BlockingIOError):
            Lock.open(path)

        with pytest.raises(Fault, match="static_stream"):
            await owner.forget("static", '"1"', None)


async def test_unbound_forget_never_opens_storage(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        await put(owner, client, "events", definition())
        root = config.storage_profiles["local"].root
        assert not (root / "events").exists()

        def forbidden(*_args, **_kwargs):
            pytest.fail("forget accessed storage")

        monkeypatch.setattr(owner, "recorded_storage", forbidden)
        monkeypatch.setattr(owner, "claim_storage", forbidden)
        monkeypatch.setattr(owner, "open_stream", forbidden)
        monkeypatch.setattr(owner, "storage_transition", forbidden)
        monkeypatch.setattr(owner, "offline", forbidden)
        assert (
            await delete(client, owner.records["events"]["revision"])
        ).status_code == 204
        assert not (root / "events").exists()


async def test_high_water_new_recreated_and_stale_each_revision(config):
    async with managed(config) as (owner, client, _):
        revisions = []
        marks = []
        for backlog in (10, 11, 12):
            response = await put(
                owner,
                client,
                "events",
                definition(max_backlog=backlog),
                revision=revisions[-1] if revisions else None,
            )
            assert response.status_code == 202
            revisions.append(response.json()["revision"])
            marks.append(owner.registry.high_water())

        mark = owner.registry.high_water()
        assert (await delete(client, revisions[-1])).status_code == 204
        assert owner.registry.high_water() == mark
        assert owner.registry.load()[1] == owner.collection
        response = await put(owner, client, "events", definition())
        recreated = response.json()["revision"]
        for stale in revisions:
            rejected = await put(
                owner, client, "events", definition(max_backlog=15), revision=stale
            )
            assert rejected.status_code == 412, (stale, recreated, rejected.text)

        assert marks == revisions
        assert revisions[1:] == [revisions[0] + 1, revisions[0] + 2]
        assert recreated == mark + 1
        assert recreated > revisions[-1]
        mark = owner.registry.high_water()
        next_record = await put(owner, client, "other", definition())
        assert next_record.json()["revision"] == mark + 1
        assert owner.registry.high_water() == mark + 1

    async with managed(config) as (owner, client, _):
        mark = owner.registry.high_water()
        next_record = await put(owner, client, "third", definition())
        assert next_record.json()["revision"] == mark + 1


class RejectMark(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        if sql.startswith("UPDATE high_water"):
            raise OSError("injected mark failure")

        return super().execute(sql, *args, **kwargs)


def test_high_water_and_record_commit_atomically():
    db = sqlite3.connect(":memory:", factory=RejectMark)
    Registry.initialize(db, "test")
    registry = Registry(db, None)
    try:
        with pytest.raises(OSError, match="injected mark failure"):
            registry.accept({"name": "events", "revision": 3})

        assert registry.load() == ({}, 0)
        assert registry.high_water() == 0
    finally:
        registry.close()


class RejectCollection(sqlite3.Connection):
    reject = False

    def execute(self, sql, *args, **kwargs):
        if self.reject and sql == "UPDATE collection SET revision=revision+1":
            raise OSError("injected collection failure")

        return super().execute(sql, *args, **kwargs)


def test_forget_and_collection_commit_atomically():
    db = sqlite3.connect(":memory:", factory=RejectCollection)
    Registry.initialize(db, "test")
    registry = Registry(db, None)
    try:
        record = {"name": "events", "revision": 3}
        registry.accept(record)
        db.reject = True
        with pytest.raises(OSError, match="injected collection failure"):
            registry.forget("events")

        assert registry.load() == ({"events": record}, 1)
        assert registry.high_water() == 3
    finally:
        registry.close()
