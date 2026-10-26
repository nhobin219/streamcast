"""Managed ownership and the controller contract, with real storage and sockets.

Most cases replace only the subprocess supervisor, keeping the registry, log,
wire clients and ASGI lifespan real. The child-readiness case uses real roles.
Concurrency cases rendezvous at the operation under test rather than sleeping.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

import httpx
import litelink
import pytest
import uvicorn
from websockets.asyncio.client import connect as raw_connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

import streamcast
from streamcast import (
    BrokerConfig,
    HistoryGuard,
    StorageProfile,
    create_app,
    initialize_registry,
)
from streamcast._broker import runtime as implementation
from streamcast._broker.registry import Registry
from streamcast._children import _supervisors
from streamcast._replicate import Sidecar

from .test_replicate import settle

SHAPE: dict[str, Any] = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
}
ADMIN = {"Authorization": "Basic " + base64.b64encode(b"admin:secret").decode()}
READER = {"Authorization": "Basic " + base64.b64encode(b"reader:secret").decode()}


@asynccontextmanager
async def running(app):
    # managed() above is the top-level lifespan owner. This socket server
    # composes that already-running application rather than entering twice.
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=0, lifespan="off", log_level="critical"
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    pytest.fail("server stopped before binding")

                await asyncio.sleep(0.001)

        yield f"ws://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    finally:
        server.should_exit = True
        await task


class Grants:
    async def authenticate(self, scope):
        header = dict(scope["headers"]).get(b"authorization")
        for name, headers in (("admin", ADMIN), ("reader", READER)):
            if header == headers["Authorization"].encode():
                return name

        return None

    async def allows(self, principal, action, resource):
        return principal == "admin" or action in ("data.read", "stats.read")


class Child:
    def __init__(self):
        self.ready = False
        self._ready_file = None
        self.stopped = False

    def start(self):
        self.ready = True

    def terminate(self):
        self.ready = False
        self.stopped = True

    async def wait_closed(self):
        assert self.stopped


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setattr(implementation, "_supervisors", lambda *_: [Child()])
    config = BrokerConfig(
        "test",
        tmp_path / "registry.db",
        {"local": StorageProfile("1", tmp_path / "logs")},
        Grants(),
        readiness_every=0.01,
        readiness_timeout=2,
    )
    initialize_registry(config)
    return config


def definition(**changes):
    return {
        "schema": copy.deepcopy(SHAPE),
        "storage_profile": "local",
        "initialization": "create_if_absent",
        **changes,
    }


@asynccontextmanager
async def managed(config):
    app = create_app(config)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers=ADMIN,
        ) as client:
            yield app.state.broker, client, app


async def put(owner, client, name, value, *, suffix="", revision=None, key=None):
    headers = {"Idempotency-Key": key or f"{name}-{suffix}-{revision}"}
    headers["If-None-Match" if revision is None else "If-Match"] = (
        "*" if revision is None else f'"{revision}"'
    )
    method = client.post if suffix == "/migrations" else client.put
    answer = await method(
        f"/control/v1/streams/{name}{suffix}", json=value, headers=headers
    )
    if answer.status_code in (200, 202):
        await owner.queue.join()

    return answer


async def serving(owner, client, name="events", **changes):
    created = await put(owner, client, name, definition(**changes))
    assert created.status_code == 202, created.text
    started = await put(
        owner, client, name, {"state": "serving"}, suffix="/desired", revision=1
    )
    assert started.status_code == 202, started.text
    assert owner.view(name)["observed"]["state"] == "serving"
    return owner.generations[name].stream


async def test_create_stopped_identity_and_restart(config):
    async with managed(config) as (owner, client, _):
        answer = await put(owner, client, "events", definition())
        assert answer.status_code == 202
        assert owner.view("events")["observed"]["state"] == "stopped"
        assert not (config.profiles["local"].root / "events").exists()
        await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=1
        )
        stream = owner.generations["events"].stream
        assert await stream.send({"value": 9}) == 1
        identity = stream._stream_id
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        assert "events" not in owner.generations
        assert owner.view("events")["stream_id"] == identity

    async with managed(config) as (owner, client, _):
        await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=3
        )
        stream = owner.generations["events"].stream
        assert stream._stream_id == identity
        assert stream.end_offset == 2
        assert await stream.send({"value": 10}) == 2


async def test_receipts_precede_stale_etag_and_are_durable(config):
    async with managed(config) as (owner, client, _):
        first = await put(owner, client, "events", definition(), key="create")
        await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=1
        )
        repeated = await put(owner, client, "events", definition(), key="create")
        assert repeated.status_code == 202
        assert repeated.json() == first.json()
        assert repeated.headers["location"] == first.headers["location"]
        mismatch = await put(
            owner, client, "events", definition(exactly_once=True), key="create"
        )
        assert mismatch.status_code == 409
        assert owner.view("events")["revision"] == 2

    async with managed(config) as (owner, client, _):
        repeated = await put(owner, client, "events", definition(), key="create")
        assert repeated.json() == first.json()
        collection = owner.collection
        noop = await put(
            owner,
            client,
            "events",
            {"state": "serving"},
            suffix="/desired",
            revision=2,
            key="noop",
        )
        assert noop.status_code == 200
        assert noop.json()["revision"] == 2
        assert owner.collection == collection


async def test_auth_before_body_lookup_and_upgrade(config):
    async with managed(config) as (owner, client, app):
        for target in (
            "/control/v1/streams/missing",
            "/control/v1/operations/missing",
            "/stats",
        ):
            response = await client.get(target, headers={"Authorization": "invalid"})
            assert response.status_code == 401
            assert response.headers["www-authenticate"].startswith("Basic")

        response = await client.put(
            "/control/v1/streams/missing", content=b"not json", headers=READER
        )
        assert response.status_code == 403
        assert not owner.records
        async with running(app) as base:
            for path, headers, status in (
                ("missing", {}, 401),
                ("missing?publish", READER, 403),
            ):
                with pytest.raises(InvalidStatus) as caught:
                    async with raw_connect(
                        f"{base}/{path}", additional_headers=headers
                    ):
                        pass

                assert caught.value.response.status_code == status


async def test_schema_validation_preconditions_and_order(config):
    async with managed(config) as (owner, client, _):
        missing = await client.put(
            "/control/v1/streams/events",
            json=definition(),
            headers={"If-None-Match": "*"},
        )
        assert missing.status_code == 428
        assert not owner.operations
        response = await client.put(
            "/control/v1/streams/events",
            json=definition(),
            headers={"Idempotency-Key": "missing-precondition"},
        )
        assert response.status_code == 428
        response = await put(
            owner,
            client,
            "events",
            definition(schema={**SHAPE, "unknown": True}),
            key="invalid",
        )
        assert response.status_code == 422
        shape = {
            "type": "object",
            "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
            "required": ["a", "b"],
        }
        await put(owner, client, "events", definition(schema=shape))
        reordered = {
            **shape,
            "properties": dict(reversed(list(shape["properties"].items()))),
        }
        rejected = await put(
            owner,
            client,
            "events",
            definition(schema=reordered),
            revision=1,
            key="reorder",
        )
        assert rejected.status_code == 409
        assert rejected.json()["error"]["code"] == "migration_required"
        conflict = await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=0
        )
        assert conflict.status_code == 412


async def test_pagination_invalidates_on_collection_change(config):
    async with managed(config) as (owner, client, _):
        for name in ("a", "b"):
            await put(owner, client, name, definition())

        page = (await client.get("/control/v1/streams?limit=1")).json()
        assert [s["name"] for s in page["streams"]] == ["a"]
        assert page["next_cursor"]
        following = (
            await client.get(
                "/control/v1/streams", params={"cursor": page["next_cursor"]}
            )
        ).json()
        assert [s["name"] for s in following["streams"]] == ["b"]
        await put(owner, client, "c", definition())
        stale = await client.get(
            "/control/v1/streams", params={"cursor": page["next_cursor"]}
        )
        assert stale.status_code == 409


async def test_wire_consumer_contract_and_separate_grants(config):
    async with managed(config) as (owner, client, app):
        stream = await serving(
            owner, client, exactly_once=True, replay_published=True, max_replay=None
        )
        async with running(app) as base:
            uri = base.replace("ws://", "ws://admin:secret@") + "/events"
            async with (
                raw_connect(uri) as subscriber,
                raw_connect(uri + "?publish&publisher=producer") as publisher,
            ):
                greeting = json.loads(await publisher.recv())
                assert greeting["publisher"]["last_seq"] == 0
                assert greeting["streamcast"] == 4
                await subscriber.recv()
                await publisher.send(json.dumps([1, {"value": 12}]))
                acknowledgement = await publisher.recv()
                live = await subscriber.recv()
                assert isinstance(live, str)
                assert json.loads(live)[0::2] == [1, {"value": 12}]
                await publisher.send(json.dumps([1, {"value": 12}]))
                assert json.loads(acknowledgement) == {"ok": [1]}
                assert json.loads(await publisher.recv()) == {"ok": [1], "dup": 1}
                assert stream.end_offset == 2

            async with raw_connect(uri + "?offset=0") as replay:
                info = json.loads(await replay.recv())
                assert info["stream_id"] == stream._stream_id
                assert await replay.recv() == live

            response = await client.get(
                base.replace("ws://", "http://") + "/stats", headers=READER
            )
            assert response.status_code == 200
            assert response.json()["streams"][0]["end_offset"] == 2


async def test_stop_fences_before_drain_and_joins_actual_worker(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        entered = threading.Event()
        release = threading.Event()
        from streamcast._tasks import thread

        def scan():
            entered.set()
            release.wait(5)

        class Lease(asyncio.Task):
            def add_done_callback(self, callback, *, context=None):
                # Release the real worker only when its owner starts joining
                # the lease. Skipping that join reaches finalize with a live
                # worker, deterministically rather than racing release.set().
                release.set()
                super().add_done_callback(callback, context=context)

        fenced = asyncio.Event()

        class Peer:
            async def close(self, *_):
                lease.cancel()
                fenced.set()

        lease = Lease(thread(scan))
        owner.generations["events"].connections[lease] = Peer()
        await asyncio.to_thread(entered.wait, 5)
        original = owner.finalize
        finalized = asyncio.Event()

        async def finalize(*args):
            assert lease.done()
            assert release.is_set()
            finalized.set()
            await original(*args)

        monkeypatch.setattr(owner, "finalize", finalize)
        stopping = asyncio.create_task(
            put(
                owner,
                client,
                "events",
                {"state": "stopped"},
                suffix="/desired",
                revision=2,
            )
        )
        try:
            await fenced.wait()
            with pytest.raises(ValueError, match="unavailable"):
                await stream.send({"value": 99})

            answer = await stopping
            assert answer.status_code == 202
            assert finalized.is_set()
            assert owner.generations.get("events") is None
        finally:
            release.set()
            await asyncio.gather(lease, return_exceptions=True)


async def test_lost_request_cannot_abandon_committed_intent(config, monkeypatch):
    async with managed(config) as (owner, client, _):
        entered = threading.Event()
        release = threading.Event()
        accept = owner.registry.accept

        def paused(*args):
            accept(*args)
            entered.set()
            release.wait(5)

        monkeypatch.setattr(owner.registry, "accept", paused)
        task = asyncio.create_task(
            put(owner, client, "events", definition(), key="lost")
        )
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        release.set()
        await asyncio.gather(*list(owner.controls))
        await owner.queue.join()
        assert owner.view("events")["observed"]["state"] == "stopped"
        repeated = await put(owner, client, "events", definition(), key="lost")
        assert repeated.status_code == 202
        assert len(owner.operations) == 1


async def test_registry_and_stream_ownership_are_exclusive(config, tmp_path):
    async with managed(config) as (owner, client, _):
        await serving(owner, client)
        with pytest.raises(BlockingIOError):
            await implementation.BrokerRuntime.open(config)

        other = BrokerConfig(
            "other", tmp_path / "other.db", config.profiles, config.authorization
        )
        initialize_registry(other)
        async with managed(other) as (second, http, _):
            await serving_without_assertion(second, http)
            assert second.view("events")["observed"]["state"] == "error"
            assert "events" not in second.generations


async def serving_without_assertion(owner, client):
    await put(owner, client, "events", definition())
    await put(
        owner, client, "events", {"state": "serving"}, suffix="/desired", revision=1
    )


async def test_missing_bound_disk_is_not_recreated(config):
    import shutil

    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        identity = stream._stream_id
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )

    shutil.rmtree(config.profiles["local"].root)
    async with managed(config) as (owner, client, _):
        response = await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=3
        )
        operation = owner.operations[response.json()["operation_id"]]
        assert operation["status"] == "failed"
        assert owner.view("events")["stream_id"] == identity
        assert "events" not in owner.generations
        assert not (config.profiles["local"].root / "events").exists()


async def test_guard_admits_complete_history_and_rejects_migration_before_journal(
    config,
):

    old = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="messages", schema=SHAPE),
            storage=streamcast.Storage(root=(config.profiles["local"].root).absolute()),
        ),
        initialization="create_if_absent",
    )
    old.ensure_metadata()
    await old.send({"value": 7})
    identity = old._stream_id
    assert identity is not None
    await old.aclose()
    guarded = replace(
        config, history_guards={"messages": HistoryGuard(identity, 1, {"value": 7})}
    )
    async with managed(guarded) as (owner, client, app):
        await serving(
            owner,
            client,
            "messages",
            initialization="open_existing",
            exactly_once=True,
            replay_published=True,
            max_replay=None,
        )
        async with running(app) as base:
            async with streamcast.connect(
                base.replace("ws://", "ws://admin:secret@") + "/messages", offset=0
            ) as sub:
                assert (await sub.recv())[2] == {"value": 7}

        await put(
            owner,
            client,
            "messages",
            {"state": "stopped"},
            suffix="/desired",
            revision=2,
        )
        before = len(owner.operations)
        response = await put(
            owner,
            client,
            "messages",
            {"stream_id": identity, "schema": SHAPE, "sort_by": []},
            suffix="/migrations",
            revision=3,
            key="migrate",
        )
        assert response.status_code == 403
        assert len(owner.operations) == before
        assert owner.view("messages")["pending"] is None
        retired = await put(
            owner,
            client,
            "messages",
            {"state": "retired"},
            suffix="/desired",
            revision=3,
            key="protected-retirement",
        )
        assert retired.status_code == 403
        assert len(owner.operations) == before


@pytest.mark.parametrize(
    "wrong", ["identity", "witness", "migration", "empty_migration", "flags"]
)
async def test_guard_rejects_incompatible_history(config, wrong):

    old = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="messages", schema=SHAPE),
            storage=streamcast.Storage(root=(config.profiles["local"].root).absolute()),
        ),
        initialization="create_if_absent",
    )
    old.ensure_metadata()
    if wrong != "empty_migration":
        await old.send({"value": 7})

    identity = old._stream_id
    assert identity is not None
    await old.aclose()
    shape = SHAPE
    if wrong in ("migration", "empty_migration"):
        shape = {
            **SHAPE,
            "properties": {
                **SHAPE["properties"],
                "extra": {"type": ["string", "null"]},
            },
        }
        migrated = streamcast.StreamHandle.migrate(
            "messages", root=config.profiles["local"].root, schema=shape
        )
        if wrong == "empty_migration":
            await migrated.send({"value": 7, "extra": None})

        await migrated.aclose()

    guarded = replace(
        config,
        history_guards={
            "messages": HistoryGuard(
                "other" if wrong == "identity" else identity,
                1,
                {"value": 7, "extra": None}
                if wrong == "empty_migration"
                else {"value": 8 if wrong == "witness" else 7},
            )
        },
    )
    async with managed(guarded) as (owner, client, _):
        await put(
            owner,
            client,
            "messages",
            definition(
                schema=shape,
                initialization="open_existing",
                exactly_once=wrong != "flags",
                replay_published=True,
                max_replay=None,
            ),
        )
        result = await put(
            owner,
            client,
            "messages",
            {"state": "serving"},
            suffix="/desired",
            revision=1,
        )
        assert owner.operations[result.json()["operation_id"]]["status"] == "failed"
        assert not owner.view("messages")["observed"]["read_admission"]


async def test_body_and_transport_limits(config):
    from dataclasses import replace

    limited = replace(config, transport_max_bytes=1024, control_max_bytes=512)
    async with managed(limited) as (owner, client, app):
        response = await client.put(
            "/control/v1/streams/events",
            content=b" " * 513,
            headers={
                "Idempotency-Key": "big",
                "Content-Type": "application/json",
                "If-None-Match": "*",
            },
        )
        assert response.status_code == 413
        await serving(owner, client)
        async with running(app) as base:
            async with raw_connect(
                base.replace("ws://", "ws://admin:secret@") + "/events?publish"
            ) as peer:
                await peer.recv()
                # A valid large publish would receive an ACK without the
                # transport limit. Invalid JSON is also closed by the parser
                # and cannot establish which bound rejected the frame.
                await peer.send(json.dumps([{"value": i} for i in range(100)]))
                with pytest.raises(ConnectionClosed) as caught:
                    await peer.recv()

                assert caught.value.rcvd is not None
                assert caught.value.rcvd.code == 1009

        # Subscribers do not parse publisher frames, so this also isolates
        # the application-wide bound from a publisher's per-stream bound.
        incoming = iter(
            [
                {"type": "websocket.connect"},
                {"type": "websocket.receive", "text": "x" * 1025},
                {"type": "websocket.disconnect", "code": 1000},
            ]
        )
        sent = []

        async def receive():
            return next(incoming)

        async def send(message):
            sent.append(message)

        await app(
            {
                "type": "websocket",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "scheme": "ws",
                "path": "/events",
                "raw_path": b"/events",
                "root_path": "",
                "query_string": b"",
                "headers": [(b"authorization", ADMIN["Authorization"].encode())],
                "server": ("test", 80),
                "client": ("test", 1234),
            },
            receive,
            send,
        )
        assert any(m["type"] == "websocket.close" and m["code"] == 1009 for m in sent)


async def test_mounted_controller_receipts_and_data_share_owner(config):
    from contextlib import asynccontextmanager

    from fastapi import FastAPI

    child = create_app(config)

    @asynccontextmanager
    async def lifespan(_app):
        async with child.router.lifespan_context(child):
            yield

    host = FastAPI(lifespan=lifespan)
    host.mount("/broker", child)
    async with host.router.lifespan_context(host):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=host),
            base_url="http://test",
            headers=ADMIN,
        ) as client:
            response = await client.put(
                "/broker/control/v1/streams/events",
                json=definition(),
                headers={"Idempotency-Key": "mounted", "If-None-Match": "*"},
            )
            assert response.status_code == 202
            await child.state.broker.queue.join()
            assert response.headers["Location"].startswith("/broker/control/v1/")
            receipt = await client.get(response.headers["Location"])
            assert receipt.status_code == 200
            assert receipt.json()["status"] == "succeeded"


async def test_unexpected_data_failure_closes_without_storage_details(
    config, monkeypatch
):
    async with managed(config) as (owner, client, app):
        await serving(owner, client)

        async def broken(*_args):
            raise OSError("private storage detail")

        monkeypatch.setattr(streamcast.StreamHandle, "serve_subscriber", broken)
        async with running(app) as base:
            async with raw_connect(
                base.replace("ws://", "ws://admin:secret@") + "/events"
            ) as peer:
                with pytest.raises(ConnectionClosed) as caught:
                    await peer.recv()

                assert caught.value.rcvd is not None
                assert caught.value.rcvd.code == 1011
                assert caught.value.rcvd.reason == "stream unavailable"


async def test_real_children_signal_exact_generation_readiness(config, monkeypatch):
    from streamcast._children import _supervisors

    # One real role proves the handshake and reap boundary. Running five
    # interpreters proves the same protocol five times and costs office CPUs.
    monkeypatch.setattr(
        implementation, "_supervisors", lambda *args: _supervisors(*args)[:1]
    )
    from dataclasses import replace

    async with managed(replace(config, readiness_timeout=15)) as (owner, client, _):
        await serving(owner, client)
        assert len(owner.children) == 1
        assert all(child.ready for child in owner.children)
        pids = {child._process.pid for child in owner.children}
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        assert not owner.children
        import os

        for pid in pids:
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)


@pytest.mark.replication
@pytest.mark.slow
@pytest.mark.timeout(120)
async def test_managed_replication_and_shared_group_replacement(
    config, monkeypatch, s3, bucket
):

    # One real role exercises replacement/readiness/reaping. Replication is
    # real as well; duplicating the role handshake in five interpreters makes
    # this lifecycle test a CPU/import benchmark on small CI workers.
    monkeypatch.setattr(
        implementation, "_supervisors", lambda *args: _supervisors(*args)[:1]
    )
    profile = replace(
        config.profiles["local"],
        config=litelink.LogConfig(wal_replication=True),
        published=bucket,
        s3_options=s3,
        replicate=True,
    )
    replicated = replace(
        config,
        profiles={"local": profile},
        readiness_timeout=40,
        readiness_every=0.05,
    )
    async with managed(replicated) as (owner, client, _):
        stream = await serving(owner, client)
        generation = owner.generations["events"]
        initial = list(owner.children)
        assert len(initial) == 2
        assert sum(isinstance(child, Sidecar) for child in initial) == 1
        assert all(child.ready for child in initial)
        await stream.send_many([{"value": i} for i in range(100)])
        found = await settle(s3, bucket + "/events", name="events")
        assert any(key.endswith(".ltx") for key in found), found
        metadata_uri = stream.metadata_uri

        await serving(owner, client, "other")
        assert owner.generations["events"] is generation
        assert all(child._process is None for child in initial)
        assert all(child.ready for child in owner.children)
        assert sum(isinstance(child, Sidecar) for child in owner.children) == 1
        assert await stream.send({"value": 100}) == 101
        stopped = await put(
            owner,
            client,
            "other",
            {"state": "stopped"},
            suffix="/desired",
            revision=2,
        )
        assert owner.operations[stopped.json()["operation_id"]]["status"] == "succeeded"
        assert owner.generations["events"] is generation
        assert await stream.send({"value": 101}) == 102

        stopped = await put(
            owner,
            client,
            "events",
            {"state": "stopped"},
            suffix="/desired",
            revision=2,
        )
        assert owner.operations[stopped.json()["operation_id"]]["status"] == "succeeded"
        assert not owner.children
        # Direct table reading is test evidence for publication; deployed
        # message consumers continue to use only the broker's WebSocket.
        async with await streamcast.StreamHandle.snapshot(
            metadata_uri, s3_options=s3
        ) as snap:
            rows = [row async for _, _, row in snap.rows(1)]
            assert [row["value"] for row in rows] == list(range(102))


async def test_pending_journal_recovers_without_http_resubmission(config):
    from streamcast._broker.model import declaration

    registry = Registry.open(config.registry, config.broker_id)
    record = {
        "name": "events",
        "definition": declaration(
            definition(), config.profiles, config.transport_max_bytes
        ),
        "desired": "serving",
        "revision": 1,
        "stream_id": None,
        "pending": "pending",
        "profile_revision": "1",
    }
    operation = {
        "id": "pending",
        "name": "events",
        "kind": "desired",
        "revision": 1,
        "target": {"state": "serving"},
        "status": "running",
        "error": None,
    }
    registry.accept(
        record,
        operation,
        "admin",
        "key",
        "digest",
        {"status": 202, "operation_id": "pending", "revision": 1},
    )
    registry.close()
    async with managed(config) as (owner, _, _):
        assert owner.operations["pending"]["status"] == "succeeded"
        assert owner.view("events")["pending"] is None
        assert owner.view("events")["observed"]["state"] == "serving"
        assert owner.generations["events"].number == 1


async def test_storage_profile_alias_cannot_open_same_physical_log(config, tmp_path):
    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        assert stream.log is not None
        # This profile resolves the existing nested layout as a flat one.
        other = BrokerConfig(
            "alias",
            tmp_path / "alias.db",
            {"local": StorageProfile("1", stream.log.root)},
            config.authorization,
        )
        initialize_registry(other)
        async with managed(other) as (second, http, _):
            await serving_without_assertion(second, http)
            assert second.view("events")["observed"]["state"] == "error"
            assert not second.generations
            assert await stream.send({"value": 15}) == 1


async def test_cancelled_reader_acquisition_closes_before_returning():
    from streamcast._tasks import reader

    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    class Reader:
        def close(self):
            closed.set()

    def acquire():
        entered.set()
        assert release.wait(5)
        return Reader()

    async def scan():
        async with reader(acquire):
            pytest.fail("cancelled scan was yielded")

    task = asyncio.create_task(scan())
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    # A second cancellation must not abandon the actual worker either.
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert closed.is_set()


async def test_control_queue_bound_preserves_data_admission(config, monkeypatch):
    from dataclasses import replace

    async with managed(replace(config, max_pending=1)) as (owner, client, _):
        stream = await serving(owner, client)
        entered = asyncio.Event()
        release = asyncio.Event()
        execute = owner.execute

        async def paused(operation):
            entered.set()
            await release.wait()
            await execute(operation)

        monkeypatch.setattr(owner, "execute", paused)
        accepted = await client.put(
            "/control/v1/streams/other",
            json=definition(),
            headers={"Idempotency-Key": "queued", "If-None-Match": "*"},
        )
        assert accepted.status_code == 202
        await entered.wait()
        refused = await client.put(
            "/control/v1/streams/third",
            json=definition(),
            headers={"Idempotency-Key": "overflow", "If-None-Match": "*"},
        )
        try:
            assert refused.status_code == 429
            assert refused.headers["Retry-After"] == "1"
            assert "third" not in owner.records
            assert await stream.send({"value": 11}) == 1
        finally:
            release.set()
            await owner.queue.join()


async def test_failed_intent_can_reuse_its_bounded_queue_slot(config, monkeypatch):
    from dataclasses import replace

    async with managed(replace(config, max_pending=1)) as (owner, client, _):
        await put(owner, client, "events", definition())
        reconcile = owner.reconcile

        async def failed(record):
            raise OSError("injected readiness failure")

        monkeypatch.setattr(owner, "reconcile", failed)
        first = await put(
            owner,
            client,
            "events",
            {"state": "serving"},
            suffix="/desired",
            revision=1,
            key="failed",
        )
        first_id = first.json()["operation_id"]
        assert owner.operations[first_id]["status"] == "failed"
        assert owner.records["events"]["pending"] == first_id
        monkeypatch.setattr(owner, "reconcile", reconcile)
        retry = await put(
            owner,
            client,
            "events",
            {"state": "serving"},
            suffix="/desired",
            revision=2,
            key="retry",
        )
        assert retry.status_code == 202, retry.text
        retry_id = retry.json()["operation_id"]
        assert retry_id != first_id
        assert owner.operations[first_id]["status"] == "failed"
        assert owner.operations[retry_id]["status"] == "succeeded"
        assert owner.records["events"]["pending"] is None
        assert owner.view("events")["observed"]["state"] == "serving"


async def test_migration_receipt_recovers_after_metadata_commit(config, monkeypatch):
    from streamcast import _metadata

    target_shape = {
        **SHAPE,
        "properties": {**SHAPE["properties"], "extra": {"type": ["string", "null"]}},
    }
    async with managed(config) as (owner, client, _):
        stream = await serving(owner, client)
        await stream.send({"value": 7})
        identity = stream._stream_id
        await put(
            owner, client, "events", {"state": "stopped"}, suffix="/desired", revision=2
        )
        transition = owner.storage_transition

        async def lost(*args):
            await transition(*args)
            raise OSError("lost completion after durable metadata")

        monkeypatch.setattr(owner, "storage_transition", lost)
        target = {"stream_id": identity, "schema": target_shape, "sort_by": []}
        response = await put(
            owner,
            client,
            "events",
            target,
            suffix="/migrations",
            revision=3,
            key="migration",
        )
        operation_id = response.json()["operation_id"]
        assert owner.operations[operation_id]["status"] == "failed"
        assert owner.view("events")["definition"]["schema"] == SHAPE
        metadata = _metadata.load(
            _metadata.home(config.profiles["local"].root, "events"), "events"
        )
        assert metadata is not None
        assert len(metadata.sealed_logs) == 1
        assert metadata.live_log.start_offset == 2

    async with managed(config) as (owner, client, _):
        assert owner.operations[operation_id]["status"] == "succeeded"
        assert owner.view("events")["definition"]["schema"] == target_shape
        repeated = await put(
            owner,
            client,
            "events",
            target,
            suffix="/migrations",
            revision=3,
            key="migration",
        )
        assert repeated.json()["operation_id"] == operation_id
        await put(
            owner, client, "events", {"state": "serving"}, suffix="/desired", revision=4
        )
        stream = owner.generations["events"].stream
        assert stream._stream_id == identity
        assert await stream.send({"value": 8, "extra": None}) == 2
        metadata = _metadata.load(stream.log.root, "events")
        assert metadata is not None
        assert len(metadata.sealed_logs) == 1
        await put(
            owner, client, "events", {"state": "retired"}, suffix="/desired", revision=5
        )
        assert owner.view("events")["observed"]["state"] == "retired"
        assert owner.generations["events"].stream.retirement is not None
        again = await put(
            owner,
            client,
            "events",
            {"state": "retired"},
            suffix="/desired",
            revision=6,
            key="noop-retire",
        )
        assert again.status_code == 200
        with pytest.raises(streamcast.StreamRetired):
            await owner.generations["events"].stream.send({"value": 9, "extra": None})


async def test_server_lifespan_owns_registry_and_drains_connection(config):
    app = create_app(config)
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=0, lifespan="on", log_level="critical"
        )
    )
    task = asyncio.create_task(server.serve())
    async with asyncio.timeout(5):
        while not server.started:
            if task.done():
                await task
                pytest.fail("startup failed")

            await asyncio.sleep(0.001)

    owner = app.state.broker
    base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    try:
        async with httpx.AsyncClient(base_url=base, headers=ADMIN) as client:
            await serving(owner, client)
            peer = await raw_connect(
                base.replace("http://", "ws://admin:secret@") + "/events"
            )
            await peer.recv()
            server.should_exit = True
            await task
            with pytest.raises(ConnectionClosed):
                await peer.recv()

        assert owner.closed
        assert not owner.generations and not owner.children and not owner.locks
        assert not owner.controls
    finally:
        server.should_exit = True
        await task

    # A second lifespan can acquire the registry; no forgotten handler keeps
    # its lock or stream generation alive after the server has stopped.
    async with managed(config) as (next_owner, _, _):
        assert next_owner.boot_id != owner.boot_id
        assert next_owner.view("events")["observed"]["state"] == "serving"


async def test_failed_group_construction_cannot_restore_neighbor_writes(
    config, monkeypatch
):
    async with managed(config) as (owner, client, _):
        neighbor = await serving(owner, client)
        original = implementation._supervisors

        def failed(*_args):
            raise OSError("child construction failed")

        monkeypatch.setattr(implementation, "_supervisors", failed)
        await serving_without_assertion_named(owner, client, "other")
        assert owner.view("other")["observed"]["state"] == "error"
        # Let the normal monitor run after the failed replacement. An empty
        # child list is not evidence that the required group is healthy.
        checked = asyncio.Event()
        health = owner.children_ready

        def observed():
            result = health()
            checked.set()
            return result

        monkeypatch.setattr(owner, "children_ready", observed)
        owner.ready_at = 0
        await checked.wait()
        await asyncio.sleep(0)
        assert not owner.generations["events"].writable
        with pytest.raises(ValueError, match="unavailable"):
            await neighbor.send({"value": 7})

        monkeypatch.setattr(implementation, "_supervisors", original)
        old_neighbor = owner.generations["events"]
        await owner.replace_children()
        assert owner.generations["events"] is old_neighbor
        assert await neighbor.send({"value": 8}) == 1


async def serving_without_assertion_named(owner, client, name):
    await put(owner, client, name, definition())
    await put(owner, client, name, {"state": "serving"}, suffix="/desired", revision=1)


async def test_registry_symlink_alias_cannot_acquire_second_owner(config, tmp_path):
    from dataclasses import replace

    alias = tmp_path / "alias.db"
    alias.symlink_to(config.registry)
    async with managed(config):
        with pytest.raises(BlockingIOError):
            await implementation.BrokerRuntime.open(replace(config, registry=alias))


@pytest.mark.parametrize("phase", ["startup", "shutdown"])
async def test_repeated_lifespan_cancellation_joins_owner(config, monkeypatch, phase):
    entered = asyncio.Event()
    release = asyncio.Event()
    owners = []
    original_open = implementation.BrokerRuntime.open
    original_close = implementation.BrokerRuntime.close

    async def opening(cls, supplied, **kwargs):
        owner = await original_open(supplied, **kwargs)
        owners.append(owner)
        if phase == "startup":
            entered.set()
            await release.wait()

        return owner

    async def closing(owner):
        if phase == "shutdown":
            entered.set()
            await release.wait()

        await original_close(owner)

    monkeypatch.setattr(implementation.BrokerRuntime, "open", classmethod(opening))
    monkeypatch.setattr(implementation.BrokerRuntime, "close", closing)
    app = create_app(config)
    lifespan = app.router.lifespan_context(app)
    if phase == "startup":
        task = asyncio.create_task(lifespan.__aenter__())
    else:
        await lifespan.__aenter__()
        task = asyncio.create_task(lifespan.__aexit__(None, None, None))

    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(owners) == 1 and owners[0].closed
    # The original opener must acquire the same lock after cleanup.
    second = await original_open(config)
    await original_close(second)
