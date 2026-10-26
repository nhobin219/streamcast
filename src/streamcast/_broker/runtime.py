"""The sole owner of the registry, generations, stream handles and children."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import time
import uuid
from collections.abc import Coroutine
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, get_args

from pydantic import ValidationError

from streamcast import _log, _metadata
from streamcast._broker.model import NAME, BrokerConfig, Fault
from streamcast._broker.registry import Lock, Registry, dumps
from streamcast._broker.transitions import storage, with_replica
from streamcast._children import _sidecars, _supervisors
from streamcast._errors import Unavailable
from streamcast._maintain import stop
from streamcast._spec import Initialization, Storage, Stream, StreamSpec
from streamcast._stream import StreamHandle
from streamcast._tasks import thread


class Generation:
    def __init__(self, stream: StreamHandle, number: int) -> None:
        self.stream = stream
        self.number = number
        self.accepting = False
        self.writable = False
        self.connections: dict[asyncio.Task, Any] = {}

    def check(self) -> None:
        if not self.accepting or not self.writable:
            raise Unavailable("stream unavailable")


class BrokerRuntime:
    def __init__(self, config: BrokerConfig, registry: Registry) -> None:
        self.config = config
        self.registry = registry
        self.boot_id = uuid.uuid4().hex
        self.records: dict[str, dict] = {}
        self.operations: dict[str, dict] = {}
        self.observed: dict[str, dict] = {}
        self.generations: dict[str, Generation] = {}
        self.locks: dict[Any, Lock] = {}
        self.children: list[Any] = []
        self.collection = 0
        self.serial = asyncio.Lock()
        self.queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.worker: asyncio.Task | None = None
        self.monitor: asyncio.Task | None = None
        self.closing = False
        self.child_ready = False
        self.children_complete = False
        self.generation = 0
        self.ready_at = 0.0
        self.changing_children = False
        self.closed = False
        self.controls: set[asyncio.Task] = set()
        self.static: dict[str, StreamHandle] = {}
        self.ready_directory = None

    @classmethod
    async def open(cls, config: BrokerConfig, *, streams=()) -> BrokerRuntime:
        registry = (
            await thread(Registry.memory, config.broker_id)
            if config.registry is None
            else await thread(Registry.open, config.registry, config.broker_id)
        )
        self = cls(config, registry)
        try:
            self.static = {stream.name: stream for stream in streams}
            self.ready_directory = TemporaryDirectory(prefix="streamcast-ready-")
            self.records, self.operations, self.collection = await thread(registry.load)
            if self.static.keys() & self.records.keys():
                raise ValueError("static and managed stream names overlap")

            # Hold canonical ownership even for stopped definitions. An
            # offline transition and a second registry must not share a log.
            for record in self.records.values():
                await self.claim(record)
                self.observe(record["name"], "unknown")

            recovered = set()
            for operation in list(self.operations.values()):
                if self.records[operation["name"]].get("pending") == operation["id"]:
                    await self.execute(operation)
                    recovered.add(operation["name"])

            for record in self.records.values():
                if record.get("pending") is None and record["name"] not in recovered:
                    try:
                        await self.reconcile(record)
                    except Exception as exc:
                        self.observe(
                            record["name"],
                            "error",
                            error=(
                                self.transition_error(exc)
                                if isinstance(exc, Fault)
                                else "recovery_failed"
                            ),
                        )

            # Injected streams share every connection/admission/child lifetime.
            # Only their construction remains the host's responsibility.
            for name, stream in self.static.items():
                if stream.log is not None:
                    await thread(self.claim_log, Path(stream.log.root), stream.log.name)

                await thread(stream.ensure_metadata)
                stream._subscriber_yield_every = config.subscriber_yield_every
                self.generation += 1
                generation = Generation(stream, self.generation)
                stream._admission = generation.check
                self.generations[name] = generation

            await self.replace_children()
            for name in self.static:
                self.generations[name].accepting = True

            self.worker = asyncio.create_task(self.work())
            self.worker.add_done_callback(self.worker_done)
            self.monitor = asyncio.create_task(self.watch())
            return self
        except BaseException:
            await self.close()
            raise

    def recorded_storage(self, record: dict) -> Storage:
        return Storage.model_validate(record["stream"]["storage"])

    def s3_options(self, record: dict):
        profile = self.config.storage_profiles.get(record["storage_profile"])
        if profile is None:
            raise Fault(422, "unknown_storage_profile")

        return profile.s3_options

    def validate_spec(self, body: Any, name: str) -> dict:
        try:
            spec = StreamSpec.model_validate(body)
        except ValidationError as exc:
            raise Fault(
                422,
                "invalid_declaration",
                errors=[{"loc": e["loc"], "type": e["type"]} for e in exc.errors()],
            ) from None

        if spec.name != name:
            raise Fault(422, "name_mismatch")

        if (
            spec.max_message_bytes is not None
            and spec.max_message_bytes > self.config.transport_max_bytes
        ):
            raise Fault(
                422,
                "invalid_declaration",
                errors=[{"loc": ["max_message_bytes"], "type": "less_than_equal"}],
            )

        return spec.model_dump(mode="json")

    @staticmethod
    def transition_error(exc: Exception) -> str:
        return (
            "unknown_storage_profile"
            if isinstance(exc, Fault) and exc.code == "unknown_storage_profile"
            else "transition_failed"
        )

    def claim_path(self, path) -> None:
        path = path.resolve()
        if path not in self.locks:
            self.locks[path] = Lock.open(path)

    def claim_log(self, home, name: str) -> None:
        # Outside the log directory: reserving a future migration successor
        # must not create that directory before litelink.new does. Every
        # layout/profile alias of a physical log derives this same path.
        location = (home / name).resolve()
        self.claim_path(
            location.parent / ".streamcast-log-owners" / f"{location.name}.lock"
        )

    def claim_storage(self, record: dict, *, opening: bool = False) -> None:
        profile = self.recorded_storage(record)
        name = record["name"]
        root = profile.root.resolve()
        self.claim_path(root / ".streamcast-owners" / f"{name}.lock")
        home = _metadata.home(root, name)
        metadata = _metadata.load(home, name)
        if metadata is not None:
            self.claim_log(home, metadata.current.name)
        elif opening or _metadata._is_log(home / name):
            self.claim_log(home, name)

    async def claim(self, record: dict) -> None:
        await thread(self.claim_storage, record)

    def observe(self, name: str, state: str, **extra: Any) -> None:
        generation = self.generations.get(name)
        self.observed[name] = {
            "state": state,
            "boot_id": self.boot_id,
            "generation": None if generation is None else generation.number,
            "revision": self.records[name]["revision"],
            "at": time.time(),
            "read_admission": bool(generation and generation.accepting),
            "write_admission": bool(
                generation and generation.accepting and generation.writable
            ),
            "children_ready": self.child_ready,
            **extra,
        }

    def view(self, name: str) -> dict:
        record = self.records.get(name)
        if record is None:
            raise Fault(404, "unknown_stream")

        return copy.deepcopy(
            {
                **record,
                "observed": self.observed.get(name, {"state": "unknown"}),
            }
        )

    async def control(self, call: Coroutine) -> dict:
        if self.closing or len(self.controls) >= self.config.max_pending:
            call.close()
            raise Fault(503 if self.closing else 429, "control_unavailable")

        task = asyncio.create_task(call)
        self.controls.add(task)

        def completed(done):
            self.controls.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
        return await asyncio.shield(task)

    async def mutate(
        self,
        principal: str,
        key: str,
        method: str,
        name: str,
        kind: str,
        body: Any,
        match: str | None,
        none: str | None,
        *,
        storage_profile: str | None = None,
        initialization: str | None = None,
    ) -> dict:
        if name in self.static:
            raise Fault(409, "static_stream")

        if self.config.registry is None:
            raise Fault(403, "management_disabled")

        digest = hashlib.sha256(
            dumps(
                [method, name, kind, body, storage_profile, initialization, match, none]
            ).encode()
        ).hexdigest()
        async with self.serial:
            prior = await thread(self.registry.receipt, principal, key, digest)
            if prior is not None:
                return prior

            if self.closing:
                raise Fault(503, "broker_stopping")

            if NAME.fullmatch(name) is None:
                raise Fault(422, "invalid_name")

            previous = self.records.get(name)
            target = body
            if kind == "definition":
                spec = self.validate_spec(body, name)
                profile_name = (
                    storage_profile
                    if storage_profile is not None
                    else (
                        "default" if previous is None else previous["storage_profile"]
                    )
                )
                intent = (
                    (initialization if initialization is not None else "open_existing")
                    if previous is None
                    else previous["initialization"]
                )
                if previous is not None and profile_name != previous["storage_profile"]:
                    raise Fault(409, "immutable_storage")

                if (
                    previous is None
                    and profile_name not in self.config.storage_profiles
                ):
                    raise Fault(
                        422,
                        "unknown_storage_profile",
                        available=sorted(self.config.storage_profiles),
                    )

                if previous is None and intent not in get_args(Initialization):
                    raise Fault(422, "invalid_initialization")

                if name in self.config.history_guards and intent != "open_existing":
                    raise Fault(403, "history_prefix_protected")

                target = {
                    "spec": spec,
                    "storage_profile": profile_name,
                    "initialization": intent,
                }

            if previous is None:
                if kind != "definition":
                    raise Fault(404, "unknown_stream")

                if none is None:
                    raise Fault(428, "precondition_required")

                if none != "*" or match is not None:
                    raise Fault(412, "precondition_failed")

                record = {
                    "name": name,
                    "stream": {
                        "spec": spec,
                        "storage": self.config.storage_profiles[
                            profile_name
                        ].model_dump(mode="json"),
                    },
                    "storage_profile": profile_name,
                    "initialization": intent,
                    "desired": "stopped",
                    "revision": 1,
                    "stream_id": None,
                    "pending": None,
                }
            else:
                if match is None:
                    raise Fault(428, "precondition_required")

                if match != f'"{previous["revision"]}"' or none is not None:
                    raise Fault(412, "precondition_failed")

                if previous.get("pending") is not None:
                    pending = self.operations[previous["pending"]]
                    # A failed irreversible intent may only be retried at
                    # exactly its stored target. Never overwrite its journal.
                    if (
                        pending["status"] != "failed"
                        or kind != pending["kind"]
                        or dumps(target) != dumps(pending["target"])
                    ):
                        raise Fault(409, "transition_pending")

                record = copy.deepcopy(previous)
                state = self.observed[name]["state"]
                if kind == "definition":
                    if dumps(spec) != dumps(record["stream"]["spec"]):
                        if state != "stopped":
                            raise Fault(409, "must_be_stopped")

                        if any(
                            dumps(spec[k]) != dumps(record["stream"]["spec"][k])
                            for k in ("schema", "sort_by")
                        ):
                            raise Fault(409, "migration_required")

                    record["stream"]["spec"] = spec
                elif kind == "desired":
                    if (
                        not isinstance(body, dict)
                        or set(body) != {"state"}
                        or body["state"] not in ("serving", "stopped", "retired")
                    ):
                        raise Fault(422, "invalid_desired")

                    if record["desired"] == "retired" and body["state"] != "retired":
                        raise Fault(409, "retired_stream")

                    if (
                        body["state"] == "retired"
                        and name in self.config.history_guards
                    ):
                        raise Fault(403, "history_prefix_protected")

                    if body["state"] == "retired" and not record["stream_id"]:
                        raise Fault(409, "unbound_stream")

                    record["desired"] = body["state"]
                elif kind == "migration":
                    # This is host admission, independent of controller grants.
                    if name in self.config.history_guards:
                        raise Fault(403, "history_prefix_protected")

                    if not isinstance(body, dict) or set(body) != {
                        "stream_id",
                        "schema",
                        "sort_by",
                    }:
                        raise Fault(422, "invalid_migration")

                    if (
                        not record["stream_id"]
                        or body["stream_id"] != record["stream_id"]
                    ):
                        raise Fault(409, "identity_mismatch")

                    if (
                        state not in ("stopped", "error")
                        or record["desired"] != "stopped"
                    ):
                        raise Fault(409, "must_be_stopped")

                    self.validate_spec(
                        record["stream"]["spec"]
                        | {"schema": body["schema"], "sort_by": body["sort_by"]},
                        name,
                    )
                else:
                    raise Fault(422, "invalid_operation")

                record["revision"] += 1

            pending_count = sum(
                r.get("pending") is not None
                for other, r in self.records.items()
                if other != name
            )
            if pending_count >= self.config.max_pending:
                raise Fault(429, "operation_queue_full")

            operation_id = uuid.uuid4().hex
            noop = (
                previous is not None
                and kind != "migration"
                and dumps(previous["stream"]) == dumps(record["stream"])
                and previous["desired"] == record["desired"]
                and self.observed[name]["state"] in (record["desired"],)
            )
            if noop:
                record["revision"] = previous["revision"]

            operation = {
                "id": operation_id,
                "name": name,
                "kind": kind,
                "revision": record["revision"],
                "target": target,
                "status": "succeeded" if noop else "accepted",
                "error": None,
            }
            record["pending"] = None if noop else operation_id
            receipt = {
                "status": 200 if noop else 202,
                "operation_id": operation_id,
                "revision": record["revision"],
            }
            # Persist before publishing desired state or scheduling any effect.
            await thread(
                self.registry.accept, record, operation, principal, key, digest, receipt
            )
            self.records[name] = record
            self.operations[operation_id] = operation
            if not noop:
                self.collection += 1
                self.observe(name, self.observed.get(name, {}).get("state", "unknown"))
                self.queue.put_nowait(operation_id)

            return receipt

    async def work(self) -> None:
        while True:
            key = await self.queue.get()
            try:
                if key is None:
                    return

                await self.execute(self.operations[key])
            finally:
                self.queue.task_done()

    def worker_done(self, worker: asyncio.Task) -> None:
        if not self.closing:
            self.closing = True
            for generation in self.generations.values():
                generation.accepting = generation.writable = False

        if not worker.cancelled():
            worker.exception()

    async def execute(self, operation: dict) -> None:
        operation = copy.deepcopy(operation)
        name = operation["name"]
        record = copy.deepcopy(self.records[name])
        operation["status"] = "running"
        await thread(self.registry.update, record, operation)
        self.operations[operation["id"]] = copy.deepcopy(operation)
        try:
            await self.claim(record)
            if operation["kind"] == "migration":
                await self.offline(record)
                self.observe(name, "migrating")
                target = operation["target"]
                await self.storage_transition(record, "migration", target)
                record["stream"]["spec"] = record["stream"]["spec"] | {
                    "schema": target["schema"],
                    "sort_by": target["sort_by"],
                }
                self.observe(name, "stopped")
            else:
                await self.reconcile(record)

            operation["status"] = "succeeded"
            operation["error"] = None
            record["pending"] = None
        except Exception as exc:
            # Keep intent and close admission; errors contain no storage paths,
            # row values, credentials, or framework exception strings.
            operation["status"] = "failed"
            operation["error"] = self.transition_error(exc)
            generation = self.generations.get(name)
            if generation is not None:
                generation.accepting = generation.writable = False

            self.observe(name, "error", error=operation["error"])

        async with self.serial:
            await thread(self.registry.update, record, operation)
            self.records[name] = record
            self.operations[operation["id"]] = operation

    async def reconcile(self, record: dict) -> None:
        name = record["name"]
        self.s3_options(record)
        desired = record["desired"]
        if desired == "stopped":
            await self.offline(record)
            # Validate bound stopped streams too; missing disk is never an
            # implicit permission to create it at the next serve request.
            if record["stream_id"]:
                await thread(self.check_identity, record)

            self.observe(name, "stopped")
            return

        await self.offline(record)
        self.observe(name, "starting" if desired == "serving" else "retiring")
        if desired == "retired":
            await self.storage_transition(record, "retire", {})

        opened = await thread(self.open_stream, record)
        try:
            await self.verify_history(record, opened)
            self.generation += 1
            generation = Generation(opened, self.generation)
            opened._admission = generation.check
            self.generations[name] = generation
            # Bind identity durably before a data route can expose it.
            record["stream_id"] = opened._stream_id
            await thread(self.registry.update, record)
            await self.replace_children()
            generation.accepting = not self.closing
            generation.writable = (
                not self.closing and desired == "serving" and self.child_ready
            )
            self.observe(name, desired)
        except BaseException:
            if name not in self.generations:
                await opened.aclose()

            raise

    def check_identity(self, record: dict) -> Any:
        profile = self.recorded_storage(record)
        home = _metadata.home(profile.root, record["name"])
        metadata = _metadata.load(home, record["name"])
        if metadata is None or metadata.stream_id != record["stream_id"]:
            raise ValueError("identity mismatch")

        if not _metadata._is_log(home / metadata.current.name):
            raise FileNotFoundError("bound log missing")

        return metadata

    def open_stream(self, record: dict) -> StreamHandle:
        if record["stream_id"]:
            self.check_identity(record)

        self.claim_storage(record, opening=True)
        resolved = self.recorded_storage(record).model_copy(
            update={"s3_options": self.s3_options(record)}
        )
        definition = Stream.model_validate(record["stream"])
        opened = StreamHandle.open(
            Stream(spec=definition.spec, storage=resolved),
            initialization=record["initialization"]
            if record["stream_id"] is None
            else "open_existing",
        )
        log = opened.log
        assert log is not None
        try:
            self.claim_log(log.root, log.name)
            opened.ensure_metadata()
            if record["stream_id"] and opened._stream_id != record["stream_id"]:
                raise ValueError("identity mismatch")

            if tuple(log.sort_by) != definition.spec.sort_by:
                raise ValueError("sort mismatch")

            opened._subscriber_yield_every = self.config.subscriber_yield_every
            return opened
        except BaseException:
            log.close()
            raise

    async def verify_history(self, record: dict, stream: StreamHandle) -> None:
        guard = self.config.history_guards.get(record["name"])
        if guard is None:
            return

        metadata = await thread(self.check_identity_for_guard, record, stream)
        if metadata.stream_id != guard.stream_id or guard.known_offset < 1:
            raise ValueError("history identity mismatch")

        log = stream.log
        end = stream.end_offset
        assert log is not None and end is not None
        expected = 1
        known = False
        async for offset, _ts, row in _log.rows(log, 1, end, published=True):
            if offset != expected:
                raise ValueError("history prefix incomplete")

            if offset == guard.known_offset:
                known = row == guard.known_row

            expected += 1

        if expected != stream.end_offset or not known:
            raise ValueError("history witness missing")

    def check_identity_for_guard(self, record: dict, stream: StreamHandle):
        log = stream.log
        assert log is not None
        metadata = _metadata.load(log.root, record["name"])
        if (
            metadata is None
            or metadata.sealed_logs
            or metadata.live_log.start_offset != 1
            or metadata.retention is not None
            or metadata.pending is not None
        ):
            raise ValueError("history prefix protected")

        definition = record["stream"]["spec"]
        if (
            not definition["exactly_once"]
            or not definition["replay_published"]
            or definition["max_replay"] is not None
        ):
            raise ValueError("history capabilities missing")

        return metadata

    async def offline(self, record: dict) -> None:
        name = record["name"]
        generation = self.generations.get(name)
        if generation is None:
            return

        generation.accepting = generation.writable = False
        if name not in self.static:
            self.observe(name, "stopping")

        connections = list(generation.connections.items())
        await asyncio.gather(
            *(peer.close(1001, "stream stopping") for _, peer in connections),
            return_exceptions=True,
        )
        # Connection tasks hold replay leases, including cancelled worker
        # scans. The router never lets ASGI cancellation cancel these tasks.
        await asyncio.gather(*(task for task, _ in connections), return_exceptions=True)
        stream = generation.stream
        if stream._writer is not None:
            await thread(stream._writer.close)

        await self.stop_children()
        # Final publication errors are visible to the operation. Maintainers'
        # best-effort exit hooks alone cannot establish a successful stop.
        if name not in self.static:
            await self.finalize(stream, record)

        await stream.aclose()
        # A host may still hold a static stream: once shut it says closed,
        # rather than naming a generation that no longer exists.
        stream._admission = None
        del self.generations[name]
        # Closing takes every stream offline in turn, writes already fenced:
        # a group restarted for the ones left would be reaped again at once.
        if not self.closing:
            await self.replace_children()

    async def finalize(self, stream: StreamHandle, record: dict) -> None:
        if stream.retirement is not None:
            return

        log = stream.log
        assert log is not None

        def finish() -> None:
            while log.seal(flush=True) is not None:
                pass

            log.publish(flush=True)

        # A replicated log's seal/publish may flush its WAL through its own
        # control socket. Spawn the offline sidecar on the lifespan thread.
        await with_replica(log, self.s3_options(record), finish)

    async def storage_transition(self, record: dict, kind: str, target: dict) -> None:
        def reserve() -> None:
            self.claim_storage(record, opening=True)
            if kind == "migration":
                profile = self.recorded_storage(record)
                home = _metadata.home(profile.root, record["name"])
                metadata = _metadata.load(home, record["name"])
                if metadata is None:
                    raise ValueError("identity missing")

                self.claim_log(home, metadata.next_name())

        await thread(reserve)
        await storage(
            record,
            self.recorded_storage(record).model_copy(
                update={"s3_options": self.s3_options(record)}
            ),
            kind,
            target,
        )

    async def stop_children(self) -> None:
        self.changing_children = True
        self.child_ready = False
        self.children_complete = False
        for generation in self.generations.values():
            generation.writable = False

        children = self.children
        await stop(children)
        self.children = []
        for child in children:
            marker = getattr(child, "_ready_file", None)
            if marker is not None:
                await thread(marker.unlink, missing_ok=True)

    async def replace_children(self) -> None:
        self.changing_children = True
        try:
            await self._replace_children()
        finally:
            self.changing_children = False

    async def _replace_children(self) -> None:
        await self.stop_children()
        routes = {name: g.stream for name, g in self.generations.items()}
        if not routes:
            self.children_complete = self.child_ready = True
            return

        children: list[Any] = list(_supervisors(routes, self.config.maintain))
        assert self.ready_directory is not None
        for index, child in enumerate(children):
            child._ready_file = (
                Path(self.ready_directory.name) / f".ready-{self.boot_id}-{index}"
            )

        # A handle carries its replication policy, whether static or managed.
        # The child factory groups each set further by its credentials.
        by_replication: dict[bool, dict] = {}
        for name, stream in routes.items():
            by_replication.setdefault(stream.replicate, {})[name] = stream

        for replicate, members in by_replication.items():
            children.extend(_sidecars(members, replicate))

        self.children = children
        for child in children:
            child.start()

        self.children_complete = True
        self.ready_at = time.monotonic() + self.config.readiness_timeout
        async with asyncio.timeout(self.config.readiness_timeout):
            while not await thread(self.children_ready):
                await asyncio.sleep(self.config.readiness_every)

        self.child_ready = True
        for name, generation in self.generations.items():
            generation.writable = not self.closing and (
                name in self.static or self.records[name]["desired"] == "serving"
            )

    def children_ready(self) -> bool:
        return self.children_complete and all(child.ready for child in self.children)

    async def watch(self) -> None:
        while True:
            await asyncio.sleep(self.config.readiness_every)
            if self.changing_children or self.closing:
                continue

            ready = await thread(self.children_ready)
            if self.changing_children or self.closing:
                continue

            if ready:
                self.ready_at = time.monotonic() + self.config.readiness_timeout

            self.child_ready = ready
            if not ready and time.monotonic() >= self.ready_at:
                for name, generation in self.generations.items():
                    generation.writable = False
                    if name not in self.static:
                        self.observe(name, "error", error="children_unavailable")

            elif ready:
                for name, generation in self.generations.items():
                    if generation.accepting:
                        generation.writable = (
                            name in self.static
                            or self.records[name]["desired"] == "serving"
                        )
                        if name not in self.static:
                            self.observe(name, self.records[name]["desired"])

    async def close(self) -> None:
        if self.closed:
            return

        self.closing = True
        if self.controls:
            await asyncio.gather(*list(self.controls), return_exceptions=True)

        for generation in self.generations.values():
            generation.accepting = generation.writable = False

        if self.monitor is not None:
            self.monitor.cancel()
            await asyncio.gather(self.monitor, return_exceptions=True)
            self.monitor = None

        if self.worker is not None:
            self.queue.put_nowait(None)
            await self.worker
            self.worker = None

        # Release no ownership lock if a worker/finalization fails. The process
        # must exit/recover rather than hand live handles to a new owner.
        for name in list(self.generations):
            await self.offline(self.records.get(name, {"name": name}))

        await self.stop_children()
        for lock in self.locks.values():
            lock.close()

        self.locks.clear()
        await thread(self.registry.close)
        if self.ready_directory is not None:
            await thread(self.ready_directory.cleanup)

        self.closed = True
