"""The sole owner of the registry, generations, stream handles and children."""

from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from collections.abc import Coroutine
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast, get_args

from pydantic import ValidationError

from streamcast import _log, _metadata
from streamcast._broker.model import NAME, BrokerConfig, CreationAuthorization, Fault
from streamcast._broker.registry import Lock, Registry, dumps
from streamcast._broker.transitions import storage, with_replica
from streamcast._children import (
    ChildSpec,
    build,
    desired,
    warn_unreplicated,
)
from streamcast._errors import Unavailable
from streamcast._maintain import Supervisor, stop
from streamcast._spec import Initialization, Maintenance, Storage, Stream, StreamSpec
from streamcast._stream import StreamHandle
from streamcast._tasks import thread

logger = logging.getLogger(__name__)
LIVE = {"maintenance"}


class Generation:
    def __init__(self, stream: StreamHandle, number: int) -> None:
        self.stream = stream
        self.number = number
        self.maintenance = (
            stream.definition.spec.maintenance
            if stream.definition is not None
            else Maintenance()
        )
        self.accepting = False
        self.writable = False
        self.connections: dict[asyncio.Task, Any] = {}
        self.sidecar_wait_at: float | None = None
        self.sidecar_ready_at: float | None = None

    def check(self) -> None:
        if not self.accepting or not self.writable:
            raise Unavailable("stream unavailable")


class BrokerRuntime:
    def __init__(self, config: BrokerConfig, registry: Registry) -> None:
        self.config = config
        self.registry = registry
        self.boot_id = uuid.uuid4().hex
        self.records: dict[str, dict] = {}
        # Observed applied definitions, distinct from accepted durable intent.
        # Bounded by the registry's max_streams; absent after a restart until
        # recovery succeeds, so replay takes the full identity-checking path.
        self.applied_specs: dict[str, dict] = {}
        self.queued: set[str] = set()
        self.attempts: dict[tuple[str, int, int], asyncio.Future] = {}
        self.waiters = 0
        self.observed: dict[str, dict] = {}
        self.generations: dict[str, Generation] = {}
        self.locks: dict[Any, Lock] = {}
        self.running: dict[ChildSpec, Any] = {}
        self.wanted: set[ChildSpec] = set()
        self.retries: dict[ChildSpec, tuple[float, float, str]] = {}
        self.child_health: dict[ChildSpec, bool] = {}
        self.child_pass: asyncio.Task | None = None
        self.children_dirty = False
        self.child_starts = 0
        self.excluding: set[str] = set()
        self.collection = 0
        self.serial = asyncio.Lock()
        self.queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.worker: asyncio.Task | None = None
        self.monitor: asyncio.Task | None = None
        self.closing = False
        self.generation = 0
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
            self.records, self.collection = await thread(registry.load)
            # Normalize defaults in active definitions and pending targets;
            # non-default maintenance remains part of the durable definition.
            for record in self.records.values():
                specs = [record["stream"]["spec"]]
                pending = record["pending"]
                if pending is not None and pending["kind"] == "definition":
                    specs.append(pending["target"]["spec"])

                for spec in specs:
                    if "maintenance" in spec:
                        normalized = self._parse_spec(spec).model_dump(mode="json")
                        spec.clear()
                        spec.update(normalized)

            if self.static.keys() & self.records.keys():
                raise ValueError("static and managed stream names overlap")

            # Hold canonical ownership even for stopped definitions. An
            # offline transition and a second registry must not share a log.
            for record in self.records.values():
                await self.claim(record)
                self.observe(record["name"], "unknown")

            recovered = set()
            for name, record in list(self.records.items()):
                if record["pending"] is not None:
                    # Recovery resumes every durable intent, including failed
                    # attempts. execute itself accepts only queued/accepted work.
                    record = copy.deepcopy(record)
                    record["pending"]["status"] = "accepted"
                    await thread(registry.update, record)
                    self.records[name] = record
                    await self.execute(name)
                    recovered.add(name)

            for record in self.records.values():
                if record.get("pending") is None and record["name"] not in recovered:
                    try:
                        await self.reconcile(record)
                        self.applied_specs[record["name"]] = copy.deepcopy(
                            record["stream"]["spec"]
                        )
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
                warn_unreplicated(stream)

            await self.reconcile_children()
            for name in self.static:
                self.generations[name].accepting = True

            self.refresh_gates(time.monotonic())
            self.worker = asyncio.create_task(self.work())
            self.worker.add_done_callback(self.worker_done)
            self.monitor = asyncio.create_task(self.watch())
            return self
        except BaseException:
            await self.close()
            raise

    def dedicated_count(self) -> int:
        return sum(
            record["desired"] != "retired"
            and record["stream"]["spec"].get("maintenance", {}).get("dedicated", False)
            for record in self.records.values()
        ) + sum(
            stream.retirement is None
            and stream.definition is not None
            and stream.definition.spec.maintenance.dedicated
            for stream in self.static.values()
        )

    def recorded_storage(self, record: dict) -> Storage:
        return Storage.model_validate(record["stream"]["storage"])

    def s3_options(self, record: dict):
        profile = self.config.storage_profiles.get(record["storage_profile"])
        if profile is None:
            raise Fault(422, "unknown_storage_profile")

        return profile.s3_options

    @staticmethod
    def _parse_spec(body: Any) -> StreamSpec:
        try:
            return StreamSpec.model_validate(body)
        except ValidationError as exc:
            raise Fault(
                422,
                "invalid_declaration",
                errors=[{"loc": e["loc"], "type": e["type"]} for e in exc.errors()],
            ) from None

    def validate_spec(self, body: Any, name: str) -> dict:
        spec = self._parse_spec(body)
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
            "children_ready": self.children_ready(name),
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

    def status(self, name: str) -> dict:
        record = self.records[name]
        pending = record["pending"]
        observed = self.observed.get(name, {"state": "unknown"})
        return copy.deepcopy(
            {
                "name": name,
                "revision": record["revision"],
                "pending": None
                if pending is None
                else {
                    key: pending[key]
                    for key in ("kind", "revision", "status", "attempt", "error")
                },
                "observed": {
                    "state": observed["state"],
                    "error": observed.get("error"),
                },
            }
        )

    def attempt(self, name: str) -> asyncio.Future:
        pending = self.records[name]["pending"]
        key = (name, pending["revision"], pending["attempt"])
        if key not in self.attempts:
            future = asyncio.get_running_loop().create_future()
            # A commit already in flight may finish after shutdown or worker
            # exit resolved the existing waiters. Its result is unknown too.
            if self.closing:
                future.set_result({"outcome": "unknown"})
                return future

            self.attempts[key] = future

        return self.attempts[key]

    def enqueue(self, name: str) -> None:
        if name not in self.queued:
            self.queued.add(name)
            self.queue.put_nowait(name)

    async def mutate(
        self,
        name: str,
        kind: str,
        body: Any,
        match: str | None,
        none: str | None,
        *,
        principal: str | None = None,
        storage_profile: str | None = None,
        initialization: str | None = None,
    ) -> dict:
        if name in self.static:
            raise Fault(409, "static_stream")

        if self.config.registry is None:
            raise Fault(403, "management_disabled")

        async with self.serial:
            if self.closing:
                raise Fault(503, "broker_stopping")

            if NAME.fullmatch(name) is None:
                raise Fault(422, "invalid_name")

            previous = self.records.get(name)
            target = body
            if kind == "definition":
                authorization = self.config.authorization
                if previous is None and isinstance(
                    authorization, CreationAuthorization
                ):
                    if principal is None or not await authorization.allows_creation(
                        principal, name, storage_profile
                    ):
                        raise Fault(403, "forbidden")

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
            elif kind == "desired":
                if (
                    not isinstance(body, dict)
                    or set(body) != {"state"}
                    or body["state"] not in ("serving", "stopped", "retired")
                ):
                    raise Fault(422, "invalid_desired")

                if body["state"] == "retired" and name in self.config.history_guards:
                    raise Fault(403, "history_prefix_protected")

            elif kind == "migration":
                if name in self.config.history_guards:
                    raise Fault(403, "history_prefix_protected")

                if not isinstance(body, dict) or set(body) != {
                    "stream_id",
                    "schema",
                    "sort_by",
                }:
                    raise Fault(422, "invalid_migration")

                if previous is not None:
                    if (
                        not previous["stream_id"]
                        or body["stream_id"] != previous["stream_id"]
                    ):
                        raise Fault(409, "identity_mismatch")

                    self.validate_spec(
                        previous["stream"]["spec"]
                        | {"schema": body["schema"], "sort_by": body["sort_by"]},
                        name,
                    )

            else:
                raise Fault(422, "invalid_operation")

            if previous is None:
                if kind != "definition":
                    raise Fault(404, "unknown_stream")

                if none is None:
                    raise Fault(428, "precondition_required")

                if none != "*" or match is not None:
                    raise Fault(412, "precondition_failed")

                if len(self.records) >= self.config.max_streams:
                    raise Fault(409, "stream_limit", limit=self.config.max_streams)

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
                if none is not None:
                    raise Fault(412, "precondition_failed")

                if match is None:
                    raise Fault(428, "precondition_required")

                if match != f'"{previous["revision"]}"' or none is not None:
                    raise Fault(412, "precondition_failed")

                pending = previous["pending"]
                if pending is not None:
                    # Validated target shapes are disjoint between routes.
                    # A different intent must never replace a failed migration.
                    if dumps(target) != dumps(pending["target"]):
                        raise Fault(409, "transition_pending")

                    if pending["status"] == "failed":
                        record = copy.deepcopy(previous)
                        record["pending"].update(
                            status="accepted",
                            error=None,
                            attempt=pending["attempt"] + 1,
                        )
                        await thread(self.registry.update, record)
                        self.records[name] = record
                        self.enqueue(name)

                    return {"future": self.attempt(name), "status": self.status(name)}

                record = copy.deepcopy(previous)
                state = self.observed[name]["state"]
                if kind == "definition":
                    if dumps(spec) != dumps(record["stream"]["spec"]):
                        if state != "stopped" and dumps(
                            {k: v for k, v in spec.items() if k not in LIVE}
                        ) != dumps(
                            {
                                k: v
                                for k, v in record["stream"]["spec"].items()
                                if k not in LIVE
                            }
                        ):
                            raise Fault(409, "must_be_stopped")

                        if any(
                            dumps(spec[k]) != dumps(record["stream"]["spec"][k])
                            for k in ("schema", "sort_by")
                        ):
                            raise Fault(409, "migration_required")

                    record["stream"]["spec"] = spec
                elif kind == "desired":
                    if record["desired"] == "retired" and body["state"] != "retired":
                        raise Fault(409, "retired_stream")

                    if body["state"] == "retired" and not record["stream_id"]:
                        raise Fault(409, "unbound_stream")

                    record["desired"] = body["state"]
                elif (
                    state not in ("stopped", "error") or record["desired"] != "stopped"
                ):
                    raise Fault(409, "must_be_stopped")

                equal = (
                    kind != "migration"
                    and dumps(previous["stream"]) == dumps(record["stream"])
                    and previous["desired"] == record["desired"]
                )
                if equal and state == record["desired"]:
                    return {"future": None, "status": self.status(name)}

                # A repair re-executes this revision, rather than inventing a
                # new intent merely because observed state was lost or failed.
                if not equal:
                    record["revision"] += 1

            if (
                kind == "definition"
                and spec.get("maintenance", {}).get("dedicated", False)
                and (
                    previous is None
                    or not previous["stream"]["spec"]
                    .get("maintenance", {})
                    .get("dedicated", False)
                )
                and self.dedicated_count() >= self.config.max_dedicated
            ):
                raise Fault(409, "dedicated_limit", limit=self.config.max_dedicated)

            pending_count = sum(r["pending"] is not None for r in self.records.values())
            if pending_count >= self.config.max_pending:
                raise Fault(429, "operation_queue_full")

            record["pending"] = {
                "kind": kind,
                "revision": record["revision"],
                "target": target,
                "status": "accepted",
                "attempt": 1,
                "error": None,
            }
            # Persist before publishing intent or scheduling any storage effect.
            if previous is None or record["revision"] != previous["revision"]:
                await thread(self.registry.accept, record)
                self.collection += 1
            else:
                await thread(self.registry.update, record)

            self.records[name] = record
            self.observe(name, self.observed.get(name, {}).get("state", "unknown"))
            future = self.attempt(name)
            self.enqueue(name)
            return {"future": future, "status": self.status(name)}

    async def work(self) -> None:
        while True:
            name = await self.queue.get()
            try:
                if name is None:
                    return

                self.queued.discard(name)
                await self.execute(name)
            finally:
                self.queue.task_done()

    def unavailable(self) -> None:
        for future in self.attempts.values():
            if not future.done():
                future.set_result({"outcome": "unknown"})

        self.attempts.clear()

    def worker_done(self, worker: asyncio.Task) -> None:
        self.unavailable()
        if not self.closing:
            self.closing = True
            for generation in self.generations.values():
                generation.accepting = generation.writable = False

        if not worker.cancelled():
            worker.exception()

    async def execute(self, name: str) -> None:
        async with self.serial:
            pending = self.records[name]["pending"]
            if pending is None or pending["status"] != "accepted":
                return

            record = copy.deepcopy(self.records[name])
            pending = record["pending"]
            pending["status"] = "running"
            await thread(self.registry.update, record)
            self.records[name] = copy.deepcopy(record)

        key = (name, pending["revision"], pending["attempt"])
        try:
            if not self.maintenance_only(record):
                await self.claim(record)

            if pending["kind"] == "migration":
                await self.offline(record)
                self.observe(name, "migrating")
                target = pending["target"]
                await self.storage_transition(record, "migration", target)
                record["stream"]["spec"] = record["stream"]["spec"] | {
                    "schema": target["schema"],
                    "sort_by": target["sort_by"],
                }
                self.observe(name, "stopped")
            else:
                await self.reconcile(record)

            record["pending"] = None
            self.applied_specs[name] = copy.deepcopy(record["stream"]["spec"])
            outcome = "applied"
        except Exception as exc:
            pending["status"] = "failed"
            pending["error"] = self.transition_error(exc)
            generation = self.generations.get(name)
            if generation is not None:
                generation.accepting = generation.writable = False

            self.observe(name, "error", error=pending["error"])
            outcome = "failed"

        async with self.serial:
            await thread(self.registry.update, record)
            self.records[name] = record
            snapshot = {
                "outcome": outcome,
                "error": pending["error"],
                "revision": record["revision"],
                "status": self.status(name),
            }
            future = self.attempts.pop(key, None)
            if future is not None:
                future.set_result(snapshot)

    def maintenance_only(self, record: dict) -> bool:
        name = record["name"]
        baseline = self.applied_specs.get(name)
        pending = record["pending"]
        return (
            baseline is not None
            and pending is not None
            and pending["kind"] == "definition"
            and self.observed.get(name, {}).get("state") == record["desired"]
            and dumps({k: v for k, v in baseline.items() if k not in LIVE})
            == dumps(
                {k: v for k, v in record["stream"]["spec"].items() if k not in LIVE}
            )
        )

    async def reconcile(self, record: dict) -> None:
        name = record["name"]
        desired = record["desired"]
        if self.maintenance_only(record):
            if desired == "serving":
                self.generations[name].maintenance = self._parse_spec(
                    record["stream"]["spec"]
                ).maintenance
                await self.reconcile_children()

            self.observe(name, desired)
            return

        self.s3_options(record)
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
            warn_unreplicated(opened)
            # Bind identity durably before a data route can expose it.
            record["stream_id"] = opened._stream_id
            await thread(self.registry.update, record)
            await self.reconcile_children()
            generation.accepting = not self.closing
            self.observe(name, desired)
            self.refresh_gates(time.monotonic())
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
        self.excluding.add(name)
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

        # An in-flight worker may enter offline just as close starts. It must
        # still reap live children before final publication; the close loop
        # itself reaches here with an already empty running set.
        if not self.closing or self.running:
            await self.reconcile_children()

        # Final publication errors are visible to the pending attempt. Maintainers'
        # best-effort exit hooks alone cannot establish a successful stop.
        if name not in self.static:
            await self.finalize(stream, record)

        await stream.aclose()
        # A host may still hold a static stream: once shut it says closed,
        # rather than naming a generation that no longer exists.
        stream._admission = None
        del self.generations[name]
        self.excluding.discard(name)

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

    @property
    def children(self) -> list[Any]:
        return list(self.running.values())

    def trigger_children(self) -> asyncio.Task:
        self.children_dirty = True
        if self.child_pass is None or self.child_pass.done():
            self.child_pass = asyncio.create_task(self.child_passes())

        return self.child_pass

    async def reconcile_children(self) -> None:
        # Cancellation of one waiter cannot abandon a half-stopped group.
        await asyncio.shield(self.trigger_children())

    async def stop_children(self) -> None:
        assert self.closing
        await self.reconcile_children()

    async def child_passes(self) -> None:
        while self.children_dirty:
            self.children_dirty = False
            generations = {
                name: g
                for name, g in self.generations.items()
                if name not in self.excluding
            }
            self.wanted = (
                set() if self.closing else desired(generations, self.config.maintain)
            )
            # Only currently desired identities may retain a retry deadline.
            self.retries = {s: r for s, r in self.retries.items() if s in self.wanted}
            obsolete = {
                spec: child
                for spec, child in self.running.items()
                if spec not in self.wanted or getattr(child, "failed", False)
            }
            for spec, child in obsolete.items():
                if spec in self.wanted and getattr(child, "failed", False):
                    task = getattr(child, "_watch", None)
                    if task is not None and not task.cancelled():
                        exc = task.exception()
                        if exc is not None:
                            logger.error(
                                "child supervision failed: %s %s",
                                spec.kind,
                                spec.role,
                                exc_info=exc,
                            )

                    self.child_failure(spec, "supervision_failed")

            # Existing stop preserves seal -> publish -> other roles. Never
            # publish a removal, or replace a child, before its reap finishes.
            await stop(list(obsolete.values()))
            for spec, child in obsolete.items():
                del self.running[spec]
                self.child_health.pop(spec, None)
                marker = getattr(child, "_ready_file", None)
                if marker is not None:
                    await thread(marker.unlink, missing_ok=True)

            if self.closing:
                continue

            routes = {name: g.stream for name, g in generations.items()}
            for spec in self.wanted - self.running.keys():
                retry = self.retries.get(spec)
                if retry is not None and time.monotonic() < retry[1]:
                    continue

                child = None
                error = "construction_failed"
                try:
                    child = build(spec, routes)
                    if spec.kind == "maintainer":
                        assert self.ready_directory is not None
                        self.child_starts += 1
                        cast(Supervisor, child)._ready_file = (
                            Path(self.ready_directory.name)
                            / f".ready-{self.boot_id}-{self.child_starts}"
                        )

                    error = "start_failed"
                    child.start()
                except Exception:
                    logger.exception("child %s: %s %s", error, spec.kind, spec.role)
                    if child is not None:
                        await stop([child])
                        marker = getattr(child, "_ready_file", None)
                        if marker is not None:
                            await thread(marker.unlink, missing_ok=True)

                    self.child_failure(spec, error)
                    continue

                self.running[spec] = child
                self.child_health[spec] = False
                # A successful construction is not observed readiness, and
                # therefore cannot refresh grace or reset repeated failures.

    def child_failure(self, spec: ChildSpec, error: str) -> None:
        previous = self.retries.get(spec)
        delay = 0.5 if previous is None else min(previous[0] * 2, 30.0)
        self.retries[spec] = (delay, time.monotonic() + delay, error)

    def required(self, name: str, *, sidecars: bool = False) -> set[ChildSpec]:
        generation = self.generations.get(name)
        if generation is None or generation.stream.log is None:
            return set()

        log = generation.stream.log
        target = (Path(log.root), log.name)
        return {
            spec
            for spec in self.wanted
            if target in spec.members and (not sidecars or spec.kind == "sidecar")
        }

    def children_ready(self, name: str | None = None) -> bool:
        required = self.wanted if name is None else self.required(name)
        return all(
            spec in self.running and self.child_health.get(spec, False)
            for spec in required
        )

    def child_status(self, *, counts: bool, visible: set[str] | None = None) -> dict:
        failing = [
            spec
            for spec in self.wanted
            if not self.child_health.get(spec, False) or spec not in self.running
        ]
        converged = self.running.keys() == self.wanted and not failing
        if counts:
            return {
                "converged": converged,
                "wanted": len(self.wanted),
                "running": len(self.running),
                "failing": len(failing),
            }

        failures = []
        for spec in failing:
            names = sorted(
                name
                for name in self.generations
                if (visible is None or name in visible) and spec in self.required(name)
            )
            if names:
                failures.append(
                    {
                        "kind": spec.kind,
                        "role": spec.role,
                        "streams": names,
                        "error": self.retries[spec][2]
                        if spec in self.retries
                        else "not_ready",
                    }
                )

        failures.sort(key=lambda f: (f["kind"], f["role"] or "", f["streams"]))
        return {"converged": converged, "failing": failures}

    async def watch_tick(self) -> None:
        # Snapshot references before leaving the loop; a pass can replace a
        # group while these filesystem/socket probes run in the executor.
        snapshot = list(self.running.items())

        def sample() -> list[bool]:
            return [
                not getattr(child, "failed", False) and child.ready
                for _, child in snapshot
            ]

        ready = await thread(sample)
        for (spec, child), healthy in zip(snapshot, ready, strict=True):
            if self.running.get(spec) is child:
                self.child_health[spec] = healthy
                if healthy:
                    self.retries.pop(spec, None)

        self.refresh_gates(time.monotonic(), observed=True)
        if not self.closing and (self.child_pass is None or self.child_pass.done()):
            now = time.monotonic()
            if any(
                getattr(child, "failed", False) for child in self.running.values()
            ) or any(
                spec not in self.running
                and (spec not in self.retries or now >= self.retries[spec][1])
                for spec in self.wanted
            ):
                self.trigger_children()

    def refresh_gates(self, now: float, *, observed: bool = False) -> None:
        for name, generation in self.generations.items():
            required = self.required(name, sidecars=True)
            if required and generation.sidecar_wait_at is None:
                generation.sidecar_wait_at = now

            ready = all(
                spec in self.running and self.child_health.get(spec, False)
                for spec in required
            )
            if observed and required and ready:
                generation.sidecar_ready_at = now

            grace = (
                generation.sidecar_ready_at is not None
                and now - generation.sidecar_ready_at < self.config.readiness_timeout
            )
            generation.writable = bool(
                not self.closing
                and generation.accepting
                and generation.stream.retirement is None
                and (not required or ready or grace)
            )
            if name not in self.static and generation.accepting:
                if required and not generation.writable:
                    if (
                        generation.sidecar_ready_at is None
                        and generation.sidecar_wait_at is not None
                        and now - generation.sidecar_wait_at
                        < self.config.readiness_timeout
                    ):
                        self.observe(name, "starting")
                    else:
                        self.observe(name, "error", error="children_unavailable")

                else:
                    self.observe(name, self.records[name]["desired"])

    async def watch(self) -> None:
        while True:
            await asyncio.sleep(self.config.readiness_every)
            if not self.closing:
                await self.watch_tick()

    async def close(self) -> None:
        if self.closed:
            return

        self.closing = True
        self.unavailable()
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
        await self.stop_children()
        for name in list(self.generations):
            await self.offline(self.records.get(name, {"name": name}))

        for lock in self.locks.values():
            lock.close()

        self.locks.clear()
        await thread(self.registry.close)
        if self.ready_directory is not None:
            await thread(self.ready_directory.cleanup)

        self.closed = True
