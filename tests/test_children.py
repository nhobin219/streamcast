"""Child identity, pass ownership, retry and gates without process timing."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from streamcast import BrokerConfig, Maintenance
from streamcast._broker import runtime
from streamcast._children import ChildSpec, desired
from streamcast._maintain import Maintain, Supervisor
from streamcast._replicate import Sidecar


def generation(
    name, *, root=Path("logs"), replicate=False, credential="one", retired=()
):
    log = SimpleNamespace(
        root=root, name=name, config=SimpleNamespace(wal_replication=replicate)
    )
    stream = SimpleNamespace(
        name=name,
        definition=None,
        log=log,
        retirement=None,
        retired=retired,
        replicate=replicate,
        _s3=SimpleNamespace(resolved=lambda: {"access_key_id": credential}),
    )
    result = runtime.Generation(cast(Any, stream), 1)
    result.accepting = True
    return result


class Child:
    def __init__(self):
        self.ready = False
        self.failed = False
        self._ready_file = None
        self.stops = 0
        self.closed = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    def start(self):
        pass

    def terminate(self):
        self.stops += 1
        self.ready = False
        self.entered.set()

    async def wait_closed(self):
        await self.release.wait()
        self.closed = True


@pytest.fixture
def owner(tmp_path):
    result = runtime.BrokerRuntime(
        BrokerConfig(maintain=False, readiness_timeout=2), cast(Any, None)
    )
    result.ready_directory = SimpleNamespace(name=str(tmp_path))
    result.static = cast(Any, {"a": None, "b": None})
    return result


def test_effective_identity_is_pure_and_role_specific(monkeypatch):
    # Credential extraction itself is pure. No constructor can run here.
    monkeypatch.setattr(
        "streamcast._children.environment", lambda options: {"key": options}
    )
    a = generation("log", root=Path("a"), replicate=True)
    a.stream._s3 = "secret-a"
    a.stream.name = "route"
    b = generation("log", root=Path("b"), replicate=True)
    b.stream._s3 = "secret-b"
    values = {"a": a, "b": b}
    initial = desired(values, Maintain())
    assert len(initial) == 12
    assert initial == desired(dict(reversed(list(values.items()))), Maintain())
    assert "secret-a" not in repr(initial)
    changed = desired(values, Maintain(compact_every=9))
    assert {s.role for s in initial ^ changed} == {"compact"}
    flushed = desired(values, Maintain(flush_every=9))
    assert {s.role for s in initial ^ flushed} == {"seal", "publish"}
    cleanup = next(
        s
        for s in initial
        if s.role == "clean-published" and s.members[0][0] == Path("a")
    )
    assert cleanup.streams == ((Path("a"), "route"),)
    a.stream.retired = ((Path("a"), "old"),)
    retired = desired(values, Maintain())
    assert {s.role for s in initial ^ retired} == {"publish"}
    assert next(
        s for s in retired if s.role == "publish" and s.members[0][0] == Path("a")
    ).retiring == ((Path("a"), "old"),)
    a.maintenance = Maintenance(dedicated=True, compact_every=3)
    changed = desired(values, Maintain())
    assert (
        next(
            s for s in changed if s.role == "compact" and s.members[0][0] == Path("a")
        ).every
        == 3
    )
    assert {s for s in changed if s.kind == "sidecar"} == {
        s for s in initial if s.kind == "sidecar"
    }
    assert desired({}, Maintain()) == set()


@pytest.mark.parametrize("flush", [None, 7])
def test_dedicated_overrides_inherit_host_defaults_without_changing_neighbors(
    flush, monkeypatch
):
    monkeypatch.setattr("streamcast._children.environment", lambda _: {})
    a, b = generation("a"), generation("b")
    host = Maintain(seal_every=2, compact_every=8, flush_every=flush)
    original = desired({"a": a, "b": b}, host)
    a.maintenance = Maintenance(dedicated=True, compact_every=3)
    split = desired({"a": a, "b": b}, host)
    assert len(split) == 10
    for spec in split:
        assert spec.role is not None
        assert len(spec.members) == 1
        name = spec.members[0][1]
        assert spec.every == (
            3 if name == "a" and spec.role == "compact" else host.every(spec.role)
        )
        assert spec.flush == (flush if spec.role in ("seal", "publish") else None)

    a.maintenance = Maintenance(dedicated=True, compact_every=3, flush_every=4)
    changed = desired({"a": a, "b": b}, host)
    assert {s for s in split if s.members[0][1] == "b"} == {
        s for s in changed if s.members[0][1] == "b"
    }
    assert {s.role for s in split ^ changed} == {"seal", "publish"}
    a.maintenance = Maintenance()
    assert desired({"a": a, "b": b}, host) == original


async def test_one_owner_repeats_fresh_state_and_reaps_before_start(owner, monkeypatch):
    first = ChildSpec("sidecar", ((Path("logs"), "a"),), ())
    second = replace(first, members=((Path("logs"), "b"),))
    keep = replace(first, credentials=(("key", "other"),))
    planned = {first, keep}
    built = {}

    def build(spec, _routes):
        assert not old.entered.is_set() or old.closed
        child = Child()
        built[spec] = child
        return child

    monkeypatch.setattr(runtime, "desired", lambda *_: planned.copy())
    monkeypatch.setattr(runtime, "build", build)
    old = Child()
    owner.running = {first: old, keep: Child()}
    unchanged = owner.running[keep]
    old.release.clear()
    planned = {second, keep}
    task = asyncio.create_task(owner.reconcile_children())
    await old.entered.wait()
    assert first in owner.running and second not in built
    assert owner.child_pass is not None
    active = owner.child_pass
    planned = {keep}
    assert owner.trigger_children() is active
    old.release.set()
    await task
    assert owner.running == {keep: unchanged}
    assert unchanged.stops == 0
    assert old.stops == 1 and old.closed
    assert built[second].stops == 1 and built[second].closed
    assert not owner.children_ready()  # pass completion never implies readiness
    owner.closing = True
    await owner.stop_children()
    assert unchanged.stops == 1
    assert not owner.running


async def test_retry_backoff_pruning_failed_start_cleanup_and_markers(
    owner, monkeypatch
):
    spec = ChildSpec("maintainer", ((Path("logs"), "a"),), (), role="seal", every=1)
    planned = {spec}
    children = []
    failing = True

    class Starting(Child):
        def start(self):
            if failing:
                raise OSError("private exception")

    def build(*_):
        child = Starting()
        children.append(child)
        return child

    monkeypatch.setattr(runtime, "desired", lambda *_: planned.copy())
    monkeypatch.setattr(runtime, "build", build)
    for delay in (0.5, 1, 2, 4, 8, 16, 30, 30):
        if spec in owner.retries:
            previous, _, error = owner.retries[spec]
            owner.retries[spec] = (previous, 0, error)

        await owner.reconcile_children()
        assert owner.retries[spec][0] == delay
        assert children[-1].closed and children[-1].stops == 1
        assert not owner.running
        count = len(children)
        await owner.reconcile_children()
        assert len(children) == count

    markers = [c._ready_file for c in children]
    assert len(set(markers)) == len(markers)
    assert all(owner.boot_id in path.name for path in markers)
    failing = False
    owner.retries[spec] = (30, 0, "start_failed")
    await owner.watch_tick()
    await owner.child_pass
    assert spec in owner.running
    assert not owner.children_ready()
    owner.running[spec].ready = True
    await owner.watch_tick()
    assert owner.children_ready() and not owner.retries
    owner.child_failure(spec, "start_failed")
    planned.clear()
    await owner.reconcile_children()
    assert not owner.retries and not owner.running


async def test_dead_supervision_is_stopped_before_retry(owner, monkeypatch):
    spec = ChildSpec("sidecar", (), ())
    old = Child()
    old.failed = True
    owner.running[spec] = old
    monkeypatch.setattr(runtime, "desired", lambda *_: {spec})
    made = []

    def build(*_):
        assert old.closed
        made.append(Child())
        return made[-1]

    monkeypatch.setattr(runtime, "build", build)
    await owner.watch_tick()
    await owner.child_pass
    assert old.closed and old.stops == 1
    assert not made and owner.retries[spec][2] == "supervision_failed"
    owner.retries[spec] = (0.5, 0, "supervision_failed")
    await owner.watch_tick()
    await owner.child_pass
    assert owner.running[spec] is made[0]


async def test_gates_are_per_stream_sidecar_only_and_grace_survives_churn(
    owner, monkeypatch
):
    a, b = generation("a", replicate=True), generation("b")
    owner.generations = {"a": a, "b": b}
    sidecar = ChildSpec("sidecar", ((Path("logs"), "a"),), ())
    maintain = replace(sidecar, kind="maintainer", role="seal", every=1)
    owner.wanted = {sidecar, maintain}
    owner.running[sidecar] = Child()
    owner.refresh_gates(10)
    assert not a.writable and b.writable
    assert not owner.children_ready("a") and owner.children_ready("b")
    owner.child_health[sidecar] = True
    owner.refresh_gates(11, observed=True)
    assert a.writable and a.sidecar_ready_at == 11
    assert not owner.children_ready("a")  # a missing maintainer is telemetry only
    owner.refresh_gates(12)
    assert a.sidecar_ready_at == 11  # a pass cannot renew an old observation
    changed = replace(sidecar, members=sidecar.members + ((Path("logs"), "other"),))
    owner.wanted = {changed, maintain}
    owner.refresh_gates(12)
    assert a.writable and a.sidecar_ready_at == 11
    owner.retries[changed] = (0.5, 0, "start_failed")
    owner.refresh_gates(13)
    assert not a.writable and b.writable and a.sidecar_ready_at == 11
    # A blocked pass must not suspend readiness/gate checks.
    child = owner.running[sidecar]
    child.release.clear()
    monkeypatch.setattr(runtime, "desired", lambda *_: {changed, maintain})
    task = asyncio.create_task(owner.reconcile_children())
    await child.entered.wait()
    await owner.watch_tick()
    assert not a.writable and b.writable
    child.release.set()
    monkeypatch.setattr(runtime, "build", lambda *_: Child())
    await task
    filtered = owner.child_status(counts=False, visible={"b"})
    assert filtered == {"converged": False, "failing": []}
    details = owner.child_status(counts=False, visible={"a"})
    assert all(f["streams"] == ["a"] for f in details["failing"])
    assert all(
        set(f) == {"kind", "role", "streams", "error"} for f in details["failing"]
    )


class Process:
    def __init__(self):
        self.reaped = False
        self.terminated = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self):
        self.reaped = True


async def dead_watch():
    raise OSError("respawn failed")


@pytest.mark.parametrize("kind", ["maintainer", "sidecar"])
async def test_dead_watch_cannot_skip_process_reap_or_lock_release(tmp_path, kind):
    process = Process()
    task = asyncio.create_task(dead_watch())
    with pytest.raises(OSError):
        await task

    if kind == "maintainer":
        child = Supervisor(((tmp_path, "a"),), Maintain(), "seal")
    else:
        child = Sidecar.__new__(Sidecar)
        child._stopping = False
        child._workdir = tmp_path / "work"
        child._workdir.mkdir()
        lock = (tmp_path / "lock").open("w")
        child._locks = {tmp_path: lock}
        child._owned = {tmp_path}

    child._process = cast(Any, process)
    child._watch = task
    assert child.failed
    child.terminate()
    await child.wait_closed()
    assert process.terminated and process.reaped
    if kind == "sidecar":
        assert isinstance(child, Sidecar)
        assert lock.closed and not child._locks and not child._owned
        assert not child._workdir.exists()


async def test_offline_excludes_before_finalization_and_close_stops_once(
    owner, monkeypatch
):
    a, b = generation("a"), generation("b")
    owner.generations = {"a": a, "b": b}
    owner.static = {}
    owner.records = {"a": {"name": "a"}, "b": {"name": "b"}}
    owner.registry = SimpleNamespace(close=lambda: None)
    owner.ready_directory.cleanup = lambda: None
    monkeypatch.setattr(owner, "observe", lambda *_args, **_kwargs: None)
    first = ChildSpec("sidecar", ((Path("logs"), "a"),), ())
    second = replace(first, members=((Path("logs"), "b"),))
    children = {first: Child(), second: Child()}
    owner.running = children.copy()
    monkeypatch.setattr(
        runtime, "desired", lambda gs, _: {s for s in children if s.members[0][1] in gs}
    )

    async def finalize(stream, _record):
        assert not any(
            (Path(stream.log.root), stream.log.name) in s.members for s in owner.running
        )
        if stream is a.stream:
            assert first not in owner.running and children[first].closed
            assert (
                owner.running[second] is children[second]
                and children[second].stops == 0
            )
        else:
            assert not owner.running and children[second].closed

    async def aclose():
        pass

    monkeypatch.setattr(owner, "finalize", finalize)
    for g in (a, b):
        g.stream._writer = None
        g.stream.aclose = aclose

    await owner.offline(owner.records["a"])
    assert "a" not in owner.generations
    monkeypatch.setattr(
        runtime, "build", lambda *_: pytest.fail("close cannot start a child")
    )
    original_offline = owner.offline

    async def closing_offline(record):
        assert not owner.running
        await original_offline(record)

    monkeypatch.setattr(owner, "offline", closing_offline)
    await owner.close()
    assert children[first].stops == children[second].stops == 1
    assert not owner.generations and owner.closed


async def test_close_during_offline_reaps_before_worker_finalizes(owner, monkeypatch):
    a = generation("a")
    owner.generations = {"a": a}
    owner.static = {}
    owner.records = {"a": {"name": "a"}}
    owner.registry = SimpleNamespace(close=lambda: None)
    owner.ready_directory.cleanup = lambda: None
    monkeypatch.setattr(owner, "observe", lambda *_args, **_kwargs: None)
    spec = ChildSpec("sidecar", ((Path("logs"), "a"),), ())
    child = Child()
    owner.running[spec] = child
    entered, release, closing = asyncio.Event(), asyncio.Event(), asyncio.Event()
    joined = asyncio.get_running_loop().create_future()
    joined.set_result(None)

    async def peer_close(*_):
        entered.set()
        await release.wait()

    a.connections[joined] = SimpleNamespace(close=peer_close)

    async def finalize(*_):
        assert child.closed and not owner.running

    async def aclose():
        pass

    a.stream._writer = None
    a.stream.aclose = aclose
    monkeypatch.setattr(owner, "finalize", finalize)
    monkeypatch.setattr(owner, "unavailable", closing.set)
    owner.worker = asyncio.create_task(owner.offline(owner.records["a"]))
    await entered.wait()
    task = asyncio.create_task(owner.close())
    await closing.wait()
    release.set()
    await task
    assert child.stops == 1 and owner.closed


def policy_generation(name):
    result = generation(name)
    result.stream._s3 = None
    return result


def duckdb_plan(tmp_path, **changes):
    return Maintain(
        duckdb_default_memory_mib=16,
        duckdb_default_threads=1,
        duckdb_retention_memory_mib=64,
        duckdb_retention_threads=2,
        duckdb_retention_spill_mib=128,
        duckdb_spill_root=tmp_path / "missing" / "maintain",
        **changes,
    )


@pytest.mark.parametrize("dedicated", [False, True])
def test_spawn_argv_carries_duckdb_policy(tmp_path, dedicated):
    plan = duckdb_plan(tmp_path)
    generations = {"a": policy_generation("a"), "b": policy_generation("b")}
    generations["a"].maintenance = Maintenance(
        dedicated=dedicated, **({"seal_every": 7} if dedicated else {})
    )
    specs = desired(generations, plan)
    assert len(specs) == (10 if dedicated else 5)
    routes = {name: g.stream for name, g in generations.items()}
    directories = set()
    for spec in specs:
        child = runtime.build(spec, routes)
        assert isinstance(child, Supervisor)
        argv = child._spawn_argv()
        for flag, expected in (
            ("default-memory-mib", "16"),
            ("default-threads", "1"),
            ("retention-memory-mib", "64"),
            ("retention-threads", "2"),
            ("retention-spill-mib", "128"),
            ("spill-dir", str(spec.spill)),
        ):
            assert argv[argv.index("--duckdb-" + flag) + 1] == expected

        assert spec.spill is not None and spec.role is not None
        assert spec.spill.parent == plan.duckdb_spill_root
        assert spec.spill.name.startswith(spec.role + "-")
        directories.add(spec.spill)
        if spec.role == "clean-published":
            assert "--stream" in argv

    assert len(directories) == len(specs)
    assert not plan.duckdb_spill_root.exists()  # planning stays pure


async def test_policy_change_replaces_children(owner, tmp_path, monkeypatch):
    owner.config = replace(owner.config, maintain=duckdb_plan(tmp_path))
    owner.generations = {"a": policy_generation("a")}
    built = []

    def build(spec, routes):
        child = Child()
        built.append(child)
        return child

    monkeypatch.setattr(runtime, "build", build)
    await owner.reconcile_children()
    old = dict(owner.running)
    owner.config = replace(
        owner.config,
        maintain=replace(owner.config.maintain, duckdb_retention_threads=3),
    )
    await owner.reconcile_children()
    assert len(owner.running) == 5
    assert old.keys().isdisjoint(owner.running)
    assert all(child.closed for child in old.values())
    assert len(built) == 10


async def test_supervisor_removes_unnamed_spill_dirs(owner, tmp_path, monkeypatch):
    plan = duckdb_plan(tmp_path)
    root = plan.duckdb_spill_root
    root.mkdir(parents=True)
    stale = root / "obsolete"
    stale.mkdir()
    (stale / "spill").touch()
    owner.config = replace(owner.config, maintain=plan)
    owner.generations = {"a": policy_generation("a")}
    monkeypatch.setattr(runtime, "build", lambda *_: Child())
    await owner.reconcile_children()
    assert root.is_dir() and not stale.exists()
    # An unnamed directory created after startup may still have a live owner.
    stale.mkdir()
    await owner.reconcile_children()
    assert stale.exists()


@pytest.mark.parametrize("failed_create", [False, True])
async def test_supervisor_creates_spill_root_parents(
    owner, tmp_path, monkeypatch, caplog, failed_create
):
    plan = duckdb_plan(tmp_path)
    root = plan.duckdb_spill_root
    owner.config = replace(owner.config, maintain=plan)
    owner.generations = {"a": policy_generation("a")}
    monkeypatch.setattr(runtime, "build", lambda *_: Child())
    if failed_create:
        real_mkdir = Path.mkdir

        def mkdir(path, *args, **kwargs):
            if path == root:
                raise PermissionError("read-only spill root")

            return real_mkdir(path, *args, **kwargs)

        monkeypatch.setattr(Path, "mkdir", mkdir)

    await owner.reconcile_children()
    assert len(owner.running) == 5
    assert root.is_dir() is not failed_create
    if failed_create:
        assert "could not create DuckDB spill directory" in caplog.text


async def test_spill_dir_removed_only_after_reap(owner, tmp_path, monkeypatch):
    plan = duckdb_plan(tmp_path)
    owner.config = replace(owner.config, maintain=plan)
    owner.generations = {"a": policy_generation("a")}
    monkeypatch.setattr(runtime, "build", lambda *_: Child())
    await owner.reconcile_children()
    spec, child = next(iter(owner.running.items()))
    spec.spill.mkdir(parents=True)
    (spec.spill / "in-flight").touch()
    child.release.clear()
    owner.generations = {}
    task = asyncio.create_task(owner.reconcile_children())
    await child.entered.wait()
    assert spec.spill.is_dir() and not child.closed
    child.release.set()
    await task
    assert child.closed and not spec.spill.exists()

    # A failed watcher replaced at the same identity must retain its leaf.
    owner.generations = {"a": policy_generation("a")}
    await owner.reconcile_children()
    spec, child = next(iter(owner.running.items()))
    spec.spill.mkdir(parents=True)
    (spec.spill / "same-spec").touch()
    child.failed = True
    await owner.reconcile_children()
    assert child.closed and (spec.spill / "same-spec").exists()
