"""Policy reaches real readers; process boundaries preserve the flat declaration."""

from __future__ import annotations

import ast
import errno
import inspect
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import duckdb
import litelink
import pyarrow as pa
import pytest
from websockets.exceptions import ConnectionClosedError

import streamcast
from streamcast import _maintain, _metadata, _spill, _stream, _versions
from streamcast._broker.runtime import BrokerRuntime

SCHEMA = {"type": "object", "properties": {"i": {"type": "integer"}}, "required": ["i"]}
WIDER = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "x": {"type": ["integer", "null"]}},
    "required": ["i"],
}
POLICY = litelink.DuckDBPolicy(64, 1)


def definition(tmp_path, *, policy=POLICY, schema=SCHEMA):
    return streamcast.Stream(
        spec=streamcast.StreamSpec(name="t", schema=schema),
        storage=streamcast.Storage(root=tmp_path, duckdb=policy),
    )


def settings(log):
    return (
        log.sql(
            "SELECT current_setting('memory_limit') AS memory, current_setting('threads') AS threads"
        )
        .read_all()
        .to_pylist()[0]
    )


@pytest.mark.parametrize(
    "fields",
    [
        {"duckdb_default_memory_mib": 16},
        {"duckdb_default_threads": 1},
        {"duckdb_retention_memory_mib": 16},
        {"duckdb_retention_threads": 1},
        {"duckdb_default_memory_mib": 15, "duckdb_default_threads": 1},
        {"duckdb_default_memory_mib": 16, "duckdb_default_threads": 0},
        {"duckdb_default_memory_mib": True, "duckdb_default_threads": 1},
        {"duckdb_default_memory_mib": 16.0, "duckdb_default_threads": 1},
        {"duckdb_retention_spill_mib": 1},
        {"duckdb_spill_root": Path("relative")},
        {"duckdb_spill_root": "/absolute"},
        {"duckdb_retention_memory_mib": 15, "duckdb_retention_threads": 1},
        {"duckdb_retention_memory_mib": 16, "duckdb_retention_threads": False},
        {"duckdb_retention_spill_mib": 0},
    ],
)
def test_maintain_rejects_bad_duckdb_fields(fields):
    with pytest.raises(ValueError, match="duckdb"):
        streamcast.Maintain(**fields)


def test_brokerconfig_takes_maintain_with_duckdb_fields():
    plan = streamcast.Maintain(duckdb_default_memory_mib=16, duckdb_default_threads=1)
    assert streamcast.BrokerConfig(maintain=plan).maintain is plan
    with pytest.raises(ValueError, match="bool or Maintain"):
        streamcast.BrokerConfig(maintain={"duckdb_default_memory_mib": 16})  # ty: ignore[invalid-argument-type]


def test_migrate_signature_takes_duckdb():
    field = inspect.signature(streamcast.StreamHandle.migrate).parameters["duckdb"]
    assert field.kind is inspect.Parameter.KEYWORD_ONLY
    assert field.default is None


def test_storage_duckdb_from_instance_and_mapping(tmp_path):
    spill = litelink.Spill(tmp_path / "spill", 128)
    policy = replace(POLICY, spill=spill)
    assert streamcast.Storage(root=tmp_path, duckdb=policy).duckdb == policy
    assert (
        streamcast.Storage(
            root=tmp_path,
            duckdb={
                "memory_limit_mib": 64,
                "threads": 1,
                "spill": {"directory": str(spill.directory), "max_mib": 128},
            },
        ).duckdb
        == policy
    )


@pytest.mark.parametrize(
    "path", ["create", "existing", "retired", "migrate", "revive", "restore_published"]
)
async def test_storage_duckdb_reaches_litelink(tmp_path, monkeypatch, path):
    source = definition(tmp_path)
    opened = streamcast.StreamHandle.open(source, initialization="create_if_absent")
    if path != "create":
        await opened.send({"i": 1})
        opened.ensure_metadata()
        await opened.aclose()
        if path in {"retired", "revive"}:
            streamcast.StreamHandle.retire("t", root=tmp_path, duckdb=POLICY)

        if path == "migrate":
            opened = streamcast.StreamHandle.migrate(
                "t", root=tmp_path, schema=WIDER, duckdb=POLICY
            )
        elif path == "revive":
            opened = streamcast.StreamHandle.restore(
                "t",
                root=tmp_path,
                published=tmp_path.as_uri(),
                revive=True,
                duckdb=POLICY,
            )
        elif path == "restore_published":
            # Local published tables are real; only the discovery of the stream
            # copy is supplied, since that copy is uploaded for remote prefixes.
            with litelink.open(_metadata.home(tmp_path, "t"), "t") as log:
                log.seal(flush=True)
                log.publish(flush=True)
                published = log.published

            source = source.model_copy(
                update={
                    "storage": streamcast.Storage(
                        root=tmp_path / "restored", published=published, duckdb=POLICY
                    )
                }
            )
            monkeypatch.setattr(
                _versions,
                "occupied",
                lambda prefix, *_: "published" if prefix == published else None,
            )
            opened = streamcast.StreamHandle.open(
                source, initialization="restore_published"
            )
        else:
            opened = streamcast.StreamHandle.open(source)

    try:
        assert settings(opened.log) == {"memory": "64.0 MiB", "threads": 1}
    finally:
        await opened.aclose()


def test_every_litelink_acquisition_forwards_policy():
    # An omitted exceptional recovery call is otherwise hard to distinguish
    # from its neighbouring success path in a small fixture.
    tree = ast.parse(inspect.getsource(_stream))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "litelink"
        and node.func.attr in {"open", "new", "restore"}
    ]
    assert len(calls) == 15
    assert all(any(key.arg == "duckdb" for key in call.keywords) for call in calls)


@pytest.mark.parametrize("operation", ["retire", "retain", "ingest"])
async def test_offline_operations_forward_policy(tmp_path, monkeypatch, operation):
    opened = streamcast.StreamHandle.open(
        definition(tmp_path), initialization="create_if_absent"
    )
    await opened.send({"i": 1})
    if operation != "retain":
        opened.ensure_metadata()

    await opened.aclose()
    real_open = litelink.open
    readers = []

    def acquire(*args, **kwargs):
        log = real_open(*args, **kwargs)
        try:
            readers.append(settings(log))
        except BaseException:
            log.close()
            raise

        return log

    monkeypatch.setattr(litelink, "open", acquire)
    if operation == "retire":
        streamcast.StreamHandle.retire("t", root=tmp_path, duckdb=POLICY)
    elif operation == "retain":
        streamcast.StreamHandle.retain(
            "t", root=tmp_path, max_age=timedelta(days=1), duckdb=POLICY
        )
    else:
        streamcast.StreamHandle.ingest(
            "t", pa.table({"i": [2]}), root=tmp_path, duckdb=POLICY
        )

    assert readers and all(
        value == {"memory": "64.0 MiB", "threads": 1} for value in readers
    )


async def test_spill_base_equal_storage_root_preserves_rows(tmp_path):
    opened = streamcast.StreamHandle.open(
        definition(tmp_path, policy=None), initialization="create_if_absent"
    )
    await opened.send_many([{"i": i} for i in range(3)])
    await opened.aclose()
    policy = replace(POLICY, spill=litelink.Spill(tmp_path, 128))
    reopened = streamcast.StreamHandle.open(
        definition(tmp_path, policy=policy), initialization="create_if_absent"
    )
    try:
        assert reopened.log is not None
        assert reopened.log.sql("SELECT i FROM log ORDER BY i").read_all()[
            "i"
        ].to_pylist() == [0, 1, 2]
        assert reopened.log.end_offset() == 4
    finally:
        await reopened.aclose()


async def test_owner_creates_spill_parents_at_start(tmp_path):
    base = tmp_path / "missing" / "parents" / "broker"
    policy = litelink.DuckDBPolicy(16, 1, spill=litelink.Spill(base, 256))
    opened = streamcast.StreamHandle.open(
        definition(tmp_path / "data", policy=policy), initialization="create_if_absent"
    )
    try:
        leaf = base / _spill.STREAMS / "t"
        assert leaf.is_dir()
        assert opened.log is not None
        assert opened.log.sql(
            "SELECT current_setting('temp_directory') AS path"
        ).read_all()["path"][0].as_py() == str(leaf)
        opened.log.sql(
            "CREATE TABLE heavy AS SELECT i, repeat('x', 100) AS payload FROM range(300000) t(i)"
        ).read_all()
        assert any(leaf.iterdir()), "query must really spill"
    finally:
        await opened.aclose()

    # Startup discards leftovers, keeping ownership per stream.
    leaf.mkdir(parents=True, exist_ok=True)
    (leaf / "leftover").touch()
    opened = streamcast.StreamHandle.open(definition(tmp_path / "data", policy=policy))
    try:
        assert list(leaf.iterdir()) == []
    finally:
        await opened.aclose()


def test_legacy_root_spill_cannot_empty_base(tmp_path):
    base = tmp_path / "spill"
    base.mkdir()
    sibling = base / "another-stream"
    sibling.mkdir()
    policy = replace(POLICY, spill=litelink.Spill(base, 128))
    bound = _stream._stream_duckdb(policy, "", tmp_path / "data")
    assert bound is not None and bound.spill is not None
    assert bound.spill.directory == base / _spill.STREAMS / ".root"
    assert bound.spill.directory.is_dir()
    assert sibling.is_dir()
    for name in ("..", "../another-stream", str(tmp_path.absolute())):
        with pytest.raises(ValueError, match="invalid stream name"):
            _stream._stream_duckdb(policy, name, tmp_path / "data")

    assert sibling.is_dir()


async def test_spill_create_failure_does_not_stop_serving(
    tmp_path, monkeypatch, caplog
):
    base = tmp_path / "blocked"
    real_mkdir = Path.mkdir

    def fail(path, *args, **kwargs):
        if path == base / _spill.STREAMS / "t":
            raise PermissionError("read-only spill directory")

        return real_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail)
    opened = streamcast.StreamHandle.open(
        definition(
            tmp_path / "data",
            policy=litelink.DuckDBPolicy(16, 1, spill=litelink.Spill(base, 128)),
        ),
        initialization="create_if_absent",
    )
    try:
        assert await opened.send({"i": 1}) == 1
        assert opened.log is not None
        assert "could not create DuckDB spill directory" in caplog.text
        with pytest.raises(duckdb.IOException):
            opened.log.sql(
                "CREATE TABLE heavy AS SELECT i, repeat('x', 100) FROM range(300000) t(i)"
            ).read_all()
    finally:
        await opened.aclose()


async def test_broker_start_bounds_default_connection(monkeypatch):
    old = duckdb.default_connection()
    try:
        plan = streamcast.Maintain(
            duckdb_default_memory_mib=16, duckdb_default_threads=1
        )
        owner = await BrokerRuntime.open(streamcast.BrokerConfig(maintain=plan))
        try:
            assert duckdb.sql(
                "SELECT current_setting('memory_limit'), current_setting('threads')"
            ).fetchone() == ("16.0 MiB", 1)
        finally:
            await owner.close()
    finally:
        duckdb.set_default_connection(old)


async def test_replay_over_cap_closes_1011_one_subscription_only(tmp_path, serve):
    # A single Parquet string page needs more than the capped pool. This is
    # a real scan allocation failure, independent of max_replay's row limit.
    with litelink.new(
        tmp_path,
        "t",
        schema=pa.schema([("payload", pa.string())]),
        duckdb=litelink.DuckDBPolicy(16, 1),
    ) as log:
        log.extend([{"payload": "x" * (8 * 1024 * 1024)} for _ in range(5)])
        log.seal(flush=True)
        stream = streamcast.StreamHandle("t", log=log)
        async with serve(stream) as uri, streamcast.connect(uri) as live:
            with pytest.raises(ConnectionClosedError) as closed:
                async with streamcast.connect(uri, offset=1) as replay:
                    await replay.recv()

            assert closed.value.rcvd is not None
            assert closed.value.rcvd.code == 1011
            assert closed.value.rcvd.reason == "stream unavailable"
            async with streamcast.publish(uri) as publisher:
                await publisher.send({"payload": "still serving"})

            assert (await live.recv())[2] == {"payload": "still serving"}


@pytest.mark.parametrize(
    "alias",
    [
        "equal",
        "contains",
        "relative",
        "trailing",
        "parent-link",
        "case-link",
        "home-link",
        "leaf-link",
    ],
)
async def test_spill_alias_refused_without_losing_rows(tmp_path, monkeypatch, alias):
    base = tmp_path / "spill"
    leaf = base / _spill.STREAMS / "t"
    root = leaf / "data" if alias == "contains" else leaf
    if alias in ("home-link", "leaf-link"):
        root = tmp_path / "data"

    if alias == "leaf-link":
        base = root
        leaf = base / _spill.STREAMS / "t"

    if alias == "home-link":
        leaf.mkdir(parents=True)
        root.mkdir()
        (root / "t").symlink_to(leaf, target_is_directory=True)

    opened = streamcast.StreamHandle.open(
        definition(root, policy=None), initialization="create_if_absent"
    )
    await opened.send_many([{"i": i} for i in range(3)])
    await opened.aclose()
    if alias in ("parent-link", "case-link"):
        link = tmp_path / ("SPILL" if alias == "case-link" else "alias")
        link.symlink_to(base, target_is_directory=True)
        base = link
    elif alias == "relative":
        monkeypatch.chdir(tmp_path)
        base = Path("spill")
    elif alias == "trailing":
        base = Path(str(base) + "/")
    elif alias == "leaf-link":
        leaf.parent.mkdir(parents=True)
        leaf.symlink_to(root / "t", target_is_directory=True)

    policy = replace(POLICY, spill=litelink.Spill(base, 128))
    with pytest.raises(ValueError, match="spill"):
        unexpected = streamcast.StreamHandle.open(definition(root, policy=policy))
        await unexpected.aclose()

    reopened = streamcast.StreamHandle.open(definition(root, policy=None))
    try:
        assert reopened.log is not None
        assert reopened.log.sql("SELECT i FROM log ORDER BY i").read_all()[
            "i"
        ].to_pylist() == [0, 1, 2]
    finally:
        await reopened.aclose()


@pytest.mark.parametrize("exists", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_prepare_rejects_symlink_before_any_write(tmp_path, exists, empty):
    target = tmp_path / "data"
    if exists:
        target.mkdir()
        (target / "buffer.db").write_bytes(b"rows")

    leaf = tmp_path / "link"
    leaf.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="spill.*symlink"):
        _spill.prepare(leaf, empty=empty)

    assert leaf.is_symlink()
    assert target.exists() is exists
    if exists:
        assert (target / "buffer.db").read_bytes() == b"rows"


@pytest.mark.parametrize("flag", ["--log", "--retire", "--stream"])
@pytest.mark.parametrize("nested", [False, True])
def test_child_refuses_spill_containing_storage_before_open(
    tmp_path, monkeypatch, flag, nested
):
    leaf = tmp_path / "spill"
    root = leaf / "data" if nested else leaf
    root.mkdir(parents=True)
    marker = root / "buffer.db"
    marker.write_bytes(b"rows")

    def no_open(*args, **kwargs):
        pytest.fail("invalid spill must fail before opening logs")

    monkeypatch.setattr(litelink, "open", no_open)
    args = [
        "--log",
        str(tmp_path / "safe"),
        "t",
        "--role",
        "seal",
        "--every",
        "1",
        "--duckdb-spill-dir",
        str(leaf),
    ]
    if flag == "--log":
        args[1] = str(root)
    else:
        args.extend([flag, str(root), "t"])

    with pytest.raises(ValueError, match="spill.*storage"):
        _maintain.main(args)

    assert marker.read_bytes() == b"rows"


@pytest.mark.parametrize(
    "base_kind", ["root", "parent", "home", "relative", "parent-link", "case-distinct"]
)
async def test_normal_spill_paths_preserve_rows(tmp_path, monkeypatch, base_kind):
    root = tmp_path / "data"
    base = {"root": root, "parent": tmp_path, "home": root / "t"}.get(
        base_kind, tmp_path / "spill"
    )
    if base_kind == "relative":
        monkeypatch.chdir(tmp_path)
        base = Path("spill")
    elif base_kind == "parent-link":
        target = tmp_path / "target"
        target.mkdir()
        base.symlink_to(target, target_is_directory=True)
    elif base_kind == "case-distinct":
        root = tmp_path / "SPILL" / _spill.STREAMS / "t"
        # Case-sensitive filesystems have two independent directories. On a
        # case-insensitive volume this declaration aliases storage and is invalid.
        root.mkdir(parents=True)
        if (base / _spill.STREAMS / "t").exists():
            with pytest.raises(ValueError, match="spill.*storage"):
                _stream._stream_duckdb(
                    replace(POLICY, spill=litelink.Spill(base, 128)), "t", root
                )

            return

    policy = replace(POLICY, spill=litelink.Spill(base, 128))
    opened = streamcast.StreamHandle.open(
        definition(root, policy=policy), initialization="create_if_absent"
    )
    await opened.send_many([{"i": i} for i in range(3)])
    await opened.aclose()
    reopened = streamcast.StreamHandle.open(definition(root, policy=policy))
    try:
        assert reopened.log is not None
        assert reopened.log.sql("SELECT i FROM log ORDER BY i").read_all()[
            "i"
        ].to_pylist() == [0, 1, 2]
    finally:
        await reopened.aclose()


@pytest.mark.parametrize(
    "operation", ["open", "migrate", "restore", "retain", "retire", "ingest"]
)
def test_all_stream_entrypoints_refuse_before_creating_spill(tmp_path, operation):
    base = tmp_path / "not-created"
    root = base / _spill.STREAMS / "t" / "data"
    policy = replace(POLICY, spill=litelink.Spill(base, 128))
    kwargs = {"root": root, "duckdb": policy}
    if operation == "migrate":
        kwargs["schema"] = SCHEMA
    elif operation == "restore":
        kwargs["published"] = str(tmp_path / "published")
    elif operation == "retain":
        kwargs["max_age"] = None
    elif operation == "ingest":
        kwargs["source"] = pa.table({"i": [1]})

    with pytest.raises(ValueError, match="spill.*storage"):
        if operation == "open":
            streamcast.StreamHandle.open(
                definition(root, policy=policy), initialization="create_if_absent"
            )
        else:
            getattr(streamcast.StreamHandle, operation)("t", **kwargs)

    assert not base.exists()


@pytest.mark.parametrize("alias", ["nested-log", "parent-link"])
async def test_spill_inside_home_refused_without_losing_rows(tmp_path, alias):
    base = tmp_path / "spill"
    name = "streams" if alias == "nested-log" else "t"
    root = base / ".spill" if alias == "nested-log" else tmp_path / "data"
    if alias == "parent-link":
        base = root

    def source(policy):
        return streamcast.Stream(
            spec=streamcast.StreamSpec(name=name, schema=SCHEMA),
            storage=streamcast.Storage(root=root, duckdb=policy),
        )

    opened = streamcast.StreamHandle.open(
        source(None), initialization="create_if_absent"
    )
    await opened.send_many([{"i": i} for i in range(3)])
    await opened.aclose()
    if alias == "parent-link":
        (root / ".spill").mkdir()
        (root / _spill.STREAMS).symlink_to(root / name, target_is_directory=True)

    policy = replace(POLICY, spill=litelink.Spill(base, 64))
    with pytest.raises(ValueError, match="spill.*storage"):
        unexpected = streamcast.StreamHandle.open(source(policy))
        await unexpected.aclose()

    reopened = streamcast.StreamHandle.open(source(None))
    try:
        assert reopened.log is not None
        assert reopened.log.sql("SELECT i FROM log ORDER BY i").read_all()[
            "i"
        ].to_pylist() == [0, 1, 2]
        assert reopened.log.end_offset() == 4
    finally:
        await reopened.aclose()


@pytest.mark.skipif(
    os.geteuid() == 0, reason="requires unprivileged directory permissions"
)
async def test_untraversable_spill_parent_still_serves(tmp_path, caplog):
    root = tmp_path / "data"
    opened = streamcast.StreamHandle.open(
        definition(root, policy=None), initialization="create_if_absent"
    )
    await opened.send_many([{"i": i} for i in range(3)])
    await opened.aclose()
    locked = tmp_path / "locked"
    locked.mkdir()
    policy = replace(POLICY, spill=litelink.Spill(locked / "spill", 64))
    locked.chmod(0)
    try:
        with pytest.raises(PermissionError):
            (locked / "spill").stat()

        reopened = streamcast.StreamHandle.open(definition(root, policy=policy))
        try:
            assert reopened.log is not None
            assert reopened.log.sql("SELECT i FROM log ORDER BY i").read_all()[
                "i"
            ].to_pylist() == [0, 1, 2]
            assert await reopened.send({"i": 3}) == 4
            assert "could not create DuckDB spill directory" in caplog.text
        finally:
            await reopened.aclose()

    finally:
        locked.chmod(0o700)

    assert not (locked / "spill").exists()


@pytest.mark.parametrize(
    "operation", ["is_symlink", "resolve", "storage-resolve", "exists", "samefile"]
)
@pytest.mark.parametrize("error", [errno.EACCES, errno.EIO])
def test_spill_validation_inspection_errors_are_nonfatal(
    tmp_path, monkeypatch, operation, error
):
    leaf, storage = tmp_path / "spill", tmp_path / "data"
    leaf.mkdir()
    storage.mkdir()
    marker = leaf / "keep"
    marker.touch()
    method = "resolve" if operation == "storage-resolve" else operation
    target = storage if operation == "storage-resolve" else leaf
    original = getattr(Path, method)
    failures = []

    def inspect(path, *args, **kwargs):
        if path == target:
            failures.append(path)
            raise OSError(error, "spill inspection unavailable")

        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, method, inspect)
    _spill.validate(leaf, (storage,))
    assert failures, "the failing inspection must be reached"
    assert marker.is_file()
