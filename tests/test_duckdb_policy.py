"""Policy reaches real readers; process boundaries preserve the flat declaration."""

from __future__ import annotations

import ast
import inspect
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import duckdb
import litelink
import pyarrow as pa
import pytest
from websockets.exceptions import ConnectionClosedError

import streamcast
from streamcast import _metadata, _stream, _versions
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


async def test_owner_creates_spill_parents_at_start(tmp_path):
    base = tmp_path / "missing" / "parents" / "broker"
    policy = litelink.DuckDBPolicy(16, 1, spill=litelink.Spill(base, 256))
    opened = streamcast.StreamHandle.open(
        definition(tmp_path / "data", policy=policy), initialization="create_if_absent"
    )
    try:
        leaf = base / "t"
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
    bound = _stream._stream_duckdb(policy, "")
    assert bound is not None and bound.spill is not None
    assert bound.spill.directory == base / ".root"
    assert bound.spill.directory.is_dir()
    assert sibling.is_dir()
    for name in ("..", "../another-stream", str(tmp_path.absolute())):
        with pytest.raises(ValueError, match="invalid stream name"):
            _stream._stream_duckdb(policy, name)

    assert sibling.is_dir()


async def test_spill_create_failure_does_not_stop_serving(
    tmp_path, monkeypatch, caplog
):
    base = tmp_path / "blocked"
    real_mkdir = Path.mkdir

    def fail(path, *args, **kwargs):
        if path == base / "t":
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
