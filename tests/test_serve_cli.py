"""Service commands keep provisioning separate from process startup."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import httpx
import pytest

from streamcast import (
    BrokerConfig,
    Maintain,
    Storage,
    Stream,
    StreamHandle,
    StreamSpec,
    _config_file,
    _plan,
    _probe,
    _serve,
    create_app,
)
from streamcast.__main__ import main
from streamcast._application import Opening
from streamcast._broker import runtime
from streamcast._broker.model import ExternalMaintain
from streamcast._broker.registry import Registry
from streamcast._maintain import ROLES

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


AUTH = Grants()


def config_file(tmp_path, *, managed=False):
    path = tmp_path / "broker.toml"
    path.write_text(
        '[server]\nhost = "127.0.0.1"\nport = 8765\n'
        '[broker]\nbroker_id = "sample"\n'
        + (
            f'registry = "{tmp_path / "registry.db"}"\n'
            'authorization = "tests.test_serve_cli:AUTH"\n'
            if managed
            else "maintain = false\n"
        )
        + f'[storage_profiles.default]\nroot = "{tmp_path / "data"}"\n'
        + (
            ""
            if managed
            else '[[streams]]\nstorage_profile = "default"\n'
            '[streams.spec]\nname = "events"\n'
            'schema = {type = "object", properties = {value = {type = "integer"}}, required = ["value"]}\n'
        )
    )
    return path


def test_serve_config_round_trips_to_broker_config(tmp_path):
    path = config_file(tmp_path, managed=True)
    with path.open("a") as file:
        file.write(
            "[broker.maintain]\nseal_every = 0.5\n"
            "duckdb_default_memory_mib = 192\nduckdb_default_threads = 1\n"
        )

    settings = _config_file.load(path)
    expected = BrokerConfig(
        broker_id="sample",
        registry=tmp_path / "registry.db",
        authorization=AUTH,
        storage_profiles={"default": Storage(root=tmp_path / "data")},
        maintain=Maintain(
            seal_every=0.5, duckdb_default_memory_mib=192, duckdb_default_threads=1
        ),
    )
    assert settings.broker == expected
    assert (settings.host, settings.port, settings.streams) == ("127.0.0.1", 8765, ())
    static = _config_file.load(config_file(tmp_path))
    assert static.streams == (
        Stream(
            spec=StreamSpec(name="events", schema=SHAPE),
            storage=expected.storage_profiles["default"],
        ),
    )


async def test_serve_open_existing_never_creates(tmp_path, monkeypatch):
    settings = _config_file.load(config_file(tmp_path))

    class Startup:
        def __init__(self, config):
            self.config = config
            self.started = False

        async def serve(self):
            async with self.config.app.router.lifespan_context(self.config.app):
                pytest.fail("missing storage completed startup")

    monkeypatch.setattr(_serve.uvicorn, "Server", Startup)
    with pytest.raises(FileNotFoundError):
        await _serve.serve(settings)

    assert not list(tmp_path.rglob("buffer.db"))
    assert not list(tmp_path.rglob("registry.db"))
    created = StreamHandle.open(settings.streams[0], initialization="create_if_absent")
    await created.aclose()
    seen = []

    class Server:
        def __init__(self, config):
            self.started = True
            self.config = config
            seen.append(config)

        async def serve(self):
            async with self.config.app.router.lifespan_context(self.config.app):
                assert (
                    self.config.app.state.broker.generations["events"].stream.log
                    is not None
                )

    monkeypatch.setattr(_serve.uvicorn, "Server", Server)
    await _serve.serve(settings)
    assert len(seen) == 1
    assert (seen[0].workers, seen[0].lifespan) == (1, "on")
    assert (seen[0].host, seen[0].port) == ("127.0.0.1", 8765)
    missing = replace(
        settings,
        broker=replace(
            settings.broker,
            registry=tmp_path / "missing.db",
            authorization=AUTH,
            maintain=True,
        ),
        streams=(),
    )
    with pytest.raises(FileNotFoundError):
        await _serve.serve(missing)

    assert not (tmp_path / "missing.db").exists()


async def test_readyz_503_until_lifespan_open(monkeypatch):
    app = create_app(BrokerConfig(maintain=False), streams=StreamHandle("events"))
    entered, release = asyncio.Event(), asyncio.Event()
    original = runtime.BrokerRuntime.open

    async def open_after_gate(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(runtime.BrokerRuntime, "open", open_after_gate)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://broker"
    ) as client:
        context = app.router.lifespan_context(app)
        opening = asyncio.create_task(context.__aenter__())
        await entered.wait()
        try:
            assert (await client.get("/livez")).status_code == 200
            assert (await client.get("/readyz")).status_code == 503
        finally:
            release.set()
            await opening

        try:
            assert (await client.get("/readyz")).status_code == 200
            monkeypatch.setattr(
                app.state.broker, "child_status", lambda **kwargs: {"ready": False}
            )
            assert (await client.get("/readyz")).status_code == 200
            app.state.broker.closing = True
            assert (await client.get("/readyz")).status_code == 503
            assert (await client.get("/livez")).status_code == 200
            app.state.broker.closing = False
        finally:
            await context.__aexit__(None, None, None)

        assert (await client.get("/readyz")).status_code == 503


def test_init_registry_idempotent(tmp_path, capsys):
    path = config_file(tmp_path, managed=True)
    args = ["init-registry", "--config", str(path)]
    assert main(args) == 0
    registry_path = tmp_path / "registry.db"
    initial = Registry.open(registry_path, "sample")
    try:
        with initial.db as db:
            db.execute("UPDATE collection SET revision = 7")
    finally:
        initial.close()

    # A fresh owner verifies the existing identity; it does not reset any rows.
    assert main(args) == 0
    registry = Registry.open(registry_path, "sample")
    try:
        assert registry.db.execute("SELECT revision FROM collection").fetchone() == (7,)
    finally:
        registry.close()

    path.write_text(
        path.read_text().replace('broker_id = "sample"', 'broker_id = "other"')
    )
    assert main(args) != 0
    assert "Traceback" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "key,variable",
    [
        ("access_key", "AWS_ACCESS_KEY_ID"),
        ("secret_key", "AWS_SECRET_ACCESS_KEY"),
        ("session_token", "AWS_SESSION_TOKEN"),
    ],
)
def test_config_refuses_credentials_without_echoing_values(
    tmp_path, capsys, key, variable
):
    path = config_file(tmp_path)
    path.write_text(
        path.read_text()
        + f'\n[storage_profiles.default.s3_options]\n{key} = "do-not-echo-this-value"\n'
    )
    assert main(["serve", "--config", str(path)]) == 2
    output = capsys.readouterr()
    assert variable in output.err
    assert "do-not-echo-this-value" not in output.err + output.out


def test_role_probe_checks_identity_generation_and_age(tmp_path, monkeypatch):
    (tmp_path / "status").mkdir()
    plan = {
        "format": 1,
        "boot_id": "boot",
        "generation": 4,
        "state": "serving",
        "roles": {"seal.default": {"every": 2.0}},
    }
    status = {"boot_id": "boot", "applied_generation": 4, "heartbeat_at": 90.0}
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    status_path = tmp_path / "status/seal.default.json"
    monkeypatch.setattr(_probe.time, "time", lambda: 100.0)
    args = [
        "probe",
        "role",
        "--run-dir",
        str(tmp_path),
        "--role",
        "seal",
        "--group",
        "default",
    ]
    for field, value in (
        (None, None),
        ("boot_id", "old"),
        ("applied_generation", 3),
        ("heartbeat_at", 69.0),
        ("heartbeat_at", 101.0),
    ):
        current = dict(status)
        if field is not None:
            assert value is not None
            current[field] = value

        status_path.write_text(json.dumps(current))
        assert main(args) == (0 if field is None else 1)

    status_path.write_text("{")
    assert main(args) == 1


def test_http_probe_uses_health_route_and_timeout(monkeypatch):
    calls = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    def get(url, *, timeout):
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(_probe, "urlopen", get)
    for kind in ("live", "ready"):
        assert main(["probe", kind, "--url", "http://localhost:8765/"]) == 0

    assert calls == [
        ("http://localhost:8765/livez", 2.0),
        ("http://localhost:8765/readyz", 2.0),
    ]
    Response.status = 503
    assert main(["probe", "ready", "--url", "http://localhost:8765"]) == 1


async def test_serve_opens_through_runtime_not_before_app(tmp_path, monkeypatch):
    settings = _config_file.load(config_file(tmp_path))
    handle = StreamHandle.open(settings.streams[0], initialization="create_if_absent")
    await handle.aclose()
    calls = []
    original_app = _serve.create_app
    original_open = StreamHandle.open

    def app(config, *, streams):
        assert calls == []
        assert all(isinstance(item, Opening) for item in streams)
        calls.append("app")
        return original_app(config, streams=streams)

    def opening(stream, **kwargs):
        assert calls == ["app", "server"]
        calls.append("open")
        return original_open(stream, **kwargs)

    class Server:
        def __init__(self, config):
            assert calls == ["app"]
            calls.append("server")
            self.config, self.started = config, True

        async def serve(self):
            async with self.config.app.router.lifespan_context(self.config.app):
                assert calls == ["app", "server", "open"]

    monkeypatch.setattr(_serve, "create_app", app)
    monkeypatch.setattr(StreamHandle, "open", opening)
    monkeypatch.setattr(_serve.uvicorn, "Server", Server)
    await _serve.serve(settings)
    assert calls == ["app", "server", "open"]


def external_config(tmp_path, *, managed=False):
    path = config_file(tmp_path, managed=managed)
    path.write_text(
        path.read_text().replace("maintain = false\n", "")
        + "[broker.maintain]\nseal_every = 0.25\n"
        + f'[broker.maintain.external]\nrun_dir = "{tmp_path / "run"}"\n'
        + 'groups = ["default"]\n'
    )
    return path


def test_external_table_builds_external_maintain_with_policy_run_dir_and_groups(
    tmp_path,
):
    for managed in (False, True):
        settings = _config_file.load(external_config(tmp_path, managed=managed))
        assert settings.broker.maintain == ExternalMaintain(
            Maintain(seal_every=0.25), tmp_path / "run", {"default": None}
        )


def test_group_maps_to_storage_profile_s3_options_or_none(tmp_path):
    path = external_config(tmp_path, managed=True)
    path.write_text(
        path.read_text().replace(
            'groups = ["default"]', 'groups = ["default", "remote"]'
        )
        + f'[storage_profiles.remote]\nroot = "{tmp_path / "remote"}"\n'
        + '[storage_profiles.remote.s3_options]\nregion = "test-region"\n'
    )
    broker = _config_file.load(path).broker
    assert isinstance(broker.maintain, ExternalMaintain)
    assert dict(broker.maintain.groups) == {
        "default": None,
        "remote": broker.storage_profiles["remote"].s3_options,
    }
    assert broker.maintain.groups["remote"] is not None
    path = external_config(tmp_path, managed=True)
    path.write_text(
        path.read_text().replace(
            f'[storage_profiles.default]\nroot = "{tmp_path / "data"}"\n', ""
        )
    )
    implicit = _config_file.load(path).broker
    assert isinstance(implicit.maintain, ExternalMaintain)
    assert dict(implicit.maintain.groups) == {"default": None}


def test_external_refuses_unknown_key_relative_run_dir_and_unknown_or_empty_groups(
    tmp_path, capsys, monkeypatch
):
    path = external_config(tmp_path)
    original = path.read_text()
    assert isinstance(_config_file.load(path).broker.maintain, ExternalMaintain)

    def forbidden(*args, **kwargs):
        pytest.fail("invalid configuration reached startup")

    monkeypatch.setattr(_serve.uvicorn, "Server", forbidden)
    for text in (
        original + "unexpected = true\n",
        original.replace(str(tmp_path / "run"), "relative"),
        original.replace('groups = ["default"]', "groups = []"),
        original.replace('groups = ["default"]', 'groups = [""]'),
        original.replace('groups = ["default"]', 'groups = ["unknown"]'),
        original.replace('groups = ["default"]', 'groups = ["default", "default"]'),
        original.replace('groups = ["default"]', 'groups = "default"'),
        original.replace(f'run_dir = "{tmp_path / "run"}"\n', ""),
    ):
        path.write_text(text)
        with pytest.raises(_config_file.ConfigurationError) as error:
            _config_file.load(path)

        assert str(error.value) == (
            "invalid service configuration; check fields and host authorization"
        )
        assert main(["serve", "--config", str(path)]) == 2
        assert capsys.readouterr().err.strip() == str(error.value)


def test_external_with_maintain_false_refused(tmp_path, capsys):
    path = external_config(tmp_path)
    assert isinstance(_config_file.load(path).broker.maintain, ExternalMaintain)
    # TOML forbids a scalar and its child table in either order.
    text = path.read_text().replace("[broker]\n", "[broker]\nmaintain = false\n")
    path.write_text(text)
    with pytest.raises(
        _config_file.ConfigurationError, match="^invalid TOML configuration$"
    ):
        _config_file.load(path)

    assert main(["serve", "--config", str(path)]) == 2
    assert capsys.readouterr().err.strip() == "invalid TOML configuration"


async def test_serve_with_external_spawns_no_children_and_writes_plan(
    tmp_path, monkeypatch
):
    settings = _config_file.load(external_config(tmp_path))
    handle = StreamHandle.open(settings.streams[0], initialization="create_if_absent")
    await handle.aclose()
    observed = []

    def forbidden(*args, **kwargs):
        pytest.fail("external maintenance spawned an embedded child")

    class Server:
        def __init__(self, config):
            self.config, self.started = config, True

        async def serve(self):
            async with self.config.app.router.lifespan_context(self.config.app):
                broker = self.config.app.state.broker
                assert broker.running == {}
                plan = _plan.read_plan(tmp_path / "run")
                assert plan.state == "serving"
                assert set(plan.roles) == {f"{role}.default" for role in ROLES}
                assert plan.roles["seal.default"].every == 0.25
                assert broker.generations["events"].writable
                observed.append(True)

    monkeypatch.setattr("streamcast._maintain.Supervisor.start", forbidden)
    monkeypatch.setattr("streamcast._broker.runtime.build", forbidden)
    monkeypatch.setattr(_serve.uvicorn, "Server", Server)
    await _serve.serve(settings)
    assert observed == [True]


async def test_static_stream_outside_listed_groups_refused_at_startup(
    tmp_path, monkeypatch
):
    path = external_config(tmp_path)
    path.write_text(
        path.read_text().replace(
            'storage_profile = "default"',
            f'storage = {{root = "{tmp_path / "other"}", s3_options = {{region = "test-region"}}}}',
        )
    )
    settings = _config_file.load(path)
    assert isinstance(settings.broker.maintain, ExternalMaintain)

    def forbidden(*args, **kwargs):
        pytest.fail("unmapped stream storage was opened")

    monkeypatch.setattr(StreamHandle, "open", forbidden)
    with pytest.raises(ValueError, match="unmapped S3 options"):
        await runtime.BrokerRuntime.open(
            settings.broker, streams=[Opening(item) for item in settings.streams]
        )
