"""Local stack contracts; real Docker cases are required by image.yml."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shlex
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import yaml

import streamcast
from streamcast import _token_auth
from streamcast._broker import runtime
from streamcast._broker.model import ExternalMaintain
from streamcast._broker.registry import Registry
from streamcast._config_file import load
from tests.test_broker import Child, running

ROOT = Path(__file__).resolve().parents[1]
ROLES = ("seal", "publish", "compact", "clean", "clean-published")


def compose_source():
    return yaml.safe_load((ROOT / "docker/compose.yml").read_text())


def test_image_build_precompiles_probe_imports(tmp_path):
    uv = shutil.which("uv")
    assert uv, "uv is required to exercise the image's installation recipe"
    commands = [
        shlex.split(line.removeprefix("RUN "))
        for line in (ROOT / "docker/Dockerfile").read_text().splitlines()
        if line.startswith("RUN uv sync ")
    ]
    assert len(commands) == 1
    environment = tmp_path / "image-env"
    result = subprocess.run(
        [uv, *commands[0][1:], "--offline", "--python", sys.executable],
        cwd=ROOT,
        env={
            **os.environ,
            "UV_PROJECT_ENVIRONMENT": str(environment),
            "UV_COMPILE_BYTECODE": "0",
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    sites = list(environment.glob("lib/python*/site-packages"))
    assert len(sites) == 1
    # The read-only runtime cannot cache these imports between health checks.
    for module in (
        "streamcast/__main__.py",
        "litelink/__init__.py",
        "pyiceberg/__init__.py",
        "pydantic/__init__.py",
    ):
        source = sites[0] / module
        assert source.is_file(), module
        assert Path(importlib.util.cache_from_source(str(source))).is_file(), module


def test_one_service_per_role_each_with_mem_limit():
    services = compose_source()["services"]
    actual = {
        name
        for name, service in services.items()
        if "maintain" in service.get("command", [])
    }
    assert actual == set(ROLES)
    for role in ROLES:
        service = services[role]
        assert service["mem_limit"]
        assert service["command"] == [
            "maintain",
            "--follow",
            "/run/streamcast",
            "--role",
            role,
            "--group",
            "default",
        ]
        assert service["healthcheck"]["test"] == [
            "CMD",
            "streamcast",
            "probe",
            "role",
            "--run-dir",
            "/run/streamcast",
            "--role",
            role,
            "--group",
            "default",
        ]
        assert service["read_only"] is True
        assert service["user"] == "10001:10001"
        assert service["restart"] == "unless-stopped"


def test_stack_config_selects_external_maintenance(tmp_path, monkeypatch):
    token = tmp_path / "token"
    _token_auth.create(token)
    monkeypatch.setenv("STREAMCAST_CONTROL_TOKEN_FILE", str(token))
    config = load(ROOT / "docker/broker.toml")
    assert isinstance(config.broker.maintain, ExternalMaintain)
    assert config.broker.maintain.run_dir == Path("/run/streamcast")
    assert dict(config.broker.maintain.groups) == {"default": None}
    assert isinstance(config.broker.authorization, streamcast.TokenFileAccess)
    assert config.broker.registry == Path("/var/lib/streamcast/registry.db")
    assert config.broker.storage_profiles["default"].replicate is False


def test_control_volume_is_exclusive_and_init_creates_token_first():
    services = compose_source()["services"]
    holders = {
        name
        for name, service in services.items()
        if any(
            volume.split(":")[0] == "control" for volume in service.get("volumes", [])
        )
    }
    assert holders == {"init", "broker", "stack-smoke"}
    assert [" ".join(command.split()) for command in services["init"]["command"]] == [
        "streamcast token-file create /run/streamcast-control/token && "
        "if [ ! -e /var/lib/streamcast/registry.db ]; then "
        "streamcast init-registry --config /etc/streamcast/broker.toml; fi"
    ]
    for role in ROLES:
        assert "STREAMCAST_CONTROL_TOKEN_FILE" not in services[role]["environment"]


def test_init_provisions_once_while_broker_owns_registry(tmp_path, monkeypatch):
    token = tmp_path / "control" / "token"
    token.parent.mkdir(mode=0o700)
    registry = tmp_path / "registry.db"
    config = tmp_path / "broker.toml"
    config.write_text(
        (ROOT / "docker/broker.toml")
        .read_text()
        .replace("/var/lib/streamcast", str(tmp_path))
        .replace("/run/streamcast", str(tmp_path / "run"))
    )
    monkeypatch.setenv("STREAMCAST_CONTROL_TOKEN_FILE", str(token))
    monkeypatch.setenv(
        "PATH", str(Path(sys.executable).parent) + os.pathsep + os.defpath
    )
    command = compose_source()["services"]["init"]["command"][0]
    command = (
        command.replace("/run/streamcast-control/token", shlex.quote(str(token)))
        .replace("/var/lib/streamcast/registry.db", shlex.quote(str(registry)))
        .replace("/etc/streamcast/broker.toml", shlex.quote(str(config)))
    )

    def initialize():
        result = subprocess.run(
            ["/bin/sh", "-ec", command], capture_output=True, text=True, timeout=10
        )
        assert result.returncode == 0, result.stderr

    initialize()
    original_token = token.read_bytes()
    original_registry = registry.read_bytes()
    broker = Registry.open(registry, "local")
    try:
        # Compose can restart completed dependencies while the broker is up.
        initialize()
        assert token.read_bytes() == original_token
        assert registry.read_bytes() == original_registry
        assert broker.db.execute("SELECT broker FROM identity").fetchall() == [
            ("local",)
        ]
    finally:
        broker.close()


async def test_smoke_client_round_trip_twice_without_docker(tmp_path, monkeypatch):
    """Execute the shipped client, without replacing its wire or store logic."""
    token = tmp_path / "token"
    _token_auth.create(token)
    monkeypatch.setenv("STREAMCAST_CONTROL_TOKEN_FILE", str(token))
    monkeypatch.setattr(runtime, "build", lambda *_: Child())
    config = streamcast.BrokerConfig(
        broker_id="smoke",
        registry=tmp_path / "registry.db",
        storage_profiles={"default": streamcast.Storage(root=tmp_path / "streams")},
        authorization=streamcast.TokenFileAccess.from_env(),
        readiness_every=0.01,
    )
    streamcast.initialize_registry(config)
    app = streamcast.create_app(config)
    script = compose_source()["services"]["stack-smoke"]["entrypoint"][-1]
    script = script.replace("/run/streamcast-control/token", str(token)).replace(
        "/var/lib/streamcast/stack-smoke.db", str(tmp_path / "client.db")
    )
    async with app.router.lifespan_context(app), running(app) as uri:
        script = script.replace(
            "http://broker:8765", uri.replace("ws://", "http://")
        ).replace("ws://broker:8765", uri)
        for _ in range(2):
            for phase in ("prepare", "verify"):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    script,
                    phase,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()
                assert process.returncode == 0, stderr.decode()
                if phase == "verify":
                    assert b"20 contiguous offsets, 20 unique rows" in stdout


class Stack:
    def __init__(self, project):
        self.project = project

    def compose(self, *args, **kwargs):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                self.project,
                "-f",
                "docker/compose.yml",
                *args,
            ],
            cwd=ROOT,
            env={**os.environ, "BROKER_PORT": "0"},
            check=False,
            capture_output=True,
            text=True,
            timeout=240,
            **kwargs,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)

        return result.stdout

    def python(self, code, *, service="broker"):
        return self.compose("exec", "-T", service, "python", "-c", code)

    def up(self):
        self.compose("up", "-d", "--wait", "--wait-timeout", "180")


def test_compose_failure_reports_captured_diagnostics(monkeypatch):
    def failed_compose(*args, **kwargs):
        return subprocess.CompletedProcess(
            args[0], 1, "broker starting\n", "role unhealthy\n"
        )

    monkeypatch.setattr(subprocess, "run", failed_compose)
    with pytest.raises(RuntimeError, match="broker starting\nrole unhealthy"):
        Stack("diagnostic-test").up()


@pytest.fixture(scope="module")
def stack():
    if os.environ.get("STREAMCAST_REQUIRE_DOCKER") != "1":
        pytest.skip("Docker tier: image.yml sets STREAMCAST_REQUIRE_DOCKER=1")

    assert shutil.which("docker"), "Docker tier required but docker is absent"
    subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=15)
    instance = Stack(
        os.environ.get("COMPOSE_PROJECT_NAME")
        or "streamcast-test-" + uuid.uuid4().hex[:10]
    )
    try:
        instance.up()
        yield instance
    finally:
        try:
            print(instance.compose("ps", "--all"))
            containers = instance.compose("ps", "--all", "--quiet").split()
            if containers:
                states = subprocess.run(
                    ["docker", "inspect", "--format", "{{json .State}}", *containers],
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
                print(states.stdout + states.stderr)

            print(instance.compose("logs", "--no-color", "--tail", "80"))
        finally:
            instance.compose("down", "--volumes", "--remove-orphans")


@pytest.mark.timeout(240)
def test_sqlite_wal_shared_across_two_containers(stack):
    # Keep the first connection open so its committed row is in the WAL,
    # rather than only testing access to a checkpointed main database.
    reader = stack.compose(
        "run",
        "-d",
        "--no-deps",
        "--entrypoint",
        "python",
        "stack-smoke",
        "-c",
        "import signal, sqlite3; from pathlib import Path; "
        "c=sqlite3.connect('/var/lib/streamcast/wal-check.db'); "
        "assert c.execute('PRAGMA journal_mode=WAL').fetchone()==('wal',); "
        "c.execute('CREATE TABLE value(n)'); c.execute('INSERT INTO value VALUES (42)'); "
        "c.commit(); Path('/var/lib/streamcast/wal-ready').touch(); signal.pause()",
    ).strip()
    try:
        stack.python(
            "import time, sqlite3; from pathlib import Path\n"
            "deadline=time.monotonic()+15\n"
            "while not Path('/var/lib/streamcast/wal-ready').exists():\n"
            "    assert time.monotonic()<deadline; time.sleep(0.05)\n"
            "assert Path('/var/lib/streamcast/wal-check.db-wal').stat().st_size>0\n"
            "c=sqlite3.connect('/var/lib/streamcast/wal-check.db')\n"
            "assert c.execute('SELECT n FROM value').fetchall()==[(42,)]\n"
            "c.execute('INSERT INTO value VALUES (43)'); c.commit()"
        )
        # Read the second container's commit through a third connection.
        stack.python(
            "import sqlite3; c=sqlite3.connect('/var/lib/streamcast/wal-check.db'); "
            "assert c.execute('SELECT n FROM value ORDER BY n').fetchall()==[(42,),(43,)]",
            service="seal",
        )
    finally:
        subprocess.run(["docker", "rm", "-f", reader], check=True, capture_output=True)
        stack.python(
            "from pathlib import Path\n"
            "for name in ('wal-check.db','wal-check.db-wal','wal-check.db-shm','wal-ready'):\n"
            "    Path('/var/lib/streamcast', name).unlink(missing_ok=True)"
        )


@pytest.mark.timeout(240)
def test_stack_smoke_no_gap_no_duplicate_after_role_kill_and_broker_restart(stack):
    stack.compose("run", "--rm", "--no-deps", "stack-smoke", "prepare")
    old = stack.compose("ps", "-q", "broker").strip()
    stack.compose("kill", "-s", "SIGKILL", "clean-published")
    stack.compose("restart", "broker")
    stack.up()
    assert stack.compose("ps", "-q", "broker").strip() == old
    result = stack.compose("run", "--rm", "--no-deps", "stack-smoke", "verify")
    assert "20 contiguous offsets, 20 unique rows" in result


@pytest.mark.timeout(240)
def test_stack_down_keeps_volume(stack):
    stack.python(
        "from pathlib import Path; Path('/var/lib/streamcast/keep-check').write_text('kept')"
    )
    container = stack.compose("ps", "-q", "broker").strip()
    mounts = json.loads(
        subprocess.check_output(["docker", "inspect", container], text=True)
    )[0]["Mounts"]
    volume = next(
        mount["Name"]
        for mount in mounts
        if mount["Destination"] == "/var/lib/streamcast"
    )
    stack.compose("down")
    subprocess.run(
        ["docker", "volume", "inspect", volume], check=True, capture_output=True
    )
    stack.up()
    stack.python(
        "from pathlib import Path; p=Path('/var/lib/streamcast/keep-check'); assert p.read_text()=='kept'; p.unlink()"
    )


def test_stack_broker_runs_external_maintenance_and_spawns_no_children(stack):
    stack.python(
        "from pathlib import Path; import json\n"
        "plan=json.loads(Path('/run/streamcast/plan.json').read_text())\n"
        "assert plan['state']=='serving'\n"
        "assert set(plan['roles'])=={r+'.default' for r in "
        "('seal','publish','compact','clean','clean-published')}\n"
        "# tini is PID 1; the broker is its only child. This probe is a sibling.\n"
        "pids=Path('/proc/1/task/1/children').read_text().split()\n"
        "assert len(pids)==1\n"
        "assert Path('/proc',pids[0],'task',pids[0],'children').read_text().strip()==''"
    )


def test_control_api_refuses_without_token_and_roles_have_no_token_volume(stack):
    stack.python(
        "from urllib.request import Request, urlopen\n"
        "from urllib.error import HTTPError\n"
        "from pathlib import Path\n"
        "url='http://127.0.0.1:8765/control/v1'\n"
        "try:\n"
        "    urlopen(url)\n"
        "except HTTPError as error:\n"
        "    assert error.code==403\n"
        "else:\n"
        "    raise AssertionError('anonymous control allowed')\n"
        "token=Path('/run/streamcast-control/token')\n"
        "assert token.stat().st_mode & 0o077==0\n"
        "assert token.parent.stat().st_mode & 0o777==0o700\n"
        "assert token.stat().st_uid==10001\n"
        "request=Request(url,headers={'Authorization':'Bearer '+token.read_text()})\n"
        "assert urlopen(request).status==200"
    )
    for role in ROLES:
        stack.python(
            "from pathlib import Path; assert not Path('/run/streamcast-control/token').exists()",
            service=role,
        )


def test_just_targets_route_commands_and_confirm_wipe(tmp_path, monkeypatch):
    just = shutil.which("just") or str(Path(sys.executable).with_name("just"))
    if not Path(just).is_file():
        if os.environ.get("STREAMCAST_REQUIRE_DOCKER") == "1":
            pytest.fail("Just is required in the Docker tier")

        pytest.skip("Just is not installed")

    trace = tmp_path / "commands.jsonl"
    executable = tmp_path / "docker"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json\nimport sys\nfrom pathlib import Path\n"
        f"with Path({str(trace)!r}).open('a') as output:\n"
        "    output.write(json.dumps(sys.argv[1:])+'\\n')\n"
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.defpath)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "stack-recipes-test")
    for arguments in (
        ("stack-up", "1", "1"),
        ("stack-ps",),
        ("stack-logs", "broker"),
        ("stack-smoke",),
        ("stack-down",),
        ("image", "streamcast:test"),
    ):
        subprocess.run(
            [just, "--justfile", str(ROOT / "Justfile"), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )

    refused = subprocess.run(
        [just, "--justfile", str(ROOT / "Justfile"), "stack-wipe"],
        input="n\n",
        capture_output=True,
        text=True,
    )
    assert refused.returncode != 0
    before = trace.read_text()
    subprocess.run(
        [just, "--yes", "--justfile", str(ROOT / "Justfile"), "stack-wipe"],
        check=True,
        capture_output=True,
        text=True,
    )
    commands = [json.loads(line) for line in before.splitlines()]
    prefix = [
        "compose",
        "--project-name",
        "stack-recipes-test",
        "-f",
        "docker/compose.yml",
    ]
    assert commands == [
        prefix + tail
        for tail in (
            ["build"],
            ["pull"],
            ["up", "-d", "--wait", "--wait-timeout", "180"],
            ["ps"],
            ["logs", "--follow", "broker"],
            ["run", "--rm", "--no-deps", "stack-smoke", "prepare"],
            ["kill", "-s", "SIGKILL", "clean-published"],
            ["restart", "broker"],
            ["up", "-d", "--wait", "--wait-timeout", "180"],
            ["run", "--rm", "--no-deps", "stack-smoke", "verify"],
            ["down"],
        )
    ] + [
        [
            "buildx",
            "build",
            "--load",
            "--tag",
            "streamcast:test",
            "--file",
            "docker/Dockerfile",
            ".",
        ]
    ]
    last = json.loads(trace.read_text().splitlines()[-1])
    assert last == prefix + ["down", "--volumes"]
