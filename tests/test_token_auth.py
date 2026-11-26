"""Token files grant control authority without putting tokens in commands."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import httpx
import pytest

import streamcast
from streamcast import _config_file
from streamcast.__main__ import main
from streamcast._broker.model import PublicAccess
from tests.test_serve_cli import config_file

ACTIONS = (
    "data.read",
    "data.publish",
    "stats.read",
    "control.read",
    "control.mutate",
    "control.migrate",
    "control.delete",
    "future.action",
)


def token_file(tmp_path, monkeypatch):
    path = tmp_path / "token"
    value = secrets.token_urlsafe(32).encode("ascii")
    path.write_bytes(value)
    path.chmod(0o600)
    monkeypatch.setenv("STREAMCAST_CONTROL_TOKEN_FILE", str(path))
    return path, value


def same(left, right):
    return hmac.compare_digest(
        hashlib.sha256(left).digest(), hashlib.sha256(right).digest()
    )


async def test_token_file_access_grants_operator_only_with_matching_bearer(
    tmp_path, monkeypatch
):
    path, value = token_file(tmp_path, monkeypatch)
    access = streamcast.TokenFileAccess.from_env()
    compared = []
    original_compare = hmac.compare_digest

    def compare(left, right):
        assert isinstance(left, bytes) and isinstance(right, bytes)
        assert len(left) == len(right) == 32
        compared.append(True)
        return original_compare(left, right)

    monkeypatch.setattr(hmac, "compare_digest", compare)
    for header in (b"Bearer " + value, b"bearer " + value):
        principal = await access.authenticate({"headers": [(b"authorization", header)]})
        assert principal == "operator"
        for action in ACTIONS:
            assert await access.allows(principal, action, "events")

    assert compared == [True, True]
    assert all(part not in repr(access) for part in (value.decode(), str(path)))
    # The configured factory, including its dotted attribute, supplies real control routes.
    config = config_file(tmp_path, managed=True)
    config.write_text(
        config.read_text()
        .replace(
            'authorization = "tests.test_serve_cli:AUTH"',
            'authorization = "streamcast:TokenFileAccess.from_env"',
        )
        .replace("authorization =", "maintain = true\nauthorization =")
    )
    settings = _config_file.load(config)
    streamcast.initialize_registry(settings.broker)
    app = streamcast.create_app(settings.broker)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://broker"
        ) as client:
            assert (await client.get("/control/v1")).status_code == 403
            response = await client.get(
                "/control/v1", headers={"Authorization": "Bearer " + value.decode()}
            )
            assert response.status_code == 200


async def test_token_file_access_public_actions_without_token(tmp_path, monkeypatch):
    _, value = token_file(tmp_path, monkeypatch)
    access = streamcast.TokenFileAccess.from_env()
    public = PublicAccess()
    for headers in (
        [],
        [(b"authorization", b"Bearer " + secrets.token_urlsafe(32).encode())],
        [(b"authorization", b"Basic " + value)],
        [(b"authorization", b"Bearer")],
        [(b"authorization", b"Bearer " + value + b"extra")],
        [(b"authorization", b"Bearer " + value), (b"authorization", b"Bearer wrong")],
    ):
        principal = await access.authenticate({"headers": headers})
        assert principal == "public"
        for action in ACTIONS:
            assert await access.allows(
                principal, action, "events"
            ) == await public.allows(principal, action, "events")

    assert await access.authenticate({}) == "public"


def test_from_env_refuses_missing_empty_or_group_readable_file(
    tmp_path, monkeypatch, capsys
):
    path, _ = token_file(tmp_path, monkeypatch)
    assert isinstance(streamcast.TokenFileAccess.from_env(), streamcast.TokenFileAccess)
    config = config_file(tmp_path, managed=True)
    config.write_text(
        config.read_text().replace(
            "tests.test_serve_cli:AUTH", "streamcast:TokenFileAccess.from_env"
        )
    )
    for mode in (0o640, 0o604, 0o610, 0o601, 0o620, 0o602):
        path.chmod(mode)
        with pytest.raises(
            ValueError, match="^unavailable or unsafe control token file$"
        ):
            streamcast.TokenFileAccess.from_env()

        with pytest.raises(_config_file.ConfigurationError):
            _config_file.load(config)

    path.chmod(0o600)
    path.write_bytes(b"")
    with pytest.raises(ValueError, match="^unavailable or unsafe control token file$"):
        streamcast.TokenFileAccess.from_env()

    path.unlink()
    with pytest.raises(ValueError, match="^unavailable or unsafe control token file$"):
        streamcast.TokenFileAccess.from_env()

    assert main(["serve", "--config", str(config)]) == 2
    assert (
        capsys.readouterr().err.strip()
        == "invalid service configuration; check fields and host authorization"
    )
    monkeypatch.delenv("STREAMCAST_CONTROL_TOKEN_FILE")
    seen = []

    def unavailable(file, *args, **kwargs):
        seen.append(str(file))
        raise FileNotFoundError

    monkeypatch.setattr("pathlib.Path.open", unavailable)
    with pytest.raises(ValueError):
        streamcast.TokenFileAccess.from_env()

    assert seen == ["/run/streamcast-control/token"]


def test_token_file_create_is_exclusive_0600_and_idempotent(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "created"
    args = ["token-file", "create", str(path)]
    claimed, release = Event(), Event()
    original_open = os.open

    def open_exclusively(file, flags, mode):
        descriptor = original_open(file, flags, mode)
        if not claimed.is_set():
            claimed.set()
            assert release.wait(timeout=5)

        return descriptor

    monkeypatch.setattr(os, "open", open_exclusively)
    generated = []
    original = secrets.token_urlsafe

    def generate(size):
        generated.append(size)
        return original(size)

    monkeypatch.setattr(secrets, "token_urlsafe", generate)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(main, args)
        try:
            assert claimed.wait(timeout=5)
            # The second creator observes the first owner's inode before its
            # token write, rather than relying on thread scheduling to race.
            second = pool.submit(main, args)
            assert second.result(timeout=5) == 0
            assert len(path.read_bytes()) == 0
        finally:
            release.set()

        assert first.result(timeout=5) == 0

    value = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    assert generated == [32]
    assert len(value) == 43 and all(
        byte in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for byte in value
    )
    assert main(args) == 0
    assert same(value, path.read_bytes())
    assert generated == [32]
    assert capsys.readouterr() == ("", "")


def test_token_never_in_argv_logs_or_errors(tmp_path, monkeypatch, capsys, caplog):
    path = tmp_path / "created"
    args = [sys.executable, "-m", "streamcast", "token-file", "create", str(path)]
    result = subprocess.run(args, capture_output=True, check=False)
    assert result.returncode == 0
    value = path.read_bytes()
    assert value
    assert all(value not in os.fsencode(arg) for arg in args)
    assert not result.stdout and not result.stderr
    monkeypatch.setenv("STREAMCAST_CONTROL_TOKEN_FILE", str(path))
    access = streamcast.TokenFileAccess.from_env()
    assert all(value.decode() not in text for text in (repr(access),))
    config = config_file(tmp_path, managed=True)
    config.write_text(
        config.read_text().replace(
            "tests.test_serve_cli:AUTH", "streamcast:TokenFileAccess.from_env"
        )
    )
    path.chmod(0o644)
    assert main(["serve", "--config", str(config)]) == 2
    assert main(["token-file", "create", str(tmp_path / "missing" / "token")]) == 2

    def unavailable(*args, **kwargs):
        raise OSError(value.decode())

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", unavailable)
        assert main(["token-file", "create", str(path)]) == 2
        patch.setattr("pathlib.Path.open", unavailable)
        with pytest.raises(ValueError) as error:
            streamcast.TokenFileAccess.from_env()

        assert all(value.decode() not in text for text in (str(error.value),))

    output = capsys.readouterr()
    assert all(
        value.decode() not in text
        for text in (output.out, output.err, caplog.text, config.read_text())
    )
    assert "Traceback" not in output.err
