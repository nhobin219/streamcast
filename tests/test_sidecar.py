"""`Sidecar.of`: one litestream for the configs a caller hands it.

These need no endpoint: a config is a file, read when the process starts, and
nothing here starts one. The tier that runs litestream and flushes through the
merged process's socket is in `test_replicate.py`.
"""

from __future__ import annotations

import asyncio
import subprocess
import tempfile
from pathlib import Path

import pytest

import streamcast
from streamcast import _process, _replicate
from streamcast._replicate import Sidecar, SidecarUnavailable


def config(
    directory: Path, socket: str = "/tmp/litelink-0123456789abcdef.sock"
) -> Path:
    """A config in the shape litelink writes: its own socket, then `dbs:`."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "litestream.yml"
    path.write_text(
        "socket:\n"
        "  enabled: true\n"
        f"  path: {socket}\n"
        "dbs:\n"
        f"  - path: {directory / 'buffer.db'}\n"
        "    replica:\n"
        "      type: s3\n"
        "      bucket: b\n"
        f"      path: {directory.name}/_wal/buffer.db\n"
    )
    return path


async def closed(sidecar: Sidecar) -> None:
    sidecar.terminate()
    await sidecar.wait_closed()


def test_the_package_exports_it():
    assert streamcast.Sidecar is Sidecar
    assert streamcast.SidecarUnavailable is SidecarUnavailable
    assert {"Sidecar", "SidecarUnavailable"} <= set(streamcast.__all__)


class TestOf:
    async def test_it_takes_a_bare_config_path_as_a_string(self, tmp_path):
        """Any litelink-written config, not only a log this process opened —
        a store, or a log another process writes to."""
        path = config(tmp_path / "data" / "trades")
        sidecar = Sidecar.of([str(path)])
        try:
            assert sidecar._logs == [("trades", path.resolve())]  # noqa: SLF001
            assert sidecar._claim() is True  # noqa: SLF001
            assert sidecar.owned == {"trades"}

        finally:
            await closed(sidecar)

    async def test_new_writes_each_config_and_hands_them_to_of(self, monkeypatch):
        """`new` is `of` on the configs it writes: one construction path."""
        written: list[str] = []

        class Log:
            name = "trades"

            def write_replication_config(self) -> str:
                path = config(Path(tempfile.mkdtemp()) / "trades")
                written.append(str(path))
                return str(path)

        seen = {}

        def of(configs, *, binary=None, s3_options=None):
            seen.update(configs=list(configs), binary=binary, s3_options=s3_options)
            return "built"

        monkeypatch.setattr(Sidecar, "of", of)

        assert Sidecar.new([Log()], binary="/x/litestream") == "built"  # ty: ignore[invalid-argument-type]
        assert seen == {
            "configs": written,
            "binary": "/x/litestream",
            "s3_options": None,
        }
        assert Path(written[0]).exists()

    def test_a_config_given_twice_is_refused(self, tmp_path):
        path = config(tmp_path / "trades")
        with pytest.raises(ValueError, match="given twice"):
            Sidecar.of([path, str(path)])

    def test_two_configs_in_one_directory_are_refused_naming_both(self, tmp_path):
        """They would share `litestream.lock`, so the second would never be
        claimed and its database never replicated — silently."""
        first = config(tmp_path / "trades")
        second = tmp_path / "trades" / "other.yml"
        second.write_text(first.read_text())

        with pytest.raises(ValueError, match="one directory") as raised:
            Sidecar.of([first, second])

        assert str(first) in str(raised.value)
        assert str(second) in str(raised.value)

    def test_a_missing_binary_raises_before_anything_is_made(
        self, tmp_path, monkeypatch
    ):
        made = []
        monkeypatch.setattr(
            _replicate.tempfile, "mkdtemp", lambda **kw: made.append(kw) or "/nope"
        )
        with pytest.raises(SidecarUnavailable, match="litestream was not found"):
            Sidecar.of([config(tmp_path / "trades")], binary="/nonexistent/litestream")

        assert made == [], "a workdir was made for a sidecar that cannot run"

    def test_a_socket_path_too_long_to_bind_is_refused(self, tmp_path, monkeypatch):
        """A litestream that cannot bind its socket exits at once, and would be
        restarted with backoff for ever while replicating nothing."""
        deep = tmp_path / ("d" * 120)
        deep.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(deep))

        with pytest.raises(SidecarUnavailable, match="longer than 100 bytes"):
            Sidecar.of([config(tmp_path / "trades")])

        assert list(deep.iterdir()) == [], "the refused workdir was left behind"

    async def test_logs_named_alike_in_two_roots_are_both_claimed(self, tmp_path):
        """A name is not unique across roots. Keyed by it, the second `x`
        would count as owned once the first was, and never be locked."""
        first = config(tmp_path / "r1" / "x")
        second = config(tmp_path / "r2" / "x")
        sidecar = Sidecar.of([first, second])
        try:
            assert sidecar._claim() is True  # noqa: SLF001
            sidecar._write_config()  # noqa: SLF001
            text = sidecar.config.read_text()

            assert str(tmp_path / "r1" / "x" / "buffer.db") in text
            assert str(tmp_path / "r2" / "x" / "buffer.db") in text

        finally:
            await closed(sidecar)


class TestTheSocket:
    async def test_the_merged_config_has_one_socket_and_it_is_the_processes(
        self, tmp_path
    ):
        """One litestream listens on one socket. Each log's own is dropped,
        and the process's is the one a caller flushes through."""
        configs = [
            config(tmp_path / "trades", "/tmp/litelink-aaaa.sock"),
            config(tmp_path / "quotes", "/tmp/litelink-bbbb.sock"),
        ]
        sidecar = Sidecar.of(configs)
        try:
            assert sidecar.socket == sidecar.config.parent / "control.sock"
            sidecar._claim()  # noqa: SLF001
            sidecar._write_config()  # noqa: SLF001
            lines = sidecar.config.read_text().splitlines()

            assert lines[:4] == [
                "socket:",
                "  enabled: true",
                f"  path: {sidecar.socket}",
                "dbs:",
            ]
            assert lines.count("socket:") == 1
            assert not any("litelink-" in line for line in lines), lines

        finally:
            await closed(sidecar)

    async def test_a_stale_socket_is_removed_before_each_spawn(
        self, tmp_path, monkeypatch
    ):
        """A dead litestream's socket file stays, and the next one fails to
        bind it — every restart, for ever."""
        sidecar = Sidecar.of([config(tmp_path / "trades")])
        sidecar.socket.write_text("stale")
        present: list[bool] = []
        spawned = asyncio.Event()

        class Exited:
            returncode = 1

            def poll(self) -> int:
                return 1

            def wait(self) -> int:
                return 1

        def spawn():
            present.append(sidecar.socket.exists())
            sidecar.socket.write_text("left by this one")
            if len(present) == 2:
                spawned.set()

            return Exited()

        monkeypatch.setattr(_replicate, "_BACKOFF", (0.0,))
        monkeypatch.setattr(Sidecar, "_spawn", lambda _self: spawn())
        sidecar.start()
        try:
            await asyncio.wait_for(spawned.wait(), timeout=10)
        finally:
            await closed(sidecar)

        assert present[:2] == [False, False]


class TestNeverTwo:
    async def test_a_widen_never_overlaps_a_litestream_that_will_not_stop(
        self, tmp_path, monkeypatch
    ):
        """Widening restarts litestream, and the old one may outlive the
        grace — finishing a last sync against slow storage. Spawning the new
        one beside it is two litestreams on every database the old one had.
        """
        first, second = config(tmp_path / "a"), config(tmp_path / "b")
        holder = Sidecar.of([second])
        assert holder._claim() is True  # noqa: SLF001

        sidecar = Sidecar.of([first, second])
        children: list[subprocess.Popen[bytes]] = []
        alive_at_spawn: list[bool] = []

        def spawn():
            alive_at_spawn.append(any(c.poll() is None for c in children))
            # Ignores SIGTERM, as a litestream mid-sync can for longer than
            # the grace.
            child = _process.popen(
                ["sh", "-c", "trap '' TERM; while :; do sleep 0.05; done"]
            )
            children.append(child)
            return child

        monkeypatch.setattr(_replicate, "_STOP_GRACE", 0.2)
        monkeypatch.setattr(_replicate, "_CLAIM_EVERY", 0.1)
        monkeypatch.setattr(_replicate, "_POLL", 0.05)
        monkeypatch.setattr(Sidecar, "_spawn", lambda _self: spawn())
        sidecar.start()
        try:

            async def spawned(count: int) -> None:
                while len(children) < count:
                    await asyncio.sleep(0.02)

            await asyncio.wait_for(spawned(1), timeout=10)
            await closed(holder)
            await asyncio.wait_for(spawned(2), timeout=10)

            assert alive_at_spawn == [False, False], "two litestreams at once"

        finally:
            await closed(sidecar)
            for child in children:
                child.kill()
                child.wait()
