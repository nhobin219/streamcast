"""The maintainer: the half of the library that keeps a log from growing.

Nothing here sealed before `_maintain` existed, and the reason it went
unnoticed is worth knowing: litelink's `target_seal_size` defaults to 8 MiB,
so a short test or a short demo never crosses it and every log looks fine.
These tests deliberately cross it.
"""

from __future__ import annotations

import asyncio

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast._maintain import Maintain, Supervisor

# ~140 bytes a row, so 100k rows is ~14 MB — comfortably past the 8 MiB
# default at which litelink's policy says a seal is due.
PAD = "x" * 120

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("tag", pa.string()),
    ]
)


def rows(lo: int, hi: int) -> list[dict]:
    return [
        {"event_ts": i, "price": 1.0 * i, "tag": f"{i:08}{PAD}"} for i in range(lo, hi)
    ]


@pytest.fixture
def wide_log(tmp_path):
    handle = litelink.new(
        tmp_path / "data", "trades", schema=SCHEMA, sort_by=("event_ts",)
    )
    with handle:
        yield handle


async def fill(stream, total=100_000, chunk=2_000):
    for start in range(0, total, chunk):
        await stream.send_many(rows(start, start + chunk))
        # A publisher yields between groups; see `Stream.send`.
        await asyncio.sleep(0)


async def settle(log, *, want_files=1, timeout=20.0):
    """Wait for the maintainer to do something, or give up."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if log.table_files() >= want_files:
            return True

        await asyncio.sleep(0.25)

    return False


class TestTheDefect:
    @pytest.mark.slow
    async def test_without_a_maintainer_nothing_ever_seals(self, wide_log):
        """The state this library shipped in, pinned.

        Not a litelink bug: `seal_due` respects the policy, and the policy is
        satisfied here — 14 MB against an 8 MiB target. It simply never runs,
        because nothing calls it. The buffer grows for the life of the server
        and the DuckDB read cache mirrors it.
        """
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(stream, "127.0.0.1", 0, maintain=False):
            await fill(stream)

            # **No sleep, deliberately.** This used to wait two seconds and
            # then check that nothing had sealed, which proves a negative by
            # hoping: on a slower box two seconds is not evidence, and on any
            # box it is two seconds of nothing. Nothing here is SCHEDULED to
            # seal — `maintain=False` starts no subprocess and the library
            # calls `seal_due` nowhere else — so there is no race for a wait
            # to lose, and a longer one could not catch anything.
            #
            # What proves the policy was satisfied is the POSITIVE CONTROL
            # below: `test_with_one_the_buffer_drains_into_parquet` runs the
            # same fixture through the same `fill` and does seal. An empty
            # table here means the maintainer, not an unmet threshold.
            # (`seal_due()` would answer it directly and must not be called —
            # it SEALS, which is the whole point of this test.)
            assert wide_log.buffered_rows() == 100_000
            assert wide_log.table_files() == 0

    @pytest.mark.slow
    async def test_with_one_the_buffer_drains_into_parquet(self, wide_log):
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(
            stream, "127.0.0.1", 0, maintain=Maintain(maintain_every=1.0)
        ):
            await fill(stream)
            assert await settle(wide_log), "the maintainer sealed nothing"

            assert wide_log.table_files() >= 1
            assert wide_log.buffered_rows() < 100_000

    @pytest.mark.slow
    async def test_a_replay_still_spans_the_seam_the_maintainer_made(self, wide_log):
        # The maintainer moves rows from the buffer into Parquet underneath a
        # running server. A replay has to cross that boundary without noticing.
        stream = streamcast.Stream("trades", log=wide_log)
        async with streamcast.serve(
            stream, "127.0.0.1", 0, maintain=Maintain(maintain_every=1.0)
        ) as server:
            # The full 100k: 40,000 rows is 5.6 MB, under the 8 MiB target,
            # so nothing would be due and this would assert on a seal the
            # policy correctly declined to make.
            await fill(stream)
            assert await settle(wide_log)

            port = server.sockets[0].getsockname()[1]
            uri = f"ws://127.0.0.1:{port}/trades"
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                got = [await sub.recv() for _ in range(500)]

        assert [offset for offset, _row in got] == list(range(1, 501))
        assert got[0][1]["event_ts"] == 0


class TestLifecycle:
    async def test_a_live_only_stream_starts_no_process(self, serve):
        # Nothing to sweep, so nothing is spawned — not a process with no work.
        from streamcast._server import _supervisors

        assert _supervisors({"a": streamcast.Stream("a")}, True) == []

    async def test_maintain_false_starts_nothing(self, log):
        from streamcast._server import _supervisors

        assert _supervisors({"a": streamcast.Stream("a", log=log)}, False) == []

    async def test_one_supervisor_per_stream_with_a_log(self, log):
        from streamcast._server import _supervisors

        made = _supervisors(
            {"a": streamcast.Stream("a", log=log), "b": streamcast.Stream("b")}, True
        )
        assert len(made) == 1

    async def test_the_maintainer_stops_when_the_server_closes(self, log):
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0)
        alive = list(server._children)  # noqa: SLF001
        assert len(alive) == 1
        assert alive[0]._process is not None  # noqa: SLF001
        assert alive[0]._process.poll() is None  # noqa: SLF001

        server.close()
        await server.wait_closed()

        # Terminated AND reaped: a dropped reference here is one zombie per
        # server for the life of the parent.
        assert alive[0]._process is None  # noqa: SLF001
        assert alive[0]._stopped is None  # noqa: SLF001

    async def test_a_maintainer_that_dies_is_restarted(self, log):
        """The direction that matters.

        A maintainer dying unnoticed puts the server straight back to never
        sealing, which is invisible until the disk fills. litelink's leases
        lapse, so the replacement takes them cleanly.
        """
        stream = streamcast.Stream("trades", log=log)
        server = await streamcast.serve(stream, "127.0.0.1", 0)
        supervisor = server._children[0]  # noqa: SLF001
        try:
            first = supervisor._process  # noqa: SLF001
            assert first is not None
            first.kill()

            for _ in range(200):
                current = supervisor._process  # noqa: SLF001
                if current is not None and current is not first:
                    break

                await asyncio.sleep(0.05)

            assert current is not first, "it was not restarted"
            assert current.poll() is None

        finally:
            server.close()
            await server.wait_closed()

    async def test_the_server_still_proxies_what_websockets_exposes(self, log):
        # `_Served` wraps rather than replaces, so everything else is reached
        # through `__getattr__` untouched.
        stream = streamcast.Stream("trades", log=log)
        async with streamcast.serve(stream, "127.0.0.1", 0) as server:
            assert server.sockets
            assert server.is_serving()
            async with streamcast.connect(
                f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/trades"
            ) as sub:
                await stream.send({"event_ts": 1, "price": 1.0})
                assert (await sub.recv())[0] == 1

            assert len(server.connections) >= 0


def test_the_cadences_differ_because_the_costs_do():
    # `seal_due` is an indexed read when idle; `maintain` reads table
    # metadata. A single interval would make one of them wrong.
    plan = Maintain()
    assert plan.seal_every < plan.maintain_every
    assert plan.maintain_every / plan.seal_every >= 10


def test_there_is_no_thread_mode_to_get_wrong():
    """litelink measured appends 45.2 ms behind an in-process seal.

    On a fan-out server that is 45 ms with nothing fanned out and no keepalive
    answered — latency spikes that look like a network problem. A flag whose
    wrong setting is invisible until production should not exist, so this
    asserts the ABSENCE of one rather than its default.
    """
    import inspect

    assert "thread" not in inspect.signature(Maintain.__init__).parameters
    assert "thread" not in inspect.signature(streamcast.serve).parameters
    assert set(Maintain.__dataclass_fields__) == {"seal_every", "maintain_every"}

    # And the child is a real subprocess, not a thread pretending to be one.
    spawn = inspect.getsource(Supervisor._spawn)
    assert "subprocess.Popen" in spawn
    assert "sys.executable" in spawn


# `TestWalReplication` lived here and tested that `serve` REFUSED a log with
# `wal_replication` on, because the maintainer did not run litestream. It now
# does — see `test_replicate.py`, which exercises the sidecar against a real
# endpoint rather than asserting an apology.
