"""Catching a consumer up from the archive when the server will not replay.

Needs an endpoint — `just rustfs` — and skips without one, which
`STREAMCAST_REQUIRE_S3` turns into a failure.
"""

from __future__ import annotations

import asyncio

import litelink
import pytest

import streamcast
from streamcast._catchup import CatchUp

pytestmark = pytest.mark.replication

SCHEMA = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "pad": {"type": "string"}},
    "required": ["i", "pad"],
}
TOTAL = 20_000
# Wide enough that `target_seal_size` is crossed and rows actually reach the
# archive. With a bare integer column 6,000 rows sealed nothing, and a test
# whose archive is empty measures nothing.
PAD = "x" * 200

# **Not smaller, and this number is measured.** Every sealed file costs an
# Iceberg commit, and pyiceberg rewrites the table metadata on each one — so
# the cost of building an archive is quadratic in the FILE COUNT, not linear
# in the rows. At 16 KiB these 20,000 rows sealed into 264 files and the
# fixture took 152s; at 512 KiB it is 9 files and 1.9s, for the same rows
# against the same endpoint. 81x, from one constant.
#
# 9 files is still a multi-file archive read in several batches, which is
# what these tests actually need. 264 was not testing anything 9 does not.
SEAL_SIZE = 512 * 1024


@pytest.fixture
def archived(tmp_path, s3, bucket):
    """A log whose archive holds rows the server will no longer replay."""
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        schema=streamcast.to_arrow(SCHEMA),
        archive=bucket,
        s3=s3,
        config=litelink.LogConfig(target_seal_size=SEAL_SIZE),
    )
    with handle:
        yield handle


async def fill(stream, log, total=TOTAL):
    for start in range(0, total, 2_000):
        await stream.send_many(
            [{"i": i, "pad": PAD} for i in range(start, start + 2_000)]
        )

    while log.seal() is not None:
        pass

    await asyncio.to_thread(log.maintain)
    # **`push_unsettled`, or the archive is empty at these sizes.** A plain
    # `sync` holds back the trailing run for compaction, so a log with only a
    # handful of files pushes NOTHING — measured: 4 files archived through 0,
    # 9 files archived through 16,996. That made the fixture's behaviour a
    # step function of the row count, and two tests here were reading an
    # empty archive without saying so. A test fixture wants the whole log in
    # the archive at whatever size it was given; the cost is undersized
    # objects, which no test cares about.
    await asyncio.to_thread(log.sync, push_unsettled=True)

    archived = log.archived_through()
    # Asserted in the fixture, so an archive that silently stops being built
    # fails HERE — naming the fixture — rather than surfacing later as a
    # confusing refusal from the code under test.
    assert archived >= total - 2_000, (
        f"the fixture archived through {archived} of {total} rows; these "
        f"tests read the archive and this one would not have one"
    )

    return archived


class TestItClosesTheGap:
    @pytest.mark.slow
    async def test_a_consumer_too_far_behind_is_caught_up_transparently(
        self, serve, archived, s3
    ):
        """One stream to the consumer; two sources underneath.

        Rows below the server's window come from object storage and the rest
        from the socket, and the consumer's loop cannot tell where the join
        was.
        """
        stream = streamcast.Stream("trades", log=archived, max_replay=6_000)
        archived_through = await fill(stream, archived)
        assert archived_through > 0

        async with serve(stream, maintain=False) as uri:
            # Without it, the refusal stands.
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=3_000)

            assert raised.value.why == "too_old"

            want = 15_000
            async with streamcast.connect(
                uri, offset=3_000, catch_up=True, s3=s3
            ) as sub:
                got = [await sub.recv() for _ in range(want)]

        offsets = [offset for offset, _row in got]
        assert offsets == list(range(3_000, 3_000 + want))
        # And the values crossed the join intact.
        assert got[0][1]["i"] == 2_999
        assert got[-1][1]["i"] == 2_999 + want - 1

    @pytest.mark.slow
    async def test_a_long_catch_up_is_not_dropped_for_falling_behind(
        self, serve, archived, s3
    ):
        """The flaw the loop exists to fix, pinned.

        The first design opened the socket at the archive's frontier and THEN
        streamed the gap, which closes the window by construction — and makes
        the server queue for a subscriber that will not read a message until
        it has pulled millions of rows out of object storage. `max_backlog`
        here is 16: the old shape would be dropped with `TooSlow` long before
        finishing. Nothing is connected while the archive is read now.
        """
        stream = streamcast.Stream(
            "trades", log=archived, max_replay=6_000, max_backlog=16
        )
        await fill(stream, archived)

        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri, offset=1, catch_up=True, s3=s3) as sub:
                got = [await sub.recv() for _ in range(TOTAL)]

        assert [offset for offset, _row in got] == list(range(1, TOTAL + 1))

    @pytest.mark.slow
    async def test_nothing_published_during_the_catch_up_is_lost(
        self, serve, archived, s3
    ):
        """The window the loop leaves, and why it is closed.

        Rows published while the gap is being read are in nobody's queue —
        nothing is connected. They are still on the SERVER, inside its replay
        window, so connecting at the offset the archive reached replays them.
        Had they aged out, the server says too old again and the loop reads
        the newly archived rows instead.
        """
        stream = streamcast.Stream("trades", log=archived, max_replay=6_000)
        await fill(stream, archived)

        async with serve(stream, maintain=False) as uri:

            async def publish():
                for i in range(TOTAL, TOTAL + 400):
                    await stream.send({"i": i, "pad": PAD})
                    await asyncio.sleep(0)

            publisher = asyncio.create_task(publish())
            async with streamcast.connect(uri, offset=1, catch_up=True, s3=s3) as sub:
                got = [await sub.recv() for _ in range(TOTAL + 400)]

            await publisher

        offsets = [offset for offset, _row in got]
        assert offsets == list(range(1, TOTAL + 401))

    @pytest.mark.slow
    async def test_the_archive_reader_is_released_when_the_gap_closes(
        self, serve, archived, s3
    ):
        # A snapshot owns a scratch directory it removes on close, so leaking
        # one leaks disk as well as a DuckDB connection.
        stream = streamcast.Stream("trades", log=archived, max_replay=6_000)
        await fill(stream, archived)

        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri, offset=1, catch_up=True, s3=s3) as sub:
                for _ in range(TOTAL):
                    await sub.recv()

                # One more, which can only come from the socket — and it is
                # the read that drains the prelude and swaps the connection
                # in.
                await stream.send({"i": TOTAL, "pad": PAD})
                assert (await sub.recv())[0] == TOTAL + 1

                # Drained past the archive: the reader is gone and the
                # socket — which was opened only once the gap closed — is
                # what the subscription is reading from now.
                assert sub._catcher is None  # noqa: SLF001
                assert sub._prelude is None  # noqa: SLF001
                assert sub._connection is not None  # noqa: SLF001

    @pytest.mark.slow
    async def test_abandoning_a_catch_up_releases_the_reader(
        self, serve, archived, s3, monkeypatch
    ):
        """A consumer that walks away mid-catch-up must not leak a snapshot.

        The generator sits suspended inside `Catcher.stream`, holding a DuckDB
        connection and the snapshot's scratch DIRECTORY — so abandoning it
        leaks disk as well as memory. `Subscription.close` runs `aclose` on
        it, which runs the `finally` that releases both. The server has the
        same guard; this one was missing until it was looked for.
        """
        closed: list[int] = []
        original = CatchUp.close

        async def spy(self):
            closed.append(1)
            await original(self)

        monkeypatch.setattr(CatchUp, "close", spy)

        stream = streamcast.Stream("trades", log=archived, max_replay=100)
        await fill(stream, archived, total=8_000)

        async with serve(stream, maintain=False) as uri:
            sub = await streamcast.connect(uri, offset=10, catch_up=True, s3=s3)
            await sub.recv()  # one row, then walk away mid-archive

            # A STRONG reference, so the generator cannot be collected —
            # which is the point. Without `aclose` the `finally` still runs
            # eventually, whenever the interpreter gets round to it, and a
            # test that accepts that is testing the garbage collector rather
            # than this code. Measured: the first version of this passed with
            # the fix deleted.
            prelude = sub._prelude  # noqa: SLF001
            assert prelude is not None
            await sub.close()

            assert closed, "the archive reader was not closed by `close`"
            assert prelude.ag_frame is None, "the generator is still suspended"

    async def test_closing_before_the_first_recv_releases_the_reader(
        self, serve, archived, s3, monkeypatch
    ):
        """The same leak, by the other door — and `aclose` does not cover it.

        `Catcher.prepare` opens the first round's reader eagerly so that a
        credentials failure lands at `connect`. A subscription closed before
        its first `recv` never STARTED the generator, and `aclose` on an
        unstarted generator runs no code at all — so the `finally` that
        releases the reader never runs, and the snapshot's scratch directory
        and DuckDB connection are left behind. `Catcher.close` is what covers
        it. Found by reading, not by a failing test.
        """
        closed: list[int] = []
        original = CatchUp.close

        async def spy(self):
            closed.append(1)
            await original(self)

        monkeypatch.setattr(CatchUp, "close", spy)

        stream = streamcast.Stream("trades", log=archived, max_replay=100)
        await fill(stream, archived, total=8_000)

        async with serve(stream, maintain=False) as uri:
            sub = await streamcast.connect(uri, offset=10, catch_up=True, s3=s3)
            # Not one message read: the archive was opened by `connect` and
            # the generator is sitting unstarted.
            prelude = sub._prelude  # noqa: SLF001
            assert prelude is not None

            # `aclose` on an UNSTARTED generator runs no code — the `finally`
            # is inside a body that never began — so the guard that covers
            # the mid-catch-up case covers exactly nothing here. Asserted
            # rather than described, because it is the whole reason
            # `Catcher.close` exists.
            await prelude.aclose()
            assert not closed, "the generator ran; this test proves nothing"

            await sub.close()

            assert closed, "`prepare` opened a reader that `close` left open"


class TestWhereTheArchiveComesFrom:
    async def test_the_greeting_publishes_it(self, serve, archived, s3):
        stream = streamcast.Stream("trades", log=archived)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.archive == archived.archive

    async def test_a_stream_without_a_log_publishes_none(self, serve):
        stream = streamcast.Stream("live")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.archive is None

    async def test_an_explicit_archive_wins(self, serve, archived, s3):
        # A caller that named one meant that one — it also covers a server
        # whose own archive is unreachable from this box.
        from streamcast._catchup import from_refusal

        exc = streamcast.NotReplayable("too_old", archive="s3://from-server/x")
        assert from_refusal(exc, "s3://from-caller/y") == "s3://from-caller/y"
        assert from_refusal(exc, None) == "s3://from-server/x"
        assert from_refusal(streamcast.NotReplayable("too_old"), None) is None

    @pytest.mark.slow
    async def test_it_recovers_when_the_refusal_could_not_carry_it(
        self, serve, archived, s3, monkeypatch
    ):
        """A long bucket URI does not fit beside the numbers in 123 bytes.

        The numbers are ordered first because they are what makes the refusal
        readable, so the archive is what drops — and the client then asks the
        greeting, which has no such limit.
        """
        stream = streamcast.Stream("trades", log=archived, max_replay=6_000)
        await fill(stream, archived)

        async with serve(stream, maintain=False) as uri:
            # Simulate the trim: a refusal that carries no archive at all.
            from streamcast import _stream as stream_module

            original = stream_module.NotReplayable

            def without_archive(why, **fields):
                fields.pop("archive", None)
                return original(why, **fields)

            monkeypatch.setattr(stream_module, "NotReplayable", without_archive)

            async with streamcast.connect(uri, offset=1, catch_up=True, s3=s3) as sub:
                assert (await sub.recv())[0] == 1


class TestWhenItCannot:
    async def test_no_archive_anywhere_says_so(self, serve, log):
        # A log with no archive configured: there is nothing to read.
        stream = streamcast.Stream("trades", log=log, max_replay=2)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([{"event_ts": i, "price": 1.0} for i in range(20)])

            with pytest.raises(streamcast.CatchUpUnavailable, match="no archive"):
                await streamcast.connect(uri, offset=1, catch_up=True)

    @pytest.mark.slow
    async def test_unreadable_credentials_say_what_to_do(self, serve, archived):
        """The message an operator meets on a box that is already behind.

        "AccessDenied" alone answers none of the questions they have, so this
        one names what was tried, which credential source, and four ways out.
        """
        stream = streamcast.Stream("trades", log=archived, max_replay=100)
        await fill(stream, archived, total=6_000)

        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.CatchUpUnavailable) as raised:
                await streamcast.connect(
                    uri,
                    offset=10,
                    catch_up=True,
                    s3=streamcast.S3Options(
                        endpoint="http://127.0.0.1:1",
                        access_key="wrong",
                        secret_key="wrong",
                        region="us-east-1",
                    ),
                )

        message = str(raised.value)
        assert "cannot read stream 'trades'" in message
        assert "credentials:" in message
        assert "AWS_ACCESS_KEY_ID" in message
        assert "catch_up=False" in message
        assert "EARLIEST" in message
        # Never the secret itself.
        assert "wrong" not in message.split("underlying:")[0]

    async def test_a_gap_neither_side_holds_is_reported_with_both_numbers(
        self, serve, archived, s3
    ):
        # The archive is further behind than the server's window: a range
        # exists that the server has forgotten and the archive never got.
        #
        # **Constructed, not stumbled into.** An earlier version asked from
        # offset 1 and passed only because the fixture happened to archive
        # NOTHING at that size — so it was asserting on an empty archive
        # rather than on a gap between two populated tiers. It kept passing
        # for the wrong reason, which is the failure mode a test cannot
        # report about itself.
        stream = streamcast.Stream("trades", log=archived, max_replay=10)
        frontier = await fill(stream, archived, total=4_000)
        # Published past the archive without syncing, so the archive stays at
        # `frontier` while the server's window moves far above it.
        await stream.send_many([{"i": i, "pad": PAD} for i in range(4_000, 24_000)])

        # Above what the archive holds, and far below what the server will
        # replay. Nothing on either side has it.
        asking = frontier + 5_000

        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.CatchUpUnavailable) as raised:
                await streamcast.connect(uri, offset=asking, catch_up=True, s3=s3)

        message = str(raised.value)
        assert "neither holds" in message
        # Both numbers, which is the point of the message: where the archive
        # stopped and where the consumer asked from.
        #
        # `frontier + 1`, and the off-by-one is the two APIs meaning
        # different things: `archived_through()` is the LAST offset archived,
        # inclusive, while the reader's `end_offset()` — which the message
        # prints — is the offset AFTER it, exclusive. Same boundary, and
        # asserting the raw number caught the difference.
        assert str(frontier + 1) in message, message
        assert str(asking) in message, message

    async def test_catch_up_is_off_by_default(self, serve, log):
        stream = streamcast.Stream("trades", log=log, max_replay=2)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([{"event_ts": i, "price": 1.0} for i in range(20)])

            # Opting in is deliberate: it reaches the network and can be slow.
            with pytest.raises(streamcast.NotReplayable):
                await streamcast.connect(uri, offset=1)


def test_only_a_gap_is_recoverable():
    """`not_durable`, `empty` and `ahead` are not archive problems.

    A stream with no log, a log with nothing in it, and a cursor from the
    future — no amount of reading object storage fixes any of them, and
    trying would turn a clear refusal into a confusing one.
    """
    from streamcast._catchup import RECOVERABLE

    assert RECOVERABLE == {"too_old", "evicted"}


def test_nothing_is_connected_while_the_archive_is_read():
    """Asserted against the source, because it is the whole design.

    A future "hold the connection so the window closes by construction" is an
    easy change to make, and it turns every long catch-up into a `TooSlow`
    drop — on exactly the consumers that needed the feature.
    """
    import inspect

    from streamcast._catchup import Catcher

    source = inspect.getsource(Catcher.stream)
    assert source.index("yield offset, row") < source.index(
        "self._handshake(self.start)"
    ), "the socket is opened before the archive is read"


def test_the_reader_crosses_into_a_thread():
    # `litelink.snapshot` resolves a catalog over the network — seconds — and
    # on the event loop that is the consumer's whole process stopped.
    import inspect

    source = inspect.getsource(CatchUp.open)
    assert source.count("asyncio.to_thread") == 2
    assert "asyncio.to_thread" in inspect.getsource(CatchUp.close)
