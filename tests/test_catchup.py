"""Catching a consumer up from the published tables when the server will not replay.

Needs an endpoint — `just rustfs` — and skips without one, which
`STREAMCAST_REQUIRE_S3` turns into a failure.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import litelink
import pytest

import streamcast
from streamcast import _log, _snapshot
from streamcast._snapshot import Snapshot

pytestmark = pytest.mark.replication

SCHEMA = {
    "type": "object",
    "properties": {"i": {"type": "integer"}, "pad": {"type": "string"}},
    "required": ["i", "pad"],
}
TOTAL = 2_000
# `fill` sends in groups this size, and the published table may trail by one.
BATCH = 200
# Wide enough that `target_seal_size` is crossed and rows actually reach the
# published table. With a bare integer column 6,000 rows sealed nothing, and
# a test whose published table is empty measures nothing.
PAD = "x" * 200

# **A multi-file published table, read in several batches, is what these tests need**,
# and the sizes are chosen for that and nothing more. Every sealed file costs
# an Iceberg commit, and pyiceberg rewrites the table metadata on each one, so
# building a published table costs by the FILE COUNT, not the rows: measured, 20,000
# rows at 16 KiB sealed into 264 files and took 152 s. TOTAL rows at SEAL_SIZE
# seal into 7 files, and compaction is held off (KEEP_FILES) so the published
# table keeps every one of them rather than merging them back into one or two.
SEAL_SIZE = 64 * 1024
KEEP_FILES = 1_000


@pytest.fixture
def published_log(tmp_path, s3, bucket):
    """A log whose published table holds rows the server will no longer replay."""
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        # Stamped, because that is the shape `Stream.new` creates and so the
        # shape a published table a consumer catches up from actually has.
        schema=_log.with_system(streamcast.to_arrow(SCHEMA)),
        published=bucket,
        s3_options=s3,
        config=litelink.LogConfig(
            target_seal_size=SEAL_SIZE, compact_min_files=KEEP_FILES
        ),
    )
    with handle:
        yield handle


async def fill(stream, log, total=TOTAL):
    for start in range(0, total, BATCH):
        await stream.send_many(
            [{"i": i, "pad": PAD} for i in range(start, start + BATCH)]
        )

    while log.seal(flush=True) is not None:
        pass

    await asyncio.to_thread(log.advance)
    # **`flush`, or the published table is empty at these sizes.** A
    # plain `publish()` holds back the trailing run for compaction, so a log with only a
    # handful of files pushes NOTHING — measured: 4 files published through 0,
    # 9 files published through 16,996. That made the fixture's behaviour a
    # step function of the row count, and two tests here were reading an
    # empty published table without saying so. A test fixture wants the whole
    # log in the published table at whatever size it was given; the cost is undersized
    # objects, which no test cares about.
    await asyncio.to_thread(log.publish, flush=True)

    # Several files, or the multi-batch read of the published table these tests
    # rely on is not happening. Asserted here for the same reason as the line below.
    assert total < TOTAL or log.staging_files() >= 5, (
        f"{log.staging_files()} files; the published table these tests read is one file"
    )
    through = log.published_through()
    # Asserted in the fixture, so a published table that silently stops being built
    # fails HERE — naming the fixture — rather than surfacing later as a
    # confusing refusal from the code under test.
    assert through >= total - BATCH, (
        f"the fixture published through {through} of {total} rows; these "
        f"tests read the published table and this one would not have one"
    )

    return through


class TestItClosesTheGap:
    @pytest.mark.slow
    async def test_a_consumer_too_far_behind_is_caught_up_transparently(
        self, serve, published_log, s3
    ):
        """One stream to the consumer; two sources underneath.

        Rows below the server's window come from object storage and the rest
        from the socket, and the consumer's loop cannot tell where the join
        was.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        published_through = await fill(stream, published_log)
        assert published_through > 0

        async with serve(stream, maintain=False) as uri:
            # Without it, the refusal stands.
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=300)

            assert raised.value.why == "too_old"

            want = 1_500
            async with streamcast.connect(
                uri, offset=300, catch_up=True, s3_options=s3
            ) as sub:
                got = [await sub.recv() for _ in range(want)]

        offsets = [offset for offset, _ts, _row in got]
        assert offsets == list(range(300, 300 + want))
        # Stamped on both sides of the join: the published table's
        # `streamcast_ts` travels positionally, like the live one.
        assert all(isinstance(ts, int) for _offset, ts, _row in got)
        # And the values crossed the join intact.
        assert got[0][2]["i"] == 299
        assert got[-1][2]["i"] == 299 + want - 1
        # And the published rows carry exactly the keys the live ones do: the
        # published table holds `streamcast_ts`, and a catch-up must not surface it.
        assert {tuple(row) for _offset, _ts, row in got} == {("i", "pad")}

    @pytest.mark.slow
    async def test_a_long_catch_up_is_not_dropped_for_falling_behind(
        self, serve, published_log, s3
    ):
        """The flaw the loop exists to fix, pinned.

        The first design opened the socket at the published table's frontier and THEN
        streamed the gap, which closes the window by construction — and makes
        the server queue for a subscriber that will not read a message until
        it has pulled millions of rows out of object storage. `max_backlog`
        here is 16: the old shape would be dropped with `TooSlow` long before
        finishing. Nothing is connected while the published table is read now.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        await fill(stream, published_log)

        async with serve(stream, maintain=False, max_backlog=16) as uri:
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                got = [await sub.recv() for _ in range(TOTAL)]

        assert [offset for offset, _ts, _row in got] == list(range(1, TOTAL + 1))

    async def test_batches_cross_the_join_whole_and_in_order(
        self, serve, published_log, s3
    ):
        """`batches` over a catch-up: published rows, then the socket's.

        The catch-up rows come from a generator a cancelled read would end.
        The read after the last published row waits while the generator
        connects to the server, so a batch that reaches the join ends there
        with that read waiting — the case where it must be kept, never
        cancelled. Every row once, in order, across the join.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        await fill(stream, published_log)
        # Not published: these can only come from the server, after the join.
        live = 10
        await stream.send_many(
            [{"i": i, "pad": PAD} for i in range(TOTAL, TOTAL + live)]
        )
        assert published_log.published_through() == TOTAL  # inclusive

        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                got: list[list[int | None]] = []
                async for batch in sub.batches(limit=1500):
                    got.append([offset for offset, _ts, _row in batch])
                    if got[-1][-1] == TOTAL + live:
                        break

        flat = [offset for batch in got for offset in batch]
        assert flat == list(range(1, TOTAL + live + 1))
        # The published rows are in memory once read, so the first batch is
        # full; the second stops at the join, its next read waiting on the
        # connection. Deterministic: no handshake completes in one loop turn.
        assert [len(batch) for batch in got[:2]] == [1500, TOTAL - 1500]

    @pytest.mark.slow
    async def test_nothing_published_during_the_catch_up_is_lost(
        self, serve, published_log, s3
    ):
        """The window the loop leaves, and why it is closed.

        Rows published while the gap is being read are in nobody's queue —
        nothing is connected. They are still on the SERVER, inside its replay
        window, so connecting at the offset the published table reached replays them.
        Had they aged out, the server says too old again and the loop reads
        the newly published rows instead.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        await fill(stream, published_log)

        async with serve(stream, maintain=False) as uri:

            async def publish():
                for i in range(TOTAL, TOTAL + 40):
                    await stream.send({"i": i, "pad": PAD})
                    await asyncio.sleep(0)

            publisher = asyncio.create_task(publish())
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                got = [await sub.recv() for _ in range(TOTAL + 40)]

            await publisher

        offsets = [offset for offset, _ts, _row in got]
        assert offsets == list(range(1, TOTAL + 41))

    @pytest.mark.slow
    async def test_the_published_reader_is_released_when_the_gap_closes(
        self, serve, published_log, s3
    ):
        # A snapshot owns a scratch directory it removes on close, so leaking
        # one leaks disk as well as a DuckDB connection.
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        await fill(stream, published_log)

        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                for _ in range(TOTAL):
                    await sub.recv()

                # One more, which can only come from the socket — and it is
                # the read that drains the prelude and swaps the connection
                # in.
                await stream.send({"i": TOTAL, "pad": PAD})
                assert (await sub.recv())[0] == TOTAL + 1

                # Drained past the published table: the reader is gone and the
                # socket — which was opened only once the gap closed — is
                # what the subscription is reading from now.
                assert sub._catcher is None  # noqa: SLF001
                assert sub._prelude is None  # noqa: SLF001
                assert sub._connection is not None  # noqa: SLF001

    @pytest.mark.slow
    async def test_abandoning_a_catch_up_releases_the_reader(
        self, serve, published_log, s3, monkeypatch
    ):
        """A consumer that walks away mid-catch-up must not leak a snapshot.

        The generator sits suspended inside `Catcher.stream`, holding a DuckDB
        connection and the snapshot's scratch DIRECTORY — so abandoning it
        leaks disk as well as memory. `Subscription.close` runs `aclose` on
        it, which runs the `finally` that releases both. The server has the
        same guard; this one was missing until it was looked for.
        """
        closed: list[int] = []
        original = Snapshot.close

        async def spy(self):
            closed.append(1)
            await original(self)

        monkeypatch.setattr(Snapshot, "close", spy)

        stream = streamcast.Stream("trades", log=published_log, max_replay=100)
        await fill(stream, published_log, total=800)

        async with serve(stream, maintain=False) as uri:
            sub = await streamcast.connect(uri, offset=10, catch_up=True, s3_options=s3)
            await sub.recv()  # one row, then walk away mid-catch-up

            # A STRONG reference, so the generator cannot be collected —
            # which is the point. Without `aclose` the `finally` still runs
            # eventually, whenever the interpreter gets round to it, and a
            # test that accepts that is testing the garbage collector rather
            # than this code. Measured: the first version of this passed with
            # the fix deleted.
            prelude = sub._prelude  # noqa: SLF001
            assert prelude is not None
            await sub.close()

            assert closed, "the published reader was not closed by `close`"
            assert prelude.ag_frame is None, "the generator is still suspended"

    async def test_closing_before_the_first_recv_releases_the_reader(
        self, serve, published_log, s3, monkeypatch
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
        original = Snapshot.close

        async def spy(self):
            closed.append(1)
            await original(self)

        monkeypatch.setattr(Snapshot, "close", spy)

        stream = streamcast.Stream("trades", log=published_log, max_replay=100)
        await fill(stream, published_log, total=800)

        async with serve(stream, maintain=False) as uri:
            sub = await streamcast.connect(uri, offset=10, catch_up=True, s3_options=s3)
            # Not one message read: the published table was opened by `connect` and
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


class TestTheLogIsNamedInTheGreeting:
    async def test_catch_up_works_when_the_log_is_named_differently(
        self, tmp_path, s3, bucket, serve
    ):
        """A stream's name is not the log's, and its published table is the log's.

        `Stream.new` feeds one name through, so the two agree. `Stream(log=)`
        takes a handle the caller opened and named, and nothing makes them
        equal. Catch-up asked the published tables for a table named after the STREAM,
        found nothing, and reported it as a credentials failure — the message
        named an endpoint and a credential chain for a problem that was
        neither. The server knows the log's name, so the greeting says it.

        Falsify by passing the stream name to `Catcher` instead of
        `probe.log.name`.
        """
        handle = litelink.new(
            tmp_path / "data",
            "raw_trades_v2",  # deliberately not "trades"
            schema=streamcast.to_arrow(SCHEMA),
            published=bucket,
            s3_options=s3,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE, compact_min_files=KEEP_FILES
            ),
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle, max_replay=10)
            await fill(stream, handle, total=800)

            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    assert sub.info.stream == "trades"
                    assert sub.info.metadata is not None
                    found = _snapshot.metadata(sub.info.metadata, s3)
                    assert found.live_log.name == "raw_trades_v2", (
                        "the metadata must name the LOG, not the stream, or a "
                        "reader cannot open it"
                    )

                async with streamcast.connect(
                    uri, offset=1, catch_up=True, s3_options=s3
                ) as sub:
                    first, _ts, _row = await sub.recv()

                assert first == 1

    async def test_the_greeting_is_enough_to_open_the_log_directly(
        self, tmp_path, s3, bucket, serve
    ):
        """The point of publishing it: a subscriber reads the history itself.

        The greeting's metadata URI is all `Stream.snapshot` needs, so a
        consumer that wants the whole history goes straight to object storage
        instead of through the socket. Credentials stay the reader's own; a
        server that sent them would be handing every subscriber its keys.
        """
        handle = litelink.new(
            tmp_path / "data",
            "raw_trades_v2",
            schema=streamcast.to_arrow(SCHEMA),
            published=bucket,
            s3_options=s3,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE, compact_min_files=KEEP_FILES
            ),
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle)
            await fill(stream, handle, total=800)

            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri) as sub:
                    info = sub.info

            assert info.metadata is not None
            # Nothing from the server but the greeting, plus the reader's own
            # credentials.
            table = await streamcast.Stream.scan(info.metadata, s3_options=s3)
            assert table.num_rows == 800


class TestTheWholeHistoryGateway:
    async def test_no_replay_bound_and_a_published_table_serves_everything(
        self, tmp_path, s3, bucket, serve
    ):
        """`max_replay=None` + a log that reads its published table: nothing is refused.

        The point is what it buys a client that is not Python. Every frame is
        JSON over a plain WebSocket, so a server configured this way lets any
        language replay a stream from the beginning with no litelink, no
        Iceberg reader and no object-storage credentials of its own — the
        server reads the published table on its behalf. `catch_up` exists because the
        default is the opposite.

        Proved from the state that would otherwise refuse: the local tier is
        evicted dry, so every row below the buffer is in object storage and
        nowhere else.
        """
        handle = litelink.new(
            tmp_path / "data",
            "trades",
            schema=streamcast.to_arrow(SCHEMA),
            published=bucket,
            s3_options=s3,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE,
                compact_min_files=KEEP_FILES,
                staging_retention=timedelta(0),
            ),
        )
        with handle:
            stream = streamcast.Stream(
                "trades", log=handle, max_replay=None, replay_published=True
            )
            await fill(stream, handle, total=800)
            await asyncio.to_thread(handle.evict)

            assert handle.staging_extent() is None, "the fixture must evict dry"

            async with serve(stream, maintain=False) as uri:
                # Bounded by nothing: the default would refuse this as
                # `too_old` long before it reached the published table.
                async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                    got = [await sub.recv() for _ in range(400)]

            assert [offset for offset, _ts, _row in got] == list(range(1, 401))
            # And it came from object storage, not from a local file.
            assert got[0][2]["i"] == 0

    async def test_the_factory_takes_it_too(self, tmp_path, s3, bucket, serve):
        """`Stream.new(replay_published=True)`, rather than opening the handle.

        The parameter is named for the replay, not for the tier: `published=`
        sits beside it and already means "where", so two near-identical names
        would be the kind a caller sets one of while meaning the other. Each
        replay passes it to litelink's reads as `published=`.
        """
        stream = streamcast.Stream.new(
            "trades",
            root=tmp_path / "data",
            schema=SCHEMA,
            published=bucket,
            s3_options=s3,
            replay_published=True,
            max_replay=None,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE,
                compact_min_files=KEEP_FILES,
                staging_retention=timedelta(0),
            ),
        )
        try:
            assert stream.log is not None

            await fill(stream, stream.log, total=800)
            await asyncio.to_thread(stream.log.evict)
            assert stream.log.staging_extent() is None, "the fixture must evict dry"

            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                    first, _ts, _row = await sub.recv()

            assert first == 1

        finally:
            await stream.aclose()

    async def test_the_bound_still_applies_when_it_is_set(self, serve, log):
        """The default is unchanged: a number still refuses."""
        stream = streamcast.Stream("trades", log=log, max_replay=5)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([{"event_ts": i, "price": 1.0} for i in range(20)])
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=1)

        assert raised.value.why == "too_old"


class TestAnEvictedLog:
    async def test_a_log_evicted_dry_refuses_rather_than_erroring(
        self, tmp_path, s3, bucket, serve
    ):
        """litelink's refusal has to arrive as `evicted`, not a 500.

        A server opens its log local-only (see `_log.rows`), and litelink
        refuses a local-only read of a log whose local table has been evicted
        dry rather than serving the buffer alone. That refusal is a
        `ValueError` from the read, and the subscriber must see it as the
        refusal it is — `evicted`, whose message already names the move — not
        as an unhandled error on a stream that is working perfectly.
        """
        handle = litelink.new(
            tmp_path / "data",
            "trades",
            schema=streamcast.to_arrow(SCHEMA),
            published=bucket,
            s3_options=s3,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE,
                compact_min_files=KEEP_FILES,
                # Evict on upload: the local tier is emptied as soon as the
                # published table has the rows, which is the state under test.
                staging_retention=timedelta(0),
            ),
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle)
            await fill(stream, handle, total=800)
            await asyncio.to_thread(handle.evict)

            assert handle.staging_extent() is None, "the fixture must evict dry"

            async with serve(stream, maintain=False) as uri:
                with pytest.raises(streamcast.NotReplayable) as raised:
                    await streamcast.connect(uri, offset=1)

                assert raised.value.why == "evicted"

                # And the consumer's move from here works, which is the whole
                # reason this is a refusal with a named remedy rather than an
                # error: nothing ages out of the published table, so an
                # offset the local tier has dropped is still there.
                async with streamcast.connect(
                    uri, offset=1, catch_up=True, s3_options=s3
                ) as sub:
                    first, _ts, _row = await sub.recv()

                assert first == 1


class TestTheGapItCannotClose:
    async def test_a_published_table_that_starts_above_the_request_is_refused(
        self, tmp_path, s3, bucket, serve
    ):
        """The hole at the join, on the catch-up path.

        `prepare` already rules out a published table that ENDS below the request.
        This is the other end: one that ENDS above it and still does not go
        back far enough. Served naively, the consumer asks for 100, is handed
        500 first, and is told nothing — 400 rows lost and a cursor advanced
        past them. Measured doing exactly that before the guard existed.

        `_stream._replay_from` pulls a row early to prevent the same thing on
        the server side; the published table needed its own check, because the
        server's refusal is what sends the consumer here in the first place.
        """
        handle = litelink.new(
            tmp_path / "data",
            "trades",
            schema=streamcast.to_arrow(SCHEMA),
            published=bucket,
            s3_options=s3,
            # Nothing below 500 exists in ANY tier.
            start_offset=500,
            config=litelink.LogConfig(
                target_seal_size=SEAL_SIZE, compact_min_files=KEEP_FILES
            ),
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle, max_replay=50)
            await fill(stream, handle, total=800)

            async with serve(stream, maintain=False) as uri:
                with pytest.raises(streamcast.CatchUpUnavailable) as raised:
                    await streamcast.connect(
                        uri, offset=100, catch_up=True, s3_options=s3
                    )

        message = str(raised.value)
        assert "in neither the server nor the published tables" in message
        # Both ends of the missing range, and how many rows it is.
        assert "500" in message, message
        assert "100" in message, message
        assert "400" in message, message


class TestWhereTheHistoryIsRead:
    async def test_the_greeting_names_the_metadata_and_the_stream(
        self, serve, published_log, s3
    ):
        """The file and the id that says it is this stream's."""
        stream = streamcast.Stream("trades", log=published_log)
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert (
                sub.info.metadata == f"{published_log.published}/trades.metadata.json"
            )
            found = _snapshot.metadata(sub.info.metadata, s3)
            assert sub.info.stream_id == found.stream_id

    async def test_a_stream_without_a_log_names_none(self, serve):
        stream = streamcast.Stream("live")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            assert sub.info.metadata is None
            assert sub.info.stream_id is None
            assert sub.info.durable is False

    @pytest.mark.slow
    async def test_the_refusal_names_no_location_and_catch_up_needs_none(
        self, serve, published_log, s3
    ):
        """A close reason has 123 bytes, and a bucket URI is not in it.

        The numbers are what make a refusal readable; where to read the gap
        is the greeting's to say, and the greeting has no such limit.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=600)
        await fill(stream, published_log)

        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=1)

            fields = raised.value.fields
            assert not any("://" in str(value) for value in fields.values()), fields
            async with streamcast.connect(
                uri, offset=1, catch_up=True, s3_options=s3
            ) as sub:
                assert (await sub.recv())[0] == 1

    async def test_an_explicit_metadata_uri_wins(
        self, serve, published_log, s3, tmp_path
    ):
        """A caller that named a file meant that one, not the greeting's."""
        stream = streamcast.Stream("trades", log=published_log, max_replay=10)
        await fill(stream, published_log, total=800)

        async with serve(stream, maintain=False) as uri:
            missing = (tmp_path / "elsewhere.metadata.json").as_uri()
            with pytest.raises(streamcast.CatchUpUnavailable, match="elsewhere"):
                await streamcast.connect(
                    uri, offset=1, catch_up=True, s3_options=s3, metadata=missing
                )


class TestWhenItCannot:
    async def test_nothing_published_says_so(self, serve, log):
        # A local-only log whose rows are all still buffered: its metadata is
        # readable on this machine, and its published table holds nothing.
        stream = streamcast.Stream("trades", log=log, max_replay=2)
        async with serve(stream, maintain=False) as uri:
            await stream.send_many([{"event_ts": i, "price": 1.0} for i in range(20)])

            with pytest.raises(streamcast.CatchUpUnavailable, match="neither holds"):
                await streamcast.connect(uri, offset=1, catch_up=True)

    @pytest.mark.slow
    async def test_unreadable_credentials_say_what_to_do(self, serve, published_log):
        """The message an operator meets on a box that is already behind.

        "AccessDenied" alone answers none of the questions they have, so this
        one names what was tried, which credential source, and four ways out.
        """
        stream = streamcast.Stream("trades", log=published_log, max_replay=100)
        await fill(stream, published_log, total=1_200)

        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.CatchUpUnavailable) as raised:
                await streamcast.connect(
                    uri,
                    offset=10,
                    catch_up=True,
                    s3_options=streamcast.S3Options(
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
        self, serve, published_log, s3
    ):
        # The published table is further behind than the server's window: a range
        # exists that the server has forgotten and the published table never got.
        #
        # **Constructed, not stumbled into.** An earlier version asked from
        # offset 1 and passed only because the fixture happened to publish
        # NOTHING at that size — so it was asserting on an empty published table
        # rather than on a gap between two populated tiers. It kept passing
        # for the wrong reason, which is the failure mode a test cannot
        # report about itself.
        stream = streamcast.Stream("trades", log=published_log, max_replay=10)
        frontier = await fill(stream, published_log, total=800)
        # Sent past the published table without publishing, so it stays at
        # `frontier` while the server's window moves far above it.
        await stream.send_many([{"i": i, "pad": PAD} for i in range(800, 4_800)])

        # Above what the published table holds, and far below what the server will
        # replay. Nothing on either side has it.
        asking = frontier + 1_000

        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.CatchUpUnavailable) as raised:
                await streamcast.connect(
                    uri, offset=asking, catch_up=True, s3_options=s3
                )

        message = str(raised.value)
        assert "neither holds" in message
        # Both numbers, which is the point of the message: where the published
        # table stopped and where the consumer asked from.
        #
        # `frontier + 1`, and the off-by-one is the two APIs meaning
        # different things: `published_through()` is the LAST offset published,
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
    """`not_durable`, `empty` and `ahead` are not published-table problems.

    A stream with no log, a log with nothing in it, and a cursor from the
    future — no amount of reading object storage fixes any of them, and
    trying would turn a clear refusal into a confusing one.
    """
    from streamcast._catchup import RECOVERABLE

    assert RECOVERABLE == {"too_old", "evicted"}


def test_nothing_is_connected_while_the_published_table_is_read():
    """Asserted against the source, because it is the whole design.

    A future "hold the connection so the window closes by construction" is an
    easy change to make, and it turns every long catch-up into a `TooSlow`
    drop — on exactly the consumers that needed the feature.
    """
    import inspect

    from streamcast._catchup import Catcher

    source = inspect.getsource(Catcher.stream)
    assert source.index("yield offset, ts, row") < source.index(
        "self._handshake(self.start)"
    ), "the socket is opened before the published table is read"


def test_the_reads_cross_into_a_thread():
    # Resolving the metadata and pinning the live log read over the network —
    # seconds — and on the event loop that is the consumer's whole process
    # stopped.
    import inspect

    from streamcast import _snapshot as snapshot_module

    source = inspect.getsource(snapshot_module.snapshot)
    assert source.count("asyncio.to_thread") >= 2
    assert "asyncio.to_thread" in inspect.getsource(Snapshot.close)
