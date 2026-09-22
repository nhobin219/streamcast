"""Every way a subscribe can be refused, and what the subscriber is told.

A refusal has to survive a 123-byte close frame and come out the other end as
a sentence the caller can act on. Five of them are `NotReplayable`, and the
whole reason they are distinguished is that the caller's next move differs:
drop the offset, ask again from a different one, or stop asking the server.
"""

from __future__ import annotations

import asyncio

import pytest

import streamcast
from tests.conftest import SCHEMA, trade


class TestNotReplayable:
    async def test_a_stream_with_no_log_refuses_any_offset(self, serve):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=1)

        assert raised.value.why == "not_durable"
        assert "no log attached" in str(raised.value)
        # And it says what to do instead, which is the point of five messages
        # rather than one.
        assert "without `offset=`" in str(raised.value)

    async def test_a_log_that_holds_nothing_refuses_EARLIEST(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=streamcast.EARLIEST)

        assert raised.value.why == "empty"

    async def test_an_offset_above_the_frontier_is_refused_with_both_numbers(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(0), trade(1)])
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=9999)

        assert raised.value.why == "ahead"
        assert "9999" in str(raised.value)
        assert "3" in str(raised.value)

    async def test_an_offset_further_back_than_max_replay_is_refused(self, serve, log):
        # The bound exists to protect the server: a replay is a scan in a
        # worker thread, and one asking for ten million rows holds one for
        # minutes. Past it the answer is to read the log directly.
        stream = streamcast.Stream("trades", log=log, max_replay=5)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(20)])
            with pytest.raises(streamcast.NotReplayable) as raised:
                await streamcast.connect(uri, offset=1)

        assert raised.value.why == "too_old"
        assert "at most 5" in str(raised.value)
        assert "20 messages behind" in str(raised.value)

    async def test_an_offset_below_what_the_log_still_holds_is_refused(
        self, serve, tmp_path
    ):
        """A resume that would silently begin above where it asked.

        This is the one wrong answer a resume must never give, and the only
        refusal that cannot be decided before the scan opens — so it is
        settled by reading the first row and comparing it, which is why
        `_replay_from` pulls one row early.

        `start_offset=500` stands in for a log whose retention has passed the
        requested offset: the effect on a reader is identical — the lowest
        offset it holds is above what was asked for — and it needs no eviction
        pass to arrange.
        """
        import litelink

        handle = litelink.new(
            tmp_path / "data", "trades", schema=SCHEMA, start_offset=500
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle)
            async with serve(stream) as uri:
                await stream.send_many([trade(i) for i in range(3)])
                with pytest.raises(streamcast.NotReplayable) as raised:
                    await streamcast.connect(uri, offset=100)

        assert raised.value.why == "evicted"
        assert "500" in str(raised.value)
        # Nothing was delivered before the refusal. A subscriber that received
        # rows and THEN got the error would have a hole it could not see.
        assert raised.value.fields.get("offset") == 100

    async def test_a_refused_subscribe_leaves_the_server_clean(self, serve, log):
        # A refusal runs before the subscriber joins the fan-out set, and the
        # handler has to unwind without leaving a task behind. Twenty-five of
        # them in a row is what makes a leak visible.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            before = len(asyncio.all_tasks())
            for _ in range(25):
                with pytest.raises(streamcast.NotReplayable):
                    await streamcast.connect(uri, offset=streamcast.EARLIEST)

            await asyncio.sleep(0.05)
            assert stream.subscribers == 0
            assert len(asyncio.all_tasks()) <= before + 2

        # And the stream still works afterwards.
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            await stream.send(trade(0))
            assert (await sub.recv())[0] == 1


class TestRoutingRefusals:
    async def test_an_unknown_stream_names_what_the_server_does_serve(self, serve):
        trades = streamcast.Stream("trades")
        quotes = streamcast.Stream("quotes")
        async with serve(trades, quotes) as uri:
            base = uri.rsplit("/", 1)[0]
            with pytest.raises(streamcast.StreamNotFound) as raised:
                await streamcast.connect(f"{base}/trade")

        assert raised.value.requested == "trade"
        assert raised.value.serves == ("quotes", "trades")
        assert "'quotes', 'trades'" in str(raised.value)

    @pytest.mark.parametrize(
        ("query", "match"),
        [
            ("?offset=abc", "not an integer"),
            ("?offset=-5", "negative"),
            ("?from=3", "unknown query parameter"),
        ],
    )
    async def test_a_subscribe_the_server_cannot_parse_is_a_protocol_error(
        self, serve, query, match
    ):
        stream = streamcast.Stream("trades")
        async with serve(stream) as uri:
            with pytest.raises(streamcast.ProtocolError, match=match):
                await streamcast.connect(uri + query)


class TestTheClientSide:
    async def test_giving_the_offset_twice_is_refused_before_connecting(self):
        # Two values that disagree is a resume from the wrong place, and
        # neither is more likely to be the intended one.
        with pytest.raises(ValueError, match="offset is given twice"):
            streamcast.connect("ws://127.0.0.1:1/trades?offset=5", offset=9)

    async def test_the_offset_may_be_written_into_the_uri_instead(self, serve, log):
        # The affordance that makes `wscat ws://server/trades?offset=0` work.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(0), trade(1)])
            async with streamcast.connect(uri + "?offset=2") as sub:
                assert sub.info.replay == (2, 3)
                assert (await sub.recv())[0] == 2

    async def test_a_peer_that_is_not_a_server_is_a_protocol_error(self):
        # An unrelated WebSocket service on a reused port. The failure has to
        # name that, not surface as an empty stream.
        import websockets

        async def impostor(connection):
            await connection.send("not a greeting")
            await connection.wait_closed()

        server = await websockets.serve(impostor, "127.0.0.1", 0)
        try:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(streamcast.ProtocolError, match="not JSON"):
                await streamcast.connect(f"ws://127.0.0.1:{port}/trades")

        finally:
            server.close()
            await server.wait_closed()


async def test_every_refusal_is_a_StreamcastError(serve):
    # One base class to catch, which is what makes a supervising loop
    # writable without enumerating five types.
    stream = streamcast.Stream("trades")
    async with serve(stream) as uri:
        with pytest.raises(streamcast.StreamcastError):
            await streamcast.connect(uri, offset=1)


async def test_a_refusal_inside_async_with_does_not_wedge_the_teardown(serve, log):
    """The shape that hung before `earliest` was fixed, kept as a guard.

    `__aenter__` raising means `__aexit__` never runs, so nothing on this side
    closes the connection — and the first version of this suite deadlocked
    exactly there, with the server's `wait_closed()` waiting on a handler and
    the handler waiting on a peer that was gone. Worth its own test because
    the bare `await connect(...)` form used elsewhere in this file does not
    reach it.
    """
    stream = streamcast.Stream("trades", log=log)
    async with serve(stream) as uri:
        with pytest.raises(streamcast.NotReplayable):
            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                await sub.recv()

        await stream.send(trade(0))
        async with streamcast.connect(uri) as sub:
            await stream.send(trade(1))
            assert (await sub.recv())[0] == 2


class TestTheCursor:
    """`connect(cursor=path)` — the loop every consumer otherwise hand-writes."""

    async def test_it_saves_and_resumes(self, serve, log, tmp_path):
        cursor = tmp_path / "trades.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            async with streamcast.connect(uri, offset=0, cursor=cursor) as sub:
                first = [await sub.recv() for _ in range(4)]

            assert [o for o, _ in first] == [1, 2, 3, 4]
            assert cursor.read_text() == "4"

            # No offset= at all: the file decides, and resumes ONE ABOVE it.
            async with streamcast.connect(uri, cursor=cursor) as sub:
                rest = [await sub.recv() for _ in range(6)]

        assert [o for o, _ in rest] == [5, 6, 7, 8, 9, 10]
        assert cursor.read_text() == "10"

    async def test_a_handler_that_raises_does_not_advance_it(
        self, serve, log, tmp_path
    ):
        """The asymmetry the whole design rests on.

        A cursor behind the work re-delivers, which is safe. A cursor ahead of
        it skips for ever, which is not. So leaving the block on an exception
        saves nothing, and the message whose handler raised comes back.
        """
        cursor = tmp_path / "trades.offset"
        cursor.write_text("3")
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            with pytest.raises(RuntimeError, match="handler"):
                async with streamcast.connect(uri, cursor=cursor) as sub:
                    assert (await sub.recv())[0] == 4
                    raise RuntimeError("handler blew up")

            assert cursor.read_text() == "3"

            # And it is re-delivered rather than skipped.
            async with streamcast.connect(uri, cursor=cursor) as sub:
                assert (await sub.recv())[0] == 4

    async def test_it_advances_only_when_the_next_message_is_asked_for(
        self, serve, log, tmp_path
    ):
        # Receiving is not finishing. Coming back for another is the only
        # evidence the library has that the last one was handled.
        cursor = tmp_path / "trades.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(5)])

            async with streamcast.connect(uri, offset=0, cursor=cursor) as sub:
                await sub.recv()
                assert not cursor.exists(), "saved before the caller came back"
                await sub.recv()
                # Throttled to once a second, so force it to observe the value.
                sub.commit(1)
                assert cursor.read_text() == "1"

    async def test_an_explicit_offset_overrides_the_file(self, serve, log, tmp_path):
        cursor = tmp_path / "trades.offset"
        cursor.write_text("7")
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            async with streamcast.connect(uri, offset=2, cursor=cursor) as sub:
                assert (await sub.recv())[0] == 2

    async def test_offset_none_overrides_the_file_with_live_only(
        self, serve, log, tmp_path
    ):
        # Which is why the default is a sentinel: `None` has to stay available
        # as "ignore the file", distinct from "no offset given".
        cursor = tmp_path / "trades.offset"
        cursor.write_text("3")
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(10)])

            async with streamcast.connect(uri, offset=None, cursor=cursor) as sub:
                assert sub.info.replay is None

    async def test_a_missing_or_unreadable_file_is_treated_as_absent(
        self, serve, log, tmp_path
    ):
        """A torn write must not be an outage.

        A cursor is a recovery hint; refusing to start because it is empty
        turns a crash mid-save into a service that will not come up.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(3)])

            for content in ("", "   ", "not-a-number"):
                cursor = tmp_path / f"c{len(content)}.offset"
                cursor.write_text(content)
                async with streamcast.connect(uri, cursor=cursor) as sub:
                    assert sub.info.replay is None

    async def test_the_save_is_atomic(self, tmp_path):
        # Written beside the target and renamed over it, so a reader sees the
        # old value or the new one and never half of either.
        from streamcast._cursor import Cursor

        path = tmp_path / "c.offset"
        cursor = Cursor(path, every=0.0)
        cursor.save(1)
        cursor.save(1234567)
        assert path.read_text() == "1234567"
        assert list(tmp_path.glob("*.tmp")) == []

    async def test_without_a_cursor_nothing_is_written(self, serve, log, tmp_path):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send(trade(0))
            async with streamcast.connect(uri, offset=0) as sub:
                await sub.recv()
                sub.commit()  # a no-op, not an error

        assert list(tmp_path.glob("*.offset")) == []
        assert list(tmp_path.glob("*.tmp")) == []


class TestTheCursorNeverRewinds:
    """Forward only, and the rewind does not come from message ordering."""

    async def test_this_library_does_not_reorder(self, serve, log):
        """What a loopback test can actually establish.

        That the pump, the queue and the replay/live join deliver in order —
        the parts this library controls. It says NOTHING about a network
        between two hosts, which is why `recv` checks rather than trusts: see
        the next test.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(100)])

            async def publish():
                for i in range(100, 300):
                    await stream.send(trade(i))
                    await asyncio.sleep(0)

            task = asyncio.create_task(publish())
            async with streamcast.connect(uri, offset=1) as sub:
                seen = [offset for offset, _ in [await sub.recv() for _ in range(300)]]

            await task

        assert seen == list(range(1, 301))

    async def test_an_out_of_order_frame_stops_the_stream(self, serve, log):
        """Checked, not reasoned about.

        TCP delivers a byte stream in order, so frames on one connection
        cannot overtake each other — but that is a claim about a network this
        library cannot test, and a middlebox that is not a conforming
        WebSocket proxy is not hypothetical. Processing a stream whose
        offsets went backwards means silently skipping data once a cursor is
        involved, so it stops instead. The retry loop recovers.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(5)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert (await sub.recv())[0] == 1
                assert (await sub.recv())[0] == 2

                # Rewind what the subscription believes it has seen, which is
                # indistinguishable from the next frame arriving stale.
                sub._offset = 99  # noqa: SLF001
                with pytest.raises(streamcast.ProtocolError, match="must increase"):
                    await sub.recv()

    async def test_a_gap_in_the_offsets_is_not_out_of_order(self, serve, log):
        # litelink's offset space has legitimate gaps — `restore` fences 2**20
        # of them — so the check is `<=`, not `!= previous + 1`. A jump
        # forward is ordinary.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(3)])

            async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                assert (await sub.recv())[0] == 1
                sub._offset = -1000  # noqa: SLF001 — a large jump forward
                assert (await sub.recv())[0] == 2

    async def test_a_live_only_stream_has_no_offsets_to_check(self, serve):
        # Every frame carries `null`, so there is nothing to compare and the
        # check must not trip on it.
        stream = streamcast.Stream("live")
        async with serve(stream) as uri, streamcast.connect(uri) as sub:
            for i in range(3):
                await stream.send(trade(i))

            assert [offset for offset, _ in [await sub.recv() for _ in range(3)]] == [
                None,
                None,
                None,
            ]

    async def test_an_explicit_offset_cannot_clobber_the_file_backwards(
        self, serve, log, tmp_path
    ):
        """Where a rewind actually comes from.

        A one-off replay sharing a production cursor: `offset=` overrides what
        is READ, and before the guard the automatic save then wrote 1, 2, 3
        over a file that said 400. Observed exactly that.
        """
        cursor = tmp_path / "trades.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(50)])

            async with streamcast.connect(uri, offset=0, cursor=cursor) as sub:
                for _ in range(40):
                    await sub.recv()

            assert cursor.read_text() == "40"

            # The one-off. It reads from 1 and must leave the file alone.
            async with streamcast.connect(uri, offset=1, cursor=cursor) as sub:
                for _ in range(5):
                    await sub.recv()

            assert cursor.read_text() == "40"

            # And the next normal run still resumes from 41.
            async with streamcast.connect(uri, cursor=cursor) as sub:
                assert (await sub.recv())[0] == 41

    async def test_a_fresh_cursor_over_an_existing_file_cannot_rewind_it_either(
        self, tmp_path
    ):
        from streamcast import Cursor

        path = tmp_path / "c.offset"
        path.write_text("500")
        cursor = Cursor(path, every=0.0)
        assert cursor.load() == 500
        cursor.save(10)
        assert path.read_text() == "500"

    async def test_an_explicit_commit_may_correct_it_downward(
        self, serve, log, tmp_path
    ):
        """Because that is the caller stating what is durable.

        A batching consumer's automatic cursor runs AHEAD of its flushed work,
        so correcting it down is the point — and the automatic path, which
        cannot know, never does it.
        """
        cursor = tmp_path / "trades.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(30)])

            async with streamcast.connect(uri, offset=0, cursor=cursor) as sub:
                for _ in range(20):
                    await sub.recv()

                sub.commit(8)

            assert cursor.read_text() == "8"
