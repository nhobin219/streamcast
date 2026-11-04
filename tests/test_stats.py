"""What a stream reports about itself, and what the numbers must not claim.

The endpoint exists to tell a QUIET stream from a DEAD one, which a subscriber
cannot do: both look like a socket with nothing arriving. So the tests that
matter here are the ones about ambiguity — a restart that has sent nothing yet
must not read like a stream that stopped, and an age must not move because a
clock did.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import urllib.error
import urllib.request

import litelink
import pytest
from starlette.responses import Response

import streamcast
from streamcast._stats import STATS_PATH, Stats, payload
from tests.hosting import Listener

from .conftest import trade


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url) as answer:  # noqa: S310 — a test-local URL
            return int(answer.status), answer.read()

    except urllib.error.HTTPError as status:
        return int(status.code), status.read()


async def _sample(stream: streamcast.StreamHandle) -> Stats:
    _ = stream.stats
    assert stream._stats_refresh is not None
    await stream._stats_refresh
    return stream.stats


async def _loop_turn() -> None:
    loop = asyncio.get_running_loop()
    ready = loop.create_future()
    loop.call_soon(ready.set_result, None)
    await ready


class TestPublicationWatermark:
    @pytest.fixture
    async def stream(self, log):
        handle = streamcast.StreamHandle("trades", log=log)
        try:
            yield handle
        finally:
            await handle.aclose()

    async def test_empty_and_live_only_are_distinct(self, stream):
        assert stream.stats.published_through is None, "no sample has completed"
        empty = await _sample(stream)
        assert (empty.end_offset, empty.published_through) == (1, 0)

        live = streamcast.StreamHandle("live")
        await live.send({"anything": 1})
        assert live.stats.published_through is None
        assert live._stats_refresh is None, "a live stream has no log to sample"
        assert json.loads(payload([live]))["streams"][0]["published_through"] is None
        await live.aclose()

    async def test_staging_ahead_and_publication_advance(self, stream, log):
        await stream.send_many([trade(i) for i in range(3)])
        before = await _sample(stream)
        assert (before.end_offset, before.published_through) == (4, 0)

        log.seal(flush=True)
        log.publish(flush=True)
        published = await _sample(stream)
        assert (published.end_offset, published.published_through) == (4, 3)

        await stream.send(trade(3))
        ahead = await _sample(stream)
        assert (ahead.end_offset, ahead.published_through) == (5, 3)

        log.seal(flush=True)
        log.publish(flush=True)
        caught_up = await _sample(stream)
        assert (caught_up.end_offset, caught_up.published_through) == (5, 4)

    async def test_failed_read_retracts_a_previous_sample(
        self, stream, log, monkeypatch
    ):
        assert (await _sample(stream)).published_through == 0
        # Finish the refresh requested by the returned snapshot before changing
        # the collaborator, so failure and recovery have a forced ordering.
        await stream._stats_refresh
        original = log.published_through

        def unavailable():
            raise OSError("local metadata unavailable")

        monkeypatch.setattr(log, "published_through", unavailable)
        failed = await _sample(stream)
        assert failed.published_through is None
        assert json.loads(payload([stream]))["streams"][0]["published_through"] is None
        await stream._stats_refresh
        monkeypatch.setattr(log, "published_through", original)
        assert (await _sample(stream)).published_through == 0

    def test_without_an_event_loop_the_sample_is_unavailable(self, log):
        stream = streamcast.StreamHandle("trades", log=log)
        assert stream.stats.published_through is None
        assert stream._stats_refresh is None

    async def test_sampling_is_local_and_cached_outside_the_loop(
        self, stream, log, monkeypatch
    ):
        await stream.send(trade(0))
        log.seal(flush=True)
        log.publish(flush=True)

        def no_network(*args, **kwargs):
            pytest.fail("sampling a local watermark must not open a socket")

        loop_thread = threading.get_ident()
        original = log.published_through
        readers = []

        def observed():
            readers.append(threading.get_ident())
            return original()

        monkeypatch.setattr(log, "published_through", observed)
        monkeypatch.setattr(socket.socket, "connect", no_network)
        assert (await _sample(stream)).published_through == 1
        await stream._stats_refresh
        assert readers and all(reader != loop_thread for reader in readers)
        sampled = stream._stats_refresh
        assert (await asyncio.to_thread(lambda: stream.stats)).published_through == 1
        assert stream._stats_refresh is sampled, "no refresh outside an event loop"

    @pytest.mark.parametrize("cancel_close", [False, True])
    async def test_a_blocked_read_does_not_block_stats_and_is_joined_on_close(
        self, log, monkeypatch, cancel_close
    ):
        stream = streamcast.StreamHandle("trades", log=log, owns_log=True)
        loop = asyncio.get_running_loop()
        entered = loop.create_future()
        release = threading.Event()
        finished = threading.Event()
        original = log.published_through
        close = log.close
        calls = 0
        closed = False

        def blocked():
            nonlocal calls
            calls += 1
            loop.call_soon_threadsafe(entered.set_result, None)
            assert release.wait(5), "test did not release the metadata read"
            try:
                return original()
            finally:
                finished.set()

        def checked_close():
            nonlocal closed
            assert finished.is_set(), "closed the log under its metadata reader"
            closed = True
            close()

        monkeypatch.setattr(log, "published_through", blocked)
        monkeypatch.setattr(log, "close", checked_close)
        closing = None
        try:
            assert stream.stats.published_through is None
            await entered
            pending = stream._stats_refresh
            for _ in range(20):
                assert stream.stats.published_through is None
                assert stream._stats_refresh is pending

            assert calls == 1, "concurrent polls must share one read"
            # Encoding is the HTTP path too; it must not wait for the lock.
            assert (
                json.loads(payload([stream]))["streams"][0]["published_through"] is None
            )
            closing = asyncio.create_task(stream.aclose())
            await _loop_turn()
            assert stream._stats_closed, "close has entered its join"
            assert not closing.done()
            assert not closed
            if cancel_close:
                closing.cancel()
                await _loop_turn()
                assert not closing.done(), "cancellation must still join the read"

        finally:
            release.set()
            if closing is None:
                await stream.aclose()
            elif cancel_close:
                with pytest.raises(asyncio.CancelledError):
                    await closing
            else:
                await closing

        assert closed
        assert stream.stats.published_through is None

    async def test_a_retired_stream_samples_its_read_only_log(self, tmp_path):
        schema = {
            "type": "object",
            "properties": {"i": {"type": "integer"}},
            "required": ["i"],
        }
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="t", schema=schema),
                storage=streamcast.Storage(root=tmp_path),
            ),
            initialization="create_if_absent",
        )
        await stream.send_many([{"i": i} for i in range(3)])
        await stream.aclose()
        streamcast.StreamHandle.retire("t", root=tmp_path)
        retired = streamcast.StreamHandle.migrate("t", root=tmp_path, schema=schema)
        try:
            assert retired.retirement is not None
            assert isinstance(retired.log, litelink.LocalReadHandle)
            stats = await _sample(retired)
            assert (stats.end_offset, stats.published_through) == (4, 3)
        finally:
            await retired.aclose()

    async def test_restore_gap_is_not_counted_as_published(self, stream, log, tmp_path):
        await stream.send_many([trade(i) for i in range(3)])
        log.seal(flush=True)
        log.publish(flush=True)
        restored_log = litelink.restore(
            tmp_path / "restored",
            "trades",
            published=log.published,
            published_reserve=100,
        )
        restored = streamcast.StreamHandle("trades", log=restored_log, owns_log=True)
        try:
            stats = await _sample(restored)
            assert stats.end_offset == 104
            assert stats.published_through == 3, "the reserve is not publication"
            offset = await restored.send(trade(3))
            assert offset == 104
            ahead = await _sample(restored)
            assert (ahead.end_offset, ahead.published_through) == (105, 3)
            restored_log.seal(flush=True)
            restored_log.publish(flush=True)
            stats = await _sample(restored)
            assert (stats.end_offset, stats.published_through) == (105, 104)
        finally:
            await restored.aclose()

    async def test_http_stats_carries_the_sample(self, stream, log, serve):
        await stream.send(trade(0))
        log.seal(flush=True)
        log.publish(flush=True)
        assert (await _sample(stream)).published_through == 1
        async with serve(stream, maintain=False) as uri:
            url = uri.replace("ws://", "http://").replace("/trades", STATS_PATH)
            status, body = await asyncio.to_thread(_get, url)

        assert status == 200
        entry = json.loads(body)["streams"][0]
        assert (entry["end_offset"], entry["published_through"]) == (2, 1)


class TestTheFacts:
    async def test_a_fresh_stream_reports_no_send_and_says_how_long_it_has_run(
        self, log
    ):
        """**The startup ambiguity, which is the whole reason `uptime_s` exists.**

        A server that just restarted holds a log with every row ever written,
        so `end_offset` is large — and nothing has been sent *by this process*,
        so `last_send_*` is null. On its own that is indistinguishable from a
        stream that died, and on a quiet stream the ambiguity can last hours.

        `uptime_s` is what resolves it: nothing sent four seconds into a
        restart is ordinary, nothing sent six hours in is not. Neither number
        says that alone, so both are published.
        """
        for i in range(3):  # rows from "before the restart"
            await streamcast.StreamHandle("trades", log=log).send(trade(i))

        restarted = streamcast.StreamHandle("trades", log=log)
        stats = restarted.stats

        assert stats.end_offset == 4, "the log's history is still there"
        assert stats.last_send_ts is None, "nothing sent in THIS process"
        assert stats.last_send_age_s is None, "and no age to report for it"
        assert stats.uptime_s >= 0, "but how long it has been up is known"
        assert stats.started_ts > 0

    async def test_a_send_sets_both_the_timestamp_and_the_age(self, log):
        stream = streamcast.StreamHandle("trades", log=log)
        await stream.send(trade(0))
        stats = stream.stats

        assert stats.last_send_ts is not None
        assert stats.last_send_age_s is not None
        assert stats.last_send_age_s < 5, "a send that just happened is not stale"

    async def test_send_many_stamps_once_for_the_group(self, log):
        """One transaction, one time. Per-row values would imply a precision
        the commit does not have."""
        stream = streamcast.StreamHandle("trades", log=log)
        await stream.send_many([trade(i) for i in range(10)])

        assert stream.stats.last_send_age_s is not None
        assert stream.stats.end_offset == 11

    async def test_the_age_grows_while_the_stream_is_quiet(self, log):
        """The measurement the endpoint exists for."""
        stream = streamcast.StreamHandle("trades", log=log)
        await stream.send(trade(0))
        first = stream.stats.last_send_age_s
        await asyncio.sleep(0.05)
        second = stream.stats.last_send_age_s

        assert first is not None
        assert second is not None
        assert second > first, "a quiet stream must look older over time"

    async def test_the_timestamp_does_not_move_while_the_stream_is_quiet(self, log):
        """The age moves; the wall-clock stamp of the last send does not."""
        stream = streamcast.StreamHandle("trades", log=log)
        await stream.send(trade(0))
        stamped = stream.stats.last_send_ts
        await asyncio.sleep(0.05)

        assert stream.stats.last_send_ts == stamped

    async def test_subscribers_is_live(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            assert stream.stats.subscribers == 0
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                await sub.recv()
                assert stream.stats.subscribers == 1

        assert stream.stats.subscribers == 0

    async def test_a_live_only_stream_reports_no_offsets_and_still_reports_time(self):
        """Nothing assigned an offset, but it is still possible to say the
        stream is receiving — which is the multicast case's whole question."""
        stream = streamcast.StreamHandle("live")
        await stream.send({"anything": 1})
        stats = stream.stats

        assert stats.durable is False
        assert stats.end_offset is None
        assert stats.last_send_age_s is not None


class TestWhatItDeliberatelyDoesNotCarry:
    def test_there_is_no_verdict_field(self):
        """**No `status`, no `healthy`, no threshold.**

        A stream that publishes once a day at 00:20 UTC is healthy while
        silent for 23 hours; a 5-second socket is broken after 30. Any
        threshold chosen here is wrong for one of them and, worse, looks
        authoritative to whoever reads it.
        """
        fields = set(Stats.__dataclass_fields__)

        assert not fields & {"status", "healthy", "health", "ok", "stale"}

    def test_there_is_no_rate_field(self):
        """`end_offset` read twice is the rate, over the caller's own window.

        A `rows_1m` here would impose a window, which is the same mistake as
        imposing a freshness threshold one field up.
        """
        fields = set(Stats.__dataclass_fields__)

        assert not fields & {"rows_1m", "rows_per_s", "rate"}

    def test_it_carries_no_server_facts(self):
        """`maintain` and `replicate` belong to whatever owns the children.

        A `StreamHandle` does not own its maintainer — the broker's lifespan does —
        so reporting them here would be this object answering for something it
        cannot see.
        """
        fields = set(Stats.__dataclass_fields__)

        assert not fields & {"maintain", "replicate", "host", "port"}


class TestTheEndpoint:
    async def test_it_answers_on_the_port_the_server_already_has(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False, stats=True) as uri:
            await stream.send(trade(0))
            url = uri.replace("ws://", "http://").replace("/trades", STATS_PATH)
            status, body = await asyncio.to_thread(_get, url)

        assert status == 200
        got = json.loads(body)
        assert got["streams"][0]["name"] == "trades"
        assert got["streams"][0]["end_offset"] == 2
        assert got["served_at"] > 0, "so a caller can measure its own skew"

    async def test_subscribing_still_works_on_the_same_port(self, serve, log):
        """The hook must defer, not swallow — a WebSocket upgrade is not a GET."""
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False, stats=True) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

    async def test_it_is_on_by_default(self, serve, log):
        """**On, unlike `publish=`, and the asymmetry is the point.**

        `publish` grants writes that nothing else on the port grants. This
        discloses strictly LESS than the socket beside it: a wrong-path
        connect is answered with `serves=` naming every stream, the greeting
        carries `end_offset`, and anyone who can reach the port can subscribe
        and read every row. Gating it would protect nothing while leaving a
        quiet stream undiagnosable by default — the failure it exists to fix.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:  # nothing passed
            url = uri.replace("ws://", "http://").replace("/trades", STATS_PATH)
            status, _body = await asyncio.to_thread(_get, url)

        assert status == 200

    async def test_the_names_it_reports_were_already_public(self, serve, log):
        """The premise of defaulting it on, asserted rather than assumed.

        If a wrong-path connect stopped naming what the server serves, the
        disclosure argument above would no longer hold and the default should
        be revisited. This is the tripwire for that.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False, stats=False) as uri:
            with pytest.raises(streamcast.StreamNotFound) as raised:
                await streamcast.connect(uri.replace("/trades", "/nope"))

        assert "trades" in str(raised.value), (
            "stream names are no longer public on a refusal; the reason "
            "`stats` defaults on has changed"
        )

    async def test_it_can_be_turned_off(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False, stats=False) as uri:
            url = uri.replace("ws://", "http://").replace("/trades", STATS_PATH)
            status, _body = await asyncio.to_thread(_get, url)

        assert status != 200

    async def test_a_custom_path_is_honoured(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False, stats="/_internal/streams") as uri:
            base = uri.replace("ws://", "http://").replace("/trades", "")
            status, _body = await asyncio.to_thread(_get, f"{base}/_internal/streams")
            missing, _ = await asyncio.to_thread(_get, f"{base}{STATS_PATH}")

        assert status == 200
        assert missing != 200, "it does not also answer the default path"

    @pytest.mark.parametrize("asynchronous", [False, True])
    async def test_host_routes_compose_with_broker_stats(self, log, asynchronous):

        stream = streamcast.StreamHandle("trades", log=log)
        app = streamcast.create_app(
            streams=stream, config=streamcast.BrokerConfig(maintain=False)
        )

        def mine():
            return Response(status_code=418)

        async def async_mine():
            return mine()

        app.get("/mine")(async_mine if asynchronous else mine)
        async with Listener(app, "127.0.0.1", 0) as server:
            base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            theirs, _ = await asyncio.to_thread(_get, f"{base}/mine")
            ours, _ = await asyncio.to_thread(_get, f"{base}{STATS_PATH}")
            async with streamcast.connect(
                base.replace("http://", "ws://") + "/trades"
            ) as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

        assert theirs == 418
        assert ours == 200

    async def test_every_served_stream_is_listed(self, serve, log, tmp_path):
        first = streamcast.StreamHandle("trades", log=log)
        second = streamcast.StreamHandle("quotes")
        async with serve(first, second, maintain=False, stats=True) as uri:
            url = uri.replace("ws://", "http://").replace("/trades", STATS_PATH)
            _status, body = await asyncio.to_thread(_get, url)

        names = {s["name"] for s in json.loads(body)["streams"]}
        assert names == {"trades", "quotes"}


class TestThePayload:
    async def test_it_is_json_and_round_trips(self, log):
        stream = streamcast.StreamHandle("trades", log=log)
        await stream.send(trade(0))
        got = json.loads(payload([stream]))

        assert got["streams"][0]["last_send_ts"] is not None
        assert isinstance(got["served_at"], float)

    async def test_nulls_survive_encoding(self, log):
        """The startup case has to be expressible on the wire, not just in
        Python — a caller reading JSON must see `null`, not a missing key."""
        got = json.loads(payload([streamcast.StreamHandle("trades", log=log)]))
        entry = got["streams"][0]

        assert entry["last_send_ts"] is None
        assert entry["last_send_age_s"] is None
        assert "uptime_s" in entry


@pytest.mark.parametrize("field", ["name", "durable", "end_offset", "subscribers"])
def test_the_greeting_and_the_stats_do_not_disagree(field):
    """Both describe one stream, so the names that overlap must mean the same.

    `Subscription.info` is the greeting — declared, fixed at connect.
    `StreamHandle.stats` is counters that move. Where they name the same thing they
    must agree, or an operator reading both gets two answers.
    """
    assert field in Stats.__dataclass_fields__
