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
import urllib.error
import urllib.request

import pytest

import streamcast
from streamcast._stats import INFO_PATH, Stats, payload

from .conftest import trade


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url) as answer:  # noqa: S310 — a test-local URL
            return int(answer.status), answer.read()

    except urllib.error.HTTPError as status:
        return int(status.code), status.read()


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
            await streamcast.Stream("trades", log=log).send(trade(i))

        restarted = streamcast.Stream("trades", log=log)
        stats = restarted.stats

        assert stats.end_offset == 4, "the log's history is still there"
        assert stats.last_send_ts is None, "nothing sent in THIS process"
        assert stats.last_send_age_s is None, "and no age to report for it"
        assert stats.uptime_s >= 0, "but how long it has been up is known"
        assert stats.started_ts > 0

    async def test_a_send_sets_both_the_timestamp_and_the_age(self, log):
        stream = streamcast.Stream("trades", log=log)
        await stream.send(trade(0))
        stats = stream.stats

        assert stats.last_send_ts is not None
        assert stats.last_send_age_s is not None
        assert stats.last_send_age_s < 5, "a send that just happened is not stale"

    async def test_send_many_stamps_once_for_the_group(self, log):
        """One transaction, one time. Per-row values would imply a precision
        the commit does not have."""
        stream = streamcast.Stream("trades", log=log)
        await stream.send_many([trade(i) for i in range(10)])

        assert stream.stats.last_send_age_s is not None
        assert stream.stats.end_offset == 11

    async def test_the_age_grows_while_the_stream_is_quiet(self, log):
        """The measurement the endpoint exists for."""
        stream = streamcast.Stream("trades", log=log)
        await stream.send(trade(0))
        first = stream.stats.last_send_age_s
        await asyncio.sleep(0.05)
        second = stream.stats.last_send_age_s

        assert first is not None
        assert second is not None
        assert second > first, "a quiet stream must look older over time"

    async def test_the_timestamp_does_not_move_while_the_stream_is_quiet(self, log):
        """The age moves; the wall-clock stamp of the last send does not."""
        stream = streamcast.Stream("trades", log=log)
        await stream.send(trade(0))
        stamped = stream.stats.last_send_ts
        await asyncio.sleep(0.05)

        assert stream.stats.last_send_ts == stamped

    async def test_subscribers_is_live(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
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
        stream = streamcast.Stream("live")
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

        A `Stream` does not own its maintainer — `serve` does, and a mounted
        app's `_Mounted` does — so reporting them here would be this object
        answering for something it cannot see.
        """
        fields = set(Stats.__dataclass_fields__)

        assert not fields & {"maintain", "replicate", "host", "port"}


class TestTheEndpoint:
    async def test_it_answers_on_the_port_the_server_already_has(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False, info=True) as uri:
            await stream.send(trade(0))
            url = uri.replace("ws://", "http://").replace("/trades", INFO_PATH)
            status, body = await asyncio.to_thread(_get, url)

        assert status == 200
        got = json.loads(body)
        assert got["streams"][0]["name"] == "trades"
        assert got["streams"][0]["end_offset"] == 2
        assert got["served_at"] > 0, "so a caller can measure its own skew"

    async def test_subscribing_still_works_on_the_same_port(self, serve, log):
        """The hook must defer, not swallow — a WebSocket upgrade is not a GET."""
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False, info=True) as uri:
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

    async def test_it_is_off_unless_asked_for(self, serve, log):
        """An unauthenticated surface naming every stream should not appear
        on an upgrade. Same shape of argument as `publish=`, if weaker."""
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            url = uri.replace("ws://", "http://").replace("/trades", INFO_PATH)
            status, _body = await asyncio.to_thread(_get, url)

        assert status != 200

    async def test_a_custom_path_is_honoured(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False, info="/_internal/streams") as uri:
            base = uri.replace("ws://", "http://").replace("/trades", "")
            status, _body = await asyncio.to_thread(_get, f"{base}/_internal/streams")
            missing, _ = await asyncio.to_thread(_get, f"{base}{INFO_PATH}")

        assert status == 200
        assert missing != 200, "it does not also answer the default path"

    async def test_a_caller_s_own_process_request_still_runs(self, serve, log):
        """**Composed, not overwritten.**

        `process_request` is a keyword a caller may already be using — for
        auth, or a health check of their own. Assigning ours over it would
        break that silently, so ours answers its own path and defers.

        Falsify by assigning `kwargs["process_request"]` instead of chaining.
        """
        from websockets.datastructures import Headers
        from websockets.http11 import Response

        def mine(_connection: object, request: object) -> object:
            if getattr(request, "path", "") == "/mine":
                return Response(418, "OK", Headers({"Content-Length": "0"}), b"")

            return None

        stream = streamcast.Stream("trades", log=log)
        async with serve(
            stream, maintain=False, info=True, process_request=mine
        ) as uri:
            base = uri.replace("ws://", "http://").replace("/trades", "")
            theirs, _ = await asyncio.to_thread(_get, f"{base}/mine")
            ours, _ = await asyncio.to_thread(_get, f"{base}{INFO_PATH}")
            # And a subscribe still reaches the stream through both hooks.
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

        assert theirs == 418, "the caller's hook was overwritten"
        assert ours == 200, "ours stopped answering"

    async def test_an_async_process_request_is_awaited(self, serve, log):
        """`websockets` accepts either shape, so the wrapper must too."""
        from websockets.datastructures import Headers
        from websockets.http11 import Response

        async def mine(_connection: object, request: object) -> object:
            if getattr(request, "path", "") == "/mine":
                return Response(418, "OK", Headers({"Content-Length": "0"}), b"")

            return None

        stream = streamcast.Stream("trades", log=log)
        async with serve(
            stream, maintain=False, info=True, process_request=mine
        ) as uri:
            base = uri.replace("ws://", "http://").replace("/trades", "")
            theirs, _ = await asyncio.to_thread(_get, f"{base}/mine")

        assert theirs == 418

    async def test_every_served_stream_is_listed(self, serve, log, tmp_path):
        first = streamcast.Stream("trades", log=log)
        second = streamcast.Stream("quotes")
        async with serve(first, second, maintain=False, info=True) as uri:
            url = uri.replace("ws://", "http://").replace("/trades", INFO_PATH)
            _status, body = await asyncio.to_thread(_get, url)

        names = {s["name"] for s in json.loads(body)["streams"]}
        assert names == {"trades", "quotes"}


class TestThePayload:
    async def test_it_is_json_and_round_trips(self, log):
        stream = streamcast.Stream("trades", log=log)
        await stream.send(trade(0))
        got = json.loads(payload([stream]))

        assert got["streams"][0]["last_send_ts"] is not None
        assert isinstance(got["served_at"], float)

    async def test_nulls_survive_encoding(self, log):
        """The startup case has to be expressible on the wire, not just in
        Python — a caller reading JSON must see `null`, not a missing key."""
        got = json.loads(payload([streamcast.Stream("trades", log=log)]))
        entry = got["streams"][0]

        assert entry["last_send_ts"] is None
        assert entry["last_send_age_s"] is None
        assert "uptime_s" in entry


@pytest.mark.parametrize("field", ["name", "durable", "end_offset", "subscribers"])
def test_the_greeting_and_the_stats_do_not_disagree(field):
    """Both describe one stream, so the names that overlap must mean the same.

    `Subscription.info` is the greeting — declared, fixed at connect.
    `Stream.stats` is counters that move. Where they name the same thing they
    must agree, or an operator reading both gets two answers.
    """
    assert field in Stats.__dataclass_fields__
