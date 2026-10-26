"""Remote publishing: a producer that is not the server's own process.

The consumer half could always live anywhere. This is the other half, and the
property that makes it safe is that it adds no authority: the server makes the
same `StreamHandle.send` / `send_many` calls a local publisher makes, on the same
handle, in the same process. litelink allows one writer per log and that
writer is still the server — which is exactly what a second `WriteHandle` on
another box is not, and what litelink can neither refuse nor detect.
"""

from __future__ import annotations

import asyncio

import pytest
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from websockets.frames import Close
from websockets.frames import Close as CloseFrame

import streamcast
from streamcast._client import _refusal
from streamcast._protocol import (
    greeting,
    parse_greeting,
    parse_publish_reply,
    parse_subscribe,
    publish_path,
    refusal,
)

from .conftest import SCHEMA, trade


class TestItPublishes:
    async def test_a_remote_publisher_appends_and_subscribers_see_it(self, serve, log):
        """The headline, end to end: publish over a socket, receive over another."""
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                async with streamcast.publish(uri) as producer:
                    offset = await producer.send(trade(0))
                    delivered, ts, row = await sub.recv()

        assert offset == 1
        assert ts is None, "`log` is unstamped"
        assert delivered == offset, "the publisher and the subscriber agree"
        assert row == trade(0), "and the row is untouched"

    async def test_send_many_is_one_transaction(self, serve, log):
        """Contiguous offsets, because it is the same call `StreamHandle.send_many` is."""
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                offsets = await producer.send_many([trade(i) for i in range(20)])

        assert offsets == list(range(1, 21))

    async def test_an_empty_batch_sends_nothing(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                assert await producer.send_many([]) == []

            assert stream.end_offset == 1, "nothing was appended"

    async def test_the_greeting_tells_a_publisher_the_schema(self, serve, log):
        """Which is what a publisher needs before it sends anything.

        A subscriber uses it to decode; a publisher uses it to check the shape
        it is about to send against the shape the server will accept.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                shape = producer.info.schema
                assert isinstance(shape, dict)
                properties = shape["properties"]
                assert isinstance(properties, dict)
                assert set(properties) == set(SCHEMA.names)
                assert producer.info.durable is True


class TestConcurrentPublishers:
    async def test_two_publishers_never_share_an_offset(self, serve, log):
        """**I1 is what makes this free.**

        `StreamHandle.send` contains no `await`, so two handlers calling it cannot
        interleave: each assigns its offset, appends and fans out in one step
        against the event loop. Nothing here coordinates the two publishers,
        and nothing needs to.

        Falsify by putting an `await` inside `StreamHandle.send` between the append
        and the fan-out — `tests/test_invariants.py` guards that directly, and
        this is the behaviour that guard protects.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:

            async def publisher(tag: int, count: int) -> list[int | None]:
                async with streamcast.publish(uri) as producer:
                    return [
                        await producer.send(trade(tag * 1000 + i)) for i in range(count)
                    ]

            first, second = await asyncio.gather(publisher(1, 30), publisher(2, 30))

        issued = sorted(o for o in first + second if o is not None)
        assert len(issued) == 60
        assert len(set(issued)) == 60, "an offset was issued twice"
        assert issued == list(range(1, 61)), "and they are contiguous"

    async def test_a_batch_stays_contiguous_under_a_racing_publisher(self, serve, log):
        """One transaction means one transaction, even mid-race.

        A batch that interleaved with another publisher's rows would still be
        durable, but it would stop being one commit — and a consumer reading
        the log could see half a group.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:

            async def batches() -> list[list[int | None]]:
                out = []
                async with streamcast.publish(uri) as producer:
                    for group in range(4):
                        out.append(
                            await producer.send_many(
                                [trade(group * 10 + i) for i in range(10)]
                            )
                        )

                return out

            async def singles() -> None:
                async with streamcast.publish(uri) as producer:
                    for i in range(40):
                        await producer.send(trade(9000 + i))

            grouped, _ = await asyncio.gather(batches(), singles())

        for group in grouped:
            assert group[0] is not None
            assert group == list(range(group[0], group[0] + 10)), (
                f"a batch was split by another publisher: {group}"
            )


class TestWhatItRefuses:
    async def test_every_served_stream_takes_publishers(self, serve, log):
        """No switch on `BrokerConfig`: whether a stream can be written is the
        stream's to say — a retired one refuses publishers (`test_retire`) —
        and the same port serves its readers."""
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                async with streamcast.publish(uri) as producer:
                    assert await producer.send(trade(0)) == 1

                assert (await sub.recv())[0] == 1

    def test_an_older_servers_refusal_still_names_the_setting(self):
        """A server from before every stream took publishers sends
        `publish_disabled`; the client still says what to change."""

        closed = ConnectionClosed(
            CloseFrame(streamcast.Close.BAD_REQUEST, refusal("publish_disabled")), None
        )
        error = _refusal(closed, stream="trades", offset=None)
        assert isinstance(error, streamcast.ProtocolError)
        assert "publish=True" in str(error)

    async def test_a_row_the_schema_refuses_is_rejected_not_committed(self, serve, log):
        """litelink's own message, and the connection survives it.

        `StreamHandle.send` raises on a bad row and the local publisher sends the
        next one. A remote publisher is no worse off — closing the connection
        would make one bad row cost every good one behind it.
        """
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                with pytest.raises(streamcast.Rejected) as raised:
                    await producer.send({"event_ts": "not a number", "price": 1.0})

                # The column, from litelink, not a summary of it.
                assert "event_ts" in str(raised.value)

                # Nothing was committed, and the next row works.
                assert await producer.send(trade(0)) == 1

    async def test_a_rejected_batch_commits_none_of_itself(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                rows: list[dict[str, object]] = [
                    dict(trade(0)),
                    {"event_ts": "bad", "price": 1.0},
                    dict(trade(2)),
                ]
                with pytest.raises(streamcast.Rejected):
                    await producer.send_many(rows)

                assert stream.end_offset == 1, "a partial batch was committed"

    async def test_a_frame_that_is_not_a_row_is_refused(self, serve, log):
        """Answered rather than closed, like any other bad publish."""
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                await producer.connection.send('"just a string"')
                with pytest.raises(streamcast.Rejected, match="not a row"):
                    parse_publish_reply(await producer.connection.recv())

    async def test_publishing_to_a_stream_that_is_not_served_is_refused(
        self, serve, log
    ):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            wrong = uri.replace("/trades", "/quotes")
            with pytest.raises(streamcast.StreamNotFound):
                await streamcast.publish(wrong)


class TestTheProducerCursorIsGone:
    @pytest.mark.parametrize(
        "keyword",
        [
            {"cursor": "producer.offset"},
            {"cursor_uri": "s3://b/p/cursor.offset"},
            {"upload_every": 1.0},
        ],
    )
    def test_its_keywords_are_refused(self, keyword):
        """Removed in 0.23.0: exactly-once publishing (`publisher=`) replaces
        the read-back it existed for. Every other keyword goes to
        `websockets.connect`, which refuses these when the connection opens,
        so the opener is driven to that point."""
        with pytest.raises(TypeError):
            asyncio.run(streamcast.publish("ws://127.0.0.1:1/t", **keyword)._open())

    def test_the_publication_has_no_cursor_api(self):
        assert not hasattr(streamcast.Publication, "resumed_from")
        assert not hasattr(streamcast.Publication, "commit")


class TestTheUrl:
    async def test_one_address_serves_both_ends(self, serve, log):
        """`publish(uri)` and `connect(uri)` take the same string.

        `?publish` is added by the client rather than asked of the caller, so
        neither end has to know the query string and a URI in a config file
        works for both.
        """

        assert publish_path("ws://h/trades") == "ws://h/trades?publish"
        # Idempotent, so a caller that spelled it out is not punished.
        assert publish_path("ws://h/trades?publish") == "ws://h/trades?publish"

    async def test_publish_and_offset_together_are_refused(self):
        """Two different requests. Serving one silently would be a guess."""

        with pytest.raises(ValueError, match="different requests"):
            parse_subscribe("/trades?publish&offset=5")


class TestAConnectionClosingUnderASend:
    async def test_the_refusal_is_raised_even_if_the_reader_got_there_first(self):
        """The reader fails and removes every pending reply when the
        connection closes; a write that raises afterwards must not find its
        own entry gone and raise `IndexError` in place of the refusal.

        Falsify by popping the pending deque instead of removing by identity.
        """

        info = parse_greeting(
            greeting(stream="t", end_offset=1, replay=None, durable=True)
        )

        class Closing:
            async def send(self, frame, text=False):
                publication._pending.clear()  # the reader, first  # noqa: SLF001
                raise ConnectionClosedError(Close(4409, '{"error":"superseded"}'), None)

        publication = streamcast.Publication(Closing(), info, "t")  # ty: ignore[invalid-argument-type]
        with pytest.raises(streamcast.Superseded):
            await publication.send(trade(0))
