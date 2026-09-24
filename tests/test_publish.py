"""Remote publishing: a producer that is not the server's own process.

The consumer half could always live anywhere. This is the other half, and the
property that makes it safe is that it adds no authority: the server makes the
same `Stream.send` / `send_many` calls a local publisher makes, on the same
handle, in the same process. litelink allows one writer per log and that
writer is still the server — which is exactly what a second `WriteHandle` on
another box is not, and what litelink can neither refuse nor detect.
"""

from __future__ import annotations

import asyncio

import pytest

import streamcast

from .conftest import SCHEMA, trade


class TestItPublishes:
    async def test_a_remote_publisher_appends_and_subscribers_see_it(self, serve, log):
        """The headline, end to end: publish over a socket, receive over another."""
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.connect(uri) as sub:
                async with streamcast.publish(uri) as producer:
                    offset = await producer.send(trade(0))
                    delivered, row = await sub.recv()

        assert offset == 1
        assert delivered == offset, "the publisher and the subscriber agree"
        assert row == trade(0), "and the row is untouched"

    async def test_send_many_is_one_transaction(self, serve, log):
        """Contiguous offsets, because it is the same call `Stream.send_many` is."""
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                offsets = await producer.send_many([trade(i) for i in range(20)])

        assert offsets == list(range(1, 21))

    async def test_an_empty_batch_sends_nothing(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                assert await producer.send_many([]) == []

            assert stream.end_offset == 1, "nothing was appended"

    async def test_the_greeting_tells_a_publisher_the_schema(self, serve, log):
        """Which is what a publisher needs before it sends anything.

        A subscriber uses it to decode; a publisher uses it to check the shape
        it is about to send against the shape the server will accept.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
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

        `Stream.send` contains no `await`, so two handlers calling it cannot
        interleave: each assigns its offset, appends and fans out in one step
        against the event loop. Nothing here coordinates the two publishers,
        and nothing needs to.

        Falsify by putting an `await` inside `Stream.send` between the append
        and the fan-out — `tests/test_invariants.py` guards that directly, and
        this is the behaviour that guard protects.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:

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
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:

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
    async def test_publishing_is_off_unless_the_server_allows_it(self, serve, log):
        """A server must not become writable on an upgrade.

        Every other refusal is about what a caller asked for. This one is
        about what the operator allowed, so it says so rather than pretending
        the stream is missing.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, maintain=False) as uri:  # no publish=True
            with pytest.raises(streamcast.ProtocolError) as raised:
                await streamcast.publish(uri)

            # The wire carries `publish_disabled`; the sentence is built by
            # the client, and names the setting to change.
            assert "does not accept publishers" in str(raised.value)
            assert "publish=True" in str(raised.value)

            # And the stream still serves readers, which is the point of the
            # refusal being narrow.
            async with streamcast.connect(uri) as sub:
                await stream.send(trade(0))
                assert (await sub.recv())[0] == 1

    async def test_a_row_the_schema_refuses_is_rejected_not_committed(self, serve, log):
        """litelink's own message, and the connection survives it.

        `Stream.send` raises on a bad row and the local publisher sends the
        next one. A remote publisher is no worse off — closing the connection
        would make one bad row cost every good one behind it.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                with pytest.raises(streamcast.Rejected) as raised:
                    await producer.send({"event_ts": "not a number", "price": 1.0})

                # The column, from litelink, not a summary of it.
                assert "event_ts" in str(raised.value)

                # Nothing was committed, and the next row works.
                assert await producer.send(trade(0)) == 1

    async def test_a_rejected_batch_commits_none_of_itself(self, serve, log):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
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
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri) as producer:
                await producer.connection.send('"just a string"')
                with pytest.raises(streamcast.Rejected, match="not a row"):
                    from streamcast._protocol import parse_publish_reply

                    parse_publish_reply(await producer.connection.recv())

    async def test_publishing_to_a_stream_that_is_not_served_is_refused(
        self, serve, log
    ):
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            wrong = uri.replace("/trades", "/quotes")
            with pytest.raises(streamcast.StreamNotFound):
                await streamcast.publish(wrong)


class TestTheProducerCursor:
    async def test_it_records_the_last_acknowledged_offset(self, serve, log, tmp_path):
        """After the ack, never before.

        A producer cursor that LAGS makes the recovery window larger — more
        to replay, still correct. One that LEADS names a row that may never
        have landed, so the window misses it and the publisher resends what
        it already wrote. Same asymmetry as a consumer cursor, mirrored.
        """
        cursor = tmp_path / "producer.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri, cursor=cursor) as producer:
                assert producer.resumed_from is None, "nothing to resume yet"
                for i in range(5):
                    await producer.send(trade(i))

        # The clean exit settled it — no explicit commit needed.
        assert cursor.read_text().strip() == "5"

    async def test_a_restart_resumes_from_what_it_was_acked_for(
        self, serve, log, tmp_path
    ):
        """The whole point: a new process knows where it got to."""
        cursor = tmp_path / "producer.offset"
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream, publish=True, maintain=False) as uri:
            async with streamcast.publish(uri, cursor=cursor) as first:
                for i in range(5):
                    await first.send(trade(i))

            # A different process, same cursor file.
            async with streamcast.publish(uri, cursor=cursor) as second:
                assert second.resumed_from == 5

    async def test_the_recovery_replay_recovers_the_sequence(self, serve, tmp_path):
        """One integer on disk is enough, which is why `Cursor` is reused.

        Scanning INCLUSIVE of the acknowledged offset makes the first row
        delivered this publisher's own last one — so the sequence it carried
        comes back out of the log and only the offset has to be persisted.
        `docs/SPEC.md` §6b.
        """
        import litelink

        keyed = {
            "type": "object",
            "properties": {
                "publisher": {"type": "string"},
                "seq": {"type": "integer"},
            },
            "required": ["publisher", "seq"],
        }
        handle = litelink.new(
            tmp_path / "data", "trades", schema=streamcast.to_arrow(keyed)
        )
        with handle:
            stream = streamcast.Stream("trades", log=handle)
            cursor = tmp_path / "producer.offset"
            async with serve(stream, publish=True, maintain=False) as uri:
                async with streamcast.publish(uri, cursor=cursor) as producer:
                    for seq in range(5):
                        await producer.send({"publisher": "a", "seq": seq})

                    # Someone else writes, and then we lose the ack for ours.
                    await producer.send({"publisher": "b", "seq": 0})
                    # Settle at OUR last row, not at the one after it: the
                    # cursor names what this publisher was acked for.
                    producer.commit(5)

                async with streamcast.publish(uri, cursor=cursor) as revived:
                    start = revived.resumed_from
                    assert start == 5

                # The replay, exactly as SPEC §6b documents it.
                landed = None
                async with streamcast.connect(uri, offset=start) as sub:
                    lo, hi = sub.info.replay or (0, 0)
                    for _ in range(hi - lo):
                        _offset, row = await sub.recv()
                        if row["publisher"] == "a":
                            landed = max(landed or -1, int(row["seq"]))  # ty: ignore[invalid-argument-type]

            assert landed == 4, "the sequence came back out of the log"

    async def test_cursor_uri_needs_a_cursor(self):
        """The same pairing rule `connect` has, for the same reason."""
        with pytest.raises(ValueError, match="needs a cursor="):
            streamcast.publish(
                "ws://127.0.0.1:1/t", cursor_uri="s3://b/p/cursor.offset"
            )


class TestTheUrl:
    async def test_one_address_serves_both_ends(self, serve, log):
        """`publish(uri)` and `connect(uri)` take the same string.

        `?publish` is added by the client rather than asked of the caller, so
        neither end has to know the query string and a URI in a config file
        works for both.
        """
        from streamcast._protocol import publish_path

        assert publish_path("ws://h/trades") == "ws://h/trades?publish"
        # Idempotent, so a caller that spelled it out is not punished.
        assert publish_path("ws://h/trades?publish") == "ws://h/trades?publish"

    async def test_publish_and_offset_together_are_refused(self):
        """Two different requests. Serving one silently would be a guess."""
        from streamcast._protocol import parse_subscribe

        with pytest.raises(ValueError, match="different requests"):
            parse_subscribe("/trades?publish&offset=5")
