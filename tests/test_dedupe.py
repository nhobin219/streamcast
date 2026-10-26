"""Exactly-once publishing: the broker dedupes each publisher on its seq.

A publisher that names itself numbers its rows 1, 2, 3, … with no holes. The
stream's writer keeps each publisher's mark -- the last seq appended and the
offset it got -- in `streamcast_publishers`, a table in the log's buffer,
committed in the same transaction as the rows (`_dedupe`, `_writer`). Most of
this is tested on the `StreamHandle` with no socket, as the rest of the suite is:
the guarantees live there, and the socket only carries them.
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING

import litelink
import pyarrow as pa
import pytest
import websockets

import streamcast
from streamcast import _dedupe, _versions, _writer
from streamcast._protocol import (
    PUBLISH,
    Publish,
    decode,
    decode_publish_seq,
    parse_publish_reply,
    parse_subscribe,
    publish_ack,
    publish_path,
)

from .conftest import SCHEMA, trade

if TYPE_CHECKING:
    from litelink import WriteHandle


def marks(log: WriteHandle) -> dict[str, tuple[int, int]]:
    """The table as committed: `{publisher: (last_seq, last_offset)}`."""
    return {p: (s, o) for p, s, o in _dedupe.read_all(log)}


async def claimed(
    stream: streamcast.StreamHandle, publisher: str, seq: int, rows: list
):
    """One exactly-once frame through the stream, as `serve_publisher` queues
    it: `(offsets, dup)`."""
    job = stream._submit_claimed(list(rows), publisher, seq)  # noqa: SLF001
    offsets = await job.done
    return offsets, job.skip


class TestTheWriterJudgesEachFrame:
    async def test_contiguous_seqs_append_and_the_mark_commits_with_them(self, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        assert await claimed(stream, "a", 1, [trade(0)]) == ([1], 0)
        assert await claimed(stream, "a", 2, [trade(1), trade(2)]) == ([2, 3], 0)
        await stream.aclose()

        assert log.end_offset() == 4
        assert marks(log) == {"a": (3, 3)}

    async def test_a_duplicate_is_answered_with_its_original_offset(self, log):
        """Single rows, a group, and a resend that regroups rows."""
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await claimed(stream, "a", 1, [trade(i) for i in range(3)])  # offsets 1..3
        await stream.send(trade(9))  # a plain publisher's row between: offset 4
        await claimed(stream, "a", 4, [trade(3), trade(4)])  # offsets 5, 6

        assert await claimed(stream, "a", 2, [trade(1)]) == ([2], 1)
        assert await claimed(stream, "a", 3, [trade(2), trade(3)]) == ([3, 5], 2), (
            "a regrouped resend spanning two commits is found row by row"
        )
        await stream.aclose()

        assert log.end_offset() == 7, "nothing was appended for a duplicate"

    async def test_a_group_straddling_the_mark_appends_only_its_suffix(
        self, log, monkeypatch
    ):

        received: list[tuple[int | None, dict]] = []
        fan_out = streamcast.StreamHandle._fan_out  # noqa: SLF001

        def capture(self, row, frame):
            offset, _ts, msg = decode(frame)
            received.append((offset, msg))
            fan_out(self, row, frame)

        monkeypatch.setattr(streamcast.StreamHandle, "_fan_out", capture)
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await claimed(stream, "a", 1, [trade(0), trade(1)])
        offsets, dup = await claimed(stream, "a", 2, [trade(1), trade(2), trade(3)])

        assert (offsets, dup) == ([2, 3, 4], 1)
        assert received == [(i + 1, {"tag": None, **trade(i)}) for i in range(4)], (
            "the duplicate row was not broadcast again"
        )
        await stream.aclose()

    async def test_a_duplicate_within_one_commit_gets_the_offset_it_landed_at(
        self, log, monkeypatch
    ):
        """Two jobs of one publisher grouped into one commit, the second
        repeating the first: answered with the offset the first got in that
        same commit, not `None`. Forced into one group by holding the writer
        until both are queued.

        Falsify by dropping the second pass of `Publishers.settle`.
        """

        gate = threading.Event()
        real = _writer.Writer._commit_claimed  # noqa: SLF001

        def held(self, loop, jobs, publishers):
            gate.wait()
            return real(self, loop, jobs, publishers)

        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await claimed(stream, "a", 1, [trade(0)])
        monkeypatch.setattr(_writer.Writer, "_commit_claimed", held)
        plug = stream._submit_claimed([trade(1)], "a", 2)  # noqa: SLF001
        first = stream._submit_claimed([trade(2)], "a", 3)  # noqa: SLF001
        repeat = stream._submit_claimed([trade(2)], "a", 3)  # noqa: SLF001
        gate.set()
        assert await plug.done == [2]
        assert await first.done == [3]
        assert (await repeat.done, repeat.skip) == ([3], 1)
        await stream.aclose()

    async def test_a_gap_is_refused_with_nothing_appended(self, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await claimed(stream, "a", 1, [trade(0)])
        with pytest.raises(streamcast.SequenceGap) as gap:
            await claimed(stream, "a", 3, [trade(2)])

        assert (gap.value.expected, gap.value.got) == (2, 3)
        assert not isinstance(gap.value, streamcast.Rejected), (
            "a sender that drops rejected rows must not drop a gap's"
        )
        assert await claimed(stream, "a", 2, [trade(1)]) == ([2], 0)
        await stream.aclose()

        assert log.end_offset() == 3

    async def test_publishers_are_judged_independently(self, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await claimed(stream, "a", 1, [trade(0)])
        await claimed(stream, "b", 1, [trade(1)])
        await stream.send(trade(2))
        await claimed(stream, "a", 2, [trade(3)])
        await stream.aclose()

        assert marks(log) == {"a": (2, 4), "b": (1, 2)}

    async def test_a_failed_commit_moves_no_mark_and_commits_no_rows(
        self, log, monkeypatch
    ):
        """The rows and the mark are one transaction, and the jobs queued behind
        a failed one from the same publisher are refused as gaps rather than
        committed out of order.

        Falsify by writing the mark outside the transaction, or by moving the
        cache before the commit returns.
        """
        real = _dedupe.Publishers.write
        calls = 0

        def failing(tx, jobs, offsets):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("disk full")

            return real(tx, jobs, offsets)

        monkeypatch.setattr(_dedupe.Publishers, "write", staticmethod(failing))
        stream = streamcast.StreamHandle(
            "trades", log=log, exactly_once=True, group_commit=False
        )
        first = stream._submit_claimed([trade(0)], "a", 1)  # noqa: SLF001
        second = stream._submit_claimed([trade(1)], "a", 2)  # noqa: SLF001
        with pytest.raises(OSError, match="disk full"):
            await first.done

        with pytest.raises(streamcast.SequenceGap):
            await second.done

        assert await claimed(stream, "a", 1, [trade(0)]) == ([1], 0), (
            "the failed job's rows never landed: offset 1 is issued again"
        )
        await stream.aclose()

        assert log.end_offset() == 2
        assert marks(log) == {"a": (1, 1)}


class TestTheMarkIsDurable:
    async def test_it_survives_a_restart_and_answers_at_the_mark(self, tmp_path):
        """After a restart only the table is left: a duplicate AT the mark gets
        its offset back, an older one gets `None` (the ranges were memory)."""
        root = tmp_path / "data"
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(
                    name="trades",
                    schema=streamcast.from_arrow(SCHEMA),
                    exactly_once=True,
                ),
                storage=streamcast.Storage(root=root.absolute()),
            ),
            initialization="create_if_absent",
        )
        await claimed(stream, "a", 1, [trade(0), trade(1), trade(2)])
        await stream.aclose()

        reopened = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(
                    name="trades",
                    schema=streamcast.from_arrow(SCHEMA),
                    exactly_once=True,
                ),
                storage=streamcast.Storage(root=root.absolute()),
            ),
            initialization="create_if_absent",
        )
        writer = reopened._writer  # noqa: SLF001
        assert writer is not None
        assert await writer.probe("a") == (3, 3)
        assert await claimed(reopened, "a", 2, [trade(1), trade(2)]) == ([None, 3], 2)
        assert await claimed(reopened, "a", 4, [trade(3)]) == ([4], 0)
        await reopened.aclose()

    async def test_an_evicted_publisher_is_read_back_from_the_table(self, log):
        stream = streamcast.StreamHandle(
            "trades", log=log, exactly_once=True, max_publishers=1
        )
        await claimed(stream, "a", 1, [trade(0)])
        await claimed(stream, "b", 1, [trade(1)])  # evicts a
        await claimed(stream, "a", 2, [trade(2)])  # evicts b, reads a back
        assert await claimed(stream, "b", 2, [trade(3)]) == ([4], 0)
        with pytest.raises(streamcast.SequenceGap):
            await claimed(stream, "a", 4, [trade(4)])

        writer = stream._writer  # noqa: SLF001
        assert writer is not None
        publishers = writer._publishers  # noqa: SLF001
        assert publishers is not None
        assert len(publishers) == 1
        await stream.aclose()

    async def test_a_stream_without_exactly_once_publishers_gets_no_table(self, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        await stream.send(trade(0))
        await stream.aclose()

        assert _dedupe.read_all(log) == []
        with log.transaction() as tx:
            assert not tx.execute(
                "SELECT 1 FROM sqlite_master WHERE name = ?", (_dedupe.TABLE,)
            )

    async def test_migrate_carries_the_marks_to_the_new_log(self, tmp_path):
        root = tmp_path / "data"
        schema = streamcast.from_arrow(SCHEMA)
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(
                    name="trades", schema=schema, exactly_once=True
                ),
                storage=streamcast.Storage(root=root.absolute()),
            ),
            initialization="create_if_absent",
        )
        await claimed(stream, "a", 1, [trade(0), trade(1)])
        await stream.aclose()

        wider = streamcast.from_arrow(SCHEMA.append(pa.field("venue", pa.string())))
        migrated = streamcast.StreamHandle.migrate(
            "trades", root=root, schema=wider, exactly_once=True
        )
        log = migrated.log
        assert log is not None and log.name != "trades"
        assert marks(log) == {"a": (2, 2)}
        assert await claimed(migrated, "a", 3, [trade(2)]) == ([3], 0)
        await migrated.aclose()
        assert not _dedupe.carried(migrated_home(root), "trades").exists()

    async def test_a_migration_that_died_after_the_retire_still_carries_them(
        self, tmp_path, monkeypatch
    ):
        """The retired log reopens read-only, with no transaction to read the
        table through; the copy beside the metadata is what carries them."""

        root = tmp_path / "data"
        schema = streamcast.from_arrow(SCHEMA)
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(
                    name="trades", schema=schema, exactly_once=True
                ),
                storage=streamcast.Storage(root=root.absolute()),
            ),
            initialization="create_if_absent",
        )
        await claimed(stream, "a", 1, [trade(0)])
        await stream.aclose()

        wider = streamcast.from_arrow(SCHEMA.append(pa.field("venue", pa.string())))
        real = _versions.commit

        def dying(*args, **kwargs):
            raise SystemExit("killed before the metadata commit")

        monkeypatch.setattr(_versions, "commit", dying)
        with pytest.raises(SystemExit):
            streamcast.StreamHandle.migrate("trades", root=root, schema=wider)

        monkeypatch.setattr(_versions, "commit", real)
        migrated = streamcast.StreamHandle.migrate(
            "trades", root=root, schema=wider, exactly_once=True
        )
        log = migrated.log
        assert log is not None
        assert marks(log) == {"a": (1, 1)}
        await migrated.aclose()


class TestTheCarriedFile:
    def test_marks_read_from_another_log_are_never_applied(self, tmp_path):
        """A file a crash left behind names the log it was read from, so a
        later migration of a different log cannot take it for its own.

        Falsify by ignoring the log name in `unstash`.
        """
        path = tmp_path / "trades.publishers.json"
        _dedupe.stash(path, "trades", [("a", 3, 7)])
        assert _dedupe.unstash(path, "trades") == [("a", 3, 7)]
        assert _dedupe.unstash(path, "trades-v2") is None
        assert _dedupe.unstash(tmp_path / "missing.json", "trades") is None


def migrated_home(root):
    from streamcast import _metadata

    return _metadata.home(root, "trades")


class TestOverTheWire:
    async def test_a_publisher_resumes_from_the_greeting(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri, publisher="a") as first:
                assert first.info.publisher is not None
                assert first.info.publisher.last_seq == 0
                assert first.info.publisher.last_offset is None
                assert await first.send_many([trade(0), trade(1)], seq=1) == [1, 2]
                assert await first.send(trade(2), seq=3) == 3

            async with streamcast.publish(uri, publisher="a") as second:
                mark = second.info.publisher
                assert mark is not None
                assert (mark.id, mark.last_seq, mark.last_offset) == ("a", 3, 3)
                assert await second.send(trade(2), seq=3) == 3, "a resend: not appended"
                with pytest.raises(streamcast.SequenceGap):
                    await second.send(trade(5), seq=5)

                assert await second.send(trade(4), seq=4) == 4, (
                    "the connection survives"
                )

        assert log.end_offset() == 5

    async def test_seq_is_required_exactly_when_the_publication_is_exactly_once(
        self, serve, log
    ):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            async with streamcast.publish(uri, publisher="a") as producer:
                with pytest.raises(ValueError, match="needs seq="):
                    await producer.send(trade(0))

            async with streamcast.publish(uri) as plain:
                with pytest.raises(ValueError, match="exactly-once publication"):
                    await plain.send(trade(0), seq=1)

                assert await plain.send(trade(0)) == 1, "plain publishers still work"

    async def test_a_stream_not_served_exactly_once_refuses_the_publisher(
        self, serve, log
    ):
        stream = streamcast.StreamHandle("trades", log=log)
        async with serve(stream, maintain=False) as uri:
            with pytest.raises(streamcast.ExactlyOnceUnavailable) as refused:
                await streamcast.publish(uri, publisher="a")

        assert refused.value.why == "stream"
        assert log.end_offset() == 1

    async def test_a_server_from_before_the_feature_is_reported_as_such(self):
        """A 0.22 broker refuses the unknown `publisher` parameter at the open,
        with 4400, before anything is appended. Simulated here with its exact
        refusal, which `parse_subscribe` still builds for any unknown key."""

        async def old(connection):
            await connection.close(
                4400,
                '{"error":"bad_request","detail":"unknown query parameter(s): publisher"}',
            )

        async with websockets.serve(old, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(streamcast.ExactlyOnceUnavailable) as refused:
                await streamcast.publish(f"ws://127.0.0.1:{port}/trades", publisher="a")

        assert refused.value.why == "broker"

    async def test_a_probe_that_raises_leaves_no_claim(self, serve, log, monkeypatch):

        def broken(self, publisher):
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(_writer.Writer, "probe", broken)
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            with pytest.raises(Exception):  # noqa: B017, PT011 — the server's 1011
                await streamcast.publish(uri, publisher="a")

        assert stream._claims == {}  # noqa: SLF001

    async def test_a_newer_connection_supersedes_an_older_one(self, serve, log):
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            first = await streamcast.publish(uri, publisher="a")
            await first.send(trade(0), seq=1)
            async with streamcast.publish(uri, publisher="a") as second:
                assert second.info.publisher is not None
                assert second.info.publisher.last_seq == 1
                with pytest.raises(streamcast.Superseded):
                    for seq in range(2, 100):  # until the close arrives
                        await first.send(trade(seq), seq=seq)
                        await asyncio.sleep(0.01)

            await first.close()

    async def test_an_older_connection_queues_nothing_after_a_newer_claim(
        self, serve, log, monkeypatch
    ):
        """Fencing, with the interleaving forced. The older connection's third
        frame is parked waiting for room when the newer connection claims the
        id and is greeted; released, it must queue nothing, so the newer
        connection's seq 3 is the row that lands at offset 3.

        Falsify by removing the claim check before `_submit_claimed`: the
        parked frame then lands at offset 3 and the newer row is answered as
        its duplicate.
        """
        parked, release = asyncio.Event(), asyncio.Event()
        room = streamcast.StreamHandle._room  # noqa: SLF001
        calls = 0

        async def hooked(self, rows):
            nonlocal calls
            calls += 1
            if calls == 3:  # the older connection's third frame
                parked.set()
                await release.wait()

            await room(self, rows)

        monkeypatch.setattr(streamcast.StreamHandle, "_room", hooked)
        stream = streamcast.StreamHandle("trades", log=log, exactly_once=True)
        async with serve(stream, maintain=False) as uri:
            first = await streamcast.publish(uri, publisher="a")
            await first.send(trade(1), seq=1)
            await first.send(trade(2), seq=2)
            third = await first.submit(trade(3), seq=3)
            await parked.wait()

            second = await streamcast.publish(uri, publisher="a")
            mark = second.info.publisher
            assert mark is not None
            assert mark.last_seq == 2
            release.set()

            with pytest.raises(streamcast.StreamcastError):
                await third

            assert await second.send(trade(30), seq=3) == 3
            await second.close()
            await first.close()

        await stream.aclose()
        table = log.sql(
            'SELECT "litelink_offset", price FROM log ORDER BY 1'
        ).read_all()
        assert table.to_pylist() == [
            {"litelink_offset": 1, "price": trade(1)["price"]},
            {"litelink_offset": 2, "price": trade(2)["price"]},
            {"litelink_offset": 3, "price": trade(30)["price"]},
        ], "offset 3 is the newer connection's row; the parked one never queued"


class TestTheWire:
    def test_publisher_is_a_query_parameter_of_a_publish(self):
        assert parse_subscribe("/trades?publish") == ("trades", PUBLISH, None)
        name, requested, _ = parse_subscribe("/trades?publish&publisher=a%2Fb")
        assert name == "trades"
        assert isinstance(requested, Publish)
        assert requested.publisher == "a/b"
        with pytest.raises(ValueError, match="needs publish"):
            parse_subscribe("/trades?publisher=a")

        with pytest.raises(ValueError, match="1 to 128 bytes"):
            parse_subscribe("/trades?publish&publisher=" + "x" * 129)

    def test_the_client_adds_it_percent_encoded(self):
        assert publish_path("ws://h/trades") == "ws://h/trades?publish"
        assert (
            publish_path("ws://h/trades", "a/b c")
            == "ws://h/trades?publish&publisher=a%2Fb%20c"
        )
        with pytest.raises(ValueError, match="1 to 128 bytes"):
            streamcast.publish("ws://h/trades", publisher="")

    def test_a_frame_carries_its_seq(self):
        assert decode_publish_seq(b'[7, {"a": 1}]') == (7, {"a": 1})
        assert decode_publish_seq(b'[7, [{"a": 1}]]') == (7, [{"a": 1}])
        for bad in (b'{"a": 1}', b'[0, {"a": 1}]', b'[true, {"a": 1}]', b"[1, []]"):
            with pytest.raises(streamcast.ProtocolError):
                decode_publish_seq(bad)

    def test_a_plain_reply_is_unchanged_and_a_duplicate_says_so(self):
        assert publish_ack([1, 2]) == '{"ok":[1,2]}'
        assert publish_ack([1, None], dup=2) == '{"ok":[1,null],"dup":2}'
        with pytest.raises(streamcast.SequenceGap):
            parse_publish_reply('{"error":"gap","expected":2,"got":5}')

    def test_a_failed_commit_is_not_a_refusal(self):
        """`commit_failed` is the broker's storage: a sender that drops a
        `Rejected` row must not catch it, or the row is lost."""
        with pytest.raises(streamcast.CommitFailed, match="disk I/O") as caught:
            parse_publish_reply('{"error":"commit_failed","detail":"disk I/O error"}')

        assert not isinstance(caught.value, streamcast.Rejected)
        with pytest.raises(streamcast.Rejected, match="no column"):
            parse_publish_reply('{"error":"rejected","detail":"no column x"}')

    def test_exactly_once_needs_a_log(self):
        with pytest.raises(ValueError, match="needs a log"):
            streamcast.StreamHandle("trades", exactly_once=True)


@pytest.fixture
def log(tmp_path):
    """`conftest.log`, without the small seal size: nothing here seals."""
    handle = litelink.new(tmp_path / "data", "trades", schema=SCHEMA)
    with handle:
        yield handle
