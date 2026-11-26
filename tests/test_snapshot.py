"""`StreamHandle.snapshot`: a stream's history as of a fixed point, from any machine.

Written with a server's own calls — `StreamHandle.open`, `send`, `migrate` — and read
back with nothing but the metadata file's URI, as another machine would.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import resource
import shutil
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast import (
    LATEST,
    SnapshotUnavailable,
    _client,
    _manifest,
    _metadata,
    _protocol,
    _published,
    _snapshot,
    _versions,
)

V1: dict[str, Any] = {
    "type": "object",
    "properties": {"event_ts": {"type": "integer"}, "price": {"type": "number"}},
    "required": ["event_ts", "price"],
}
V2: dict[str, Any] = {
    "type": "object",
    "properties": {**V1["properties"], "venue": {"type": ["string", "null"]}},
    "required": ["event_ts", "price"],
}


def row(i: int, **extra: object) -> dict[str, object]:
    return {"event_ts": i, "price": 100.0 + i, **extra}


def publish(log: litelink.WriteHandle) -> None:
    """Everything sent so far, sealed and in the published table."""
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)


async def stream_of(
    root: Path,
    rows: int,
    *,
    published: str | None = None,
    s3_options: litelink.S3Options | None = None,
) -> streamcast.StreamHandle:
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="trades", schema=V1),
            storage=streamcast.Storage(
                root=Path(root).absolute(), published=published, s3_options=s3_options
            ),
        ),
        initialization="create_if_absent",
    )
    await stream.send_many([row(i) for i in range(rows)])
    return stream


async def served_once(stream: streamcast.StreamHandle, serve) -> None:
    """The broker writes the metadata file a reader resolves."""
    async with serve(stream, maintain=False):
        pass


def offsets(table) -> list[int]:
    return table.column("litelink_offset").to_pylist()


class TestOneLog:
    async def test_it_reads_everything_published_and_stops_there(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(5, 8)])  # not published
        await served_once(stream, serve)

        home = _metadata.home(tmp_path, "trades")
        uri = (home / _versions.directory("trades") / _versions.HINT).resolve().as_uri()
        async with await streamcast.StreamHandle.snapshot(uri) as snap:
            assert snap.end_offset == 6
            table = await snap.scan().read_all()

        assert offsets(table) == [1, 2, 3, 4, 5]
        assert table.column("price").to_pylist() == [100.0 + i for i in range(5)]

    async def test_as_of_an_offset_it_stops_at_that_offset(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        table = await streamcast.StreamHandle.scan(uri, as_of_offset=3).read_all()
        assert offsets(table) == [1, 2, 3]

    async def test_past_the_published_end_it_needs_the_broker(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 3)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        with pytest.raises(SnapshotUnavailable, match="broker="):
            await streamcast.StreamHandle.snapshot(uri, as_of_offset=10)


class TestTheBroker:
    async def test_latest_reads_the_tail_the_tables_do_not_hold(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 4)
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(4, 7)])  # only on the broker
        uri = stream.metadata_uri
        assert uri is not None

        async with serve(stream, maintain=False) as broker:
            async with await streamcast.StreamHandle.snapshot(
                uri, as_of_offset=LATEST, broker=broker
            ) as snap:
                assert snap.end_offset == 8
                table = await snap.scan().read_all()
                streamed = [(offset, ts) async for offset, ts, _row in snap.rows(1)]

        assert offsets(table) == list(range(1, 8))
        assert table.column("price").to_pylist() == [100.0 + i for i in range(7)]
        assert [offset for offset, _ts in streamed] == list(range(1, 8))
        # The tables and the broker's tail both carry the stamp.
        assert all(isinstance(ts, int) for _offset, ts in streamed)

    async def test_no_socket_when_the_tables_cover_it(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 4)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        # Nothing listens here: reaching for the broker would fail.
        table = await streamcast.StreamHandle.scan(
            uri, as_of_offset=2, broker="ws://127.0.0.1:9/trades"
        ).read_all()
        assert offsets(table) == [1, 2]

    @pytest.mark.parametrize("reduced", [False, True])
    async def test_a_gap_neither_side_holds_is_refused_with_both_numbers(
        self, tmp_path, serve, reduced
    ):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="trades", schema=V1, max_replay=2),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        await stream.send_many([row(i) for i in range(3)])
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(3, 20)])  # past max_replay
        uri = stream.metadata_uri
        assert uri is not None

        async with serve(stream, maintain=False) as broker:
            with pytest.raises(SnapshotUnavailable) as raised:
                await streamcast.StreamHandle.snapshot(
                    uri,
                    as_of_offset=LATEST,
                    broker=broker,
                    tail_columns=["event_ts"] if reduced else None,
                    tail_filters=[("event_ts", ">", 1000)] if reduced else (),
                    max_tail_bytes=0 if reduced else None,
                )

        message = str(raised.value)
        assert "published tables end at 3" in message
        assert "Neither holds" in message


class TestAMigratedStream:
    async def test_its_logs_read_as_one_table(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send_many([row(i, venue="x") for i in range(5, 8)])
        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        table = await streamcast.StreamHandle.scan(uri).read_all()

        # Dense across the seam, and the column the first log lacked is NULL there.
        assert offsets(table) == list(range(1, 9))
        assert table.column("venue").to_pylist() == [None] * 5 + ["x"] * 3

    async def test_a_filter_skips_a_sealed_log_without_opening_it(
        self, tmp_path, serve
    ):
        stream = await stream_of(tmp_path, 5)  # prices 100 .. 104
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send_many([row(i, venue="x") for i in range(500, 503)])
        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        # The first log's table, gone: reading it would fail.
        with litelink.open(
            _metadata.home(tmp_path, "trades"), "trades", read_only=True
        ) as old:
            shutil.rmtree(_published_dir(old.published, "trades"))

        async with await streamcast.StreamHandle.snapshot(uri) as snap:
            table = await snap.scan(filters=[("price", ">", 500.0)]).read_all()
            assert offsets(table) == [6, 7, 8]

            with pytest.raises(Exception, match="version-hint|No files found|IO Error"):
                await snap.scan().read_all()  # unfiltered: it is opened, and it is gone

    async def test_as_of_a_time_it_reads_what_was_stamped_by_then(
        self, tmp_path, serve
    ):
        # One send each, a millisecond apart: `send_many` stamps its whole
        # group with one `streamcast_ts`, which would leave nothing to divide.
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="trades", schema=V1),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        for i in range(5):
            await stream.send(row(i))
            await asyncio.sleep(0.001)

        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        everything = await streamcast.StreamHandle.scan(uri).read_all()
        stamps = everything.column("streamcast_ts").to_pylist()
        middle = sorted(stamps)[2]

        assert len(set(stamps)) == 5, "the stamps must differ for this to test anything"

        table = await streamcast.StreamHandle.scan(uri, as_of_ts=middle).read_all()
        assert all(ts <= middle for ts in table.column("streamcast_ts").to_pylist())
        assert table.num_rows == 3

        with pytest.raises(SnapshotUnavailable, match="published rows up to"):
            await streamcast.StreamHandle.snapshot(uri, as_of_ts=max(stamps) + 1)

    async def test_a_retired_log_published_short_is_refused(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 5)
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V2)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)
        # The manifest says the first log held one row more than its table does.
        home = _metadata.home(tmp_path, "trades")
        metadata = _metadata.load(home, "trades")
        manifest = _manifest.load(home, "trades")
        assert metadata is not None
        assert manifest is not None
        index = manifest.schema.get_field_index("record_count")
        counts = [count + 1 for count in manifest.column(index).to_pylist()]
        _versions.commit(
            home,
            metadata,
            manifest=manifest.set_column(
                index, "record_count", pa.array(counts, pa.int64())
            ),
        )

        with pytest.raises(SnapshotUnavailable, match="holds 5 of the 6 rows"):
            await streamcast.StreamHandle.scan(uri).read_all()


class TestConditionsAcrossTheSeam:
    """`where`, `filters` and the snapshot's own limits over logs that differ.

    Each log is read on its own and the results unioned, so a condition that
    names a column only the later log has must not be bound inside the
    earlier one, where it does not exist.
    """

    async def migrated(self, tmp_path, serve) -> str:
        stream = await stream_of(tmp_path, 3)  # prices 100 .. 102, no venue
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send_many(
            [row(500, venue="x"), row(501, venue="y"), row(502, venue="x")]
        )
        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)
        return uri

    async def test_a_column_the_earlier_log_lacks_reads_as_null(self, tmp_path, serve):
        uri = await self.migrated(tmp_path, serve)

        where = await streamcast.StreamHandle.scan(uri, where="venue = 'x'").read_all()
        filtered = await streamcast.StreamHandle.scan(
            uri, filters=[("venue", "==", "x")]
        ).read_all()
        missing = await streamcast.StreamHandle.scan(
            uri, where="venue IS NULL"
        ).read_all()

        assert offsets(where) == offsets(filtered) == [4, 6]
        assert offsets(missing) == [1, 2, 3]

    async def test_sql_filters_prune_and_filter(self, tmp_path, serve):
        uri = await self.migrated(tmp_path, serve)
        with litelink.open(
            _metadata.home(tmp_path, "trades"), "trades", read_only=True
        ) as old:
            shutil.rmtree(_published_dir(old.published, "trades"))

        async with await streamcast.StreamHandle.snapshot(uri) as snap:
            # The first log is ruled out on the manifest and never opened, and
            # the term filters the rows of the log it keeps.
            table = await snap.sql(
                "SELECT litelink_offset FROM log ORDER BY 1",
                filters=[("price", ">", 600.5)],
            ).read_all()
            assert offsets(table) == [5, 6]

            with pytest.raises(Exception, match="version-hint|No files found|IO Error"):
                await snap.sql("SELECT count(*) FROM log").read_all()

    async def test_sql_offsets_narrow_the_table_and_prune(self, tmp_path, serve):
        uri = await self.migrated(tmp_path, serve)
        with litelink.open(
            _metadata.home(tmp_path, "trades"), "trades", read_only=True
        ) as old:
            shutil.rmtree(_published_dir(old.published, "trades"))

        table = await streamcast.StreamHandle.sql(
            uri, "SELECT litelink_offset FROM log ORDER BY 1", start_offset=5
        ).read_all()
        assert offsets(table) == [5, 6]

        bounded = await streamcast.StreamHandle.sql(
            uri,
            "SELECT litelink_offset FROM log ORDER BY 1",
            start_offset=4,
            end_offset=6,
        ).read_all()
        assert offsets(bounded) == [4, 5]

    async def test_as_of_a_time_keeps_an_unstamped_log_before_it(self, tmp_path, serve):
        # A log from before `streamcast_ts`: no column to bound on, and every
        # row of it is before the stamped log that follows.
        legacy = litelink.new(tmp_path, "trades", schema=streamcast.to_arrow(V1))
        with legacy:
            legacy.extend([row(i) for i in range(3)])

        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V1)
        for i in range(3, 6):
            await migrated.send(row(i))
            await asyncio.sleep(0.001)

        assert migrated.log is not None
        publish(migrated.log)
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        stamps = (
            await streamcast.StreamHandle.scan(uri, start_offset=4).read_all()
        ).column("streamcast_ts")
        middle = sorted(stamps.to_pylist())[1]

        table = await streamcast.StreamHandle.scan(uri, as_of_ts=middle).read_all()
        assert offsets(table) == [1, 2, 3, 4, 5]


class TestAnEmptyStream:
    async def test_a_query_over_nothing_published_is_an_empty_answer(
        self, tmp_path, serve
    ):
        """Not a binder error on the first column the query names."""
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="trades", schema=V1),
                storage=streamcast.Storage(root=Path(tmp_path).absolute()),
            ),
            initialization="create_if_absent",
        )
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        result = await streamcast.StreamHandle.sql(
            uri, "SELECT count(*) AS n, max(price) AS top FROM log WHERE price > 1"
        ).read_all()
        assert result.to_pylist() == [{"n": 0, "top": None}]

        table = await streamcast.StreamHandle.scan(uri, where="price > 1").read_all()
        assert table.num_rows == 0
        assert {"litelink_offset", "streamcast_ts", "price"} <= set(table.column_names)


class TestALiveLogThatHasPublishedNothing:
    async def test_its_columns_are_still_in_the_table(self, tmp_path, serve):
        """A column a migration added is NULL in the old log, not missing.

        The new live log holds back its few rows from publishing until
        compaction is done with them, so for a while it has no table at all —
        and the snapshot's table would otherwise end at the old log's columns.
        """
        stream = await stream_of(tmp_path, 3)
        await stream.aclose()
        migrated = streamcast.StreamHandle.migrate("trades", root=tmp_path, schema=V2)
        await migrated.send(row(3, venue="x"))  # unpublished
        uri = migrated.metadata_uri
        assert uri is not None
        await served_once(migrated, serve)

        table = await streamcast.StreamHandle.scan(uri).read_all()
        assert table.column("litelink_offset").to_pylist() == [1, 2, 3]
        assert table.column("venue").to_pylist() == [None, None, None]
        result = await streamcast.StreamHandle.sql(
            uri, "SELECT count(venue) AS n FROM log"
        ).read_all()
        assert result.to_pylist() == [{"n": 0}]


class TestTheMetadata:
    async def test_another_streams_file_at_the_same_path_is_refused(
        self, tmp_path, serve
    ):
        stream = await stream_of(tmp_path, 2)
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        await served_once(stream, serve)

        with pytest.raises(SnapshotUnavailable, match="another stream's file"):
            await _snapshot.snapshot(uri, stream_id="not-this-one")

    async def test_a_missing_file_says_the_history_is_local(self, tmp_path):
        with pytest.raises(SnapshotUnavailable, match="local to the server's machine"):
            await streamcast.StreamHandle.snapshot(
                (tmp_path / "nope.metadata.json").as_uri()
            )

    async def test_both_points_at_once_are_refused(self, tmp_path):
        with pytest.raises(ValueError, match="not both"):
            await streamcast.StreamHandle.snapshot(
                (tmp_path / "x.metadata.json").as_uri(), as_of_offset=1, as_of_ts=1
            )


def _published_dir(published: str, name: str) -> Path:

    return Path(_published.path(f"{published}/{name}"))


@pytest.mark.replication
async def test_a_stream_published_to_s3_reads_from_there(tmp_path, s3, bucket, serve):
    stream = await stream_of(tmp_path, 4, published=bucket, s3_options=s3)
    assert stream.log is not None
    publish(stream.log)
    uri = stream.metadata_uri
    assert uri is not None
    assert uri.startswith(bucket)
    await served_once(stream, serve)

    table = await streamcast.StreamHandle.scan(uri, s3_options=s3).read_all()
    assert offsets(table) == [1, 2, 3, 4]


class TestTheTailBound:
    async def test_a_broker_tail_past_max_tail_is_refused(self, tmp_path, serve):
        """A stalled publisher must not run the reader out of memory."""
        stream = await stream_of(tmp_path, 2)
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(2, 7)])  # five unpublished
        uri = stream.metadata_uri
        assert uri is not None
        async with serve(stream, maintain=False) as broker:
            with pytest.raises(SnapshotUnavailable, match="max_tail=3"):
                await streamcast.StreamHandle.snapshot(
                    uri, as_of_offset=LATEST, broker=broker, max_tail=3
                )

            # Within the bound, the same read works.
            table = await streamcast.StreamHandle.scan(
                uri, as_of_offset=LATEST, broker=broker, max_tail=5
            ).read_all()
            assert offsets(table) == [1, 2, 3, 4, 5, 6, 7]


class TestTailReduction:
    async def test_tail_columns_projected_before_append(
        self, tmp_path, serve, monkeypatch
    ):
        stream = await stream_of(tmp_path, 3)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        build = Mock(wraps=_snapshot._tail_table)
        monkeypatch.setattr(_snapshot, "_tail_table", build)
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_columns=["event_ts"],
            ) as snap:
                assert snap._tail is not None
                assert snap._tail.column_names == [
                    "litelink_offset",
                    "streamcast_ts",
                    "event_ts",
                ]
                assert offsets(snap._tail) == [1, 2, 3]
                assert all(
                    isinstance(ts, int)
                    for ts in snap._tail["streamcast_ts"].to_pylist()
                )
                records = build.call_args.args[1]
                assert all(
                    set(record) == set(snap._tail.column_names) for record in records
                )

    async def test_tail_filters_drop_rows_keep_frontier(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 2)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        publish(stream.log)
        await stream.send_many([row(i) for i in range(2, 7)])
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri, as_of_offset=LATEST, broker=broker
            ) as full:
                end = full.end_offset

            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_columns=["event_ts"],
                tail_filters=[("price", ">=", 104.0), ("event_ts", "in", [4, 6])],
            ) as snap:
                assert snap._tail is not None
                assert snap.end_offset == end == 8
                table = await snap.scan(
                    columns=["litelink_offset", "event_ts"]
                ).read_all()
                assert offsets(table) == [1, 2, 5, 7]

            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_filters=[("missing", "==", None)],
                max_tail_bytes=0,
            ) as empty_tail:
                assert empty_tail.end_offset == end
                assert offsets(await empty_tail.scan().read_all()) == [1, 2]

    @pytest.mark.parametrize(
        "operator,value",
        [
            ("==", 2),
            ("<", 2),
            ("<=", 2),
            (">", 2),
            (">=", 2),
            ("in", [0, 2]),
            ("in", []),
            ("==", None),
        ],
    )
    async def test_tail_terms_match_scan(self, tmp_path, serve, operator, value):
        stream = await stream_of(tmp_path, 4)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        terms = [("event_ts", operator, value)]
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
            ) as full:
                expected = await full.scan(filters=terms).read_all()

            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_filters=terms,
            ) as reduced:
                assert (await reduced.scan().read_all()).equals(expected)
                assert reduced.end_offset == 5

    async def test_max_tail_bytes_refuses(self, tmp_path, serve, monkeypatch):
        stream = await stream_of(tmp_path, 4)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_columns=["event_ts"],
                tail_filters=[("event_ts", ">=", 2)],
            ) as snap:
                assert snap._tail is not None
                size = sum(
                    len(_protocol.encode_publish(record))
                    for record in snap._tail.to_pylist()
                )

            options: dict[str, Any] = {
                "as_of_offset": LATEST,
                "broker": broker,
                "tail_columns": ["event_ts"],
                "tail_filters": [("event_ts", ">=", 2)],
            }
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri, **options, max_tail_bytes=size
            ) as exact:
                assert offsets(await exact.scan().read_all()) == [3, 4]

            close = Mock()
            original = _snapshot.Snapshot.close

            async def closed(snap):
                close(snap)
                await original(snap)

            monkeypatch.setattr(_snapshot.Snapshot, "close", closed)
            with pytest.raises(
                SnapshotUnavailable,
                match=f"max_tail_bytes={size - 1}.*Publishing is behind",
            ):
                await streamcast.StreamHandle.snapshot(
                    stream.metadata_uri, **options, max_tail_bytes=size - 1
                )

            assert close.call_count == 1
            assert close.call_args.args[0]._shut

    async def test_defaults_unchanged(self, tmp_path, serve):
        stream = await stream_of(tmp_path, 3)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri, as_of_offset=LATEST, broker=broker
            ) as original:
                expected = await original.scan().read_all()

            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_columns=None,
                tail_filters=(),
                max_tail_bytes=None,
            ) as explicit:
                assert explicit._tail is not None
                assert (await explicit.scan().read_all()).equals(expected)
                assert explicit._tail.column_names == [
                    "litelink_offset",
                    "streamcast_ts",
                    "event_ts",
                    "price",
                ]
                assert explicit._tail["price"].to_pylist() == [100.0, 101.0, 102.0]
                assert explicit.end_offset == original.end_offset == 4

    async def test_unknown_tail_column_rejected(self, tmp_path, serve, monkeypatch):
        stream = await stream_of(tmp_path, 1)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        async with serve(stream) as broker:
            connect = Mock(
                side_effect=AssertionError("must validate before connecting")
            )
            monkeypatch.setattr(_client, "connect", connect)
            with pytest.raises(ValueError, match="unknown.*missing"):
                await streamcast.StreamHandle.snapshot(
                    stream.metadata_uri,
                    as_of_offset=LATEST,
                    broker=broker,
                    tail_columns=["event_ts", "missing"],
                )

            connect.assert_not_called()

    @pytest.mark.parametrize("published_rows", [0, 2])
    async def test_scan_beyond_tail_columns_is_null(
        self, tmp_path, serve, published_rows
    ):
        stream = await stream_of(tmp_path, published_rows)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        if published_rows:
            publish(stream.log)

        await stream.send_many([row(i) for i in range(published_rows, 4)])
        async with serve(stream) as broker:
            async with await streamcast.StreamHandle.snapshot(
                stream.metadata_uri,
                as_of_offset=LATEST,
                broker=broker,
                tail_columns=["event_ts"],
            ) as snap:
                assert snap._tail is not None
                table = await snap.scan(columns=["event_ts", "price"]).read_all()
                assert table["price"].to_pylist() == [100.0, 101.0][:published_rows] + [
                    None
                ] * (4 - published_rows)
                assert table["event_ts"].to_pylist() == list(range(4))

    async def test_disconnect_mid_tail_returns_no_snapshot(
        self, tmp_path, serve, monkeypatch
    ):
        stream = await stream_of(tmp_path, 3)
        assert stream.metadata_uri is not None
        assert stream.log is not None
        connect = _client.connect

        @asynccontextmanager
        async def disconnected(*args, **kwargs):
            async with connect(*args, **kwargs) as sub:
                recv = sub.recv
                calls = 0

                async def receive(self):
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise ConnectionError("disconnected mid-tail")

                    return await recv()

                monkeypatch.setattr(type(sub), "recv", receive)
                yield sub

        async with serve(stream) as broker:
            monkeypatch.setattr(_client, "connect", disconnected)
            with pytest.raises(ConnectionError, match="mid-tail"):
                await streamcast.StreamHandle.snapshot(
                    stream.metadata_uri,
                    as_of_offset=LATEST,
                    broker=broker,
                    tail_columns=["event_ts"],
                )


async def _tail_peak(uri, broker):
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    async with await streamcast.StreamHandle.snapshot(
        uri,
        as_of_offset=LATEST,
        broker=broker,
        tail_columns=["a", "b", "c", "d"],
    ) as snap:
        assert snap._tail is not None
        assert snap._tail.column_names == [
            "litelink_offset",
            "streamcast_ts",
            "a",
            "b",
            "c",
            "d",
        ]
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return before, peak, snap._tail.num_rows


def _measure_tail_peak(uri, broker, result):
    with result:
        result.send(asyncio.run(_tail_peak(uri, broker)))


async def test_tail_memory_flat_when_projected(tmp_path, serve):
    """Fresh readers isolate their peaks from the writer and other scales."""
    schema = {
        "type": "object",
        "properties": {
            **{name: {"type": "integer"} for name in "abcd"},
            "text": {"type": "string"},
        },
        "required": [*"abcd", "text"],
    }
    context = multiprocessing.get_context("forkserver")
    deltas = []
    for count in (4, 40, 400):
        stream = streamcast.StreamHandle.open(
            streamcast.Stream(
                spec=streamcast.StreamSpec(name="wide", schema=schema),
                storage=streamcast.Storage(root=tmp_path / str(count)),
            ),
            initialization="create_if_absent",
        )
        await stream.send_many(
            [
                {"a": i, "b": 1, "c": 2, "d": 3, "text": "x" * (64 * 1024)}
                for i in range(count)
            ]
        )
        async with serve(stream) as broker:
            receive, send = context.Pipe(duplex=False)
            child = context.Process(
                target=_measure_tail_peak, args=(stream.metadata_uri, broker, send)
            )
            child.start()
            send.close()
            try:
                await asyncio.to_thread(child.join, 20)
                assert not child.is_alive(), "snapshot reader did not finish"
                assert child.exitcode == 0
                before, peak, rows = receive.recv()
                assert rows == count
                unit = 1024 if sys.platform == "darwin" else 1
                deltas.append((peak - before) / unit / 1024)
            finally:
                if child.is_alive():
                    child.terminate()
                    await asyncio.to_thread(child.join)

                child.close()
                receive.close()

    print(f"projected tail peak RSS deltas (MiB), 4/40/400 rows: {deltas}")
    # One transient wire row and fixed reader setup fit here; retaining 25 MiB
    # of text at 100x must not. CI runs this on both supported Python versions.
    assert max(deltas) - min(deltas) < 16, deltas
