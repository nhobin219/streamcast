"""`Stream.migrate`: a stream becomes a sequence of logs, one schema each.

The properties worth the file: offsets stay ONE dense sequence across the seam,
a column's type is fixed for the life of the stream, a migration is safe to
run at every start, and a server never passes off the seam as "nothing to
replay" — the hole at the join invariant 4 exists to prevent.
"""

from __future__ import annotations

import json
import shutil
from typing import Any

import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast import _log, _manifest, _metadata, _schema, _versions
from streamcast._maintain import finish_retiring
from streamcast._server import _supervisors
from tests.conftest import current_json

V1: dict[str, Any] = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "price": {"type": "number"},
        "side": {"type": "integer", "format": "int32"},
    },
    "required": ["event_ts", "price", "side"],
}


def evolve(base: dict[str, Any], **columns: Any) -> dict[str, Any]:
    """`base` with columns added (a spec) or removed (None)."""
    properties = dict(base["properties"])
    for name, spec in columns.items():
        if spec is None:
            properties.pop(name)
        else:
            properties[name] = spec

    required = [name for name in base["required"] if name in properties]
    return {"type": "object", "properties": properties, "required": required}


V2 = evolve(V1, venue={"type": ["string", "null"]})

NO_SYSTEM: dict[str, Any] = {"type": "object", "properties": {}, "required": []}
SYSTEM_NOW: dict[str, Any] = {
    "type": "object",
    "properties": {_log.STAMP: {"type": "integer", "format": "int64"}},
    "required": [_log.STAMP],
}


def row(i: int, **extra: object) -> dict[str, object]:
    return {
        "event_ts": 1_790_000_000_000_000 + i,
        "price": 100.0 + i,
        "side": i % 2,
        **extra,
    }


async def seeded(root, count: int = 5) -> None:
    """A never-migrated stream at `root` holding `count` rows, closed again."""
    stream = streamcast.Stream.new("trades", root=root, schema=V1)
    await stream.send_many([row(i) for i in range(count)])
    await stream.aclose()


class TestTheSeam:
    async def test_offsets_carry_on_densely_into_the_new_log(self, tmp_path):
        await seeded(tmp_path)

        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
            # No fence: the server is stopped, so nothing sent in between.
            assert await stream.send(row(5, venue="bitstamp")) == 6
            assert stream.schema is not None
            assert "venue" in stream.schema["properties"]  # ty: ignore[unsupported-operator]
        finally:
            await stream.aclose()

    async def test_the_old_log_is_sealed_for_good(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        # Retired: litelink itself refuses a writer from here on.
        with pytest.raises(litelink.RetiredError):
            litelink.open(_metadata.home(tmp_path, "trades"), "trades")

        # And every row is in its published table, none left local.
        with litelink.open(
            _metadata.home(tmp_path, "trades"), "trades", read_only=True
        ) as old:
            coverage = old.coverage()
            assert coverage.buffer is None, "rows left in the buffer"
            assert coverage.staging is None, "rows left in the staging table"
            assert coverage.published == (1, 6)

    async def test_the_metadata_records_both_logs(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        assert [(e.name, e.start_offset, e.end_offset) for e in metadata.logs] == [
            ("trades", 1, 6),
            ("trades-v2", 6, None),
        ]
        # Where each log's rows are read from, and the sealed one's time span.
        assert all(e.published is not None for e in metadata.logs)
        sealed = metadata.sealed_logs[0]
        assert sealed.start_ts is not None
        assert sealed.end_ts is not None
        assert sealed.start_ts <= sealed.end_ts
        assert "venue" in metadata.current.schema["properties"]  # ty: ignore[unsupported-operator]
        assert "venue" not in metadata.logs[0].schema["properties"]  # ty: ignore[unsupported-operator]
        # Owned columns are not declared ones.
        assert _log.STAMP not in metadata.current.schema["properties"]  # ty: ignore[unsupported-operator]

    async def test_a_second_migration_continues_the_sequence(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()
        stream = streamcast.Stream.migrate(
            "trades", root=tmp_path, schema=evolve(V2, side=None)
        )
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v3"
            assert await stream.send({"event_ts": 1, "price": 1.0}) == 6
        finally:
            await stream.aclose()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        assert [e.name for e in metadata.logs] == ["trades", "trades-v2", "trades-v3"]
        assert metadata.logs[1].end_offset == 6


class TestItIsSafeAtEveryStart:
    async def test_migrating_again_to_the_same_shape_changes_nothing(self, tmp_path):
        # The shape a server's startup takes: `Stream.migrate(...)` then
        # `serve`, run on every restart.
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        again = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            assert again.log is not None
            assert again.log.name == "trades-v2"
            assert not (_metadata.home(tmp_path, "trades") / "trades-v3").exists()
        finally:
            await again.aclose()

    async def test_stream_new_opens_the_current_log(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        stream = streamcast.Stream.new("trades", root=tmp_path, schema=V2)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
        finally:
            await stream.aclose()

    async def test_stream_new_with_the_old_schema_points_at_migrate(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        with pytest.raises(ValueError, match="Stream.migrate"):
            streamcast.Stream.new("trades", root=tmp_path, schema=V1)

    async def test_a_log_from_before_streamcast_ts_gains_it(self, tmp_path):
        # Same schema, and still a migration: the upgrade path for a log
        # created before the column existed.
        litelink.new(tmp_path, "trades", schema=streamcast.to_arrow(V1)).close()

        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V1)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
            assert _log.stamped(stream.log)
        finally:
            await stream.aclose()

    async def test_there_must_be_a_stream_to_migrate(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Stream.new"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V1)


class TestSystemColumns:
    async def test_the_metadata_records_each_logs_system_columns(self, tmp_path):
        # Per log, because they differ: the legacy log has none, and the log
        # the migration created has every one there is today.
        litelink.new(tmp_path, "trades", schema=streamcast.to_arrow(V1)).close()
        (await _migrated(tmp_path, V1)).close()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        assert [e.system_schema for e in metadata.logs] == [NO_SYSTEM, SYSTEM_NOW]

    async def test_a_new_system_column_is_an_upgrade_by_migrate(
        self, tmp_path, monkeypatch
    ):
        """What a future release adding a system column does to a stream.

        The log is current for today's columns, so migrating to the same
        schema changes nothing — until there is a column it lacks.
        """
        await seeded(tmp_path)
        again = streamcast.Stream.migrate("trades", root=tmp_path, schema=V1)
        assert again.log is not None
        assert again.log.name == "trades"
        await again.aclose()

        extra = "streamcast_future"
        monkeypatch.setattr(
            _log,
            "SYSTEM",
            {**_log.SYSTEM, extra: {"type": "integer", "format": "int64"}},
        )
        monkeypatch.setattr(
            _log,
            "_SYSTEM_ARROW",
            _log._SYSTEM_ARROW.append(pa.field(extra, pa.int64(), nullable=False)),
        )

        upgraded = streamcast.Stream.migrate("trades", root=tmp_path, schema=V1)
        try:
            assert upgraded.log is not None
            assert upgraded.log.name == "trades-v2"
            assert extra in upgraded.log.schema.names
            # Owned, so neither declared nor sent: not in the greeting's
            # schema, and not among the columns that fix a frame's keys. (A
            # real one would also need `send` to fill it — that is the code
            # change a release adding one makes.)
            assert extra not in _log.columns(upgraded.log)
            assert upgraded.schema is not None
            assert extra not in upgraded.schema["properties"]  # ty: ignore[unsupported-operator]
        finally:
            await upgraded.aclose()

    async def test_no_system_column_can_be_declared(self, tmp_path):
        with pytest.raises(ValueError, match="streamcast owns"):
            streamcast.Stream.new(
                "trades",
                root=tmp_path,
                schema=evolve(V1, streamcast_ts={"type": ["integer", "null"]}),
            )


class TestATypeIsForLife:
    async def test_a_kept_column_cannot_change_type(self, tmp_path):
        await seeded(tmp_path)
        narrowed = evolve(V1, price={"type": "number", "format": "float"})

        with pytest.raises(ValueError, match="fixed for the life"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=narrowed)

        # Refused before anything changed: still one log, still current.
        assert _metadata.load(_metadata.home(tmp_path, "trades"), "trades") is None
        assert not (_metadata.home(tmp_path, "trades") / "trades-v2").exists()

    async def test_widening_is_refused_too(self, tmp_path):
        await seeded(tmp_path)
        widened = evolve(V1, side={"type": "integer"})

        with pytest.raises(ValueError, match="fixed for the life"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=widened)

    async def test_a_removed_column_comes_back_only_as_what_it_was(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, evolve(V1, side=None))).close()

        back_wrong = evolve(V1, side={"type": "string"})
        with pytest.raises(ValueError, match="'side' was int32 in log 'trades'"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=back_wrong)

        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V1)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v3"
        finally:
            await stream.aclose()

    async def test_nullability_may_change(self, tmp_path):
        await seeded(tmp_path)
        relaxed = {**V1, "required": ["event_ts", "price"]}
        relaxed["properties"] = {
            **V1["properties"],
            "side": {"type": ["integer", "null"], "format": "int32"},
        }

        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=relaxed)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
        finally:
            await stream.aclose()

    async def test_the_owned_column_still_cannot_be_declared(self, tmp_path):
        await seeded(tmp_path)
        with pytest.raises(ValueError, match="another name"):
            streamcast.Stream.migrate(
                "trades",
                root=tmp_path,
                schema=evolve(V1, streamcast_ts={"type": ["integer", "null"]}),
            )


class TestAnInterruptedMigration:
    async def test_an_empty_orphan_of_the_right_shape_is_adopted(self, tmp_path):
        # Died after creating the log and before saving the metadata.
        await seeded(tmp_path)
        litelink.new(
            _metadata.home(tmp_path, "trades"),
            "trades-v2",
            schema=_log.with_system(streamcast.to_arrow(V2)),
            start_offset=6,
        ).close()

        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            assert stream.log is not None
            assert stream.log.name == "trades-v2"
            assert await stream.send(row(5, venue=None)) == 6
        finally:
            await stream.aclose()

    async def test_an_orphan_holding_rows_is_refused(self, tmp_path):
        await seeded(tmp_path)
        with litelink.new(
            _metadata.home(tmp_path, "trades"),
            "trades-v2",
            schema=_log.with_system(streamcast.to_arrow(V2)),
            start_offset=6,
        ) as orphan:
            orphan.append({**row(9), "venue": None, _log.STAMP: 0})

        with pytest.raises(FileExistsError, match="does not name it"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)

        assert _metadata.load(_metadata.home(tmp_path, "trades"), "trades") is None

    async def test_an_orphan_whose_rows_end_at_the_seam_is_refused(self, tmp_path):
        # Its `end_offset` matches where the new log should start, so only
        # the question "does it hold anything" tells it from an empty one.
        await seeded(tmp_path)
        with litelink.new(
            _metadata.home(tmp_path, "trades"),
            "trades-v2",
            schema=_log.with_system(streamcast.to_arrow(V2)),
            start_offset=2,
        ) as orphan:
            orphan.extend([{**row(i), "venue": None, _log.STAMP: 0} for i in range(4)])
            assert orphan.end_offset() == 6

        with pytest.raises(FileExistsError, match="does not name it"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)


class TestServingAMigratedStream:
    async def test_a_consumer_caught_up_at_the_seam_resumes_there(
        self, tmp_path, serve
    ):
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri, offset=6) as sub:
                    await stream.send(row(5, venue="x"))
                    offset, ts, message = await sub.recv()

            assert offset == 6
            assert isinstance(ts, int)
            assert message["venue"] == "x"
        finally:
            await stream.aclose()

    async def test_below_the_seam_is_refused_not_served_empty(self, tmp_path, serve):
        """The hole at the join, which the scan alone cannot see.

        The current log holds nothing below 6, so without the floor a replay
        of `[2, 6)` reads nothing, looks like "nothing outstanding", and the
        consumer carries on without offsets 2-5.
        """
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            async with serve(stream, maintain=False) as uri:
                with pytest.raises(streamcast.NotReplayable) as raised:
                    await streamcast.connect(uri, offset=2)

            assert raised.value.why == "evicted"
            assert raised.value.fields["earliest"] == 6
        finally:
            await stream.aclose()

    async def test_catch_up_reads_below_the_seam_from_the_retired_log(
        self, tmp_path, serve
    ):
        """The refusal above, closed: a retired log is published in full."""
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri, offset=2, catch_up=True) as sub:
                    caught_up = [await sub.recv() for _ in range(4)]
                    await stream.send(row(5, venue="x"))
                    live = await sub.recv()

            assert [offset for offset, _ts, _ in caught_up] == [2, 3, 4, 5]
            assert [message["price"] for _, _ts, message in caught_up] == [
                101.0,
                102.0,
                103.0,
                104.0,
            ]
            assert live[0] == 6
            assert isinstance(live[1], int)
            assert live[2]["venue"] == "x"
        finally:
            await stream.aclose()

    async def test_a_retired_log_published_short_is_refused_not_stepped_over(
        self, tmp_path, serve
    ):
        """Catch-up steps to the snapshot's end, so a short table is a hole.

        Two rows fewer in the table than the manifest says the log held: the
        shape of a log retired before every row was published.
        """
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        manifest = _manifest.load(_metadata.home(tmp_path, "trades"), "trades")
        assert manifest is not None
        counts = manifest.column("record_count").to_pylist()
        # A new version naming the doctored manifest, as a reader finds it.
        home = _metadata.home(tmp_path, "trades")
        current = _metadata.load(home, "trades")
        assert current is not None
        _versions.commit(
            home,
            current,
            manifest=manifest.set_column(
                manifest.schema.get_field_index("record_count"),
                "record_count",
                pa.array([count + 2 for count in counts], pa.int64()),
            ),
        )
        try:
            async with serve(stream, maintain=False) as uri:
                with pytest.raises(
                    streamcast.CatchUpUnavailable, match="holds 5 of the 7 rows"
                ):
                    await streamcast.connect(uri, offset=2, catch_up=True)
        finally:
            await stream.aclose()

    async def test_earliest_on_a_fresh_seam_is_where_the_log_begins(
        self, tmp_path, serve
    ):
        # Not "empty": the stream has history, it is just not in this log.
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            async with serve(stream, maintain=False) as uri:
                async with streamcast.connect(uri, offset=streamcast.EARLIEST) as sub:
                    assert sub.info.replay == (6, 6)
                    await stream.send(row(5, venue=None))
                    offset, ts, _message = await sub.recv()

            assert offset == 6
            assert isinstance(ts, int)
        finally:
            await stream.aclose()

    async def test_the_retired_log_is_not_maintained(self, tmp_path):
        """`retire()` left nothing to do, and litelink refuses it a writer.

        Handed to the maintainer, each role would print "cannot open" for it
        at every start and do nothing else.
        """
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            assert stream.retired == ((_metadata.home(tmp_path, "trades"), "trades"),)
            made = _supervisors({"trades": stream}, True)
            for supervisor in made:
                assert supervisor.targets == [
                    (_metadata.home(tmp_path, "trades"), "trades-v2")
                ]

            # Handed to one process only, to finish what a migration before
            # streamcast 0.10 left undone; already retired, it skips it.
            assert [s.role for s in made if s.retiring] == ["publish"]
            [publish] = [s for s in made if s.retiring]
            assert publish.retiring == [(_metadata.home(tmp_path, "trades"), "trades")]
        finally:
            await stream.aclose()


class TestAStreamMigratedBeforeRetire:
    """streamcast 0.9 sealed an old log its own way, and never retired it.

    With no archive it was never published, and litelink 0.7 opens it as an
    ordinary log that has published nothing. The maintainer's publish role
    retires it through litelink, so its rows reach the published table and a
    snapshot reads across the seam.
    """

    async def test_the_publish_role_retires_it(self, tmp_path, monkeypatch):
        await seeded(tmp_path)

        def sealed_only(log: litelink.WriteHandle) -> None:
            # What 0.9's migration did: everything sealed, nothing published,
            # and the log not marked retired.
            while log.seal(flush=True) is not None:
                pass

        with monkeypatch.context() as patched:
            patched.setattr(litelink.WriteHandle, "retire", sealed_only)
            stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)

        try:
            with litelink.open(
                _metadata.home(tmp_path, "trades"), "trades"
            ) as old:  # a writer: not retired
                assert old.published_through() == 0

            # What the publish role runs for the retired logs it is handed.
            assert (
                finish_retiring([(_metadata.home(tmp_path, "trades"), "trades")]) == []
            )
            assert retired(_metadata.home(tmp_path, "trades"), "trades")
            # And again, as at the next start: already retired, nothing to do.
            assert (
                finish_retiring([(_metadata.home(tmp_path, "trades"), "trades")]) == []
            )

            uri = stream.metadata_uri
            assert uri is not None
            stream.ensure_metadata()
            table = await streamcast.Stream.scan(uri).read_all()
            assert table.column("litelink_offset").to_pylist() == [1, 2, 3, 4, 5]
        finally:
            await stream.aclose()

    async def test_the_publish_role_is_told_which(self, tmp_path):
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            [publish] = [
                s for s in _supervisors({"trades": stream}, True) if s.retiring
            ]
            argv = publish._spawn_argv()  # noqa: SLF001
            index = argv.index("--retire")
            assert argv[index : index + 3] == [
                "--retire",
                str(_metadata.home(tmp_path, "trades")),
                "trades",
            ]
        finally:
            await stream.aclose()


def retired(root, name: str) -> bool:
    try:
        litelink.open(root, name).close()
    except litelink.RetiredError:
        return True

    return False


class TestTheManifest:
    """Each migration adds the retired log's statistics, before metadata.json."""

    async def test_the_retired_log_gets_a_row_with_its_bounds(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        manifest = _manifest.load(_metadata.home(tmp_path, "trades"), "trades")
        assert manifest is not None
        [found] = manifest.to_pylist()
        assert (found["log"], found["start_offset"], found["end_offset"]) == (
            "trades",
            1,
            6,
        )
        assert found["record_count"] == 5
        assert (found["event_ts"]["min"], found["event_ts"]["max"]) == (
            row(0)["event_ts"],
            row(4)["event_ts"],
        )
        assert (found["side"]["min"], found["side"]["max"]) == (0, 1)
        assert found["side"]["null_count"] == 0

    async def test_the_metadata_points_at_it(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        assert metadata.manifest == "trades.manifest.parquet"
        version = current_json(_metadata.home(tmp_path, "trades"), "trades")
        directory = _metadata.home(tmp_path, "trades") / "trades.metadata"
        assert (directory / version["manifest"]).exists(), (
            "beside the version naming it, so one pointer serves the published copy"
        )

    async def test_each_migration_adds_a_row_and_keeps_the_rest(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=V2)
        await stream.send(row(5, venue="x"))
        await stream.aclose()
        (await _migrated(tmp_path, evolve(V2, side=None))).close()

        manifest = _manifest.load(_metadata.home(tmp_path, "trades"), "trades")
        assert manifest is not None
        assert manifest["log"].to_pylist() == ["trades", "trades-v2"]
        # `venue` is a string, and string bounds are truncated, so it has no
        # statistics column at all: it can never prune.
        assert "venue" not in manifest.column_names

    async def test_a_snapshot_can_skip_the_log_that_cannot_match(self, tmp_path):
        """End to end: statistics from litelink, pruned against a predicate."""
        await seeded(tmp_path)  # event_ts in [base, base + 4]
        (await _migrated(tmp_path, V2)).close()
        stream = streamcast.Stream.new("trades", root=tmp_path, schema=V2)
        await stream.send_many([row(i, venue=None) for i in range(100, 105)])
        await stream.aclose()
        (await _migrated(tmp_path, evolve(V2, side=None))).close()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        sealed = [entry.name for entry in metadata.sealed_logs]
        late = row(100)["event_ts"]

        assert _manifest.prune(
            _manifest.load(_metadata.home(tmp_path, "trades"), "trades"),
            sealed,
            [("event_ts", ">=", late)],
        ) == ["trades-v2"]
        assert _manifest.prune(
            _manifest.load(_metadata.home(tmp_path, "trades"), "trades"),
            sealed,
            [("event_ts", "<", late)],
        ) == ["trades"]

    async def test_a_float_column_prunes_now_litelink_holds_only_finite_floats(
        self, tmp_path
    ):
        """litelink refuses NaN and ±inf (0.5.0), so it reports a NaN count of 0.

        Before that its count was unknown, and an unknown count never prunes a
        float column. This is the case that release switched on.
        """
        await seeded(tmp_path)  # price in [100.0, 104.0]
        (await _migrated(tmp_path, V2)).close()

        manifest = _manifest.load(_metadata.home(tmp_path, "trades"), "trades")
        assert manifest is not None
        assert manifest["price"].to_pylist()[0]["nan_count"] == 0
        assert _manifest.prune(manifest, ["trades"], [("price", ">", 500.0)]) == []
        assert _manifest.prune(manifest, ["trades"], [("price", ">", 103.0)]) == [
            "trades"
        ]

    async def test_reading_statistics_failing_leaves_the_stream_as_it_was(
        self, tmp_path, monkeypatch
    ):
        await seeded(tmp_path)

        def refuse(self, **_):
            raise OSError("the published table is unreachable")

        monkeypatch.setattr(litelink.WriteHandle, "column_statistics", refuse)
        with pytest.raises(OSError, match="unreachable"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)

        assert not (_metadata.home(tmp_path, "trades") / "trades-v2").exists()
        assert _manifest.load(_metadata.home(tmp_path, "trades"), "trades") is None
        assert _metadata.load(_metadata.home(tmp_path, "trades"), "trades") is None

    async def test_a_manifest_that_cannot_be_written_commits_nothing(
        self, tmp_path, monkeypatch
    ):
        """A commit that fails is retried: nothing names the new log until one lands.

        The new log exists by then — an orphan the metadata does not name —
        and the retry adopts it, because it is empty and of the right shape.
        """
        await seeded(tmp_path)
        original = _versions.commit

        def refuse(*_args, **_kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(_versions, "commit", refuse)
        with pytest.raises(OSError, match="disk full"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)

        assert _metadata.load(_metadata.home(tmp_path, "trades"), "trades") is None, (
            "nothing committed"
        )

        monkeypatch.setattr(_versions, "commit", original)
        (await _migrated(tmp_path, V2)).close()

        metadata = _metadata.load(_metadata.home(tmp_path, "trades"), "trades")
        assert metadata is not None
        assert metadata.live_log.name == "trades-v2"
        assert _manifest.load(_metadata.home(tmp_path, "trades"), "trades")[
            "log"
        ].to_pylist() == ["trades"]  # ty: ignore[not-subscriptable]

    async def test_migrating_to_the_same_shape_leaves_it_alone(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()
        home = _metadata.home(tmp_path, "trades")
        before = current_json(home, "trades")["manifest"]

        again = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        await again.aclose()

        assert current_json(home, "trades")["manifest"] == before


def test_extending_replaces_a_row_rather_than_duplicating_it():
    """A migration retried after dying between the manifest and metadata.json."""
    schema: dict[str, object] = {
        "type": "object",
        "properties": {"x": {"type": "integer"}},
        "required": ["x"],
    }
    stats = litelink.TierStatistics(
        tier=None,
        record_count=2,
        file_count=1,
        columns={"x": litelink.ColumnStatistics(1, 2, 0, 2, None)},
    )
    entry = _manifest.entry("trades", 1, 3, _schema.to_arrow(schema), stats)

    once = _manifest.extend(None, entry)
    twice = _manifest.extend(once, entry)

    assert twice["log"].to_pylist() == ["trades"]
    assert twice == once


@pytest.mark.replication
class TestThePublishedCopy:
    async def test_the_metadata_is_published_beside_the_logs(
        self, tmp_path, s3, bucket
    ):
        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=V1, published=bucket, s3_options=s3
        )
        await stream.send_many([row(i) for i in range(5)])
        await stream.aclose()

        migrated = streamcast.Stream.migrate(
            "trades", root=tmp_path, schema=V2, s3_options=s3
        )
        try:
            assert migrated.log is not None
            assert migrated.log.published == f"{bucket}/trades"
        finally:
            await migrated.aclose()

        published = _versions.fetch(f"{bucket}/trades", "trades", s3)
        assert published == _metadata.load(_metadata.home(tmp_path, "trades"), "trades")

        # And the manifest beside the version naming it, written before it.
        import pyarrow as pa
        import pyarrow.parquet as pq

        store = _versions._Remote(f"{bucket}/trades", "trades", s3)  # noqa: SLF001
        found = _versions.current(store)
        assert found is not None
        version = json.loads(store.read(found[1]) or b"")
        raw = store.read(version["manifest"])
        assert raw is not None
        assert pq.read_table(pa.BufferReader(raw)) == _manifest.load(
            _metadata.home(tmp_path, "trades"), "trades"
        )

        # And the retired log is in its published table whole, not just its
        # settled prefix: nothing will push its tail later.
        with litelink.open(
            _metadata.home(tmp_path, "trades"), "trades", read_only=True
        ) as old:
            assert old.published_through() == 5

    async def test_a_stream_that_never_migrated_has_no_metadata_there(
        self, tmp_path, s3, bucket
    ):
        assert _versions.fetch(bucket, "trades", s3) is None


def test_the_metadata_round_trips():
    metadata = _metadata.Metadata(
        stream="trades",
        stream_id="6f1c0b8e-0000-4000-8000-000000000000",
        sealed_logs=(
            _metadata.Entry(
                "trades",
                1,
                6,
                V1,
                NO_SYSTEM,
                published="s3://bucket/prod",
                start_ts=1_790_000_000_000_000,
                end_ts=1_790_000_000_500_000,
            ),
        ),
        live_log=_metadata.Entry(
            "trades-v2",
            6,
            None,
            V2,
            SYSTEM_NOW,
            published="s3://bucket/prod",
            start_ts=1_790_000_000_600_000,
        ),
        manifest="trades.manifest.parquet",
    )
    assert _metadata.Metadata.from_json(metadata.to_json()) == metadata
    assert json.loads(metadata.to_json())["streamcast_metadata"] == 2


def test_a_version_1_file_is_read_with_its_new_fields_unknown():
    """What `serve` wrote in 0.9.0: no published prefix, no timestamps."""
    v1 = {
        "streamcast_metadata": 1,
        "stream": "trades",
        "stream_id": "6f1c0b8e-0000-4000-8000-000000000000",
        "sealed_logs": [
            {
                "name": "trades",
                "start_offset": 1,
                "end_offset": 6,
                "schema": V1,
                "system_schema": NO_SYSTEM,
            }
        ],
        "live_log": {
            "name": "trades-v2",
            "start_offset": 6,
            "schema": V2,
            "system_schema": SYSTEM_NOW,
        },
        "manifest": None,
    }
    read = _metadata.Metadata.from_json(json.dumps(v1))
    assert [(e.name, e.published, e.start_ts, e.end_ts) for e in read.logs] == [
        ("trades", None, None, None),
        ("trades-v2", None, None, None),
    ]


async def test_serve_upgrades_a_version_1_file(tmp_path, serve):
    """`ensure` fills in what version 1 lacked, and rewrites the file at 2."""
    stream = streamcast.Stream.new("trades", root=tmp_path, schema=V1)
    await stream.send_many([row(i) for i in range(3)])
    async with serve(stream, maintain=False):
        pass

    written = current_json(_metadata.home(tmp_path, "trades"), "trades")
    written["streamcast_metadata"] = 1
    for key in ("published", "start_ts"):
        del written["live_log"][key]

    _metadata.path(_metadata.home(tmp_path, "trades"), "trades").write_text(
        json.dumps(written)
    )
    # From before the versions too: the plain file is all there is.
    shutil.rmtree(_metadata.home(tmp_path, "trades") / "trades.metadata")
    again = streamcast.Stream.new("trades", root=tmp_path, schema=V1)
    assert again.log is not None
    while (
        again.log.seal(flush=True) is not None
    ):  # so its statistics hold a `streamcast_ts`
        pass

    async with serve(again, maintain=False):
        pass

    upgraded = current_json(_metadata.home(tmp_path, "trades"), "trades")
    assert upgraded["streamcast_metadata"] == 2
    assert upgraded["live_log"]["published"].startswith("file://")
    assert upgraded["live_log"]["start_ts"] is not None
    assert upgraded["stream_id"] == written["stream_id"], "the id is kept"


def test_an_unknown_metadata_version_is_refused():
    with pytest.raises(ValueError, match="version"):
        _metadata.Metadata.from_json(
            json.dumps({"streamcast_metadata": 99, "stream": "t", "logs": []})
        )


async def _migrated(root, schema):
    """Migrate, and hand back the handle so the caller closes it."""
    stream = streamcast.Stream.migrate("trades", root=root, schema=schema)
    assert stream.log is not None
    return stream.log
