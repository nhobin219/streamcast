"""`Stream.migrate`: a stream becomes a sequence of logs, one schema each.

The properties worth the file: offsets stay ONE dense sequence across the seam,
a column's type is fixed for the life of the stream, a migration is safe to
run at every start, and a server never passes off the seam as "nothing to
replay" — the hole at the join invariant 4 exists to prevent.
"""

from __future__ import annotations

import json
from typing import Any

import litelink
import pytest

import streamcast
from streamcast import _log, _metadata
from streamcast._server import _supervisors

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

        with litelink.open(tmp_path, "trades") as old:
            assert old.coverage().buffered is None, "rows left in the buffer"
            assert old.table_extent() == (1, 5)

    async def test_the_metadata_records_both_logs(self, tmp_path):
        await seeded(tmp_path)
        (await _migrated(tmp_path, V2)).close()

        metadata = _metadata.load(tmp_path, "trades")
        assert metadata is not None
        assert [(e.name, e.start_offset, e.end_offset) for e in metadata.logs] == [
            ("trades", 1, 6),
            ("trades-v2", 6, None),
        ]
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

        metadata = _metadata.load(tmp_path, "trades")
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
            assert not (tmp_path / "trades-v3").exists()
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


class TestATypeIsForLife:
    async def test_a_kept_column_cannot_change_type(self, tmp_path):
        await seeded(tmp_path)
        narrowed = evolve(V1, price={"type": "number", "format": "float"})

        with pytest.raises(ValueError, match="fixed for the life"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=narrowed)

        # Refused before anything changed: still one log, still current.
        assert _metadata.load(tmp_path, "trades") is None
        assert not (tmp_path / "trades-v2").exists()

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
            tmp_path,
            "trades-v2",
            schema=_log.with_stamp(streamcast.to_arrow(V2)),
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
            tmp_path,
            "trades-v2",
            schema=_log.with_stamp(streamcast.to_arrow(V2)),
            start_offset=6,
        ) as orphan:
            orphan.append({**row(9), "venue": None, _log.STAMP: 0})

        with pytest.raises(FileExistsError, match="does not name it"):
            streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)

        assert _metadata.load(tmp_path, "trades") is None

    async def test_an_orphan_whose_rows_end_at_the_seam_is_refused(self, tmp_path):
        # Its `end_offset` matches where the new log should start, so only
        # the question "does it hold anything" tells it from an empty one.
        await seeded(tmp_path)
        with litelink.new(
            tmp_path,
            "trades-v2",
            schema=_log.with_stamp(streamcast.to_arrow(V2)),
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
                    offset, message = await sub.recv()

            assert offset == 6
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
                    offset, _message = await sub.recv()

            assert offset == 6
        finally:
            await stream.aclose()

    async def test_the_retired_log_is_still_maintained(self, tmp_path):
        await seeded(tmp_path)
        stream = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2)
        try:
            assert stream.retired == ((tmp_path, "trades"),)
            [supervisor] = _supervisors({"trades": stream}, True)
            assert set(supervisor.targets) == {
                (tmp_path, "trades-v2"),
                (tmp_path, "trades"),
            }
        finally:
            await stream.aclose()


@pytest.mark.replication
class TestTheArchive:
    async def test_the_metadata_is_published_beside_the_logs(
        self, tmp_path, s3, bucket
    ):
        stream = streamcast.Stream.new(
            "trades", root=tmp_path, schema=V1, archive=bucket, s3=s3
        )
        await stream.send_many([row(i) for i in range(5)])
        await stream.aclose()

        migrated = streamcast.Stream.migrate("trades", root=tmp_path, schema=V2, s3=s3)
        try:
            assert migrated.log is not None
            assert migrated.log.archive == bucket
        finally:
            await migrated.aclose()

        published = _metadata.fetch(bucket, "trades", s3)
        assert published == _metadata.load(tmp_path, "trades")

        # And the retired log is in the archive whole, not just its settled
        # prefix: nothing will push its tail later.
        with litelink.open(tmp_path, "trades") as old:
            assert old.archived_through() == 5

    async def test_a_stream_that_never_migrated_has_no_metadata_there(
        self, tmp_path, s3, bucket
    ):
        assert _metadata.fetch(bucket, "trades", s3) is None


def test_the_metadata_round_trips():
    metadata = _metadata.Metadata(
        "trades",
        (
            _metadata.Entry("trades", 1, 6, V1),
            _metadata.Entry("trades-v2", 6, None, V2),
        ),
    )
    assert _metadata.Metadata.from_json(metadata.to_json()) == metadata
    assert json.loads(metadata.to_json())["streamcast_metadata"] == 1


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
