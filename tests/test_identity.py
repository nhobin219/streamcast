"""Declared identity columns, and the `_streamcast_id` the writer derives (#159).

The encoding is pinned by golden vectors written out byte by byte, so a
change to it fails here rather than splitting the keys of one stream across
versions. The rest is written with a server's own calls and read back as
another machine would, through the stream's metadata.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import litelink
import pyarrow as pa
import pytest

import streamcast
from streamcast import LATEST, _identity, _metadata, _snapshot, _versions

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "session_id": {"type": "string"},
        "line_number": {"type": "integer"},
        "text": {"type": "string"},
    },
    "required": ["session_id", "line_number", "text"],
}
KEY = ("session_id", "line_number")


def line(session: str, number: int, text: str = "") -> dict[str, object]:
    return {"session_id": session, "line_number": number, "text": text}


def publish(log: litelink.WriteHandle) -> None:
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)


async def published_stream(
    root: Path, name: str, rows: list[dict[str, object]], serve, **kwargs: Any
) -> str:
    """A stream of `rows`, all published, and its metadata URI."""
    stream = streamcast.Stream.new(
        name, root=root, schema=kwargs.pop("schema", SCHEMA), identity=KEY, **kwargs
    )
    await stream.send_many(rows)
    assert stream.log is not None
    publish(stream.log)
    uri = stream.metadata_uri
    assert uri is not None
    async with serve(stream, maintain=False):
        pass

    await stream.aclose()
    return uri


def key(*values: object, schema: pa.Schema | None = None) -> bytes:
    arrow = schema or pa.schema(
        [
            pa.field("session_id", pa.string(), nullable=False),
            pa.field("line_number", pa.int64(), nullable=False),
        ]
    )
    return _identity.Identity(arrow.names, arrow).key(
        dict(zip(arrow.names, values, strict=True))
    )


def rows_of(n: int) -> list[dict[str, object]]:
    return [line(f"s{i % 3}", i, f"line {i}") for i in range(n)]


class TestTheEncoding:
    """Golden vectors: the bytes each value encodes to, and the key."""

    def test_the_hash_is_xxh3_128_in_canonical_byte_order(self):
        # xxHash's own published value for XXH3-128 of the empty input. A
        # key stored little-endian, or a seeded hash, would not match it.
        assert (
            _identity.xxhash.xxh3_128_digest(b"").hex()
            == "99aa06d3014798d86001c324468d497f"
        )

    def test_strings_are_length_prefixed_so_a_boundary_cannot_move(self):
        schema = pa.schema(
            [
                pa.field("a", pa.string(), nullable=False),
                pa.field("b", pa.string(), nullable=False),
            ]
        )
        identity = _identity.Identity(["a", "b"], schema)

        ab_c = identity.encode({"a": "ab", "b": "c"})
        a_bc = identity.encode({"a": "a", "b": "bc"})

        assert ab_c == b"\x02\x00\x00\x00ab" + b"\x01\x00\x00\x00c"
        assert a_bc == b"\x01\x00\x00\x00a" + b"\x02\x00\x00\x00bc"
        assert identity.key({"a": "ab", "b": "c"}).hex() == (
            "ef2fcd6c26fa0aa6250f3d2016ddaed6"
        )
        assert identity.key({"a": "a", "b": "bc"}).hex() == (
            "6e496efc8f77eebc83650241d6affe03"
        )

    def test_a_string_and_an_int64(self):
        identity = _identity.Identity(
            KEY,
            pa.schema(
                [
                    pa.field("session_id", pa.string(), nullable=False),
                    pa.field("line_number", pa.int64(), nullable=False),
                ]
            ),
        )
        values = {"session_id": "s7", "line_number": 7}

        assert identity.encode(values) == (
            b"\x02\x00\x00\x00s7" + b"\x07\x00\x00\x00\x00\x00\x00\x00"
        )
        assert identity.key(values).hex() == "436e7a256b228c5e490be5d01bdee36d"

    def test_fixed_width_values_are_little_endian_with_no_prefix(self):
        schema = pa.schema(
            [
                pa.field("n", pa.int32(), nullable=False),
                pa.field("ok", pa.bool_(), nullable=False),
            ]
        )
        identity = _identity.Identity(["n", "ok"], schema)

        assert identity.encode({"n": -1, "ok": True}) == b"\xff\xff\xff\xff\x01"
        assert identity.key({"n": -1, "ok": True}).hex() == (
            "7bdeec88bd3d6f667782892872eb158e"
        )

    def test_a_fixed_size_binary_is_its_bytes_and_a_binary_is_prefixed(self):
        schema = pa.schema(
            [
                pa.field("id", pa.binary(4), nullable=False),
                pa.field("raw", pa.binary(), nullable=False),
            ]
        )
        identity = _identity.Identity(["id", "raw"], schema)
        values = {"id": b"\xde\xad\xbe\xef", "raw": b"\x00\x01\x02"}

        assert identity.encode(values) == (
            b"\xde\xad\xbe\xef" + b"\x03\x00\x00\x00\x00\x01\x02"
        )
        assert identity.key(values).hex() == "fb8e5a1adc515858067626aeaeb4fa67"

    def test_a_string_is_counted_in_utf8_bytes_not_characters(self):
        schema = pa.schema([pa.field("s", pa.string(), nullable=False)])

        assert _identity.Identity(["s"], schema).encode({"s": "é"}) == (
            b"\x02\x00\x00\x00\xc3\xa9"
        )

    def test_declaration_order_is_part_of_the_key(self):
        schema = pa.schema(
            [
                pa.field("a", pa.string(), nullable=False),
                pa.field("b", pa.string(), nullable=False),
            ]
        )
        values = {"a": "x", "b": "y"}

        assert _identity.Identity(["a", "b"], schema).key(values) != (
            _identity.Identity(["b", "a"], schema).key(values)
        )

    def test_a_value_of_another_type_is_refused_not_coerced(self):
        identity = _identity.Identity(
            ["n"], pa.schema([pa.field("n", pa.int64(), nullable=False)])
        )
        for wrong in (True, 1.0, "1"):
            with pytest.raises(ValueError, match="'n' cannot hold"):
                identity.key({"n": wrong})

        with pytest.raises(ValueError, match="part of the stream's identity"):
            identity.key({})

    def test_a_batch_is_keyed_as_its_rows_are_on_the_logs_types(self):
        identity = _identity.Identity(
            KEY,
            pa.schema(
                [
                    pa.field("session_id", pa.string(), nullable=False),
                    pa.field("line_number", pa.int64(), nullable=False),
                ]
            ),
        )
        # A float64 source column, which `ingest` casts to the int64 the log
        # stores: keyed on that value, so a row keys alike either way in.
        batch = pa.record_batch(
            {
                "session_id": ["a", "bb"],
                "line_number": pa.array([1.0, -2.0], pa.float64()),
            }
        )

        assert identity.keys(batch).to_pylist() == [key("a", 1), key("bb", -2)]
        with pytest.raises(ValueError, match="'session_id' is part"):
            identity.keys(pa.record_batch({"session_id": [None], "line_number": [1]}))


class TestTheDeclaration:
    @pytest.mark.parametrize(
        ("spec", "why"),
        [
            ({"type": "number"}, "equality on a float"),
            ({"type": ["string", "null"]}, "nullable"),
            ({"type": "array", "items": {"type": "string"}}, "nested"),
        ],
    )
    def test_a_column_that_cannot_be_an_identity_is_refused(self, tmp_path, spec, why):
        schema = {
            "type": "object",
            "properties": {"k": spec},
            "required": ["k"] if "null" not in str(spec) else [],
        }
        with pytest.raises(ValueError, match=why):
            streamcast.Stream.new("s", root=tmp_path, schema=schema, identity=("k",))

        assert not (tmp_path / "s" / "s").exists()

    def test_a_column_the_schema_lacks_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="'nope'"):
            streamcast.Stream.new(
                "s", root=tmp_path, schema=SCHEMA, identity=("session_id", "nope")
            )

    def test_the_column_name_is_reserved(self, tmp_path):
        schema = {
            "type": "object",
            "properties": {"_streamcast_id": {"type": "string"}},
            "required": ["_streamcast_id"],
        }
        with pytest.raises(ValueError, match="streamcast owns"):
            streamcast.Stream.new("s", root=tmp_path, schema=schema)


class TestTheWriter:
    async def test_every_row_is_keyed_on_ingest_and_the_frame_does_not_carry_it(
        self, tmp_path, serve
    ):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        assert stream.log is not None
        async with serve(stream, maintain=False) as uri:
            async with streamcast.connect(uri) as subscription:
                assert "_streamcast_id" not in str(subscription.info.schema)
                await stream.send_many([line("a", 1, "x"), line("a", 1, "dup")])
                frames = [await subscription.recv() for _ in range(2)]

            stored = stream.log.scan().read_all()

        assert all("_streamcast_id" not in row for _offset, _ts, row in frames)
        assert stored.schema.field("_streamcast_id").type == pa.binary(16)
        # Not a primary key: the duplicate is appended, under the same key.
        assert stored.column("_streamcast_id").to_pylist() == [key("a", 1)] * 2
        await stream.aclose()

    async def test_a_row_cannot_supply_the_key(self, tmp_path):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        with pytest.raises(ValueError, match="derived by the server"):
            await stream.send({**line("a", 1), "_streamcast_id": b"\x00" * 16})

        await stream.aclose()

    async def test_ingest_keys_rows_as_send_does(self, tmp_path):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.send(line("a", 1))
        await stream.aclose()

        source = pa.table(
            {"session_id": ["b", "c"], "line_number": [2, 3], "text": ["", ""]}
        )
        streamcast.Stream.ingest("lines", source, root=tmp_path)

        log = litelink.open(_metadata.home(tmp_path, "lines"), "lines")
        stored = log.scan().read_all()
        log.close()
        assert stored.column("_streamcast_id").to_pylist() == [
            key("a", 1),
            key("b", 2),
            key("c", 3),
        ]

    async def test_a_source_cannot_supply_the_key(self, tmp_path):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.aclose()
        source = pa.table(
            {
                "session_id": ["b"],
                "line_number": [2],
                "text": [""],
                "_streamcast_id": pa.array([b"\x00" * 16], pa.binary(16)),
            }
        )
        with pytest.raises(ValueError, match="derived by the server"):
            streamcast.Stream.ingest("lines", source, root=tmp_path)

    @pytest.mark.parametrize("declared", [None, ("line_number", "session_id")])
    async def test_reopening_with_another_identity_is_refused(self, tmp_path, declared):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.aclose()

        with pytest.raises(ValueError, match="fixed when the stream is created"):
            streamcast.Stream.new(
                "lines", root=tmp_path, schema=SCHEMA, identity=declared
            )

        reopened = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=list(KEY)
        )
        await reopened.aclose()

    async def test_a_stream_without_one_cannot_gain_one_on_open(self, tmp_path):
        stream = streamcast.Stream.new("lines", root=tmp_path, schema=SCHEMA)
        await stream.aclose()

        with pytest.raises(ValueError, match="fixed when the stream is created"):
            streamcast.Stream.new("lines", root=tmp_path, schema=SCHEMA, identity=KEY)


class TestReading:
    async def test_select_star_leaves_it_out_and_naming_it_returns_it(
        self, tmp_path, serve
    ):
        uri = await published_stream(tmp_path, "lines", rows_of(6), serve)

        star = await streamcast.Stream.sql(uri, "SELECT * FROM log").read_all()
        named = await streamcast.Stream.sql(
            uri, 'SELECT line_number, "_streamcast_id" FROM log ORDER BY line_number'
        ).read_all()
        scanned = await streamcast.Stream.scan(uri).read_all()
        projected = await streamcast.Stream.scan(
            uri, columns=["session_id", "line_number", "_streamcast_id"]
        ).read_all()

        assert "_streamcast_id" not in star.column_names
        assert "_streamcast_id" not in scanned.column_names
        assert set(star.column_names) >= {"session_id", "line_number", "text"}
        expected = [key(f"s{i % 3}", i) for i in range(6)]
        assert named.column("_streamcast_id").to_pylist() == expected
        assert projected.column("_streamcast_id").to_pylist() == expected

    async def test_a_name_inside_a_string_or_comment_does_not_count(self):
        assert _snapshot._names('SELECT "_streamcast_id" FROM log', "_streamcast_id")
        assert _snapshot._names("select _STREAMCAST_ID from log", "_streamcast_id")
        assert _snapshot._names('SELECT "_STREAMCAST_ID" FROM log', "_streamcast_id")
        assert not _snapshot._names(
            "SELECT * FROM log -- _streamcast_id\nWHERE text = '_streamcast_id'",
            "_streamcast_id",
        )

    async def test_two_streams_join_on_the_key(self, tmp_path, serve):
        lines = await published_stream(tmp_path, "lines", rows_of(9), serve)
        notes_schema = {
            "type": "object",
            "properties": {
                "session_id": {"type": "string"},
                "line_number": {"type": "integer"},
                "note": {"type": "string"},
            },
            "required": ["session_id", "line_number", "note"],
        }
        notes = await published_stream(
            tmp_path,
            "notes",
            [
                {"session_id": "s1", "line_number": 4, "note": "four"},
                {"session_id": "s2", "line_number": 5, "note": "five"},
                {"session_id": "s0", "line_number": 4, "note": "no such line"},
            ],
            serve,
            schema=notes_schema,
        )

        keyed_lines = await streamcast.Stream.scan(
            lines, columns=["_streamcast_id", "text"]
        ).read_all()
        keyed_notes = await streamcast.Stream.scan(
            notes, columns=["_streamcast_id", "note"]
        ).read_all()
        connection = duckdb.connect()
        connection.register("keyed_lines", keyed_lines)
        connection.register("keyed_notes", keyed_notes)
        joined = (
            connection.execute(
                "SELECT text, note FROM keyed_lines JOIN keyed_notes "
                "USING (_streamcast_id) ORDER BY note"
            )
            .arrow()
            .read_all()
        )
        connection.close()

        assert joined.column("text").to_pylist() == ["line 5", "line 4"]
        assert joined.column("note").to_pylist() == ["five", "four"]

    async def test_a_key_filter_returns_the_rows_the_natural_predicate_does(
        self, tmp_path, serve
    ):
        rows = [*rows_of(9), line("s1", 4, "again")]
        uri = await published_stream(tmp_path, "lines", rows, serve)
        filters = [("session_id", "==", "s1"), ("line_number", "==", 4)]

        natural = await streamcast.Stream.scan(
            uri, where="session_id = 's1' AND line_number = 4"
        ).read_all()
        rewritten = await streamcast.Stream.scan(uri, filters=filters).read_all()

        assert natural.column("text").to_pylist() == ["line 4", "again"]
        assert rewritten.equals(natural)

    async def test_a_key_filter_is_applied_to_the_stored_key(self, tmp_path, serve):
        """The rewrite really filters on `_streamcast_id`: a row whose stored
        key disagrees with its columns — written past the writer, through
        litelink — matches the natural terms and not the key term."""
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.send(line("s1", 4, "keyed"))
        assert stream.log is not None
        stream.log.extend(
            [
                {
                    **line("s1", 4, "forged"),
                    "streamcast_ts": 0,
                    "_streamcast_id": bytes(16),
                }
            ]
        )
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        async with serve(stream, maintain=False):
            pass

        await stream.aclose()
        filters = [("session_id", "==", "s1"), ("line_number", "==", 4)]
        natural = await streamcast.Stream.scan(
            uri, where="session_id = 's1' AND line_number = 4"
        ).read_all()
        keyed = await streamcast.Stream.scan(uri, filters=filters).read_all()

        assert natural.column("text").to_pylist() == ["keyed", "forged"]
        assert keyed.column("text").to_pylist() == ["keyed"]

    async def test_the_rewrite_names_the_key_and_alone_finds_the_same_rows(
        self, tmp_path, serve
    ):
        rows = [*rows_of(9), line("s1", 4, "again")]
        uri = await published_stream(tmp_path, "lines", rows, serve)
        async with await streamcast.Stream.snapshot(uri) as snap:
            live = snap.metadata.live_log
            term = _snapshot._keyed(
                live, [("line_number", "==", 4), ("session_id", "==", "s1")]
            )
            assert term is not None
            assert term.startswith('"_streamcast_id" = ')
            # Only the key term: the stored keys are what it computes.
            alone = await snap.sql(
                f"SELECT text FROM log WHERE {term} ORDER BY litelink_offset"
            ).read_all()

            assert _snapshot._keyed(live, [("session_id", "==", "s1")]) is None
            assert (
                _snapshot._keyed(
                    live, [("session_id", "==", "s1"), ("line_number", "==", "4")]
                )
                is None
            )

        assert alone.column("text").to_pylist() == ["line 4", "again"]

    async def test_the_brokers_tail_is_keyed_too(self, tmp_path, serve):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.send_many(rows_of(3))
        assert stream.log is not None
        publish(stream.log)
        await stream.send(line("tail", 99))  # on the broker only
        uri = stream.metadata_uri
        assert uri is not None
        async with serve(stream, maintain=False) as broker:
            async with await streamcast.Stream.snapshot(
                uri, as_of_offset=LATEST, broker=broker
            ) as snap:
                star = await snap.scan().read_all()
                found = await snap.scan(
                    columns=["line_number"],
                    filters=[("session_id", "==", "tail"), ("line_number", "==", 99)],
                ).read_all()
                keys = await snap.sql(
                    "SELECT _streamcast_id FROM log ORDER BY litelink_offset"
                ).read_all()
                streamed = [row async for _offset, _ts, row in snap.rows(1)]

        await stream.aclose()
        assert "_streamcast_id" not in star.column_names
        assert found.column("line_number").to_pylist() == [99]
        assert keys.column("_streamcast_id").to_pylist()[-1] == key("tail", 99)
        # Catch-up rows are what a subscriber is sent: no key in them.
        assert all("_streamcast_id" not in row for row in streamed)

    async def test_a_live_view_keys_the_rows_it_receives(self, tmp_path, serve):
        stream = streamcast.Stream.new(
            "lines", root=tmp_path, schema=SCHEMA, identity=KEY
        )
        await stream.send_many(rows_of(2))
        assert stream.log is not None
        publish(stream.log)
        async with serve(stream, maintain=False) as broker:
            async with await streamcast.Stream.live(
                broker, where={"session_id": "late"}
            ) as live:
                offset = await stream.send(line("late", 7, "arrived"))
                await live.wait_for(offset)
                star = await live.sql("SELECT * FROM log").read_all()
                keys = await live.sql("SELECT _streamcast_id, text FROM log").read_all()

        await stream.aclose()
        assert "_streamcast_id" not in star.column_names
        assert keys.to_pylist() == [
            {"_streamcast_id": key("late", 7), "text": "arrived"}
        ]


class TestMigrate:
    V2: dict[str, Any] = {
        "type": "object",
        "properties": {**SCHEMA["properties"], "speaker": {"type": ["string", "null"]}},
        "required": SCHEMA["required"],
    }

    async def created(self, root: Path) -> None:
        stream = streamcast.Stream.new("lines", root=root, schema=SCHEMA, identity=KEY)
        await stream.send(line("a", 1))
        await stream.aclose()

    async def test_removing_an_identity_column_is_refused(self, tmp_path):
        await self.created(tmp_path)
        without = {
            "type": "object",
            "properties": {
                "session_id": {"type": "string"},
                "text": {"type": "string"},
            },
            "required": ["session_id", "text"],
        }
        with pytest.raises(ValueError, match="cannot be removed"):
            streamcast.Stream.migrate(
                "lines", root=tmp_path, schema=without, identity=KEY
            )

    async def test_retyping_an_identity_column_is_refused(self, tmp_path):
        await self.created(tmp_path)
        retyped = {
            "type": "object",
            "properties": {
                **SCHEMA["properties"],
                "line_number": {"type": "integer", "format": "int32"},
            },
            "required": SCHEMA["required"],
        }
        with pytest.raises(ValueError, match="changes the type of \\['line_number'\\]"):
            streamcast.Stream.migrate(
                "lines", root=tmp_path, schema=retyped, identity=KEY
            )

    @pytest.mark.parametrize("declared", [None, ("session_id",)])
    async def test_changing_the_identity_is_refused(self, tmp_path, declared):
        await self.created(tmp_path)
        with pytest.raises(ValueError, match="fixed for the life"):
            streamcast.Stream.migrate(
                "lines", root=tmp_path, schema=self.V2, identity=declared
            )

        # Nothing moved: the stream is still its one log.
        assert _metadata.load(_metadata.home(tmp_path, "lines"), "lines") is None

    async def test_the_next_log_is_keyed_alike(self, tmp_path, serve):
        await self.created(tmp_path)
        stream = streamcast.Stream.migrate(
            "lines", root=tmp_path, schema=self.V2, identity=KEY
        )
        await stream.send({**line("b", 2), "speaker": "x"})
        assert stream.log is not None
        publish(stream.log)
        uri = stream.metadata_uri
        assert uri is not None
        async with serve(stream, maintain=False):
            pass

        await stream.aclose()
        home = _metadata.home(tmp_path, "lines")
        _name, found, _manifest = _versions.load(home, "lines")
        assert found is not None
        assert [entry.identity for entry in found.logs] == [KEY, KEY]
        assert _metadata.shape(found.live_log).field("_streamcast_id").metadata == {
            _identity.DECLARATION: b'["session_id", "line_number"]'
        }

        keys = await streamcast.Stream.sql(
            uri, "SELECT _streamcast_id FROM log ORDER BY litelink_offset"
        ).read_all()
        assert keys.column("_streamcast_id").to_pylist() == [key("a", 1), key("b", 2)]
