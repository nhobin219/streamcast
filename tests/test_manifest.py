"""Manifest pruning: skipping a log must never skip a match.

The one property that matters is checked against DuckDB, the engine a reader
queries with: **every log `prune` excludes holds no row matching the
predicate.** Including a log that holds nothing is a wasted scan and passes;
excluding one that holds a match is a wrong answer with no symptom and fails.
The generator covers what a fixed table of cases forgets — NULLs, all-null
columns, and logs that lack a column entirely.

**It generates NaN and infinity on purpose.** litelink refuses both
(litelink#87), so no real log holds one; the pruner's NaN rule is a defence,
and a defence nothing exercises is one nobody would notice breaking.
"""

from __future__ import annotations

import math
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from streamcast import _manifest, _schema
from streamcast._manifest import ColumnStatistics, LogStatistics, build, prune
from streamcast._metadata import Entry

POOL: dict[str, pa.DataType] = {
    "i32": pa.int32(),
    "i64": pa.int64(),
    "f32": pa.float32(),
    "f64": pa.float64(),
    "flag": pa.bool_(),
}
NO_SYSTEM: dict[str, Any] = {"type": "object", "properties": {}, "required": []}


# -- statistics the way litelink#85 reports them -------------------------------


def statistics(table: pa.Table) -> LogStatistics:
    """Min and max over the non-null, non-NaN values, as Iceberg's bounds are.

    Plain Python rather than `pyarrow.compute`, so the stand-in for litelink#85
    does not share pyarrow's own ideas about NaN with the code it checks.
    """
    found = {}
    for name in table.column_names:
        cells = table[name].to_pylist()
        floating = pa.types.is_floating(table[name].type)
        nans = sum(1 for c in cells if isinstance(c, float) and math.isnan(c))
        real = [
            c
            for c in cells
            if c is not None and not (isinstance(c, float) and math.isnan(c))
        ]
        found[name] = ColumnStatistics(
            min=min(real) if real else None,
            max=max(real) if real else None,
            null_count=sum(1 for c in cells if c is None),
            value_count=len(cells),
            nan_count=nans if floating else None,
        )

    return LogStatistics(record_count=table.num_rows, columns=found)


def entry(name: str, start: int, table: pa.Table) -> Entry:
    fields = [pa.field(f.name, f.type, nullable=True) for f in table.schema]
    return Entry(
        name,
        start,
        start + table.num_rows,
        _schema.from_arrow(pa.schema(fields)),
        NO_SYSTEM,
    )


# -- what DuckDB says ------------------------------------------------------------


def literal(value: object) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, float):
        if math.isnan(value):
            return "'nan'::DOUBLE"

        if math.isinf(value):
            return "'inf'::DOUBLE" if value > 0 else "'-inf'::DOUBLE"

        return f"{value!r}::DOUBLE"

    return str(value)


def matching(table: pa.Table, terms: list[_manifest.Term]) -> int:
    """How many rows of `table` DuckDB says match every term.

    A column the log lacks is NULL, which is what `UNION ALL BY NAME` makes of
    it — and a NULL matches no comparison.
    """
    clauses = []
    for column, operator, value in terms:
        reference = f'"{column}"' if column in table.column_names else "NULL"
        if operator == "in":
            options = ", ".join(literal(v) for v in value)  # ty: ignore[not-iterable]
            clauses.append(f"{reference} IN ({options})")
        else:
            sql = "<>" if operator == "!=" else operator.replace("==", "=")
            clauses.append(f"{reference} {sql} {literal(value)}")

    # **A native table, which reads every row.** DuckDB compares NaN above
    # every float in any row it reads, but a scan that skips on statistics —
    # a registered Arrow table, `read_parquet`, `iceberg_scan` — may never
    # read a NaN row, because those statistics leave NaN out. Measured on
    # 1.5.5: whether `x > 50` returns a stored NaN depends on what else is in
    # its file. The native answer is the most inclusive, so it is the one a
    # sound pruner has to agree with.
    connection = duckdb.connect()
    connection.register("arrow_log", table)
    connection.execute("CREATE TABLE log AS SELECT * FROM arrow_log")
    where = " AND ".join(clauses) or "TRUE"
    return connection.sql(f"SELECT count(*) FROM log WHERE {where}").fetchone()[0]  # ty: ignore[not-subscriptable]


# -- generation ------------------------------------------------------------------


def values_for(kind: pa.DataType) -> st.SearchStrategy[object]:
    if kind == pa.bool_():
        return st.booleans()

    if kind == pa.int32():
        return st.integers(-(2**31), 2**31 - 1)

    if kind == pa.int64():
        return st.integers(-(2**63), 2**63 - 1)

    if kind == pa.float32():
        return st.floats(width=32, allow_nan=True, allow_infinity=True)

    return st.floats(allow_nan=True, allow_infinity=True)


def small_values_for(kind: pa.DataType, centre: int = 0) -> st.SearchStrategy[object]:
    """Values that land near each other, so bounds and predicates overlap.

    Tight on purpose: a pruner only gets the chance to be wrong when it
    excludes something, and wide random values almost never let it.
    """
    if kind == pa.bool_():
        return st.booleans()

    if pa.types.is_integer(kind):
        return st.integers(centre - 1, centre + 1)

    return st.one_of(
        st.integers(centre - 1, centre + 1).map(float),
        st.integers(centre - 1, centre + 1).map(float),
        st.sampled_from([math.nan, math.inf, -math.inf, -0.0]),
    )


@st.composite
def column(draw: st.DrawFn, kind: pa.DataType, rows: int, centre: int) -> pa.Array:
    shape = draw(st.sampled_from(["mostly", "mostly", "mostly", "all_null", "wide"]))
    if shape == "all_null":
        return pa.array([None] * rows, type=kind)

    pick = values_for(kind) if shape == "wide" else small_values_for(kind, centre)
    # NULLs present but rare, so the bounds come from real values.
    cell = st.one_of(pick, pick, pick, pick, st.none())
    return pa.array(draw(st.lists(cell, min_size=rows, max_size=rows)), type=kind)


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[list[pa.Table], list[_manifest.Term]]:
    """Logs and a predicate drawn together, so terms mostly name real columns."""
    tables = []
    centres = []
    for _ in range(draw(st.integers(1, 4))):
        names = draw(st.lists(st.sampled_from(list(POOL)), min_size=1, unique=True))
        rows = draw(st.integers(0, 8))
        # Each log in its own narrow band, as sealed logs are — different
        # hours, different prices — so bounds differ and predicates exclude.
        centre = draw(st.integers(-6, 6))
        centres.append(centre)
        tables.append(pa.table({n: draw(column(POOL[n], rows, centre)) for n in names}))

    present = sorted({n for t in tables for n in t.column_names})
    predicate: list[_manifest.Term] = []
    for _ in range(draw(st.integers(1, 3))):
        name = draw(st.sampled_from([*present, *present, *present, "absent"]))
        kind = POOL.get(name, pa.int64())
        operator = draw(st.sampled_from([*sorted(_manifest.OPERATORS), "!="]))

        # None rare: it can never prune, so it mostly just hides the others.
        # Mostly from a log's own band, so the value falls INSIDE some log's
        # bounds: that is where a wrong bound comparison excludes a match.
        # Sometimes from anywhere, so values outside every band occur too.
        # **Where a wrong rule shows.** A value inside some log's band is
        # where a bound compared the wrong way excludes a match; one just past
        # the edge of a band is where a max that ignores NaN does. Each `in`
        # value draws its own place, so a list can straddle a log's bounds.
        def near() -> int:
            centre = draw(st.sampled_from(centres))
            return draw(
                st.sampled_from([centre, centre, centre - 2, centre + 2])
                if draw(st.integers(0, 3))
                else st.integers(-6, 6)
            )

        def value_for(kind: pa.DataType = kind) -> object:
            if draw(st.integers(0, 6)) == 0:
                return None

            return draw(small_values_for(kind, near()))

        value: object = (
            [value_for() for _ in range(draw(st.integers(1, 3)))]
            if operator == "in"
            else value_for()
        )
        predicate.append((name, operator, value))

    return tables, predicate


# -- the property ----------------------------------------------------------------


@settings(
    max_examples=600,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(scenario=scenarios())
def test_an_excluded_log_never_holds_a_match(scenario):
    tables, predicate = scenario
    sealed = []
    start = 1
    for index, table in enumerate(tables):
        sealed.append((entry(f"log{index}", start, table), statistics(table)))
        start += max(table.num_rows, 1)

    manifest = build(sealed)
    kept = set(prune(manifest, [e.name for e, _ in sealed], predicate))

    for (log, _), table in zip(sealed, tables, strict=True):
        if log.name not in kept:
            assert matching(table, predicate) == 0, (
                f"{log.name} was pruned but holds a match for {predicate}"
            )


# -- the rules, one at a time -----------------------------------------------------


def one(table: pa.Table, name: str = "log0") -> tuple[Entry, LogStatistics]:
    return entry(name, 1, table), statistics(table)


class TestItPrunes:
    def test_bounds_that_cannot_match_exclude_the_log(self):
        cheap = pa.table({"price": pa.array([1.0, 5.0, 9.0])})
        dear = pa.table({"price": pa.array([100.0, 150.0])})
        manifest = build([one(cheap, "cheap"), one(dear, "dear")])

        assert prune(manifest, ["cheap", "dear"], [("price", ">", 50.0)]) == ["dear"]
        assert prune(manifest, ["cheap", "dear"], [("price", "<", 50.0)]) == ["cheap"]
        assert prune(manifest, ["cheap", "dear"], [("price", "==", 5.0)]) == ["cheap"]
        assert prune(manifest, ["cheap", "dear"], [("price", "in", [7.0, 120.0])]) == [
            "cheap",
            "dear",
        ]

    def test_terms_are_anded(self):
        table = pa.table({"a": pa.array([1, 2]), "b": pa.array([10, 20])})
        manifest = build([one(table)])

        assert prune(manifest, ["log0"], [("a", "==", 1), ("b", "==", 99)]) == []


class TestItIncludesWhatItCannotDecide:
    def test_a_float_column_with_a_nan_is_never_pruned(self):
        """Iceberg's max excludes NaN; DuckDB says NaN > 5."""
        table = pa.table({"x": pa.array([1.0, math.nan])})
        manifest = build([one(table)])

        assert matching(table, [("x", ">", 5.0)]) == 1
        assert prune(manifest, ["log0"], [("x", ">", 5.0)]) == ["log0"]

    def test_an_unknown_nan_count_is_a_maybe(self):
        """What every pyiceberg-written file reports: `nan_value_count` None.

        Its bounds say [1, 2], which would exclude `x > 50` — but a NaN
        beside those values would match, and nothing says there is none.
        """
        stats = LogStatistics(
            record_count=2,
            columns={"x": ColumnStatistics(1.0, 2.0, 0, 2, nan_count=None)},
        )
        table = pa.table({"x": pa.array([1.0, 2.0])})
        manifest = build([(entry("log0", 1, table), stats)])

        assert prune(manifest, ["log0"], [("x", ">", 50.0)]) == ["log0"]

    def test_an_all_null_column_has_no_bounds_to_prune_on(self):
        table = pa.table({"x": pa.array([None, None], type=pa.int64())})
        manifest = build([one(table)])

        assert prune(manifest, ["log0"], [("x", "==", 3)]) == ["log0"]

    def test_a_sealed_log_with_no_row_is_included(self):
        manifest = build([one(pa.table({"x": pa.array([1])}), "known")])

        assert prune(manifest, ["known", "unknown"], [("x", "==", 99)]) == ["unknown"]

    def test_a_log_lacking_the_column_is_included(self):
        # Its struct is NULL: no statistics. Sound to include; pruning it on
        # the union's all-NULL reading is #27's open question, not this.
        with_x = pa.table({"x": pa.array([1])})
        without = pa.table({"y": pa.array([1])})
        manifest = build([one(with_x, "with"), one(without, "without")])

        assert prune(manifest, ["with", "without"], [("x", "==", 99)]) == ["without"]

    def test_an_unknown_column_operator_or_value_includes(self):
        manifest = build([one(pa.table({"x": pa.array([1, 2])}))])

        assert prune(manifest, ["log0"], [("nope", "==", 99)]) == ["log0"]
        assert prune(manifest, ["log0"], [("x", "!=", 1)]) == ["log0"]
        assert prune(manifest, ["log0"], [("x", "==", None)]) == ["log0"]
        assert prune(manifest, ["log0"], [("x", "==", "a string")]) == ["log0"]
        assert prune(manifest, ["log0"], [("x", "in", 3)]) == ["log0"]

    def test_no_manifest_includes_every_sealed_log(self):
        assert prune(None, ["a", "b"], [("x", "==", 1)]) == ["a", "b"]


class TestTheReadersListIsTheAuthority:
    def test_a_row_for_a_log_the_reader_does_not_list_is_ignored(self):
        """A migration landed between reading metadata.json and the manifest."""
        manifest = build(
            [
                one(pa.table({"x": pa.array([1])}), "a"),
                one(pa.table({"x": pa.array([1])}), "b"),
            ]
        )

        assert prune(manifest, ["a"], []) == ["a"]

    def test_the_order_is_the_readers(self):
        manifest = build(
            [
                one(pa.table({"x": pa.array([1])}), "a"),
                one(pa.table({"x": pa.array([1])}), "b"),
            ]
        )

        assert prune(manifest, ["b", "a"], []) == ["b", "a"]


class TestTheFile:
    def test_only_prunable_columns_get_statistics(self):
        table = pa.table({"x": pa.array([1]), "tag": pa.array(["a"])})
        manifest = build([one(table)])

        assert "x" in manifest.column_names
        assert "tag" not in manifest.column_names, "string bounds are truncated"

    def test_it_round_trips_on_disk(self, tmp_path):
        manifest = build([one(pa.table({"x": pa.array([1, 2])}))])
        path = _manifest.save(tmp_path, "trades", manifest)

        assert path.name == "trades.manifest.parquet"
        assert _manifest.load(tmp_path, "trades") == manifest
        assert _manifest.load(tmp_path, "other") is None

    @pytest.mark.replication
    def test_it_publishes_beside_the_metadata(self, s3, bucket):
        import pyarrow.parquet as pq

        from streamcast import _remote

        manifest = build([one(pa.table({"x": pa.array([1, 2])}))])
        _manifest.publish(bucket, "trades", manifest, s3)

        filesystem, key = _remote._filesystem(f"{bucket}/trades.manifest.parquet", s3)
        with filesystem.open_input_file(key) as source:
            assert pq.read_table(source) == manifest
