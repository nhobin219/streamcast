"""Manifest pruning: skipping a log must never skip a match.

The one property that matters is checked against DuckDB, the engine a reader
queries with: **every log `prune` excludes holds no row matching the
predicate.** Including a log that holds nothing is a wasted scan and passes;
excluding one that holds a match is a wrong answer with no symptom and fails.

**Every case, not a sample.** The space the pruner decides over is small —
type, what the statistics say, operator, where the value sits against the
bounds — so it is enumerated, and each case is checked on every run. Measured
against planted bugs, a random sample of 100 cases caught a bound-equality bug
in about half its runs; the enumeration catches it in every run.

**It includes NaN and infinity on purpose.** litelink refuses both
(litelink#87), so no real log holds one; the pruner's NaN rule is a defence,
and a defence nothing exercises is one nobody would notice breaking.
"""

from __future__ import annotations

import math
from typing import Any

import duckdb
import pyarrow as pa
import pytest

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


# -- every case ------------------------------------------------------------------
#
# The pruner's decision for one log depends on a small space: the column's
# type, what its statistics say (no rows, all NULL, bounds, NaN, infinities),
# the operator, and where the value sits against the bounds: below, at the
# minimum, inside, at the maximum, above, or NULL. That space is enumerated
# here, every case, so a wrong comparison at a bound fails every run rather
# than when a random draw happens to land on it.

INF, NAN = math.inf, math.nan

# One log per content, all in one manifest per type. `None` is a log that lacks
# the column, which `UNION ALL BY NAME` reads as NULL.
NUMBERS: dict[str, list[object] | None] = {
    "empty": [],
    "nulls": [None, None],
    "one": [2],
    "span": [1, 3],
    "span_null": [1, None, 3],
    "absent": None,
}
FLOATS: dict[str, list[object] | None] = {
    **NUMBERS,
    "span_nan": [1.0, 3.0, NAN],
    "nan": [NAN],
    "nan_null": [NAN, None],
    "up_to_inf": [1.0, INF],
    "from_minus_inf": [-INF, 3.0],
    "zeros": [-0.0, 0.0],
}
FLAGS: dict[str, list[object] | None] = {
    "empty": [],
    "nulls": [None],
    "true": [True],
    "false": [False],
    "both": [True, False],
    "true_null": [True, None],
    "absent": None,
}

# Below, at the minimum, inside (whole and not), at the maximum, above.
AGAINST_NUMBERS: list[object] = [None, 0, 1, 2, 2.5, 3, 4]
AGAINST_FLOATS: list[object] = [*AGAINST_NUMBERS, NAN, INF, -INF, -0.0, 0.0]
AGAINST_FLAGS: list[object] = [None, True, False]

# `!=` is not an operator the pruner decides on; it must include, not guess.
COMPARISONS = ("==", "!=", "<", "<=", ">", ">=")

KINDS: dict[str, tuple[pa.DataType, dict[str, list[object] | None], list[object]]] = {
    "int32": (pa.int32(), NUMBERS, AGAINST_NUMBERS),
    "int64": (pa.int64(), NUMBERS, AGAINST_NUMBERS),
    "float32": (pa.float32(), FLOATS, AGAINST_FLOATS),
    "float64": (pa.float64(), FLOATS, AGAINST_FLOATS),
    "bool": (pa.bool_(), FLAGS, AGAINST_FLAGS),
}


def predicates(against: list[object]) -> list[_manifest.Term]:
    """Every operator against every value, and `in` lists that straddle bounds."""
    terms: list[_manifest.Term] = [
        ("x", operator, value) for operator in COMPARISONS for value in against
    ]
    singles = [[value] for value in against]
    pairs = [[a, b] for index, a in enumerate(against) for b in against[index + 1 :]]
    terms += [("x", "in", values) for values in [[], *singles, *pairs]]
    return terms


# One connection for every table: opening one costs ~110 ms.
_DUCKDB = duckdb.connect()


def counts(table: pa.Table, terms: list[_manifest.Term]) -> list[int]:
    """How many rows DuckDB says match each term, in one query for the table.

    A native table, for the reason `matching` gives. One query with a
    `FILTER` per term rather than one per term: the space is a few thousand
    cases, and a query each would be most of this file's run time.
    """
    _DUCKDB.register("arrow_log", table)
    _DUCKDB.execute("CREATE OR REPLACE TABLE log AS SELECT * FROM arrow_log")
    _DUCKDB.unregister("arrow_log")
    reference = '"x"' if "x" in table.column_names else "NULL"
    filters = []
    for _, operator, value in terms:
        if operator == "in":
            options = ", ".join(literal(v) for v in value) or "NULL"  # ty: ignore[not-iterable]
            condition = f"{reference} IN ({options})"
        else:
            sql = "<>" if operator == "!=" else operator.replace("==", "=")
            condition = f"{reference} {sql} {literal(value)}"

        filters.append(f"count(*) FILTER (WHERE {condition})")

    return list(_DUCKDB.sql(f"SELECT {', '.join(filters)} FROM log").fetchone())  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_an_excluded_log_never_holds_a_match(kind):
    arrow, contents, against = KINDS[kind]
    tables = {
        name: pa.table({"x": pa.array(cells, type=arrow)})
        if cells is not None
        else pa.table({"y": pa.array([1, 2], type=pa.int64())})
        for name, cells in contents.items()
    }
    sealed, start = [], 1
    for name, table in tables.items():
        sealed.append((entry(name, start, table), statistics(table)))
        start += max(table.num_rows, 1)

    manifest = build(sealed)
    names = [log.name for log, _ in sealed]
    terms = predicates(against)
    truth = {name: counts(table, terms) for name, table in tables.items()}

    wrong = []
    for index, term in enumerate(terms):
        kept = set(prune(manifest, names, [term]))
        wrong += [
            f"{name} {contents[name]!r}: pruned on {term}, but {truth[name][index]} row(s) match"
            for name in names
            if name not in kept and truth[name][index]
        ]

    assert not wrong, "\n".join(wrong[:20])


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
