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
from litelink import ColumnStatistics, TierStatistics
from litelink import manifest as _litelink

from streamcast import _manifest
from streamcast._manifest import prune

POOL: dict[str, pa.DataType] = {
    "i32": pa.int32(),
    "i64": pa.int64(),
    "f32": pa.float32(),
    "f64": pa.float64(),
    "flag": pa.bool_(),
}
NO_SYSTEM: dict[str, Any] = {"type": "object", "properties": {}, "required": []}


# -- statistics the way litelink#85 reports them -------------------------------


def statistics(table: pa.Table) -> TierStatistics:
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

    return TierStatistics(
        tier=None, record_count=table.num_rows, file_count=1, columns=found
    )


def entry(
    name: str, start: int, table: pa.Table, stats: TierStatistics | None = None
) -> _litelink.Entry:
    """A sealed log holding `table`, as the manifest entry `migrate` writes."""
    return _manifest.entry(
        name,
        start,
        start + table.num_rows,
        table.schema,
        statistics(table) if stats is None else stats,
    )


def build(entries: list[_litelink.Entry]) -> pa.Table:
    return _litelink.build(entries, key=_manifest.KEY)


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
#
# A table covers what it lists. Combinations nobody listed — many logs with
# mixed columns, three or more terms, values anywhere in a type's range — are
# what a seeded generative test would add beside it: #54.

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
    # Not exact in binary: a float32 column stores 0.1 as 0.100000001490116,
    # so its bounds and a double predicate of 0.1 differ in the last place.
    "point_one": [0.1],
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
AGAINST_FLOATS: list[object] = [
    *AGAINST_NUMBERS,
    NAN,
    INF,
    -INF,
    -0.0,
    0.0,
    0.1,
    # 0.1 as a float32 holds it, as a double.
    float(pa.array([0.1], type=pa.float32())[0].as_py()),
]
AGAINST_FLAGS: list[object] = [None, True, False]


def extremes(low: float, high: float) -> tuple[list[object], list[object]]:
    """A column at a type's limits, and values at and just past them."""
    return [low, high], [low - 1, low, high, high + 1]


I32_LOW, I32_HIGH = -(2**31), 2**31 - 1
I64_LOW, I64_HIGH = -(2**63), 2**63 - 1
F32_MAX, F32_TINY = 3.4028234663852886e38, 1.401298464324817e-45  # max, least subnormal
F64_MAX, F64_TINY = 1.7976931348623157e308, 5e-324

KINDS: dict[str, tuple[pa.DataType, dict[str, list[object] | None], list[object]]] = {
    "int32": (
        pa.int32(),
        {**NUMBERS, "limits": extremes(I32_LOW, I32_HIGH)[0]},
        [*AGAINST_NUMBERS, *extremes(I32_LOW, I32_HIGH)[1]],
    ),
    "int64": (
        pa.int64(),
        {**NUMBERS, "limits": extremes(I64_LOW, I64_HIGH)[0]},
        [*AGAINST_NUMBERS, *extremes(I64_LOW, I64_HIGH)[1]],
    ),
    "float32": (
        pa.float32(),
        {**FLOATS, "limits": [-F32_MAX, F32_MAX], "tiny": [F32_TINY]},
        [*AGAINST_FLOATS, -F32_MAX, F32_MAX, F32_TINY, F64_MAX],
    ),
    "float64": (
        pa.float64(),
        {**FLOATS, "limits": [-F64_MAX, F64_MAX], "tiny": [F64_TINY]},
        [*AGAINST_FLOATS, -F64_MAX, F64_MAX, F64_TINY],
    ),
    "bool": (pa.bool_(), FLAGS, AGAINST_FLAGS),
}

# `!=` is not an operator the pruner decides on; it must include, not guess.
COMPARISONS = ("==", "!=", "<", "<=", ">", ">=")


def predicates(against: list[object], column: str = "x") -> list[_manifest.Term]:
    """Every operator against every value, and `in` lists that straddle bounds."""
    terms: list[_manifest.Term] = [
        (column, operator, value) for operator in COMPARISONS for value in against
    ]
    singles = [[value] for value in against]
    pairs = [[a, b] for index, a in enumerate(against) for b in against[index + 1 :]]
    terms += [(column, "in", values) for values in [[], *singles, *pairs]]
    return terms


# One connection for every table: opening one costs ~110 ms.
_DUCKDB = duckdb.connect()


def condition(table: pa.Table, term: _manifest.Term) -> str:
    """One term as SQL, with a column the log lacks read as NULL."""
    column, operator, value = term
    reference = f'"{column}"' if column in table.column_names else "NULL"
    if operator == "in":
        options = ", ".join(literal(v) for v in value) or "NULL"  # ty: ignore[not-iterable]
        return f"{reference} IN ({options})"

    sql = "<>" if operator == "!=" else operator.replace("==", "=")
    return f"{reference} {sql} {literal(value)}"


def counts(table: pa.Table, predicates: list[list[_manifest.Term]]) -> list[int]:
    """How many rows DuckDB says match each predicate.

    DuckDB decides every term on every row — NaN's ordering, NULL's — in one
    query per table, a plain `SELECT` of each distinct term's condition. A
    predicate's terms are then combined per row: a row matches when every
    term is TRUE on it, which is what a `WHERE` of their `AND` counts.
    Measured against one `FILTER` aggregate per predicate, which planned a
    930-aggregate query per log in ~1.7 s.
    """
    _DUCKDB.register("arrow_log", table)
    _DUCKDB.execute("CREATE OR REPLACE TABLE log AS SELECT * FROM arrow_log")
    _DUCKDB.unregister("arrow_log")
    distinct = list(dict.fromkeys(repr(t) for terms in predicates for t in terms))
    terms_by_key = {repr(t): t for terms in predicates for t in terms}
    columns = ", ".join(
        f"coalesce({condition(table, terms_by_key[key])}, FALSE)" for key in distinct
    )
    rows = (
        _DUCKDB.sql(f"SELECT {columns} FROM log").fetchall() if table.num_rows else []
    )
    position = {key: index for index, key in enumerate(distinct)}
    return [
        sum(all(row[position[repr(t)]] for t in terms) for row in rows)
        for terms in predicates
    ]


def excluded_matches(
    tables: dict[str, pa.Table], predicates: list[list[_manifest.Term]]
) -> list[str]:
    """Every (log, predicate) the pruner excludes although DuckDB finds a row."""
    sealed, start = [], 1
    for name, table in tables.items():
        sealed.append(entry(name, start, table))
        start += max(table.num_rows, 1)

    manifest = build(sealed)
    names = [log.name for log in sealed]
    truth = {name: counts(table, predicates) for name, table in tables.items()}
    wrong = []
    for index, terms in enumerate(predicates):
        kept = set(prune(manifest, names, terms))
        wrong += [
            f"{name}: pruned on {terms}, but {truth[name][index]} row(s) match"
            for name in names
            if name not in kept and truth[name][index]
        ]

    return wrong


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_an_excluded_log_never_holds_a_match(kind):
    arrow, contents, against = KINDS[kind]
    tables = {
        name: pa.table({"x": pa.array(cells, type=arrow)})
        if cells is not None
        else pa.table({"y": pa.array([1, 2], type=pa.int64())})
        for name, cells in contents.items()
    }
    wrong = excluded_matches(tables, [[term] for term in predicates(against)])
    assert not wrong, "\n".join(wrong[:20])


def test_terms_over_two_columns_are_anded_soundly():
    """Every pair of terms on two columns, against logs that differ in each.

    A log is excluded when ANY term rules it out, so a pair is where one
    sound term and one wrong one meet, and where a column one log lacks
    meets one it has.
    """
    tables = {
        "both": pa.table({"x": [1, 3], "z": [1.0, 3.0]}),
        "x_nulls": pa.table({"x": pa.array([None, 2], pa.int64()), "z": [2.0, 2.0]}),
        "z_nan": pa.table({"x": [1, 3], "z": [1.0, NAN]}),
        "no_z": pa.table({"x": [1, 3]}),
        "empty": pa.table(
            {"x": pa.array([], pa.int64()), "z": pa.array([], pa.float64())}
        ),
    }
    x_terms = [t for t in predicates([None, 0, 1, 3, 4], "x") if t[1] != "in"]
    z_terms = [t for t in predicates([NAN, 0.0, 1.0, 3.0, 4.0], "z") if t[1] != "in"]
    pairs = [[a, b] for a in [*x_terms, ("x", "in", [0, 3])] for b in z_terms]
    wrong = excluded_matches(tables, pairs)
    assert not wrong, "\n".join(wrong[:20])


# -- the rules, one at a time -----------------------------------------------------


def one(table: pa.Table, name: str = "log0") -> _litelink.Entry:
    return entry(name, 1, table)


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
        stats = TierStatistics(
            tier=None,
            file_count=1,
            record_count=2,
            columns={"x": ColumnStatistics(1.0, 2.0, 0, 2, nan_count=None)},
        )
        table = pa.table({"x": pa.array([1.0, 2.0])})
        manifest = build([entry("log0", 1, table, stats)])

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

    def test_one_from_before_the_versions_is_read(self, tmp_path):
        """A stream from before the versions kept it beside its metadata,
        unversioned; a version names its own (`test_versions`, `test_migrate`)."""
        import pyarrow.parquet as pq

        manifest = build([one(pa.table({"x": pa.array([1, 2])}))])
        pq.write_table(manifest, tmp_path / _manifest.name("trades"))

        assert _manifest.load(tmp_path, "trades") == manifest
        assert _manifest.load(tmp_path, "other") is None
