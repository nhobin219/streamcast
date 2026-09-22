"""JSON Schema in, Arrow out — and back again for the greeting.

**The conversion belongs here, not in litelink.** litelink speaks Arrow and is
deliberately format-agnostic about what it stores; streamcast is specifically
about JSON websockets. The layer that maps one onto the other sits on the side
that knows about JSON, and putting it in litelink would make a JSON codec part
of the public surface of a library whose value is being general.

What that buys is an import list of one — a caller declares columns without
reaching for `pyarrow` — and a stream that can publish its own shape, which is
what a subscriber in another language actually wants:

    {"type": "object",
     "properties": {"event_ts": {"type": "integer"},
                    "price":    {"type": "number"},
                    "side":     {"type": "integer", "format": "int32"}},
     "required": ["event_ts", "price", "side"]}

**JSON Schema does not say enough, which is the whole difficulty.** `integer`
does not choose between int32 and int64, and `number` does not choose between
float32 and float64 — so `format` carries the width, using the names JSON
Schema already reserves for it (`int32`, `int64`, `float`, `double`). Left out,
the wider of each pair is chosen: a feed that overflows an int32 is a silent
wrong answer, and a feed that would have fitted one costs four bytes a row.

**`required` is about PRESENCE, not nullability**, and conflating the two is
the easy mistake here. Checked against a real validator rather than believed —
`tests/test_schema.py` asserts this table against `jsonschema`, which rejects
a null by the `type` keyword and an absence by `required`:

    schema                      {"c": "x"}   {"c": null}   {}
    required + "string"         valid        INVALID type  INVALID required
    required + ["string","null"] valid       valid         INVALID required
    optional + "string"         valid        INVALID type  valid
    optional + ["string","null"] valid       valid         valid

Arrow has two states, not four: a column is nullable or it is not, and there
is no "absent" — a row that omits a column stores NULL for it. So

    nullable = (not in `required`) or ("null" in its type)

and three of those four rows map exactly. The fourth, **optional with a
non-null type, is refused** rather than widened. It means "may be absent, but
never null when present", which this cannot express: an absent key IS a null
here. Accepting it would make streamcast take rows the declared schema
rejects, and a schema that means something other than what it says is the
failure this library has already been bitten by. The message names both fixes
— mark it required, or add `"null"` to its type.

The consequence is a clean rule: **every property is either required with a
plain type, or nullable through its type.** And every schema this publishes is
one it would accept, because `from_arrow` emits the null union.

**It refuses up front what litelink would refuse at the first append.** A
schema with a `date-time` or a nested object is rejected here, where the
message can name JSON Schema's own vocabulary, rather than inside `litelink.new`
where it names Arrow's.

One caveat this module cannot fix, only document: **JSON integers beyond 2^53
do not survive every parser.** msgspec and Python carry int64 exactly, but a
JavaScript subscriber silently rounds — so a nanosecond `event_ts` is past it
and a microsecond one, which is what the examples use, is not.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Mapping

# `(type, format)` to Arrow. `format` is where the width lives, because JSON
# Schema's `integer` and `number` do not carry one — and these are the format
# names JSON Schema already reserves, rather than a vocabulary invented here.
_TO_ARROW: Final[dict[tuple[str, str | None], pa.DataType]] = {
    ("boolean", None): pa.bool_(),
    ("integer", None): pa.int64(),
    ("integer", "int32"): pa.int32(),
    ("integer", "int64"): pa.int64(),
    ("number", None): pa.float64(),
    ("number", "float"): pa.float32(),
    ("number", "double"): pa.float64(),
    ("string", None): pa.string(),
}

# The inverse, and it is not simply `_TO_ARROW` reversed: the widths are stated
# explicitly on the way out, so a schema that round-trips is a schema whose
# reader is told which one it got. An `integer` published without a format
# would be read back as int64 and be wrong for an int32 column.
#
# A nullable column is published as a UNION with "null", not merely left out
# of `required`: a subscriber validating against this needs to know the value
# may be null, and `required` does not say that (see the module docstring).
_FROM_ARROW: Final[list[tuple[object, dict[str, str]]]] = [
    (pa.types.is_boolean, {"type": "boolean"}),
    (pa.types.is_int32, {"type": "integer", "format": "int32"}),
    (pa.types.is_int64, {"type": "integer", "format": "int64"}),
    (pa.types.is_float32, {"type": "number", "format": "float"}),
    (pa.types.is_float64, {"type": "number", "format": "double"}),
    (pa.types.is_string, {"type": "string"}),
    (pa.types.is_large_string, {"type": "string"}),
]

# Refused with the reason rather than with a lookup failure. Each of these is
# something a JSON Schema may legitimately say and this stream cannot store,
# so the message names what litelink would have said one layer down.
_REASONS: Final[dict[str, str]] = {
    "object": "nested objects are not a column; flatten it, or send it as a JSON string",
    "array": "arrays are not a column; flatten it, or send it as a JSON string",
    "null": "a null-only column carries nothing; give it a type and leave it out of `required`",
}

_FORMATS: Final[dict[str, str]] = {
    "date-time": "litelink stores epoch integers, not temporal types — use "
    '{"type": "integer"} and say microseconds in your own docs',
    "date": "litelink stores epoch integers, not temporal types",
    "time": "litelink stores epoch integers, not temporal types",
    "byte": 'litelink refuses binary columns; use {"type": "string"}',
    "binary": 'litelink refuses binary columns; use {"type": "string"}',
}


def _field(name: str, spec: Mapping[str, object], *, required: bool) -> pa.Field:
    if not isinstance(spec, dict):
        msg = f"column {name!r}: expected a JSON Schema object, got {spec!r}"
        raise TypeError(msg)

    declared = spec.get("type")
    # The two signals are separate: `required` is about the KEY being present,
    # and `"null"` in the type is about the VALUE. Arrow has one bit for both.
    accepts_null = isinstance(declared, list) and "null" in declared
    if isinstance(declared, list):
        rest = [entry for entry in declared if entry != "null"]
        if len(rest) != 1:
            msg = (
                f"column {name!r}: a union of {declared} is not a column; "
                f"exactly one type, optionally with 'null'"
            )
            raise TypeError(msg)

        declared = rest[0]

    if not isinstance(declared, str):
        msg = f"column {name!r}: missing a 'type'"
        raise TypeError(msg)

    if declared in _REASONS:
        msg = f"column {name!r}: {_REASONS[declared]}"
        raise TypeError(msg)

    fmt = spec.get("format")
    if isinstance(fmt, str) and fmt in _FORMATS:
        msg = f"column {name!r}: format {fmt!r} — {_FORMATS[fmt]}"
        raise TypeError(msg)

    try:
        arrow = _TO_ARROW[(declared, fmt if isinstance(fmt, str) else None)]
    except KeyError:
        known = sorted({f'"{t}"' for t, _ in _TO_ARROW})
        detail = f" with format {fmt!r}" if fmt else ""
        msg = (
            f"column {name!r}: type {declared!r}{detail} is not a column type. "
            f"Types: {', '.join(known)}; formats: int32, int64, float, double."
        )
        raise TypeError(msg) from None

    if not required and not accepts_null:
        # "May be absent, but never null when present" — which this cannot
        # express, because an absent key IS a null here. Refused rather than
        # widened, so streamcast never accepts a row its own declared schema
        # would reject. See the module docstring's table.
        #
        # Checked LAST, after the type is known good: a `{"type": "array"}`
        # that is also optional should be told arrays are not a column, which
        # is the more specific complaint and the one worth fixing first.
        msg = (
            f"column {name!r} is optional with a non-null type, which a stream "
            f"cannot express: a row that omits it stores NULL. Either add it to "
            f"'required', or make it nullable with "
            f'{{"type": [{declared!r}, "null"]}}.'
        )
        raise TypeError(msg)

    return pa.field(name, arrow, nullable=accepts_null)


def to_arrow(schema: Mapping[str, object]) -> pa.Schema:
    """A JSON Schema object, as the `pa.schema` litelink wants.

    Column ORDER is the order `properties` is written in, which is the order
    every frame's keys appear in and therefore part of the wire contract.
    Python dicts preserve insertion order and so does `json.loads`, so a
    schema read from a file keeps the shape its author gave it.
    """
    if schema.get("type") not in (None, "object"):
        msg = f"a stream's schema is a JSON object schema, not {schema.get('type')!r}"
        raise TypeError(msg)

    properties = schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        msg = "a stream's schema needs a non-empty 'properties'"
        raise TypeError(msg)

    declared_required = schema.get("required", [])
    if not isinstance(declared_required, (list, tuple)):
        msg = f"'required' is a list of column names, not {declared_required!r}"
        raise TypeError(msg)

    unknown = [name for name in declared_required if name not in properties]
    if unknown:
        # Caught here rather than ignored, because a typo in `required` is a
        # column that silently became nullable.
        msg = f"'required' names columns that are not in 'properties': {unknown}"
        raise TypeError(msg)

    required = set(declared_required)

    return pa.schema(
        [
            _field(name, spec, required=name in required)
            for name, spec in properties.items()
        ]
    )


def from_arrow(schema: pa.Schema) -> dict[str, object]:
    """The inverse, for publishing a stream's shape to its subscribers.

    Widths are stated explicitly — see `_FROM_ARROW` — so what a subscriber
    reads back is what the column actually is, and so that `to_arrow` of this
    is the schema it started from.

    A nullable column is published as `["string", "null"]` rather than merely
    omitted from `required`, because `required` is about presence and a
    subscriber validating against this needs to know the value may be null.
    That also makes everything published here something `to_arrow` accepts.
    """
    properties: dict[str, object] = {}
    required: list[str] = []
    for field in schema:
        for matches, spec in _FROM_ARROW:
            if matches(field.type):  # ty: ignore[call-non-callable]
                published: dict[str, object] = dict(spec)
                if field.nullable:
                    published["type"] = [spec["type"], "null"]

                properties[field.name] = published
                break

        else:
            msg = f"column {field.name!r}: {field.type} has no JSON Schema spelling"
            raise TypeError(msg)

        if not field.nullable:
            required.append(field.name)

    return {"type": "object", "properties": properties, "required": required}


__all__ = ["from_arrow", "to_arrow"]
