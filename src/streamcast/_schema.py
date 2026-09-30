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
schema with a `date-time`, an unsigned or narrow integer, or a union is
rejected here, where the message can name JSON Schema's own vocabulary, rather
than inside `litelink.new` where it names Arrow's.

**Nested and binary columns follow the same rules at every depth:**

    object + properties             struct   (fields follow every rule above)
    object + additionalProperties   map      (string keys; JSON has no others)
    array  + items                  list
    string + contentEncoding        binary   ("base16" or "base64"; see ENCODINGS)
      + format "bytesN"             fixed_size_binary(N)

A binary column's encoding is how its value is written on the JSON wire, and
it is kept in the Arrow field's metadata so a reopened log still knows it —
see `_codec`, which does the converting.

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
    "null": "a null-only column carries nothing; give it a type and leave it out of `required`",
}

_FORMATS: Final[dict[str, str]] = {
    "date-time": "litelink stores epoch integers, not temporal types — use "
    '{"type": "integer"} and say microseconds in your own docs',
    "date": "litelink stores epoch integers, not temporal types",
    "time": "litelink stores epoch integers, not temporal types",
    "byte": 'binary is {"type": "string", "contentEncoding": "base64"} (or "base16")',
    "binary": 'binary is {"type": "string", "contentEncoding": "base64"} (or "base16")',
}

ENCODING: Final = b"streamcast.encoding"
"""The Arrow field-metadata key a binary column's wire encoding is kept under.

In the field rather than beside the schema because litelink keeps field
metadata through `new` and `open`, at every depth — measured — so a log
reopened by a later process still knows which of its columns go out as hex.
"""

ENCODINGS: Final = ("base16", "base64")
"""How a binary value is written as JSON text, per column.

JSON has no bytes, so a binary column declares which: `base16` (hex) is what
OTLP/JSON uses for trace and span ids and what every trace tool shows, and
`base64` is JSON Schema's convention and a third smaller, for payloads.
"""


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

    arrow, metadata = _nested_or_binary(name, declared, spec)
    if arrow is not None:
        _refuse_optional_non_null(
            name, declared, required=required, nullable=accepts_null
        )
        return pa.field(name, arrow, nullable=accepts_null, metadata=metadata)

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

    _refuse_optional_non_null(name, declared, required=required, nullable=accepts_null)

    return pa.field(name, arrow, nullable=accepts_null)


def _refuse_optional_non_null(
    name: str, declared: str, *, required: bool, nullable: bool
) -> None:
    """ "May be absent, but never null when present" — which this cannot express.

    An absent key IS a null here, so it is refused rather than widened, and
    streamcast never accepts a row its own declared schema would reject. See
    the module docstring's table. The same rule at every depth: a struct's
    fields are columns in miniature.

    Checked LAST, after the type is known good: a bad type that is also
    optional should hear about the type, which is the more specific complaint
    and the one worth fixing first.
    """
    if required or nullable:
        return

    msg = (
        f"column {name!r} is optional with a non-null type, which a stream "
        f"cannot express: a row that omits it stores NULL. Either add it to "
        f"'required', or make it nullable with "
        f'{{"type": [{declared!r}, "null"]}}.'
    )
    raise TypeError(msg)


def _nested_or_binary(
    name: str, declared: str, spec: Mapping[str, object]
) -> tuple[pa.DataType | None, dict[bytes, bytes] | None]:
    """The Arrow type for a struct, list, map or binary spelling, or `(None, None)`.

    - `object` with `properties` is a **struct**: its fields follow every rule
      a top-level column does, `required` and nullability included.
    - `object` with `additionalProperties` set to a schema, and no
      `properties`, is a **map** from string keys — JSON has no other kind —
      to that schema. Both at once is refused: it is neither.
    - `array` with `items` is a **list** of that schema.
    - `string` with `contentEncoding` is **binary**, written on the wire in
      that encoding; `format: "bytesN"` makes it fixed-size, N bytes.
    """
    if declared == "object":
        properties = spec.get("properties")
        values = spec.get("additionalProperties")
        if isinstance(properties, dict) and properties:
            if isinstance(values, dict):
                msg = (
                    f"column {name!r}: both 'properties' and a schema for "
                    f"'additionalProperties' — a struct has fixed fields and a "
                    f"map has open ones; declare one"
                )
                raise TypeError(msg)

            required = _required(name, spec, properties)
            fields = [
                _field(f"{name}.{key}", child, required=key in required)
                for key, child in properties.items()
            ]
            return (
                pa.struct(
                    [
                        f.with_name(key)
                        for f, key in zip(fields, properties, strict=True)
                    ]
                ),
                None,
            )

        if isinstance(values, dict):
            value = _field(f"{name}[…]", values, required=True)
            return pa.map_(pa.string(), value.with_name("value")), None

        msg = (
            f"column {name!r}: an object needs 'properties' (a struct) or a "
            f"schema for 'additionalProperties' (a map from string keys)"
        )
        raise TypeError(msg)

    if declared == "array":
        items = spec.get("items")
        if not isinstance(items, dict):
            msg = f"column {name!r}: an array needs a schema for 'items'"
            raise TypeError(msg)

        item = _field(f"{name}[]", items, required=True)
        return pa.list_(item.with_name("item")), None

    if declared == "string" and "contentEncoding" in spec:
        encoding = spec["contentEncoding"]
        if encoding not in ENCODINGS:
            msg = (
                f"column {name!r}: contentEncoding {encoding!r} is not one this "
                f"wire carries; use {' or '.join(ENCODINGS)}"
            )
            raise TypeError(msg)

        fmt = spec.get("format")
        if fmt is None:
            arrow: pa.DataType = pa.binary()
        elif isinstance(fmt, str) and fmt.startswith("bytes") and fmt[5:].isdigit():
            arrow = pa.binary(int(fmt[5:]))
        else:
            msg = (
                f"column {name!r}: format {fmt!r} on a binary column; the only "
                f"one is 'bytesN', for a fixed size of N bytes"
            )
            raise TypeError(msg)

        return arrow, {ENCODING: str(encoding).encode()}

    return None, None


def _required(name: str, spec: Mapping[str, object], properties: dict) -> set[str]:
    """A struct's `required`, checked as the top level's is."""
    declared = spec.get("required", [])
    if not isinstance(declared, (list, tuple)):
        msg = f"column {name!r}: 'required' is a list of field names, not {declared!r}"
        raise TypeError(msg)

    unknown = [key for key in declared if key not in properties]
    if unknown:
        msg = f"column {name!r}: 'required' names fields not in 'properties': {unknown}"
        raise TypeError(msg)

    return set(declared)


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
    is the schema it started from. Nested types and binary follow the same
    rule at every depth.

    A nullable column is published as `["string", "null"]` rather than merely
    omitted from `required`, because `required` is about presence and a
    subscriber validating against this needs to know the value may be null.
    That also makes everything published here something `to_arrow` accepts.
    """
    return _object(list(schema))


def _object(fields: list[pa.Field]) -> dict[str, object]:
    properties = {field.name: _spell(field) for field in fields}
    required = [field.name for field in fields if not field.nullable]

    return {"type": "object", "properties": properties, "required": required}


def _spell(field: pa.Field) -> dict[str, object]:
    """One field's JSON Schema, nullability included."""
    kind = field.type
    published: dict[str, object]
    if pa.types.is_struct(kind):
        published = {
            **_object([kind.field(i) for i in range(kind.num_fields)]),
            "additionalProperties": False,
        }
    elif pa.types.is_map(kind):
        published = {"type": "object", "additionalProperties": _spell(kind.item_field)}
    elif pa.types.is_list(kind):
        published = {"type": "array", "items": _spell(kind.value_field)}
    elif pa.types.is_fixed_size_binary(kind) or pa.types.is_binary(kind):
        published = {"type": "string", "contentEncoding": encoding(field)}
        if pa.types.is_fixed_size_binary(kind):
            published["format"] = f"bytes{kind.byte_width}"
    else:
        for matches, spec in _FROM_ARROW:
            if matches(kind):  # ty: ignore[call-non-callable]
                published = dict(spec)
                break

        else:
            msg = f"column {field.name!r}: {kind} has no JSON Schema spelling"
            raise TypeError(msg)

    if field.nullable:
        published["type"] = [published["type"], "null"]

    return published


def encoding(field: pa.Field) -> str:
    """A binary field's wire encoding, from its metadata.

    base64 when none is recorded — a log created before this existed, or
    opened from litelink directly — because that is what the wire already
    sent for bytes: msgspec writes them as base64.
    """
    found = (field.metadata or {}).get(ENCODING)
    return found.decode() if found else "base64"


__all__ = ["ENCODING", "ENCODINGS", "encoding", "from_arrow", "to_arrow"]
