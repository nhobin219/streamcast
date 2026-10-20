"""A stream's declared identity, and the key the writer derives from it (#159).

    Stream.new("lines", root=..., schema=SCHEMA, identity=("session_id", "line_number"))

A row's identity is the columns that make it this row. A stream that declares
one gets a column the writer fills on every row, `_streamcast_id`: XXH3-128
over the identity columns' values, 16 bytes, so a dedupe or a join reads one
fixed-width column instead of several, and a lookup by the whole key is
filtered on it (`Snapshot.scan`'s `filters`).

**The encoding is the identity columns' values only, in declaration order**,
each as:

* a fixed-width value — boolean, int32, int64, a `bytesN` binary — as its
  little-endian bytes: one byte for a boolean (0 or 1), two's complement for
  an integer, the bytes themselves for a fixed-size binary;
* a string or a binary as a 4-byte little-endian length, then the bytes (a
  string's UTF-8).

The hash is XXH3-128 with seed 0, stored as its canonical 16 bytes (big-endian,
`XXH128_canonicalFromHash`: what every xxHash binding's `digest()` returns).
`docs/SPEC.md` §5, "A declared identity", states the same, for a reader in another language.

**No per-stream domain tag.** A constant prefix adds nothing to the key's
collision properties, and a per-stream one makes the same key hash differently
in two streams, so a join across them on `_streamcast_id` would find nothing.
**Length prefixes on variable-width values**, because without them
`("ab", "c")` and `("a", "bc")` are the same bytes. A fixed-width value needs
none: its width is its column's.

**Not a primary key.** The log is append-only, and a row whose key is already
in it is appended again; the key makes the duplicate cheap to see.

**Floats and nested values cannot be part of an identity.** Equality is not
identity for a float (`0.0 == -0.0`, `NaN != NaN`), and a nested value has no
encoding here. Identity columns are required and non-null: a null has no
encoding either, and a row without its identity is not a row of the stream.
"""

from __future__ import annotations

import json
import struct
from typing import TYPE_CHECKING, Final

import pyarrow as pa
import xxhash

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

COLUMN: Final = "_streamcast_id"
"""The derived key's column, a `fixed_size_binary(16)` the writer fills.

Underscore-prefixed because it is derived plumbing, not a value with meaning
of its own: hidden from `SELECT *` in streamcast's read paths and selectable by
name (SPEC §5, "A declared identity"). A raw `iceberg_scan` outside streamcast sees it as an ordinary
column, since Iceberg has no hidden ones.
"""

WIDTH: Final = 16

TYPE: Final = pa.binary(WIDTH)

DECLARATION: Final = b"streamcast.identity"
"""The field-metadata key the declaration is kept under, on `COLUMN`'s field.

On the log's own schema, beside the column it describes, because litelink
keeps field metadata through `new` and `open` (`_schema.ENCODING` relies on
the same): the log carries its identity, so a reopen needs no other file. The
stream's metadata records it per log as well (`Entry.identity`), for readers
and for a restore, since an Iceberg schema keeps no field metadata.
"""

_LENGTH: Final = struct.Struct("<I")


def _boolean(value: object) -> bytes:
    if not isinstance(value, bool):
        msg = "not a boolean"
        raise TypeError(msg)

    return b"\x01" if value else b"\x00"


def _integer(width: int) -> Callable[[object], bytes]:
    def encode(value: object) -> bytes:
        # Exactly an int: `True` is one in Python and not in the column, and
        # a float or a numeric string coerced here would key a row on a value
        # it does not hold.
        if not isinstance(value, int) or isinstance(value, bool):
            msg = "not an integer"
            raise TypeError(msg)

        return value.to_bytes(width, "little", signed=True)

    return encode


def _bytes(value: object) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        msg = "not bytes"
        raise TypeError(msg)

    return bytes(value)


def _string(value: object) -> bytes:
    if not isinstance(value, str):
        msg = "not a string"
        raise TypeError(msg)

    return _prefixed(value.encode())


_FIXED: Final[dict[pa.DataType, Callable[[object], bytes]]] = {
    pa.bool_(): _boolean,
    pa.int32(): _integer(4),
    pa.int64(): _integer(8),
}


def _encoder(field: pa.Field) -> Callable[[object], bytes]:
    """How one identity column's value becomes bytes; refuses a column that
    cannot be part of an identity, naming why."""
    kind = field.type
    if field.nullable:
        msg = (
            f"identity column {field.name!r} is nullable; an identity column "
            f"is required and never null"
        )
        raise ValueError(msg)

    if kind in _FIXED:
        return _FIXED[kind]

    if pa.types.is_fixed_size_binary(kind):
        width = kind.byte_width

        def fixed(value: object) -> bytes:
            data = _bytes(value)
            if len(data) != width:
                msg = f"a {width}-byte value, got {len(data)}"
                raise ValueError(msg)

            return data

        return fixed

    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return _string

    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind):
        return lambda value: _prefixed(_bytes(value))

    msg = (
        f"identity column {field.name!r} is {kind}; an identity is booleans, "
        f"integers, strings and binaries — equality on a float is not "
        f"identity, and a nested value has no encoding"
    )
    raise ValueError(msg)


def _prefixed(data: bytes) -> bytes:
    return _LENGTH.pack(len(data)) + data


class Identity:
    """A declaration checked against the columns it names, compiled once.

    `key(row)` is what the writer fills `_streamcast_id` with and what a
    reader's filter is rewritten to; both build it here, so there is one
    encoding.
    """

    __slots__ = ("_encoders", "_types", "columns")

    def __init__(self, columns: Sequence[str], schema: pa.Schema) -> None:
        names = tuple(columns)
        if not names:
            msg = "identity= names no columns; leave it out for a stream without one"
            raise ValueError(msg)

        if len(set(names)) != len(names):
            msg = f"identity={names!r} names a column twice"
            raise ValueError(msg)

        missing = [name for name in names if name not in schema.names]
        if missing:
            msg = (
                f"identity column(s) {', '.join(map(repr, missing))} are not "
                f"among the stream's columns {schema.names}"
            )
            raise ValueError(msg)

        self.columns = names
        self._types = tuple(schema.field(name).type for name in names)
        self._encoders = tuple((name, _encoder(schema.field(name))) for name in names)

    def encode(self, values: Mapping[str, object]) -> bytes:
        """The bytes the key hashes. Raises `ValueError` naming the column
        whose value is missing, null or not of its type."""
        parts = []
        for name, encode in self._encoders:
            value = values.get(name)
            if value is None:
                msg = (
                    f"column {name!r} is part of the stream's identity; every "
                    f"row carries it, non-null"
                )
                raise ValueError(msg)

            try:
                parts.append(encode(value))
            except (TypeError, ValueError, OverflowError) as exc:
                msg = f"column {name!r} cannot hold {value!r}: {exc}"
                raise ValueError(msg) from None

        return b"".join(parts)

    def key(self, values: Mapping[str, object]) -> bytes:
        """`_streamcast_id` for a row: XXH3-128 of `encode`, canonical bytes."""
        return xxhash.xxh3_128_digest(self.encode(values))

    def keys(self, batch: pa.RecordBatch) -> pa.Array:
        """`_streamcast_id` for every row of `batch`, as a column.

        Encoded a column at a time and joined per row, rather than through
        `key` on a dict per row: the same bytes, at about a quarter of the cost
        on a bulk load (measured in the change that added it).
        """
        encoded = []
        for (name, encode), kind in zip(self._encoders, self._types, strict=True):
            column = batch.column(name)
            if column.null_count:
                msg = (
                    f"column {name!r} is part of the stream's identity; every "
                    f"row carries it, non-null"
                )
                raise ValueError(msg)

            try:
                # Cast as litelink casts a source, so a row is keyed on the
                # value the log stores: an int32 source column in an int64
                # identity is encoded in 8 bytes.
                values = column.cast(kind).to_pylist()
                encoded.append([encode(value) for value in values])
            except (TypeError, ValueError, OverflowError, NotImplementedError) as exc:
                msg = f"column {name!r} cannot be keyed as {kind}: {exc}"
                raise ValueError(msg) from None

        digest = xxhash.xxh3_128_digest
        return pa.array(
            [digest(b"".join(parts)) for parts in zip(*encoded, strict=True)],
            TYPE,
        )


def field(columns: Iterable[str]) -> pa.Field:
    """`COLUMN`'s field, carrying the declaration it is derived from."""
    return pa.field(
        COLUMN,
        TYPE,
        nullable=False,
        metadata={DECLARATION: json.dumps(list(columns)).encode()},
    )


def declared(schema: pa.Schema) -> tuple[str, ...] | None:
    """The identity a log's schema declares, or None if it has none.

    A `_streamcast_id` column without the declaration, or of another type, is
    refused rather than read as no identity: it is someone else's column
    wearing this library's name, or one whose declaration was lost (a log
    rebuilt from an Iceberg schema alone), and filling it — or silently not —
    would both be wrong about what it holds.
    """
    if COLUMN not in schema.names:
        return None

    found = schema.field(COLUMN)
    recorded = (found.metadata or {}).get(DECLARATION)
    if found.type != TYPE or recorded is None:
        msg = (
            f"the log has a {COLUMN!r} column ({found.type}) without an "
            f"identity declaration; streamcast owns that name. A log rebuilt "
            f"from its published table alone loses the declaration: restore "
            f"it with the stream's metadata"
        )
        raise ValueError(msg)

    return tuple(json.loads(recorded))


__all__ = ["COLUMN", "DECLARATION", "TYPE", "WIDTH", "Identity", "declared", "field"]
