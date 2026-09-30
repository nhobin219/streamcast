"""What the JSON wire needs done to a row that msgspec does not do by itself.

JSON has no bytes and no map type, so two of the column types a stream can
declare need help crossing it — and the help has to be the same on the way out
of a live `send`, out of a replay, and back in at the other end, or a replayed
frame stops matching the live one it repeats (invariant 10).

- **Binary, inbound.** A publisher over the socket can only send text, and
  litelink refuses a `str` for a binary column. So every binary value, at any
  depth, is decoded with its column's encoding — `base16` or `base64`, see
  `_schema.ENCODINGS` — before the row is validated or stored. The client does
  the same to what it receives, so a consumer gets `bytes` whether a row came
  off the socket or out of the archive by catch-up.
- **Binary, outbound.** msgspec writes `bytes` as base64 by itself, so only a
  `base16` column is converted, to hex, before the frame is encoded.
- **Maps.** Arrow hands a replayed map back as a dict (see `_log.rows`), and a
  live frame encodes the caller's value — so the caller's value has to be a
  dict too. litelink would also take a list of pairs, and a stream that
  accepted one would send `[["k","v"]]` live and `{"k":"v"}` on replay. It is
  refused BEFORE the append, when refusing still costs nothing.

**Compiled once per schema, and absent when there is nothing to do.** Each of
the three is None unless the schema has a column that needs it, so a stream of
scalars pays for none of this on the hot path.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from streamcast import _schema

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    Convert = Callable[[object], object]
    Check = Callable[[object], None]
    Leaves = Callable[[Mapping[str, object]], dict[str, object]]

_BYTES = (bytes, bytearray, memoryview)


@dataclass(frozen=True, slots=True)
class Codec:
    """A stream's wire conversions. Each is None when the schema needs none."""

    inbound: Callable[[Mapping[str, object]], dict[str, object]] | None
    """Text to bytes for every binary value, at any depth. Raises `ValueError`."""
    outbound: Callable[[Mapping[str, object]], dict[str, object]] | None
    """Bytes to hex for every `base16` value, at any depth."""
    check: Callable[[Mapping[str, object]], None] | None
    """Refuse a map value that is not a dict, at any depth. Raises `ValueError`."""


NONE = Codec(None, None, None)
"""What a stream of scalars, or one with no schema, gets."""


def compile_codec(schema: pa.Schema | None) -> Codec:
    """The conversions `schema`'s columns need, each None if none do."""
    if schema is None:
        return NONE

    # **Top-level binary columns are special-cased**, because they are the
    # common case — an OTel row's trace and span ids — and the per-row cost is
    # all plumbing: `bytes.hex()` itself is C, ~60 ns. Measured in one process
    # on a six-column row with two hex ids: a generic loop of per-column
    # closures cost about 4x what msgspec takes to encode the whole row, and
    # this costs about the same as that encode, level with a hand-written
    # version. msgspec has no hex option for bytes (only `uuid_format`), and
    # `enc_hook` never fires for a type it already knows, so this is the floor.
    leaves = [f for f in schema if _is_binary(f.type)]
    hexed = tuple(f.name for f in leaves if _schema.encoding(f) == "base16")
    decoders = tuple((f.name, _decoder(_schema.encoding(f), f.name)) for f in leaves)
    branches = [f for f in schema if not _is_binary(f.type)]
    out_nested = {f.name: fn for f in branches if (fn := _out(f)) is not None}
    in_nested = {f.name: fn for f in branches if (fn := _in(f, f.name)) is not None}
    checks = {f.name: fn for f in schema if (fn := _check(f, f.name)) is not None}

    return Codec(
        inbound=(
            _row(_decode_leaves(decoders), in_nested) if decoders or in_nested else None
        ),
        outbound=_row(_hex_leaves(hexed), out_nested) if hexed or out_nested else None,
        check=_row_check(checks) if checks else None,
    )


def _row(leaves: Leaves | None, nested: dict[str, Convert]) -> Leaves:
    """The top-level conversions, then the nested ones, on a copy of a row.

    A copy, never the caller's dict: the row was theirs, and the fan-out and
    `where=` still read the original. With nothing nested, the unrolled
    top-level function IS the conversion — one call per row, no wrapper.
    """
    if not nested and leaves is not None:
        return leaves

    items = tuple(nested.items())

    def convert(row: Mapping[str, object]) -> dict[str, object]:
        out = dict(row) if leaves is None else leaves(row)
        for name, fn in items:
            if name in out:
                out[name] = fn(out[name])

        return out

    return convert


def _hex_leaves(names: tuple[str, ...]) -> Leaves | None:
    """Copy a row with the named top-level columns hexed, specialised on how many.

    Unrolled for one to three, like `_filter.compile_where`: a loop over a
    tuple of names measured at twice the cost of the unrolled form.
    """
    if not names:
        return None

    if len(names) == 1:
        (a,) = names

        def one(row: Mapping[str, object]) -> dict[str, object]:
            out = dict(row)
            v = out.get(a)
            if isinstance(v, _BYTES):
                out[a] = v.hex()

            return out

        return one

    if len(names) == 2:
        a, b = names

        def two(row: Mapping[str, object]) -> dict[str, object]:
            out = dict(row)
            v = out.get(a)
            if isinstance(v, _BYTES):
                out[a] = v.hex()

            v = out.get(b)
            if isinstance(v, _BYTES):
                out[b] = v.hex()

            return out

        return two

    if len(names) == 3:
        a, b, c = names

        def three(row: Mapping[str, object]) -> dict[str, object]:
            out = dict(row)
            v = out.get(a)
            if isinstance(v, _BYTES):
                out[a] = v.hex()

            v = out.get(b)
            if isinstance(v, _BYTES):
                out[b] = v.hex()

            v = out.get(c)
            if isinstance(v, _BYTES):
                out[c] = v.hex()

            return out

        return three

    def many(row: Mapping[str, object]) -> dict[str, object]:
        out = dict(row)
        for name in names:
            v = out.get(name)
            if isinstance(v, _BYTES):
                out[name] = v.hex()

        return out

    return many


def _decode_leaves(decoders: tuple[tuple[str, Convert], ...]) -> Leaves | None:
    """Copy a row with the named top-level columns' text decoded, by arity.

    Only a `str` is decoded; bytes (a local caller's own) and None pass
    without a call.
    """
    if not decoders:
        return None

    if len(decoders) == 1:
        ((a, da),) = decoders

        def one(row: Mapping[str, object]) -> dict[str, object]:
            out = dict(row)
            v = out.get(a)
            if isinstance(v, str):
                out[a] = da(v)

            return out

        return one

    if len(decoders) == 2:
        (a, da), (b, db) = decoders

        def two(row: Mapping[str, object]) -> dict[str, object]:
            out = dict(row)
            v = out.get(a)
            if isinstance(v, str):
                out[a] = da(v)

            v = out.get(b)
            if isinstance(v, str):
                out[b] = db(v)

            return out

        return two

    def many(row: Mapping[str, object]) -> dict[str, object]:
        out = dict(row)
        for name, decode in decoders:
            v = out.get(name)
            if isinstance(v, str):
                out[name] = decode(v)

        return out

    return many


def _row_check(fns: dict[str, Check]) -> Callable[[Mapping[str, object]], None]:
    def check(row: Mapping[str, object]) -> None:
        for name, fn in fns.items():
            fn(row.get(name))

    return check


# -- per type, recursively ------------------------------------------------------


def _children(kind: pa.DataType) -> list[tuple[str, pa.Field]]:
    """`(path suffix, field)` for a nested type's children; empty for a leaf."""
    if pa.types.is_struct(kind):
        return [
            (f".{kind.field(i).name}", kind.field(i)) for i in range(kind.num_fields)
        ]

    if pa.types.is_map(kind):
        return [("[…]", kind.item_field)]

    if pa.types.is_list(kind):
        return [("[]", kind.value_field)]

    return []


def _is_binary(kind: pa.DataType) -> bool:
    return pa.types.is_binary(kind) or pa.types.is_fixed_size_binary(kind)


def _nested(kind: pa.DataType, fns: list[Convert | None]) -> Convert | None:
    """Lift child conversions over a struct, map or list value; None if none."""
    if not any(fns):
        return None

    if pa.types.is_struct(kind):
        by_name = {kind.field(i).name: fn for i, fn in enumerate(fns) if fn is not None}

        def struct(value: object) -> object:
            if not isinstance(value, dict):
                return value

            return {
                key: (by_name[key](item) if key in by_name else item)
                for key, item in value.items()
            }

        return struct

    (only,) = fns
    if only is None:  # pragma: no cover — `any(fns)` above, and there is one
        return None

    fn = only

    if pa.types.is_map(kind):

        def mapping(value: object) -> object:
            if not isinstance(value, dict):
                return value

            return {key: fn(item) for key, item in value.items()}

        return mapping

    def sequence(value: object) -> object:
        if not isinstance(value, list):
            return value

        return [fn(item) for item in value]

    return sequence


def _in(field: pa.Field, path: str) -> Convert | None:
    kind = field.type
    if _is_binary(kind):
        return _decoder(_schema.encoding(field), path)

    children = _children(kind)
    return _nested(kind, [_in(child, path + suffix) for suffix, child in children])


def _out(field: pa.Field) -> Convert | None:
    kind = field.type
    if _is_binary(kind):
        return _to_hex if _schema.encoding(field) == "base16" else None

    return _nested(kind, [_out(child) for _, child in _children(kind)])


def _check(field: pa.Field, path: str) -> Check | None:
    """A map must arrive as a dict; nested values are checked the same way."""
    kind = field.type
    inner = [_check(child, path + suffix) for suffix, child in _children(kind)]
    lifted = _nested_check(kind, inner)
    if not pa.types.is_map(kind):
        return lifted

    def check(value: object) -> None:
        if value is None:
            return

        if not isinstance(value, dict):
            msg = (
                f"column {path!r} is a map, and a map is a JSON object; got "
                f"{type(value).__name__}. A list of pairs would store the same "
                f"entries but send a different frame live than on replay."
            )
            raise ValueError(msg)

        if lifted is not None:
            lifted(value)

    return check


def _nested_check(kind: pa.DataType, fns: list[Check | None]) -> Check | None:
    if not any(fns):
        return None

    if pa.types.is_struct(kind):
        by_name = {kind.field(i).name: fn for i, fn in enumerate(fns) if fn is not None}

        def struct(value: object) -> None:
            if isinstance(value, dict):
                for key, fn in by_name.items():
                    fn(value.get(key))

        return struct

    (only,) = fns
    if only is None:  # pragma: no cover — `any(fns)` above, and there is one
        return None

    fn = only

    if pa.types.is_map(kind):

        def mapping(value: object) -> None:
            if isinstance(value, dict):
                for item in value.values():
                    fn(item)

        return mapping

    def sequence(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                fn(item)

    return sequence


# -- the encodings ---------------------------------------------------------------


def _to_hex(value: object) -> object:
    # `.hex()` directly: bytes, bytearray and memoryview all have it, and a
    # `bytes(value)` first would copy the value only to throw the copy away.
    return value.hex() if isinstance(value, _BYTES) else value


def _decoder(encoding: str, path: str) -> Convert:
    """Text to bytes in `encoding`; bytes and None pass through.

    Bytes pass because a local `send` hands over Python values, and None
    because a nullable column's null is not text. Anything else is left for
    litelink to refuse with the type it found.
    """

    def decode(value: object) -> object:
        if not isinstance(value, str):
            return value

        try:
            if encoding == "base16":
                return bytes.fromhex(value)

            return base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            msg = f"column {path!r} is binary in {encoding}, and {value[:40]!r} is not"
            raise ValueError(msg) from exc

    return decode


def decode_value(field: pa.Field, value: object) -> object:
    """One binary value from text, for a `where=` term. Raises `ValueError`."""
    return _decoder(_schema.encoding(field), field.name)(value)


__all__ = ["NONE", "Codec", "compile_codec", "decode_value"]
