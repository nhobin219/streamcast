"""`where=` — the predicate a subscriber is served through.

    connect(uri, where={"ticker": "AAPL"})          # equality
    connect(uri, where={"ticker": ["AAPL", "MSFT"]})  # membership

**The row is already a dict at the only point this can run.** `Stream.send`
encodes once and hands the same bytes to every subscriber, so a filter decides
whether to ENQUEUE that frame rather than what to build — it sees the mapping
the caller passed to `send`, and nothing is deserialised because nothing has
been serialised yet. That is what makes filtering cheap here and why the
predicate is ordinary Python: measured on a four-column row, 162 ns for one
term against 961 ns for the `msgspec` encode already on the path.

**Compiled once, at subscribe.** The generic `all(...)` form measured at
1,162 ns a row — more than the encode it rides on — so `compile_where`
specialises on arity and the one- and two-term cases, which are the ones
anybody writes, become a dict lookup and a compare.

**Equality and membership, and nothing else.** The predicate arrives from a
client over a socket, so there is no expression language to parse and no
`eval` to reach: a JSON object of column to value. A list value means
membership, unambiguously, because a filter compares scalar and binary
columns only — `prepare` refuses a struct, list or map column — so the column
a list value names can never hold a list itself.

**Total, and raise-free.** `offer` promises never to raise, and it is where
this is called. `dict.get` is defined for a missing key and `==`/`in` are
defined for any pair of types, so a predicate cannot fail on a row — a row
whose column holds a string where the filter names an int simply does not
match.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa

from streamcast._codec import decode_value
from streamcast._errors import ProtocolError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    Row = Mapping[str, object]
    Where = Mapping[str, object]
    Predicate = Callable[[Row], bool]

_SCALARS = (str, int, float, bool, bytes, type(None))

_MATCH_ALL: Predicate = lambda _row: True  # noqa: E731
"""No terms is no restriction, which keeps `where={}` from meaning `where` at all."""


def compile_where(where: Where) -> Predicate:
    """A predicate over rows, specialised on the number of terms.

    Arity cases rather than one loop, because the loop is the expensive part:
    a two-term `all(...)` genexp allocates a generator per row per subscriber,
    and at fan-out that is the dominant cost of filtering.
    """
    terms = tuple((name, _as_test(value)) for name, value in where.items())
    if not terms:
        return _MATCH_ALL

    if len(terms) == 1:
        ((name, test),) = terms

        return lambda row: test(row.get(name))

    if len(terms) == 2:
        (first, one), (second, two) = terms

        return lambda row: one(row.get(first)) and two(row.get(second))

    return lambda row: all(test(row.get(name)) for name, test in terms)


def _as_test(value: object) -> Callable[[object], bool]:
    """One column's test: membership for a list, equality for a scalar."""
    if isinstance(value, (list, tuple)):
        allowed = frozenset(
            item for item in value if isinstance(item, (str, int, float))
        )
        if len(allowed) == len(tuple(value)):
            # A frozenset only when every member is hashable — `None` and
            # floats are, but a caller could pass something that is not, and
            # falling back to a tuple keeps the predicate total.
            return lambda got: got in allowed

        members = tuple(value)

        return lambda got: got in members

    return lambda got: got == value


def validate(where: Where, columns: tuple[str, ...] | None) -> None:
    """Refuse a filter this stream cannot serve. Raises `ProtocolError`.

    `ProtocolError` rather than `ValueError` so the server's catch is exact:
    a replay can raise `ValueError` of its own, and a blanket catch would
    report a storage failure as a malformed request.

    **A column the schema does not have is a refusal, not a filter that
    matches nothing.** Silently matching nothing is the worst available
    answer: the subscription succeeds, the socket stays open, and no message
    ever arrives — which looks exactly like a quiet stream. `serve` turns this
    into a 4400 naming the column.

    `columns` is None for a live-only stream, which declares no schema, so
    there is nothing to check a name against and any name is accepted. A typo
    there does silently match nothing, and that is a property of having no
    schema rather than of this function.
    """
    for name, value in where.items():
        if columns is not None and name not in columns:
            msg = (
                f"where names column {name!r}, which this stream does not have. "
                f"Declared: {list(columns)}"
            )
            raise ProtocolError(msg)

        if isinstance(value, (list, tuple)):
            if not value:
                msg = f"where[{name!r}] is an empty list, which matches nothing"
                raise ProtocolError(msg)

            bad = [item for item in value if not isinstance(item, _SCALARS)]
            if bad:
                msg = (
                    f"where[{name!r}] holds {bad[0]!r}, which is not a scalar; "
                    f"a list value means membership over scalars"
                )
                raise ProtocolError(msg)

        elif not isinstance(value, _SCALARS):
            msg = (
                f"where[{name!r}] is {value!r}; a filter value is a scalar, or a "
                f"list of them for membership"
            )
            raise ProtocolError(msg)


def prepare(where: Where, schema: pa.Schema | None) -> dict[str, object]:
    """`where` with each value in the form the column's rows hold. Raises `ProtocolError`.

    Run after `validate`, for a stream whose columns are typed.

    - **A binary column** is compared as bytes, because that is what a live row
      and a replayed one both carry. The filter arrives as JSON, so its value
      is text in the column's encoding — hex for `base16`, as a trace id is
      written — and is decoded here, once, at subscribe.
    - **A struct, list or map column is refused.** Equality on one compares
      whole nested values per row, which is not a filter anyone means, and a
      list value here already means membership.
    """
    if schema is None:
        return dict(where)

    out: dict[str, object] = {}
    for name, value in where.items():
        index = schema.get_field_index(name)
        field = schema.field(index) if index >= 0 else None
        kind = None if field is None else field.type
        if kind is not None and (
            pa.types.is_struct(kind) or pa.types.is_list(kind) or pa.types.is_map(kind)
        ):
            msg = (
                f"where names column {name!r}, which is {kind}; a filter compares "
                f"scalar and binary columns only"
            )
            raise ProtocolError(msg)

        if field is not None and (
            pa.types.is_binary(kind) or pa.types.is_fixed_size_binary(kind)
        ):
            try:
                if isinstance(value, (list, tuple)):
                    value = [decode_value(field, item) for item in value]
                else:
                    value = decode_value(field, value)
            except ValueError as exc:
                raise ProtocolError(str(exc)) from exc

        out[name] = value

    return out


def decode(raw: str) -> dict[str, object]:
    """`where=` off a query string. Raises `ValueError` for anything malformed.

    JSON rather than a syntax of its own, because the wire is already JSON and
    a second grammar is a second parser to get wrong — and because there is
    nothing here that JSON cannot say.
    """
    try:
        got = msgspec.json.decode(raw)

    except msgspec.DecodeError as exc:
        msg = f"where is not JSON: {raw[:60]!r}"
        raise ValueError(msg) from exc

    if not isinstance(got, dict):
        msg = f"where must be a JSON object of column to value, not {raw[:60]!r}"
        raise ValueError(msg)

    return got


def encode(where: Where) -> str:
    """A filter as the query-string value `decode` reads."""
    return msgspec.json.encode(dict(where)).decode()


__all__ = ["compile_where", "decode", "encode", "prepare", "validate"]
