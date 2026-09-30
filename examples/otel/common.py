"""What OTel's log and span records share: values, ids, scope, and publishing.

OTel's data model uses the same pieces in both signals, so both schemas are
built from these, and a value converts the same way whichever record it is in.
"""

from __future__ import annotations

import asyncio
import json
import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    import streamcast

# -- the schema pieces --------------------------------------------------------------

ANY_VALUE: dict[str, Any] = {
    "type": "object",
    "properties": {
        "string_value": {"type": ["string", "null"]},
        "bool_value": {"type": ["boolean", "null"]},
        "int_value": {"type": ["integer", "null"]},
        "double_value": {"type": ["number", "null"]},
        "bytes_value": {"type": ["string", "null"], "contentEncoding": "base64"},
        "json_value": {"type": ["string", "null"]},
    },
    "required": [],
    "additionalProperties": False,
}
"""OTel's `AnyValue`: exactly one field set, or none for an empty value."""

ATTRIBUTES: dict[str, Any] = {
    "type": ["object", "null"],
    "additionalProperties": ANY_VALUE,
}

TRACE_ID: dict[str, Any] = {
    "type": ["string", "null"],
    "contentEncoding": "base16",
    "format": "bytes16",
}

SPAN_ID: dict[str, Any] = {
    "type": ["string", "null"],
    "contentEncoding": "base16",
    "format": "bytes8",
}

SCOPE: dict[str, Any] = {
    "type": ["object", "null"],
    "properties": {
        "name": {"type": ["string", "null"]},
        "version": {"type": ["string", "null"]},
    },
    "required": [],
    "additionalProperties": False,
}

# -- the conversion ---------------------------------------------------------------


def any_value(value: object) -> dict[str, object] | None:
    """An OTel attribute or body value, as the `AnyValue` struct."""
    if value is None:
        return None

    # bool before int: in Python a bool IS an int, and True is not 1 here.
    if isinstance(value, bool):
        return {"bool_value": value}

    if isinstance(value, int):
        if -(2**63) <= value < 2**63:
            return {"int_value": value}

        return {"json_value": json.dumps(value)}

    if isinstance(value, float):
        # A stream refuses NaN and ±inf, so they are stored as null — the
        # record is kept, the one unrepresentable value is not.
        return {"double_value": value if math.isfinite(value) else None}

    if isinstance(value, str):
        return {"string_value": value}

    if isinstance(value, (bytes, bytearray)):
        return {"bytes_value": bytes(value)}

    # Arrays and key-value lists: no column type is recursive, so JSON text.
    return {"json_value": json.dumps(value, default=str)}


def attributes(values: object) -> dict[str, object] | None:
    if not values:
        return None

    return {str(key): any_value(value) for key, value in dict(values).items()}  # ty: ignore[no-matching-overload]


def trace_id(value: int) -> bytes | None:
    # An id of 0 is OTel's "not in a trace"; stored as null, not as zeros.
    return value.to_bytes(16, "big") if value else None


def span_id(value: int) -> bytes | None:
    return value.to_bytes(8, "big") if value else None


def scope(value: Any) -> dict[str, object] | None:  # noqa: ANN401 — InstrumentationScope
    if value is None:
        return None

    return {"name": value.name, "version": value.version or None}


# -- publishing from OTel's export thread -----------------------------------------


def publish(
    publication: streamcast.Publication,
    loop: asyncio.AbstractEventLoop,
    rows: Sequence[dict[str, object]],
) -> bool:
    """Publish `rows` from an OTel exporter; True once the stream has them.

    OTel's batch processors call `export` on their own worker thread, and a
    streamcast publisher lives on an event loop — so the batch is handed to
    the loop and the thread waits for the stream's acknowledgement. That makes
    a flush mean the rows are DURABLE, not merely sent.
    """
    sent = asyncio.run_coroutine_threadsafe(publication.send_many(rows), loop)
    try:
        sent.result(timeout=10)
    except Exception:  # noqa: BLE001 — the SDK wants a result, not an exception
        return False

    return True
