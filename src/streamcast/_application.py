"""`create_app`: the one broker, a FastAPI application.

Static streams handed to it and, with a registry, streams managed over HTTP
share one runtime that the application's lifespan opens and closes
(`_broker.runtime`). Construction opens no files or sockets, so the settings
are checked here, where a mistake raises at the call rather than at startup.
Serve it under one ASGI worker with lifespan enabled; docs/API.md is the
contract.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from fastapi import FastAPI

from streamcast._broker.api import application
from streamcast._broker.model import BrokerConfig, ExternalMaintain
from streamcast._broker.registry import Registry
from streamcast._spec import Stream
from streamcast._stream import StreamHandle


@dataclass(frozen=True)
class Opening:
    """A definition whose storage is opened by the runtime after ownership."""

    stream: Stream
    open: Callable[[], StreamHandle] | None = None


def initialize_registry(config: BrokerConfig) -> None:
    """Provision an empty registry exclusively; never called during startup."""
    if config.registry is None:
        raise ValueError("a registry path is required")

    Registry.open(config.registry, config.broker_id, initialize=True).close()


def create_app(
    config: BrokerConfig | None = None,
    *,
    streams: StreamHandle | Opening | Iterable[StreamHandle | Opening] = (),
) -> FastAPI:
    """Build the broker application, without opening storage or sockets.

    Serve with one ASGI worker and lifespan enabled. A mounting host explicitly
    enters ``app.router.lifespan_context(app)`` in its own top-level lifespan.
    Static streams cannot be reconfigured through HTTP. Registry-backed dynamic
    streams use the same runtime, admission, connection and shutdown paths.
    """
    chosen = (
        (streams,) if isinstance(streams, (StreamHandle, Opening)) else tuple(streams)
    )
    names = [
        item.name if isinstance(item, StreamHandle) else item.stream.spec.name
        for item in chosen
    ]
    if len(set(names)) != len(names):
        raise ValueError("two streams are both named the same")

    if not chosen and (config is None or config.registry is None):
        raise ValueError("create_app needs streams or a registry")

    settings = config or BrokerConfig()
    if isinstance(settings.maintain, ExternalMaintain) and any(
        isinstance(item, StreamHandle) for item in chosen
    ):
        raise ValueError("an already-open stream cannot be fenced by the role barrier")

    limits = [
        item._max_message_bytes
        if isinstance(item, StreamHandle)
        else item.stream.spec.max_message_bytes
        for item in chosen
    ]
    if any(
        limit is not None and limit > settings.transport_max_bytes for limit in limits
    ):
        raise ValueError("max_message_bytes exceeds transport_max_bytes")

    return application(settings, chosen)
