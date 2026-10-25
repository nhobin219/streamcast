"""`create_app`: the one broker, a FastAPI application.

Static streams handed to it and, with a registry, streams managed over HTTP
share one runtime that the application's lifespan opens and closes
(`_broker.runtime`). Construction opens no files or sockets, so the settings
are checked here, where a mistake raises at the call rather than at startup.
Serve it under one ASGI worker with lifespan enabled; docs/API.md is the
contract.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from fastapi import FastAPI

from streamcast._broker.api import application
from streamcast._broker.model import BrokerConfig
from streamcast._broker.registry import Registry
from streamcast._limits import _bound
from streamcast._stream import Stream


def initialize_registry(config: BrokerConfig) -> None:
    """Provision an empty registry exclusively; never called during startup."""
    if config.registry is None:
        raise ValueError("a registry path is required")

    Registry.open(config.registry, config.broker_id, initialize=True).close()


def create_app(
    config: BrokerConfig | None = None, *, streams: Stream | Iterable[Stream] = ()
) -> FastAPI:
    """Build the broker application, without opening storage or sockets.

    Serve with one ASGI worker and lifespan enabled. A mounting host explicitly
    enters ``app.router.lifespan_context(app)`` in its own top-level lifespan.
    Static streams cannot be reconfigured through HTTP. Registry-backed dynamic
    streams use the same runtime, admission, connection and shutdown paths.
    """
    chosen = (streams,) if isinstance(streams, Stream) else tuple(streams)
    names = [stream.name for stream in chosen]
    if len(set(names)) != len(names):
        raise ValueError("two streams are both named the same")

    if not chosen and (config is None or config.registry is None):
        raise ValueError("create_app needs streams or a registry")

    settings = config or BrokerConfig()
    ceilings = settings.max_message_bytes
    values = ceilings.values() if isinstance(ceilings, Mapping) else (ceilings,)
    if any(
        type(value) is int and value > settings.transport_max_bytes for value in values
    ):
        raise ValueError("max_message_bytes exceeds transport_max_bytes")

    # Checked in full before any stream is touched; the runtime sets them
    # again at startup, on the same streams.
    _bound(
        dict(zip(names, chosen, strict=True)),
        settings.max_backlog,
        settings.max_inbound,
        settings.max_in_flight,
        settings.max_message_bytes,
    )
    return application(settings, chosen)
