"""A lifespan-owned FastAPI broker with data and management on one listener.

Provision the registry explicitly with ``initialize_registry`` before running
``create_app(config)`` under one ASGI worker with lifespan enabled. Host code
supplies storage profiles and authorization; HTTP accepts neither credentials
nor filesystem locations. See docs/BROKER.md for the controller contract.
"""

from __future__ import annotations

from streamcast._broker.model import (
    Authorization,
    BrokerConfig,
    HistoryGuard,
    StorageProfile,
)
from streamcast._broker.registry import Registry


def initialize_registry(config: BrokerConfig) -> None:
    """Provision an empty registry exclusively; never called during startup."""
    Registry.open(config.registry, config.broker_id, initialize=True).close()


def create_app(config: BrokerConfig):
    """Build one FastAPI application without opening storage or sockets.

    A mounting host must explicitly enter ``app.router.lifespan_context(app)``
    in its top-level lifespan. Mounting alone does not start the runtime.
    """
    try:
        from streamcast._broker.api import application
    except ModuleNotFoundError as missing:
        if missing.name == "fastapi":
            raise ModuleNotFoundError(
                "Install streamcast[broker] to use create_app"
            ) from missing

        raise

    return application(config)


__all__ = [
    "Authorization",
    "BrokerConfig",
    "HistoryGuard",
    "StorageProfile",
    "create_app",
    "initialize_registry",
]
