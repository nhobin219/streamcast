"""Test-only socket ownership around the canonical application.

No broker algorithms live here: the app owns routing, resources and shutdown.
"""

from __future__ import annotations

from dataclasses import fields

import streamcast
from examples._hosting import Listening


def application(streams, **settings):
    return streamcast.create_app(
        streams=streams, config=streamcast.BrokerConfig(**settings)
    )


class Listener(Listening):
    """The examples' listener, plus what tests inspect about the runtime."""

    @property
    def _children(self):
        return self.app.state.broker.children

    @property
    def connections(self):
        return {
            peer
            for generation in self.app.state.broker.generations.values()
            for peer in generation.connections.values()
        }


def serve_streams(streams, host=None, port=None, *, sock=None, **kwargs):
    names = {field.name for field in fields(streamcast.BrokerConfig)}
    settings = {key: kwargs.pop(key) for key in list(kwargs) if key in names}
    if "max_size" in kwargs:
        if "max_message_bytes" in settings:
            raise ValueError("max_size and max_message_bytes together")

        size = kwargs.pop("max_size")
        settings["max_message_bytes"] = size
        settings["transport_max_bytes"] = size

    if "max_message_bytes" in settings:
        value = settings["max_message_bytes"]
        settings["transport_max_bytes"] = (
            max(value.values()) if isinstance(value, dict) else value
        )

    transport = {}
    if "compression" in kwargs:
        compression = kwargs.pop("compression")
        if compression not in (None, "deflate"):
            raise ValueError("invalid compression")

        transport["ws_per_message_deflate"] = compression == "deflate"

    if kwargs:
        raise ValueError(f"unsupported test listener options: {sorted(kwargs)}")

    return Listener(
        application(streams, **settings), host, port, sock=sock, **transport
    )
