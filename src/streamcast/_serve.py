"""The broker process owns its static handles, including failed startup."""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from contextlib import AsyncExitStack
from pathlib import Path

import uvicorn

from streamcast._application import create_app, initialize_registry
from streamcast._broker.registry import Registry
from streamcast._config_file import ConfigurationError, ServiceConfig, load
from streamcast._stream import StreamHandle


async def serve(settings: ServiceConfig) -> None:
    async with AsyncExitStack() as handles:
        streams = []
        for definition in settings.streams:
            stream = StreamHandle.open(definition, initialization="open_existing")
            handles.push_async_callback(stream.aclose)
            streams.append(stream)

        app = create_app(settings.broker, streams=streams)
        server = uvicorn.Server(
            uvicorn.Config(
                app, host=settings.host, port=settings.port, workers=1, lifespan="on"
            )
        )
        await server.serve()
        if not server.started:
            raise RuntimeError("broker failed to start")


def init_registry(settings: ServiceConfig) -> None:
    try:
        initialize_registry(settings.broker)
    except FileExistsError:
        # An existing file is success only if it opens with this identity and
        # format. Locked, corrupt or incompatible registries remain failures.
        if settings.broker.registry is None:
            raise ValueError("a registry path is required") from None

        Registry.open(settings.broker.registry, settings.broker.broker_id).close()


def main(argv: list[str], *, initialize: bool = False) -> int:
    parser = argparse.ArgumentParser(
        prog="streamcast init-registry" if initialize else "streamcast serve"
    )
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        settings = load(args.config)
        if initialize:
            init_registry(settings)
        else:
            asyncio.run(serve(settings))

    except ConfigurationError as error:
        print(str(error), file=sys.stderr)
        return 2
    except (OSError, ValueError, RuntimeError, sqlite3.Error):
        print("invalid configuration or unavailable broker storage", file=sys.stderr)
        return 2

    return 0
