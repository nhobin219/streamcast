"""Offline storage work with child creation on the long-lived loop thread."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Callable
from typing import Any

from streamcast import _metadata, _replicate
from streamcast._process import popen
from streamcast._stream import Stream
from streamcast._tasks import thread


async def storage_worker(call: Callable[[], Any]) -> Any:
    loop = asyncio.get_running_loop()

    def spawn(argv, env=None):
        answer: concurrent.futures.Future = concurrent.futures.Future()

        def start() -> None:
            try:
                answer.set_result(popen(argv, env))
            except BaseException as exc:
                answer.set_exception(exc)

        loop.call_soon_threadsafe(start)
        return answer.result()

    token = _replicate._retiring_spawn.set(spawn)
    try:
        return await thread(call)
    finally:
        _replicate._retiring_spawn.reset(token)


async def with_replica(log, options, call: Callable[[], Any]) -> Any:
    def work():
        if log.config.wal_replication:
            with _replicate.retiring(log, s3_options=options):
                return call()

        return call()

    return await storage_worker(work)


async def storage(record: dict, profile, kind: str, target: dict) -> None:
    def work() -> None:
        name = record["name"]
        metadata = _metadata.load(_metadata.home(profile.root, name), name)
        if metadata is None or metadata.stream_id != record["stream_id"]:
            raise ValueError("identity mismatch")

        if kind == "retire":
            Stream.retire(name, root=profile.root, s3_options=profile.s3_options)
        else:
            opened = Stream.migrate(
                name,
                root=profile.root,
                schema=target["schema"],
                sort_by=target["sort_by"],
                config=profile.config,
                s3_options=profile.s3_options,
            )
            log = opened.log
            assert log is not None
            try:
                opened.ensure_metadata()
            finally:
                log.close()

        found = _metadata.load(_metadata.home(profile.root, name), name)
        if found is None or found.stream_id != record["stream_id"]:
            raise ValueError("identity changed")

        if kind == "retire" and found.retirement is None:
            raise ValueError("retirement incomplete")

        if kind == "migration":
            # Open performs the authoritative compiler/schema check. Metadata
            # normalizes JSON annotations, so raw JSON equality is incorrect.
            check = Stream.open(
                name,
                root=profile.root,
                schema=target["schema"],
                sort_by=target["sort_by"],
                s3_options=profile.s3_options,
            )
            assert check.log is not None
            try:
                if list(check.log.sort_by) != target["sort_by"]:
                    raise ValueError("migration incomplete")
            finally:
                check.log.close()

    await storage_worker(work)
