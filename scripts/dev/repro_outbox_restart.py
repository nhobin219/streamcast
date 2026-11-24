"""Reproduce a Postgres restart during an instance resize, without external services.

Run from the checkout after `uv sync`:
    uv run python scripts/dev/repro_outbox_restart.py --pg-bin PATH --workdir PATH
    uv run python scripts/dev/repro_outbox_restart.py --pg-bin PATH --workdir PATH --expect stuck

PATH for --pg-bin contains initdb/pg_ctl/postgres. --workdir must not exist.
The cluster uses trust authentication on loopback, disposable data, and no
credentials. Only this script's cluster is stopped. Run as a non-root user.
The same synthetic DNS name resolves to a different loopback address after
restart (process-local resolver override; no machine DNS changes).
--outage-seconds can exceed the capped backoff or model a longer resize.
The adapter mode opens a new connection per transaction, like an embedding
application that already reconnects; builtin exercises PostgresStore as well.
Exit 0 means the selected expectation was demonstrated, not always recovery.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import socket
import subprocess
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import litelink
import psycopg
import pyarrow as pa
import uvicorn

import streamcast


class Transaction:
    def __init__(self, store, connection):
        self.store = store
        self.connection = connection
        self.callbacks = []

    async def execute(self, sql, params=()):
        return (await self.connection.execute(sql, params or None)).rowcount

    async def fetch(self, sql, params=()):
        return await (await self.connection.execute(sql, params or None)).fetchall()

    def on_commit(self, callback):
        self.callbacks.append(callback)


class ReconnectingAdapter:
    dialect: Literal["postgres"] = "postgres"

    def __init__(self, dsn):
        self.dsn = dsn

    @contextlib.asynccontextmanager
    async def transaction(self):
        async with await psycopg.AsyncConnection.connect(self.dsn) as connection:
            tx = Transaction(self, connection)
            yield tx

        for callback in tx.callbacks:
            callback()

    async def close(self):
        pass


async def run(args):
    root = args.workdir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    data = root / "db"
    binaries = args.pg_bin.resolve()
    subprocess.run(
        [str(binaries / "initdb"), "-D", str(data), "-U", "repro", "-A", "trust"],
        check=True,
        capture_output=True,
    )
    address = str(ipaddress.IPv4Address(socket.INADDR_LOOPBACK))
    with socket.socket() as reserve:
        reserve.bind((address, 0))
        port = reserve.getsockname()[1]

    def control(*command):
        subprocess.run(
            [str(binaries / "pg_ctl"), "-D", str(data), "-w", *command],
            check=True,
            capture_output=True,
        )

    def start():
        control(
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-h {address} -p {port} -k {root} -c fsync=off",
            "start",
        )

    lookups = 0
    getaddrinfo = socket.getaddrinfo

    def resolve(host, *positional, **keywords):
        nonlocal lookups
        if host == "database.invalid":
            lookups += 1
            host = address

        return getaddrinfo(host, *positional, **keywords)

    start()
    try:
        with patch("socket.getaddrinfo", resolve):
            dsn = f"host=database.invalid port={port} user=repro dbname=postgres connect_timeout=2"
            initial = await streamcast.PostgresStore.connect(dsn)
            if args.store == "adapter":
                await initial.close()
                store = ReconnectingAdapter(dsn)
            else:
                store = initial

            schema = pa.schema([pa.field("n", pa.int64())])
            with litelink.new(root / "log", "events", schema=schema) as log:
                stream = streamcast.StreamHandle("events", log=log, exactly_once=True)
                app = streamcast.create_app(
                    streams=[stream], config=streamcast.BrokerConfig(maintain=False)
                )
                server = uvicorn.Server(
                    uvicorn.Config(app, host="localhost", port=0, log_level="error")
                )
                task = asyncio.create_task(server.serve())
                try:
                    async with asyncio.timeout(10):
                        while not server.started:
                            if task.done():
                                await task
                                raise RuntimeError("broker stopped during startup")

                            await asyncio.sleep(0.01)

                    broker_port = server.servers[0].sockets[0].getsockname()[1]
                    uri = f"ws://localhost:{broker_port}/events"
                    out = streamcast.outbox(
                        uri,
                        publisher="repro",
                        store=store,
                        poll_every=0.05,
                        backoff=(0.05, 0.1, 0.25),
                    )
                    async with out as sender:
                        async with store.transaction() as tx:
                            await sender.send({"n": 1}, tx=tx)

                        await sender.flush(timeout=10)
                        control("-m", "immediate", "stop")
                        print(
                            "cluster stopped immediately; observing sends during outage"
                        )
                        for _ in range(2):
                            try:
                                async with store.transaction() as tx:
                                    await sender.send({"n": 99}, tx=tx)
                            except psycopg.Error as exc:
                                print(f"during outage: {type(exc).__name__}")

                            await asyncio.sleep(args.outage_seconds / 2)

                        address = str(ipaddress.IPv4Address(socket.INADDR_LOOPBACK + 1))
                        start()
                        before = lookups
                        failures = []
                        for n in (2, 3):
                            try:
                                async with store.transaction() as tx:
                                    await sender.send({"n": n}, tx=tx)

                                await sender.flush(timeout=10)
                            except psycopg.Error as exc:
                                failures.append(exc)
                                print(f"after restart: {type(exc).__name__}")

                        stale = sender._failure is not None
                        if args.expect == "stuck":
                            assert len(failures) == 2 and stale
                            if args.store == "adapter":
                                assert failures[0] is failures[1] is sender._failure

                            print(
                                "REPRODUCED: sender is permanently failed after database recovery"
                            )
                            # Avoid re-raising the expected permanent failure on exit.
                            sender._failure = None
                        else:
                            assert not failures and not stale
                            assert lookups > before, (
                                "reconnect must resolve the name again"
                            )
                            rows = (
                                log.sql("SELECT n FROM log ORDER BY litelink_offset")
                                .read_all()
                                .to_pylist()
                            )
                            assert rows == [{"n": 1}, {"n": 2}, {"n": 3}], rows
                            print(
                                "RECOVERED: same sender, fresh DNS lookup, ordered rows exactly once"
                            )

                finally:
                    server.should_exit = True
                    await task
                    await store.close()

    finally:
        with contextlib.suppress(subprocess.CalledProcessError):
            control("-m", "immediate", "stop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--store", choices=("adapter", "builtin"), default="adapter")
    parser.add_argument("--expect", choices=("stuck", "recovered"), default="recovered")
    parser.add_argument("--outage-seconds", type=float, default=3)
    args = parser.parse_args()
    if args.outage_seconds <= 0:
        parser.error("--outage-seconds must be positive")

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
