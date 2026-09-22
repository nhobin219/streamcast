"""Fixtures. No network, no container, no credentials — by construction.

Every test here binds to port 0 on loopback and every log is a `tmp_path`
directory, so the suite is a pure local-file workload and a change that breaks
that is a change to reject.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from typing import TYPE_CHECKING, Any

import litelink
import pyarrow as pa
import pytest

import streamcast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from pathlib import Path

    from litelink import WriteHandle
    from websockets.asyncio.server import Server

    from streamcast import Stream


SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        # 0 buy, 1 sell, as the feed spells it.
        pa.field("side", pa.int64()),
        pa.field("tag", pa.string()),
    ]
)
"""A stream's columns — the CALLER's, which is the whole point.

streamcast declares none of this. `tag` is nullable and the helper below
leaves it out half the time, because a column a caller omits has to survive
the round trip as NULL and come back the same way live or replayed.
"""


def trade(i: int) -> dict:
    """One row. `i` is recoverable from it, so ordering is checkable."""
    row = {
        "event_ts": 1_790_038_800_000_000 + i,
        "price": 85_565.0 + i,
        "amount": 0.015,
        "side": i % 2,
    }
    if i % 2 == 0:
        row["tag"] = f"t{i}"

    return row


@pytest.fixture
def log(tmp_path: Path) -> Iterator[WriteHandle]:
    """A log with the caller's schema, as litelink intends.

    `target_seal_size` is small so a test that seals actually seals rather
    than leaving every row in the SQLite buffer — a replay that never reads
    Parquet is a replay that never exercised the tier it is testing.
    """
    handle = litelink.new(
        tmp_path / "data",
        "trades",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=litelink.LogConfig(target_seal_size=4 * 1024, compact_min_files=2),
    )
    with handle:
        yield handle


@pytest.fixture
def serve() -> Callable[..., contextlib.AbstractAsyncContextManager[str]]:
    """`async with serve(stream) as uri:` — a server on an ephemeral port.

    Returns the URI rather than the server, because every test that starts one
    immediately wants to connect to it and port 0 means nobody can spell the
    URI ahead of time. The server itself is reachable through the stream for
    the handful of tests that need it.
    """

    @contextlib.asynccontextmanager
    async def _serve(*streams: Stream, **kwargs: Any) -> AsyncIterator[str]:
        server: Server = await streamcast.serve(list(streams), "127.0.0.1", 0, **kwargs)
        try:
            port = server.sockets[0].getsockname()[1]
            name = streams[0].name
            yield f"ws://127.0.0.1:{port}/{name}"
        finally:
            server.close()
            await server.wait_closed()

    return _serve


# -- the replication tier ----------------------------------------------------
#
# Ported from litelink's conftest, and for the same reason it exists there: a
# tier that needs infrastructure will skip without it, and a skip is not a
# pass. `just rustfs` brings up an endpoint; `STREAMCAST_REQUIRE_S3` turns the
# skip into a failure, which is what CI sets.

_BUCKET = "STREAMCAST_TEST_BUCKET"


def s3_options() -> litelink.S3Options:
    """Explicit for rustfs, environment for anything else.

    `just rustfs` is the default because it needs no credentials to exist
    anywhere. Naming a bucket through `STREAMCAST_TEST_BUCKET` — or pointing
    `AWS_ENDPOINT_URL` elsewhere — switches to whatever the environment
    resolves, which on AWS is the ordinary chain: profile, instance metadata,
    SSO.
    """
    if os.environ.get("AWS_ENDPOINT_URL") or os.environ.get(_BUCKET):
        return litelink.S3Options().resolved()

    return litelink.S3Options(
        endpoint="http://127.0.0.1:9002",
        access_key="streamcast",
        secret_key="streamcast-secret",
        region="us-east-1",
    ).resolved()


def filesystem(s3: litelink.S3Options):  # noqa: ANN201 — s3fs is a dev import
    s3fs = pytest.importorskip(
        "s3fs",
        reason="s3fs is a dev dependency used by these fixtures. Run `uv sync`.",
    )

    return s3fs.S3FileSystem(
        key=s3.access_key,
        secret=s3.secret_key,
        client_kwargs={"endpoint_url": s3.endpoint, "region_name": s3.region},
    )


@pytest.fixture(scope="session")
def s3() -> Iterator[litelink.S3Options]:
    """The endpoint, or a skip. Reachability is checked once, by listing.

    A connection error means no endpoint is running and the tier is untestable
    here; anything else is a real failure and must not be swallowed into a
    skip, or a broken endpoint would look like an absent one.
    """
    resolved = s3_options()
    fs = filesystem(resolved)
    try:
        fs.ls("/")
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("STREAMCAST_REQUIRE_S3"):
            pytest.fail(
                f"STREAMCAST_REQUIRE_S3 is set but the endpoint at "
                f"{resolved.endpoint or 'the AWS default'} did not answer: {exc}"
            )

        pytest.skip(f"no S3 endpoint ({exc}); `just rustfs` starts one")

    # **Into the environment, because the children read it from there.**
    # `serve` starts two subprocesses — the maintainer and litestream — and
    # neither is handed an `S3Options`: litelink deliberately never persists
    # credentials, and its model is that they resolve from the ordinary AWS
    # chain at the point of use. Without this the maintainer talks to real
    # AWS and reports NO_SUCH_BUCKET about the local one, which is how this
    # was first found.
    #
    # litestream reads its own pair rather than the AWS ones, because the
    # config litelink generates is safe to commit and carries no secret.
    with pytest.MonkeyPatch.context() as patch:
        for name, value in (
            ("AWS_ENDPOINT_URL", resolved.endpoint),
            ("AWS_ACCESS_KEY_ID", resolved.access_key),
            ("AWS_SECRET_ACCESS_KEY", resolved.secret_key),
            ("AWS_REGION", resolved.region),
            ("LITESTREAM_ACCESS_KEY_ID", resolved.access_key),
            ("LITESTREAM_SECRET_ACCESS_KEY", resolved.secret_key),
        ):
            if value:
                patch.setenv(name, value)

        yield resolved


@pytest.fixture
def bucket(s3: litelink.S3Options) -> str:
    """A prefix inside the shared bucket, unique to this test."""
    name = os.environ.get(_BUCKET, "streamcast-demo")
    fs = filesystem(s3)
    if not fs.exists(name):
        fs.mkdir(name)

    return f"s3://{name}/{uuid.uuid4().hex}"
