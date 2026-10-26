"""A stream reopened with `s3_options` reads its published table with THEM.

`StreamHandle.open` passed its options to `litelink.new`, which creates a log, and
not to `litelink.open`, which reopens one. A reopened log then resolved its
credentials from the environment. Where the environment holds another
store's key — a server whose streams publish to different buckets — every
restart after the stream's first publish failed reading its own metadata,
and the server did not start.
"""

from __future__ import annotations

import dataclasses

import pytest

import streamcast

SCHEMA = {
    "type": "object",
    "properties": {"i": {"type": "integer"}},
    "required": ["i"],
}


async def _published_stream(root, bucket, s3) -> None:
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=root.absolute(), published=bucket, s3_options=s3
            ),
        ),
        initialization="create_if_absent",
    )
    try:
        await stream.send_many([{"i": i} for i in range(20)])
        log = stream.log
        assert log is not None
        while log.seal(flush=True) is not None:
            pass

        log.publish(flush=True)
        assert log.published_through() == 20, "the stream must have published"
    finally:
        await stream.aclose()


@pytest.mark.parametrize("environment", ["wrong", "absent"])
async def test_a_reopened_stream_reads_with_its_own_options(
    tmp_path, bucket, s3, monkeypatch, environment
):
    await _published_stream(tmp_path, bucket, s3)

    # The environment now holds another store's key, or none at all; only the
    # options the stream is given can read its table.
    if environment == "wrong":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "someone-else")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-this-stores-secret")
    else:
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.delenv(name, raising=False)

    explicit = dataclasses.replace(s3)  # the stream's own, passed explicitly
    reopened = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(name="t", schema=SCHEMA),
            storage=streamcast.Storage(
                root=tmp_path.absolute(), published=bucket, s3_options=explicit
            ),
        ),
        initialization="create_if_absent",
    )
    try:
        # What the broker does for every stream before it admits a connection.
        reopened.ensure_metadata()
        log = reopened.log
        assert log is not None
        assert log.published_through() == 20
    finally:
        await reopened.aclose()
