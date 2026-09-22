"""Shipping a consumer's cursor to object storage, for recovery on another box.

A local cursor recovers a consumer that restarted; it does not recover one
whose machine is gone. These need an endpoint — `just rustfs` — and skip
without one, which `STREAMCAST_REQUIRE_S3` turns into a failure.
"""

from __future__ import annotations

import logging

import pytest

import streamcast
from streamcast._cursor import Cursor
from streamcast._remote import RemoteCursor

from .conftest import trade

pytestmark = pytest.mark.replication


@pytest.fixture
def prefix(bucket: str) -> str:
    """An `s3://.../` prefix unique to this test, so the key is derived."""
    return f"{bucket}/consumer/"


class TestCrossBoxRecovery:
    async def test_a_box_with_no_local_cursor_resumes_from_the_bucket(
        self, serve, log, tmp_path, prefix, s3
    ):
        """The case this exists for: the machine is gone.

        Not a restart — a restart has its local file. This is a different box
        with the same cursor key and nothing on disk.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(50)])

            first = tmp_path / "box1" / "trades.offset"
            first.parent.mkdir()
            async with streamcast.connect(
                uri,
                offset=0,
                cursor=first,
                cursor_uri=prefix,
                s3=s3,
                # **Long on purpose, and no sleep.** This used to set 0.2s
                # and then sleep 0.6s hoping the interval had fired — a race
                # against a daemon thread, which on a loaded box it can lose.
                # Nothing here needs the interval: `__aexit__` calls
                # `RemoteCursor.stop`, which joins the thread, and the thread
                # pushes one last time on its way out. Leaving the block IS
                # the upload. 30s makes that the only thing that can have
                # written the key, so a pass means the guarantee held rather
                # than that the timer got lucky.
                upload_every=30.0,
            ) as sub:
                for _ in range(30):
                    await sub.recv()

            assert first.read_text() == "30"

            second = tmp_path / "box2" / "trades.offset"
            second.parent.mkdir()
            assert not second.exists()
            async with streamcast.connect(
                uri, cursor=second, cursor_uri=prefix, s3=s3, upload_every=0.2
            ) as sub:
                assert (await sub.recv())[0] == 31

            # Written down locally too, so a second restart there needs no
            # bucket at all.
            assert second.exists()

    async def test_a_local_cursor_wins_over_the_remote(
        self, serve, log, tmp_path, prefix, s3
    ):
        """Local first, remote only when there is no local.

        The remote lags by up to `upload_every`, so preferring it would mean a
        consumer's own position losing to a stale copy of itself.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(50)])

            cursor = tmp_path / "trades.offset"
            async with streamcast.connect(
                uri,
                offset=0,
                cursor=cursor,
                cursor_uri=prefix,
                s3=s3,
                # As above: the push on close is the guarantee, not the timer.
                upload_every=30.0,
            ) as sub:
                for _ in range(10):
                    await sub.recv()

            # The local file moves on while nothing uploads.
            cursor.write_text("40")

            async with streamcast.connect(
                uri, cursor=cursor, cursor_uri=prefix, s3=s3, upload_every=30.0
            ) as sub:
                assert (await sub.recv())[0] == 41

    async def test_the_final_upload_happens_on_a_clean_exit(
        self, serve, log, tmp_path, prefix, s3
    ):
        # Otherwise the bucket holds what it held `upload_every` ago, and a
        # short-lived consumer would ship nothing at all.
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(20)])

            cursor = tmp_path / "trades.offset"
            async with streamcast.connect(
                uri,
                offset=0,
                cursor=cursor,
                cursor_uri=prefix,
                s3=s3,
                upload_every=600.0,  # never on the interval
            ) as sub:
                for _ in range(15):
                    await sub.recv()

            remote = RemoteCursor(Cursor(cursor), prefix, s3=s3)
            assert remote.load() == 15


class TestItIsBestEffort:
    async def test_an_unreachable_bucket_does_not_stop_the_consumer(
        self, serve, log, tmp_path, caplog
    ):
        """A disaster-recovery convenience is not a dependency of the stream.

        A consumer that could not reach the bucket and therefore refused to
        run would have traded an outage for a backup.
        """
        stream = streamcast.Stream("trades", log=log)
        async with serve(stream) as uri:
            await stream.send_many([trade(i) for i in range(5)])

            cursor = tmp_path / "trades.offset"
            with caplog.at_level(logging.DEBUG, logger="streamcast.cursor"):
                async with streamcast.connect(
                    uri,
                    offset=0,
                    cursor=cursor,
                    cursor_uri="s3://nope-does-not-exist-xyz/consumer/",
                    s3=streamcast.S3Options(
                        endpoint="http://127.0.0.1:1",
                        access_key="x",
                        secret_key="y",
                        region="us-east-1",
                    ),
                    # The push on close is what fails and logs, so there is
                    # no interval to wait for. `caplog` is still capturing:
                    # the inner block exits — and uploads — inside it.
                    upload_every=30.0,
                ) as sub:
                    assert (await sub.recv())[0] == 1

            # It said so, at debug, rather than raising.
            assert any("cursor" in record.message for record in caplog.records)
            assert cursor.read_text() == "1"

    async def test_a_missing_remote_is_not_an_error(self, tmp_path, prefix, s3):
        remote = RemoteCursor(Cursor(tmp_path / "absent.offset"), prefix, s3=s3)
        assert remote.load() is None


class TestConfiguration:
    def test_cursor_uri_needs_a_cursor(self):
        with pytest.raises(ValueError, match="needs a cursor="):
            streamcast.connect("ws://127.0.0.1:1/t", cursor_uri="s3://b/p/")

    def test_a_bad_scheme_raises_at_construction_not_silently(self, tmp_path):
        """Configuration errors fail loudly; only I/O is best-effort.

        `load` swallows everything that touches the network, deliberately — a
        bucket that is briefly unreachable must not become the consumer's
        problem. A mistyped scheme swallowed with it would leave a consumer
        that believed it was shipping a cursor and never had.
        """
        with pytest.raises(ValueError, match="must be an s3:// URI"):
            RemoteCursor(Cursor(tmp_path / "c.offset"), "file:///tmp/nope")

        with pytest.raises(ValueError, match="must be an s3:// URI"):
            streamcast.connect(
                "ws://127.0.0.1:1/t",
                cursor=tmp_path / "c.offset",
                cursor_uri="/tmp/nope",
            )

    def test_a_trailing_slash_is_a_prefix_and_the_filename_is_appended(self, tmp_path):
        cursor = Cursor(tmp_path / "trades.offset")
        assert (
            RemoteCursor(cursor, "s3://bucket/consumer1/")._key
            == "bucket/consumer1/trades.offset"
        )

    def test_without_one_it_is_the_whole_key(self, tmp_path):
        cursor = Cursor(tmp_path / "trades.offset")
        assert (
            RemoteCursor(cursor, "s3://bucket/exact/key.offset")._key
            == "bucket/exact/key.offset"
        )

    def test_the_thread_is_a_daemon(self, tmp_path, prefix, s3):
        # A consumer exiting with an upload in flight simply exits; a
        # best-effort backup has no business holding the interpreter open.
        remote = RemoteCursor(Cursor(tmp_path / "c.offset"), prefix, s3=s3)
        remote.start()
        try:
            assert remote._thread is not None
            assert remote._thread.daemon
        finally:
            remote.stop()

    def test_credentials_come_from_the_environment_and_are_overridable(self, s3):
        # litelink's model: `resolved()` fills unset fields from the
        # environment, and explicit wins.
        from streamcast._remote import _filesystem

        filesystem, _path = _filesystem("s3://bucket/key", None)
        assert filesystem is not None
        explicit, _ = _filesystem("s3://bucket/key", s3)
        assert explicit is not None
