"""`_published`: reading a log's published table the way another machine does.

No litelink handle on the read side — DuckDB's `iceberg_scan` over the table
litelink publishes — so these tests write with litelink and read with nothing
of its but the extensions it provisions.
"""

from __future__ import annotations

import litelink
import pyarrow as pa
import pytest

from streamcast import _published

SCHEMA = pa.schema([pa.field("i", pa.int64(), nullable=False)])


def published_log(root, rows: int) -> litelink.WriteHandle:
    """A log with `rows` rows, every one sealed and published."""
    log = litelink.new(root, "trades", schema=SCHEMA)
    log.extend([{"i": i} for i in range(rows)])
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)
    return log


class TestTheConnection:
    """One database per process and credential set; a connection per reader."""

    def test_readers_share_a_database_and_not_their_views(self):
        first = _published.connection(None, remote=False)
        second = _published.connection(None, remote=False)
        try:
            # Shared: what one creates in the database, the other sees — so
            # `iceberg`, loaded once, is loaded for both.
            first.execute("CREATE OR REPLACE TABLE shared_probe AS SELECT 1 AS x")
            assert second.execute("SELECT x FROM shared_probe").fetchall() == [(1,)]
            # Private: a snapshot's `log` view is its own.
            first.execute("CREATE TEMP VIEW log AS SELECT 1 AS x")
            with pytest.raises(Exception, match="log"):
                second.execute("SELECT * FROM log")
        finally:
            first.execute("DROP TABLE shared_probe")
            first.close()
            second.close()

    def test_different_credentials_never_share_a_database(self):
        one = litelink.S3Options(access_key="a", secret_key="1", endpoint="http://x:9")
        two = litelink.S3Options(access_key="b", secret_key="2", endpoint="http://x:9")
        first = _published.connection(one, remote=True)
        second = _published.connection(two, remote=True)
        again = _published.connection(one, remote=True)
        try:
            keys = "SELECT secret_string FROM duckdb_secrets()"
            assert "key_id=a;" in str(first.execute(keys).fetchall())
            assert "key_id=b;" in str(second.execute(keys).fetchall())
            assert "key_id=a;" in str(again.execute(keys).fetchall())
        finally:
            for connected in (first, second, again):
                connected.close()

    def test_the_ambient_chain_refreshes_itself(self, monkeypatch, tmp_path):
        """A database outlives an STS session, so its chain secret must renew.

        litelink creates it with `REFRESH auto`; this pins that the shared
        database got that secret, not one that resolves once and expires.
        """
        # No keys in the environment, so the secret is the chain — and a
        # credentials file of the test's own for the chain to find: DuckDB
        # refuses to create a chain secret that resolves to nothing, and a
        # test must not depend on the machine's ~/.aws.
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.delenv(name, raising=False)

        credentials = tmp_path / "credentials"
        credentials.write_text(
            "[default]\naws_access_key_id = chain\naws_secret_access_key = chain\n"
        )
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))

        chain = litelink.S3Options(endpoint="http://chain:9")
        connected = _published.connection(chain, remote=True)
        try:
            [(provider, described)] = connected.execute(
                "SELECT provider, secret_string FROM duckdb_secrets()"
            ).fetchall()
            assert provider == "credential_chain"
            assert "'refresh': auto" in described
        finally:
            connected.close()


class TestALocalTable:
    def test_it_reads_the_rows_and_their_extent(self, tmp_path):
        with published_log(tmp_path, 5) as log:
            table = _published.Table.open(log.published, "trades", None)
            try:
                assert table.extent == (1, 6)
                assert "i" in table.schema.names
                read = table.scan(
                    columns=["litelink_offset", "i"], start_offset=2, end_offset=5
                )
                assert read.read_all().to_pylist() == [
                    {"litelink_offset": o, "i": o - 1} for o in (2, 3, 4)
                ]
            finally:
                table.close()

    def test_an_open_table_stays_at_its_snapshot(self, tmp_path):
        """The extent and the rows come from one commit, whatever lands after."""
        with published_log(tmp_path, 3) as log:
            pinned = _published.Table.open(log.published, "trades", None)
            try:
                log.extend([{"i": i} for i in range(3, 6)])
                while log.seal(flush=True) is not None:
                    pass

                log.publish(flush=True)

                assert pinned.extent == (1, 4)
                assert pinned.scan().read_all().num_rows == 3

                fresh = _published.Table.open(log.published, "trades", None)
                try:
                    assert fresh.extent == (1, 7)
                finally:
                    fresh.close()
            finally:
                pinned.close()

    def test_a_table_never_published_is_an_error_not_an_empty_read(self, tmp_path):
        with litelink.new(tmp_path, "trades", schema=SCHEMA) as log:
            with pytest.raises(Exception, match="version-hint"):
                _published.Table.open(log.published, "trades", None)


@pytest.mark.replication
def test_an_s3_table_reads_with_the_readers_own_credentials(tmp_path, s3, bucket):
    log = litelink.new(
        tmp_path, "trades", schema=SCHEMA, published=bucket, s3_options=s3
    )
    with log:
        log.extend([{"i": i} for i in range(4)])
        while log.seal(flush=True) is not None:
            pass

        log.publish(flush=True)

    table = _published.Table.open(bucket, "trades", s3)
    try:
        assert table.extent == (1, 5)
        assert table.scan().read_all().num_rows == 4
    finally:
        table.close()


def publish(log: litelink.WriteHandle, start: int, count: int) -> None:
    log.extend([{"i": i} for i in range(start, start + count)])
    while log.seal(flush=True) is not None:
        pass

    log.publish(flush=True)


class TestTheReadCache:
    """litelink's read caches, as the caller asks for them (#77)."""

    def test_different_caching_never_shares_a_database(self):
        cached = _published.connection(None, remote=False)
        uncached = _published.connection(
            None, remote=False, cache=_published.ReadCache(memory_cache=False)
        )
        # Disk settings mean nothing to a local read, so they do not split it.
        disk = _published.connection(
            None,
            remote=False,
            cache=_published.ReadCache(disk_cache=True, cache_key="anything"),
        )
        setting = "SELECT current_setting('enable_external_file_cache')"
        try:
            assert cached.execute(setting).fetchone() == (True,)
            assert uncached.execute(setting).fetchone() == (False,)
            cached.execute("CREATE OR REPLACE TABLE cache_probe AS SELECT 1 AS x")
            assert disk.execute("SELECT x FROM cache_probe").fetchall() == [(1,)]
            with pytest.raises(Exception, match="cache_probe"):
                uncached.execute("SELECT x FROM cache_probe")
        finally:
            cached.execute("DROP TABLE cache_probe")
            for connected in (cached, uncached, disk):
                connected.close()

    def test_a_disk_cached_reader_sees_every_publish(self, tmp_path, s3, bucket):
        """The hint is the one object that changes, and `cache_httpfs` would
        serve an old one for ever (litelink#141): a reader pinned to the
        first snapshot it saw. So it is read around the cache."""
        cache = tmp_path / "cache"
        connected = _published.connection(
            s3,
            remote=True,
            cache=_published.ReadCache(disk_cache=True, cache_key=str(cache)),
        )
        log = litelink.new(
            tmp_path / "data", "trades", schema=SCHEMA, published=bucket, s3_options=s3
        )
        try:
            with log:
                publish(log, 0, 100)
                first = _published.Table.open(bucket, "trades", s3, shared=connected)
                assert first.record_count == 100

                publish(log, 100, 50)
                again = _published.Table.open(bucket, "trades", s3, shared=connected)
                assert again.record_count == 150
                assert again.extent == (1, 151)

            # And the disk cache is on: what was read is under the key.
            assert any(path.is_file() for path in cache.rglob("*"))
        finally:
            connected.close()

    async def test_the_settings_reach_every_read_of_a_stream(
        self, tmp_path, serve, monkeypatch
    ):
        """`Stream.snapshot`, `live` — its base and every rebase — pass the
        caller's settings to the connection every published read goes through."""
        import streamcast  # noqa: PLC0415

        asked: list[_published.ReadCache] = []
        real = _published.connection

        def recording(s3_options, *, remote, cache=_published.DEFAULT_CACHE):  # noqa: ANN001, ANN202
            asked.append(cache)
            return real(s3_options, remote=remote, cache=cache)

        monkeypatch.setattr(_published, "connection", recording)
        schema = {
            "type": "object",
            "properties": {"i": {"type": "integer"}},
            "required": ["i"],
        }
        stream = streamcast.Stream.new("t", root=tmp_path, schema=schema)
        await stream.send_many([{"i": i} for i in range(3)])
        assert stream.log is not None
        while stream.log.seal(flush=True) is not None:
            pass

        stream.log.publish(flush=True)
        key = tmp_path / "k"
        wanted = _published.ReadCache(False, True, str(tmp_path / "k"), 0.5)
        try:
            async with serve(stream, maintain=False) as uri:
                assert stream.metadata_uri is not None
                async with await streamcast.Stream.snapshot(
                    stream.metadata_uri,
                    memory_cache=False,
                    disk_cache=True,
                    cache_key=key,
                    disk_cache_volume_limit=0.5,
                ):
                    pass

                async with await streamcast.Stream.live(
                    uri,
                    memory_cache=False,
                    disk_cache=True,
                    cache_key=key,
                    disk_cache_volume_limit=0.5,
                ) as live:
                    await live.rebase()

            # A snapshot, the live base, and the rebase.
            assert asked == [wanted] * 3
        finally:
            await stream.aclose()
