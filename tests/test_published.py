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
    while log.seal() is not None:
        pass

    log.publish(push_unsettled=True)
    return log


def test_litelinks_provisioning_is_still_where_this_reads_it():
    """Private in litelink 0.6 (litelink#108). Fail here, by name, if it moves."""
    from litelink import _read

    for name in ("duckdb_connection", "load_extension", "secret_sql"):
        assert callable(getattr(_read, name)), f"litelink._read.{name} moved"


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

    def test_the_ambient_chain_is_resolved_again_per_connection(
        self, monkeypatch, tmp_path
    ):
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
        config = tmp_path / "config"
        config.write_text("[default]\nregion = us-east-1\n")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))

        chain = litelink.S3Options(endpoint="http://chain:9")
        first = _published.connection(chain, remote=True)
        try:
            # Gone from the database, as an expired one effectively is ...
            first.execute("DROP SECRET litelink_s3")
            second = _published.connection(chain, remote=True)
            try:
                # ... and back, because the next reader re-created it.
                secrets = "SELECT provider FROM duckdb_secrets()"
                assert second.execute(secrets).fetchall() == [("credential_chain",)]
            finally:
                second.close()
        finally:
            first.close()


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
                while log.seal() is not None:
                    pass

                log.publish(push_unsettled=True)

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
    log = litelink.new(tmp_path, "trades", schema=SCHEMA, published=bucket, s3=s3)
    with log:
        log.extend([{"i": i} for i in range(4)])
        while log.seal() is not None:
            pass

        log.publish(push_unsettled=True)

    table = _published.Table.open(bucket, "trades", s3)
    try:
        assert table.extent == (1, 5)
        assert table.scan().read_all().num_rows == 4
    finally:
        table.close()
