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
