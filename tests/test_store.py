"""A log consumer's state (SPEC §8c): declared tables, the offset committed
with them, and replication through litelink's own and `Sidecar`.

The replica tests run the real sidecar against a local endpoint (`just
rustfs`); everything else is local files.
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
import tomllib
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from litelink import S3Options, WriteHandle
from litelink.replication import control_socket, litestream_binary

from streamcast import Sidecar, Store, StoreTable
from streamcast import _store as store_module

ITEMS = StoreTable(
    schema=pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("state", pa.string(), nullable=False),
            pa.field("score", pa.float64()),
        ]
    ),
    key=("id",),
)
CHECKPOINT = StoreTable(
    schema=pa.schema([pa.field("seen", pa.int64(), nullable=False)]),
    single_row=True,
)
TABLES = {"items": ITEMS, "checkpoint": CHECKPOINT}

# A syntactically valid location. Nothing in the local tests reaches it.
NOWHERE = "s3://bucket/stores"


def opened(root: Path, **overrides: object) -> Store:
    arguments: dict[str, object] = {"tables": TABLES} | overrides
    return Store.open(root, "state", **arguments)  # ty: ignore[invalid-argument-type]


def work(store: Store, upto: int, start: int = 1) -> None:
    """One transaction per message: the consumer's write and its offset."""
    for i in range(start, upto + 1):
        with store.transaction() as tx:
            tx.execute(
                "INSERT INTO items (id, state, score) VALUES (?, ?, ?)",
                (i, f"s{i}", i / 2),
            )
            tx.execute(
                'INSERT INTO checkpoint ("_streamcast_row", seen) VALUES (0, ?) '
                'ON CONFLICT ("_streamcast_row") DO UPDATE SET seen = excluded.seen',
                (i,),
            )
            tx.advance("sub", i)


def sidecar_binary() -> str:
    binary = litestream_binary()
    if shutil.which(binary) is None and not os.access(binary, os.X_OK):
        # Fails where the replication tier is required, as `conftest.s3` does:
        # a litelink built from source without its platform hook has no
        # binary, and these tests would otherwise pass by skipping.
        if os.environ.get("STREAMCAST_REQUIRE_S3"):
            pytest.fail("STREAMCAST_REQUIRE_S3 is set but litestream is not there")

        pytest.skip("litestream is not provisioned; litelink's wheel carries it")

    return binary


# -- the replication config is the log's --------------------------------------


def _blocks(config: str) -> tuple[str, dict[str, str]]:
    """The config's header (the socket), and each database's block by file."""
    header, *databases = config.split("  - path: ")
    blocks = {}
    for block in databases:
        path, _, body = block.partition("\n")
        blocks[Path(path).name] = body

    return header, blocks


def test_the_store_config_is_the_logs_for_the_same_inputs(tmp_path: Path) -> None:
    """Done when: the config the Store emits matches what the Log emits for the
    same inputs, from one shared helper.

    Same root, name, published location, endpoint and retention: the socket
    is the same, and the store's one database gets exactly the block the
    log's buffer gets, with the file name changed — the replica key, the
    snapshot window that prunes, and the endpoint lines.

    Falsify by building the store's config anywhere but `litestream_config`
    (e.g. a destination without `/_wal`, or no `snapshot_block`): the block
    differs from the log's.
    """
    s3 = S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1")
    retention = timedelta(hours=6)
    log = WriteHandle.replication_config_for(tmp_path, "state", NOWHERE, s3, retention)
    for_store = Store.replication_config_for(tmp_path, "state", NOWHERE, s3, retention)

    log_header, log_blocks = _blocks(log)
    store_header, store_blocks = _blocks(for_store)
    assert store_header == log_header
    assert set(store_blocks) == {"store.db"}
    assert store_blocks["store.db"] == log_blocks["buffer.db"].replace(
        "buffer.db", "store.db"
    )
    # The pieces that block carries, so an empty match cannot pass.
    assert "path: stores/state/_wal/store.db" in for_store
    assert "retention: 21600s" in for_store
    assert "interval: 10800s" in for_store

    # An open store emits the same file, from what it recorded, beside its
    # database.
    with opened(tmp_path, published=NOWHERE, s3_options=s3, retention=retention) as s:
        assert s.replication_config() == for_store
        written = s.write_replication_config()
        assert written == s.path.parent / "litestream.yml"
        assert written.read_text() == for_store


def test_replication_needs_an_object_store(tmp_path: Path) -> None:
    """A replica on this machine is no replica; a store that names none has
    nothing to emit or flush."""
    with pytest.raises(ValueError, match="s3://"):
        opened(tmp_path, published=f"file://{tmp_path}/elsewhere")

    # Refused when it is recorded, not at the first config written from it:
    # `published` is split positionally, so a malformed one names another
    # bucket.
    with pytest.raises(ValueError, match="bucket"):
        opened(tmp_path, published="s3://a:b/stores")

    assert not (tmp_path / "state" / "store.db").exists()

    with opened(tmp_path) as store:
        assert store.published is None
        with pytest.raises(ValueError, match="no published location"):
            store.replication_config()

        with pytest.raises(ValueError, match="no published location"):
            store.flush()


def test_the_published_location_is_fixed_once_recorded(tmp_path: Path) -> None:
    opened(tmp_path, published=NOWHERE).close()
    with pytest.raises(ValueError, match="replicates to"):
        opened(tmp_path, published="s3://bucket/other")

    # Reopening without it keeps the recorded one.
    with opened(tmp_path) as store:
        assert store.published == NOWHERE


def test_a_store_refuses_a_logs_directory(tmp_path: Path) -> None:
    """The config and the control socket are both derived from the directory,
    so a store inside a log's would take over the log's sidecar."""
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "buffer.db").touch()
    with pytest.raises(FileExistsError, match="holds a log"):
        opened(tmp_path)


def test_a_store_inside_a_logs_directory_is_refused(tmp_path: Path) -> None:
    """litelink removes a retired log's directory whole, so a store rooted in
    one would be deleted with it.

    Falsify by checking only the store's own directory in `refuse_a_log`: the
    store is created inside the log's.
    """
    (tmp_path / "log").mkdir()
    (tmp_path / "log" / "buffer.db").touch()
    with pytest.raises(FileExistsError, match="is inside it"):
        Store.open(tmp_path / "log", "state", tables=TABLES)

    with pytest.raises(FileExistsError, match="is inside it"):
        Store.restore(
            tmp_path / "log" / "deeper", "state", published=NOWHERE, tables=TABLES
        )

    assert not (tmp_path / "log" / "state").exists()


def test_a_failed_restore_leaves_no_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A restore that raises brings no store back, so it leaves no config
    either: a config beside an empty directory reads as a store to run.

    Falsify by dropping the `except` around `restore_database`: the config
    stays.
    """

    def unreachable(*_: object, **__: object) -> bool:
        raise RuntimeError("the endpoint did not answer")

    monkeypatch.setattr(store_module, "restore_database", unreachable)
    with pytest.raises(RuntimeError, match="did not answer"):
        Store.restore(tmp_path, "state", published=NOWHERE, tables=TABLES)

    assert not (tmp_path / "state" / "litestream.yml").exists()
    assert not (tmp_path / "state" / "store.db").exists()


def test_a_returning_cursor_left_half_read_commits_nothing(tmp_path: Path) -> None:
    """SQLite will not commit with a statement in progress; the transaction
    rolls back whole rather than committing part of it (S1)."""
    with opened(tmp_path) as store:
        with pytest.raises(sqlite3.OperationalError), store.transaction() as tx:
            # Held, as a consumer holding the cursor would: one collected at
            # once is reset, and the commit then succeeds.
            cursor = tx.execute(
                "INSERT INTO items (id, state) VALUES (1, 'a'), (2, 'b') RETURNING id"
            )
            assert cursor.fetchone() is not None
            tx.advance("sub", 1)

        assert store.offset("sub") is None
        assert store.query("SELECT count(*) FROM items") == [(0,)]


def test_a_store_restore_leaves_a_logs_directory_untouched(tmp_path: Path) -> None:
    """The refusal comes before `restore` writes anything: its config would
    replace the log's `litestream.yml`, or be removed with it when there is no
    replica, and the log's sidecar would stop replicating its buffer.

    Falsify by moving `refuse_a_log()` in `restore` after the config is
    written: the log's config is gone or replaced.
    """
    schema = pa.schema([pa.field("event_ts", pa.int64(), nullable=False)])
    with WriteHandle.new(tmp_path, "state", schema=schema, published=NOWHERE) as log:
        config = log.write_replication_config()
        before = config.read_text()

    with pytest.raises(FileExistsError, match="holds a log"):
        Store.restore(tmp_path, "state", published=NOWHERE, tables=TABLES)

    assert config.read_text() == before
    assert not (tmp_path / "state" / "store.db").exists()


# -- flush --------------------------------------------------------------------


def test_flush_without_a_sidecar_raises_and_starts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Done when: with no sidecar, `flush()` raises and starts nothing.

    It asks the control socket and, finding nothing there, raises rather than
    start a litestream of its own. Every process it ran is a `sync` client.

    Falsify by having `flush` fall back to `litestream replicate` when the
    socket does not answer: a second command appears, or no error.
    """
    binary = sidecar_binary()
    ran: list[list[str]] = []
    popen = subprocess.Popen

    # Every process goes through `Popen`, `subprocess.run` included, so this
    # sees whatever `flush` starts.
    def recorded(command: list[str], *args: object, **kwargs: object) -> object:
        ran.append(list(command))
        return popen(command, *args, **kwargs)  # noqa: S603  # ty: ignore[no-matching-overload]

    monkeypatch.setattr(subprocess, "Popen", recorded)
    with opened(tmp_path, published=NOWHERE) as store:
        work(store, 3)
        with pytest.raises(RuntimeError, match="control socket"):
            store.flush(binary, timeout=2)

    assert ran, "flush never asked the sidecar"
    assert all(command[1] == "sync" for command in ran), ran
    assert all("-socket" in command for command in ran), ran


def test_flush_asks_the_socket_it_is_given_and_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`flush(socket=X)` asks `X`, not the socket the config derives, and the
    error names `X` when nothing answers there.

    A sidecar that merges several configs listens on one socket of its own,
    so a flush that kept asking the derived one would never reach it.

    Falsify by ignoring `socket` in `_replication.flush`: the command carries
    the derived path, and the error names it.
    """
    binary = sidecar_binary()
    ran: list[list[str]] = []
    popen = subprocess.Popen

    def recorded(command: list[str], *args: object, **kwargs: object) -> object:
        ran.append(list(command))
        return popen(command, *args, **kwargs)  # noqa: S603  # ty: ignore[no-matching-overload]

    monkeypatch.setattr(subprocess, "Popen", recorded)
    given = tmp_path / "merged.sock"
    with opened(tmp_path, published=NOWHERE) as store:
        derived = control_socket(store._layout)  # noqa: SLF001
        assert given != derived
        work(store, 1)
        with pytest.raises(RuntimeError, match="control socket") as raised:
            store.flush(binary, timeout=2, socket=given)

    assert str(given) in str(raised.value)
    assert str(derived) not in str(raised.value)
    assert len(ran) == 1, ran
    command = ran[0]
    assert command[command.index("-socket") + 1] == str(given)
    assert str(derived) not in command


def test_flush_inside_a_transaction_is_refused(tmp_path: Path) -> None:
    """It ships what is committed, which would not include the transaction."""
    with opened(tmp_path, published=NOWHERE) as store, store.transaction():
        with pytest.raises(RuntimeError, match="inside a store transaction"):
            store.flush()


@pytest.mark.replication
def test_flush_then_restore_brings_back_the_tables_and_the_offset(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Done when: with a sidecar running, `flush()`, then the local file gone,
    then `restore()`, brings back the tables and the offset.

    The sidecar's own sync is an hour away, so only `flush` can have shipped
    the writes. The local file is gone in the sense a failover means: restored
    onto a root that has none, while the sidecar still runs, because stopping
    it ships on the way out and would hide a missing flush.

    Falsify by removing `store.flush(...)` below: the restore finds no replica,
    or one without the writes.
    """
    binary = sidecar_binary()
    published = f"{bucket}/stores"
    primary = tmp_path / "primary"
    store = opened(primary, published=published, s3_options=s3)
    config = store.write_replication_config()
    config.write_text(
        config.read_text().replace(
            "      force-path-style: true\n",
            "      force-path-style: true\n      sync-interval: 1h\n",
        )
    )
    environment = dict(os.environ)
    resolved = s3.resolved()
    environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key or ""
    environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key or ""
    sidecar = subprocess.Popen(  # noqa: S603
        [binary, "replicate", "-config", str(config)],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 15
        while not control_socket(store._layout).exists():  # noqa: SLF001
            assert time.monotonic() < deadline, "the sidecar never listened"
            time.sleep(0.1)

        work(store, 200)
        store.flush(binary)
        store.close()

        replacement = tmp_path / "replacement"
        with Store.restore(
            replacement,
            "state",
            published=published,
            tables=TABLES,
            s3_options=s3,
            binary=binary,
        ) as restored:
            assert restored.offset("sub") == 200
            assert restored.query("SELECT count(*), max(id) FROM items") == [(200, 200)]
            assert restored.query("SELECT seen FROM checkpoint") == [(200,)]
            assert restored.published == published
            assert (replacement / "state" / "litestream.yml").exists()
            # It carries on where it stopped.
            work(restored, 201, start=201)
            assert restored.offset("sub") == 201
    finally:
        sidecar.terminate()
        sidecar.wait(timeout=10)


@pytest.mark.replication
async def test_flush_through_a_sidecar_on_its_own_socket(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """`Sidecar.of` on a store's config listens on its own socket, which no
    config of the store derives: `flush(socket=sidecar.socket)` ships through
    it and a restore brings the rows back; without `socket=` the flush raises.

    The sidecar's own sync is an hour away, so only the flush can have
    shipped the writes, and it still runs during the restore, as in the
    test above.

    Falsify by having `Store.flush` drop `socket`: the flush given the socket
    raises like the one without it.
    """
    binary = sidecar_binary()
    published = f"{bucket}/stores"
    store = opened(tmp_path / "primary", published=published, s3_options=s3)
    derived = control_socket(store._layout)  # noqa: SLF001
    config = store.write_replication_config()
    config.write_text(
        config.read_text().replace(
            "      force-path-style: true\n",
            "      force-path-style: true\n      sync-interval: 1h\n",
        )
    )
    sidecar = Sidecar.of([config], binary=binary, s3_options=s3)
    sidecar.start()
    try:
        for _ in range(150):
            if sidecar.socket.exists():
                break

            await asyncio.sleep(0.1)

        assert sidecar.socket.exists(), "the sidecar never listened"
        assert sidecar.socket != derived
        assert not derived.exists()
        work(store, 50)
        with pytest.raises(RuntimeError, match="control socket"):
            await asyncio.to_thread(store.flush, binary, 2)

        await asyncio.to_thread(store.flush, binary, socket=sidecar.socket)
        store.close()

        with Store.restore(
            tmp_path / "replacement",
            "state",
            published=published,
            tables=TABLES,
            s3_options=s3,
            binary=binary,
        ) as restored:
            assert restored.offset("sub") == 50
            assert restored.query("SELECT count(*), max(id) FROM items") == [(50, 50)]
    finally:
        sidecar.terminate()
        await sidecar.wait_closed()


@pytest.mark.replication
def test_restore_without_a_replica_names_where_it_looked(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    sidecar_binary()
    with pytest.raises(FileNotFoundError, match=f"{bucket}/stores/state/_wal"):
        Store.restore(
            tmp_path,
            "state",
            published=f"{bucket}/stores",
            tables=TABLES,
            s3_options=s3,
        )

    assert not (tmp_path / "state" / "store.db").exists()


def test_restore_never_overwrites_a_local_store(tmp_path: Path) -> None:
    """A local database is the source of truth; a restore over it would drop
    whatever the replica never received. A stray `-wal` is refused too, since
    SQLite would replay it into the restored file.

    Refused before anything is written: the running store's `litestream.yml`
    is what its sidecar reads, and a restore that wrote its own config first
    would replace a deployment's edit of it. Falsify by dropping the store's
    own existence check from `restore`, leaving litelink's: the config is
    rewritten before litelink refuses.
    """
    with opened(tmp_path, published=NOWHERE) as store:
        config = store.write_replication_config()

    config.write_text(config.read_text() + "# the deployment's\n")
    before = config.read_text()
    with pytest.raises(FileExistsError, match="never restored over"):
        Store.restore(tmp_path, "state", published=NOWHERE, tables=TABLES)

    assert config.read_text() == before

    other = tmp_path / "other"
    (other / "state").mkdir(parents=True)
    (other / "state" / "store.db-wal").touch()
    with pytest.raises(FileExistsError, match="store.db-wal"):
        Store.restore(other, "state", published=NOWHERE, tables=TABLES)


# -- the write and its offset -------------------------------------------------

_CHILD = textwrap.dedent(
    """
    import sys
    import pyarrow as pa
    from streamcast import Store
    from tests.test_store import TABLES, work

    store = Store.open(sys.argv[1], "state", tables=TABLES)
    work(store, 50)
    with store.transaction() as tx:
        tx.execute("INSERT INTO items (id, state) VALUES (51, 's51')")
        tx.advance("sub", 51)
        print("inside", flush=True)
        sys.stdin.read()
    """
)


def test_a_write_and_its_offset_survive_kill_9_together(tmp_path: Path) -> None:
    """Done when: a write and its offset are atomic across `kill -9`.

    The child commits fifty messages, then is killed inside the fifty-first,
    after both its row and its offset are written. Every committed message is
    there with its offset, and the one in flight is there with neither.

    Falsify by having `advance` commit the offset on its own (ROLLBACK, BEGIN,
    write the offset, COMMIT, BEGIN): the store comes back at offset 51 with
    50 rows.
    """
    child = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", _CHILD, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=Path(__file__).parents[1],
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline() == b"inside\n"
    finally:
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=10)

    assert child.returncode == -signal.SIGKILL
    with opened(tmp_path) as store:
        assert store.offset("sub") == 50
        assert store.query("SELECT count(*), max(id) FROM items") == [(50, 50)]
        assert store.query("SELECT seen FROM checkpoint") == [(50,)]


def test_a_raising_transaction_keeps_neither(tmp_path: Path) -> None:
    with opened(tmp_path) as store:
        with pytest.raises(KeyError), store.transaction() as tx:
            tx.execute("INSERT INTO items (id, state) VALUES (1, 'a')")
            tx.advance("sub", 1)
            raise KeyError

        assert store.offset("sub") is None
        assert store.query("SELECT count(*) FROM items") == [(0,)]


def test_a_transaction_sqlite_rolled_back_commits_nothing_after(
    tmp_path: Path,
) -> None:
    """An `OR ROLLBACK` conflict ends the transaction inside SQLite. A consumer
    that catches it and carries on must not autocommit the rest.

    Falsify by dropping the in-transaction check from `_live`: the offset
    lands on its own, without the row it describes.
    """
    with opened(tmp_path) as store:
        work(store, 1)
        with pytest.raises(RuntimeError, match="rolled this transaction back"):  # noqa: SIM117
            with store.transaction() as tx:
                tx.execute("INSERT INTO items (id, state) VALUES (2, 'b')")
                with pytest.raises(sqlite3.IntegrityError):
                    tx.execute(
                        "INSERT OR ROLLBACK INTO items (id, state) VALUES (1, 'x')"
                    )

                tx.advance("sub", 2)

        assert store.offset("sub") == 1
        assert store.query("SELECT count(*) FROM items") == [(1,)]


@pytest.mark.parametrize(
    "sql",
    [
        "COMMIT",
        "SAVEPOINT a",
        "PRAGMA synchronous=OFF",
        "CREATE TABLE extra (x)",
        "DROP TABLE items",
        "ALTER TABLE items ADD COLUMN x",
        "ATTACH ':memory:' AS other",
        "UPDATE _streamcast_offsets SET litelink_offset = 99",
        "DELETE FROM _streamcast_meta",
    ],
)
def test_consumer_sql_cannot_break_the_transaction(tmp_path: Path, sql: str) -> None:
    with opened(tmp_path) as store:
        work(store, 1)
        with store.transaction() as tx:
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                tx.execute(sql)

            tx.advance("sub", 2)

        assert store.offset("sub") == 2


def test_offsets_only_advance(tmp_path: Path) -> None:
    """A replayed message is refused, not applied twice."""
    with opened(tmp_path) as store:
        work(store, 3)
        for stale in (3, 2):
            with (
                pytest.raises(ValueError, match="already at 3"),
                store.transaction() as tx,
            ):
                tx.advance("sub", stale)

        with (
            pytest.raises(ValueError, match="non-negative int"),
            store.transaction() as tx,
        ):
            tx.advance("sub", -1)

        # Subscriptions are independent.
        with store.transaction() as tx:
            tx.advance("other", 1)
            assert tx.offset("other") == 1

        assert store.offset("sub") == 3


def test_nothing_ends_a_transaction_from_inside_it(tmp_path: Path) -> None:
    with opened(tmp_path) as store, store.transaction() as tx:
        with pytest.raises(RuntimeError, match="inside a store transaction"):  # noqa: SIM117
            with store.transaction():
                pass

        with pytest.raises(RuntimeError, match="inside a store transaction"):
            store.offset("sub")

        # `close` too: the lock is held for the transaction, so it would wait
        # for itself.
        with pytest.raises(RuntimeError, match="inside a store transaction"):
            store.close()

        tx.advance("sub", 1)


def test_a_finished_transaction_is_unusable(tmp_path: Path) -> None:
    with opened(tmp_path) as store:
        with store.transaction() as tx:
            tx.advance("sub", 1)

        with pytest.raises(RuntimeError, match="has ended"):
            tx.advance("sub", 2)


# -- declared tables ----------------------------------------------------------


def test_declared_types_hold_through_raw_sql(tmp_path: Path) -> None:
    """The log's I17 DDL: a wrong type is refused, not converted."""
    with opened(tmp_path) as store, store.transaction() as tx:
        for sql in (
            "INSERT INTO items (id, state) VALUES ('7', 'a')",
            "INSERT INTO items (id, state) VALUES (1, 12345)",
            "INSERT INTO items (id, state) VALUES (1, NULL)",
            'INSERT INTO checkpoint ("_streamcast_row", seen) VALUES (1, 1)',
        ):
            with pytest.raises(sqlite3.IntegrityError):
                tx.execute(sql)


def test_query_is_read_only(tmp_path: Path) -> None:
    with opened(tmp_path) as store:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            store.query("DELETE FROM items")


def _with(table: StoreTable, *fields: pa.Field) -> StoreTable:
    schema = table.schema
    for field in fields:
        schema = schema.append(field)

    return StoreTable(schema=schema, key=table.key, single_row=table.single_row)


def test_schema_changes_are_additive_only(tmp_path: Path) -> None:
    """A new table or a new nullable column is applied at open; anything that
    changes what an existing row means is refused, naming the difference."""
    with opened(tmp_path) as store:
        work(store, 2)

    wider = {
        "items": _with(ITEMS, pa.field("note", pa.string())),
        "checkpoint": CHECKPOINT,
        "extra": StoreTable(schema=pa.schema([pa.field("x", pa.int64())])),
    }
    with Store.open(tmp_path, "state", tables=wider) as store:
        assert store.query("SELECT id, note FROM items ORDER BY id") == [
            (1, None),
            (2, None),
        ]
        assert set(store.tables) == {"items", "checkpoint", "extra"}
        assert store.offset("sub") == 2

    refused = {
        "dropped table": {"items": wider["items"], "checkpoint": CHECKPOINT},
        "dropped column": TABLES | {"extra": wider["extra"]},
        "retyped": wider
        | {
            "extra": StoreTable(schema=pa.schema([pa.field("x", pa.string())])),
        },
        "tightened": wider
        | {
            "extra": StoreTable(
                schema=pa.schema([pa.field("x", pa.int64(), nullable=False)])
            ),
        },
        "rekeyed": wider
        | {"items": StoreTable(schema=wider["items"].schema, key=("id", "state"))},
        "non-nullable addition": wider
        | {"extra": _with(wider["extra"], pa.field("y", pa.int64(), nullable=False))},
    }
    for case, tables in refused.items():
        with pytest.raises(ValueError, match="additive only"):
            Store.open(tmp_path, "state", tables=tables)
            pytest.fail(case)


@pytest.mark.parametrize(
    ("tables", "error"),
    [
        ({"_streamcast_x": CHECKPOINT}, "reserved"),
        (
            {
                "t": StoreTable(
                    schema=pa.schema([pa.field("_Streamcast_row", pa.int64())])
                )
            },
            "reserved",
        ),
        (
            {"t": StoreTable(schema=pa.schema([pa.field("a", pa.decimal128(5, 2))]))},
            "column t.a: unsupported column type",
        ),
        (
            {
                "t": StoreTable(
                    schema=pa.schema(
                        [pa.field("a", pa.int64()), pa.field("A", pa.int64())]
                    )
                )
            },
            "twice",
        ),
        (
            {"t": StoreTable(schema=pa.schema([pa.field("a", pa.list_(pa.int64()))]))},
            "scalar",
        ),
        (
            {
                "t": StoreTable(
                    schema=pa.schema([pa.field("a", pa.int64())]), key=("a",)
                )
            },
            "non-nullable",
        ),
        (
            {"t": StoreTable(schema=CHECKPOINT.schema, key=("seen",), single_row=True)},
            "single-row",
        ),
    ],
)
def test_declarations_refused(
    tmp_path: Path, tables: dict[str, StoreTable], error: str
) -> None:
    with pytest.raises((ValueError, TypeError), match=error):
        Store.open(tmp_path, "state", tables=tables)


def test_a_release_floor_carries_the_api_the_store_imports() -> None:
    """`Store` imports `litelink.replication` and `litelink.column_ddl`, which
    litelink 0.12.5 is the first to ship. While `[tool.uv.sources]` names a
    litelink commit the floor may trail; a tree without it is what a release
    builds, and a floor below 0.12.5 there would let a resolver pick a litelink
    `import streamcast` fails on.

    Falsify by deleting `[tool.uv.sources]` with the floor left at 0.12.4.
    """
    pyproject = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text()
    )
    if "litelink" in pyproject.get("tool", {}).get("uv", {}).get("sources", {}):
        return

    (requirement,) = [
        r for r in pyproject["project"]["dependencies"] if r.startswith("litelink")
    ]
    floor = re.search(r">=\s*([0-9.]+)", requirement)
    assert floor is not None, requirement
    assert tuple(int(p) for p in floor.group(1).split(".")) >= (0, 12, 5), requirement
