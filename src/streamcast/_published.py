"""Reading a log's published table from any machine, pinned to one snapshot.

Every litelink log publishes an ordinary Iceberg table — to an `s3://` prefix,
or by default a directory inside the log — at `<published>/<log name>`. A
machine that is not the writer reads it with DuckDB's `iceberg_scan` and needs
nothing from litelink's handles: no catalog, no buffer, no local root. That is
litelink's own guidance since 0.6, which removed its remote read path.

**Pinned at open.** The table's `version-hint.text` names its current
`metadata.json`, and moves with every publish. Read once, at `Table.open`, and
every query on the `Table` names that one metadata file — so the extent it
reports and the rows it returns come from the same commit, and a publish
landing in between cannot make them disagree. A reader that wants newer rows
opens the table again.

**The DuckDB connection is litelink's**, with the `iceberg` and `httpfs`
extensions litelink provisions rather than installs at first read.

**One database per process, a connection per reader.** Loading `iceberg` into
a fresh DuckDB database costs 400-580 ms (measured, `just bench-snapshot`), and
was the whole cost of opening a snapshot; a connection to a database that has
it loaded costs 0.2 ms. Each reader still gets its own connection, so its temp
view and registered tail are its own. Those are
private in litelink 0.6 (`litelink._read`); litelink#108 asks for a public
spelling, and `tests/test_published.py` fails loudly if they move first.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

import pyarrow as pa
from litelink import S3Options
from litelink._read import duckdb_connection, load_extension, secret_sql

from streamcast._log import COLUMN

if TYPE_CHECKING:
    from collections.abc import Sequence

    import duckdb


def path(uri: str) -> str:
    """What DuckDB reads `uri` as: a local path for `file://`, the URI otherwise."""
    if uri.startswith("file://"):
        return unquote(urlsplit(uri).path)

    return uri


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# The databases readers connect to, by what they can read with. Never evicted:
# a process reads with a handful of credential sets at most.
_DATABASES: dict[tuple[bool, str | None], duckdb.DuckDBPyConnection] = {}
_LOCK = threading.Lock()


def connection(s3: S3Options | None, *, remote: bool) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that can read published tables. The caller closes it.

    `httpfs` and credentials only when `remote`, for `s3://` tables: a local
    one needs neither, and a machine reading only local tables never loads them.

    **One database per credential set**, keyed by the secret itself. DuckDB's
    secrets belong to the database, not the connection, so two readers with
    different keys or endpoints sharing one would read with whichever wrote
    the secret last.

    **The ambient chain is resolved again for every connection.** A
    `credential_chain` secret fetches its credentials when it is created, and a
    database that outlives an STS session would go on presenting an expired
    token. Re-creating it is what a database per reader used to do implicitly.
    """
    secret = secret_sql((s3 or S3Options()).resolved()) if remote else None
    with _LOCK:
        database = _DATABASES.get((remote, secret))
        if database is None:
            database = duckdb_connection()
            if secret is not None:
                load_extension(database, "httpfs", remote=True)
                database.execute(secret)

            _DATABASES[(remote, secret)] = database

        elif secret is not None and "credential_chain" in secret:
            database.execute(secret)

        connected = database.cursor()

    return connected


class Table:
    """One published table, pinned to the snapshot it had when opened.

    Duck-typed as the part of a litelink handle that `_log.rows` reads —
    `schema`, `scan(columns=, start_offset=, end_offset=)` and `close()` — so a
    row read from here is built by the same function, in the same order, as a
    row a server replays (I6).
    """

    __slots__ = (
        "_connection",
        "_owned",
        "extent",
        "metadata",
        "name",
        "record_count",
        "schema",
    )

    def __init__(
        self,
        name: str,
        connection: duckdb.DuckDBPyConnection,
        metadata: str,
        schema: pa.Schema,
        extent: tuple[int, int] | None,
        *,
        record_count: int = 0,
        owned: bool = True,
    ) -> None:
        self.name = name
        self._connection = connection
        self._owned = owned
        self.metadata = metadata
        """The pinned `metadata.json`: what every query on this table reads."""
        self.schema = schema
        self.extent = extent
        """`[start, end)` of the offsets this snapshot holds, or None if it holds none."""
        self.record_count = record_count
        """How many rows it holds. Below `extent`'s width across a restore fence."""

    @classmethod
    def open(
        cls,
        published: str,
        name: str,
        s3: S3Options | None,
        *,
        shared: duckdb.DuckDBPyConnection | None = None,
    ) -> Table:
        """The table for log `name` under the `published` prefix, as it is now.

        Blocking — a hint read, a schema read and an extent query, each a GET
        or more against an `s3://` table — so a caller on an event loop runs it
        in a thread.

        `shared` is a connection to read through rather than a new one, which
        is how a snapshot reads all of a stream's logs on one connection; it
        is the caller's to close.
        """
        uri = f"{published.rstrip('/')}/{name}"
        connected = (
            connection(s3, remote=uri.startswith("s3://")) if shared is None else shared
        )
        try:
            table = path(uri)
            hint = connected.execute(
                f"SELECT content FROM read_text({_quoted(f'{table}/metadata/version-hint.text')})"
            ).fetchone()
            if hint is None:  # pragma: no cover — read_text yields a row or raises
                msg = f"{uri} has no version-hint.text"
                raise FileNotFoundError(msg)

            metadata = f"{table}/metadata/{hint[0].strip()}.metadata.json"
            scan = f"iceberg_scan({_quoted(metadata)})"
            schema = connected.execute(f"SELECT * FROM {scan} LIMIT 0").arrow().schema
            low, high, count = connected.execute(
                f'SELECT min("{COLUMN}"), max("{COLUMN}"), count(*) FROM {scan}'
            ).fetchone() or (None, None, 0)
        except BaseException:
            if shared is None:
                connected.close()

            raise

        extent = None if low is None or high is None else (int(low), int(high) + 1)
        return cls(
            name,
            connected,
            metadata,
            schema,
            extent,
            record_count=int(count),
            owned=shared is None,
        )

    def scan(
        self,
        *,
        columns: Sequence[str] | None = None,
        start_offset: int | None = None,
        end_offset: int | None = None,
        where: str | None = None,
        published: bool = True,  # noqa: ARG002 — `_log.rows` passes it; this IS the published table
    ) -> pa.RecordBatchReader:
        """The pinned snapshot's rows in `[start_offset, end_offset)`, oldest first."""
        projection = "*" if columns is None else ", ".join(f'"{c}"' for c in columns)
        terms = []
        if start_offset is not None:
            terms.append(f'"{COLUMN}" >= {int(start_offset)}')

        if end_offset is not None:
            terms.append(f'"{COLUMN}" < {int(end_offset)}')

        if where is not None:
            terms.append(f"({where})")

        query = (
            f"SELECT {projection} FROM {self.relation()}"
            + (f" WHERE {' AND '.join(terms)}" if terms else "")
            + f' ORDER BY "{COLUMN}"'
        )
        return self._connection.execute(query).to_arrow_reader()

    def relation(self) -> str:
        """The pinned snapshot as a DuckDB table function, for composing queries."""
        return f"iceberg_scan({_quoted(self.metadata)})"

    def close(self) -> None:
        if self._owned:
            self._connection.close()


__all__ = ["Table", "connection", "path"]
