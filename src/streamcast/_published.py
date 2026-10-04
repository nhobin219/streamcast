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

**The DuckDB connection is litelink's** (`litelink.duckdb_connection`), with
the `iceberg` and `httpfs` extensions litelink provisions rather than installs
at first read, the S3 secret it creates, and the read caches the caller asks
for (`ReadCache`).

**The hint is read around DuckDB, never through it.** `version-hint.text` is
the one object a reader touches that changes: everything it names — a
`metadata.json`, its manifests, the data files — is written once under a
name of its own. So every cached layer is safe except for the hint, and
`cache_httpfs`'s on-disk cache serves a hint that has since moved —
measured: a rewritten hint read back as the old one in the same process
(its file-handle cache, an hour) and in a new process sharing the cache
directory (its data cache), and excluding the path left the first. A stale
hint pins a reader to an old snapshot for ever, so it is read with the
filesystem `read` uses, which caches nothing.

**One database per process, a connection per reader.** Loading `iceberg` into
a fresh DuckDB database costs 400-580 ms (measured, `just bench-snapshot`), and
was the whole cost of opening a snapshot; a connection to a database that has
it loaded costs 0.2 ms. Each reader still gets its own connection, so its temp
view and registered tail are its own.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final
from urllib.parse import unquote, urlsplit

import pyarrow as pa
from litelink import S3Options, duckdb_connection

from streamcast import _remote
from streamcast._log import COLUMN

if TYPE_CHECKING:
    from collections.abc import Sequence
    from os import PathLike

    import duckdb


def path(uri: str) -> str:
    """What DuckDB reads `uri` as: a local path for `file://`, the URI otherwise."""
    if uri.startswith("file://"):
        return unquote(urlsplit(uri).path)

    return uri


def read(uri: str, s3_options: S3Options | None) -> bytes:
    """The bytes at `uri`, local or `s3://`, uncached."""
    if uri.startswith("file://"):
        with open(path(uri), "rb") as file:  # noqa: PTH123
            return file.read()

    filesystem, key = _remote._filesystem(uri, s3_options)  # noqa: SLF001
    with filesystem.open_input_stream(key) as source:
        return source.read()


@dataclass(frozen=True, slots=True)
class ReadCache:
    """How a reader caches what it reads: `litelink.duckdb_connection`'s four
    settings, with its defaults. Hashable, because it is part of the key a
    database is shared under."""

    memory_cache: bool = True
    """DuckDB's external file cache, for the database's life."""
    disk_cache: bool = False
    """`cache_httpfs` on disk, surviving restarts. `s3://` tables only."""
    cache_key: str | None = None
    """The disk cache's directory: relative to litelink's cache root, absolute
    as given, or None for its `default` directory. The caller's choice — only
    it knows what deserves a cache of its own, a stream id for one."""
    disk_cache_volume_limit: float = 0.8
    """How full the disk cache's VOLUME may get, everything on it counted."""

    @classmethod
    def of(
        cls,
        *,
        memory_cache: bool,
        disk_cache: bool,
        cache_key: str | PathLike[str] | None,
        disk_cache_volume_limit: float,
    ) -> ReadCache:
        """From the keywords the public calls take, which are litelink's."""
        return cls(
            memory_cache,
            disk_cache,
            None if cache_key is None else os.fspath(cache_key),
            disk_cache_volume_limit,
        )

    def keywords(self) -> dict[str, object]:
        """Back to those keywords, for a call that takes them."""
        return {
            "memory_cache": self.memory_cache,
            "disk_cache": self.disk_cache,
            "cache_key": self.cache_key,
            "disk_cache_volume_limit": self.disk_cache_volume_limit,
        }


DEFAULT_CACHE: Final = ReadCache()
"""litelink's defaults: memory on, disk off."""


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# The databases readers connect to, by what they can read with and how they
# cache. Never evicted: a process reads with a handful of each at most.
_DATABASES: dict[tuple[S3Options | None, ReadCache], duckdb.DuckDBPyConnection] = {}
_LOCK = threading.Lock()


def connection(
    s3_options: S3Options | None, *, remote: bool, cache: ReadCache = DEFAULT_CACHE
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that can read published tables. The caller closes it.

    `httpfs` and credentials only when `remote`, for `s3://` tables: a local
    one needs neither, and a machine reading only local tables never loads them.

    **One database per credential set**, keyed by the resolved options — the
    keys, endpoint and region the secret is made from. DuckDB's secrets belong
    to the database, not the connection, so two readers with different keys or
    endpoints sharing one would read with whichever wrote the secret last.
    `None` is the local database, which has no secret at all.

    A secret from the ambient credential chain refreshes itself (litelink sets
    `REFRESH auto`), so a database that outlives an STS session keeps reading.
    Keys rotated in the environment resolve to different options, and so to
    a database of their own.

    **And one per `cache`**, because a cache belongs to the database too: two
    readers asking for different caching, or different directories, cannot
    share one. A local database caches only in memory — the disk cache is
    for `s3://` reads — so its disk settings do not split it.
    """
    credentials = (s3_options or S3Options()).resolved() if remote else None
    if credentials is None:
        cache = ReadCache(memory_cache=cache.memory_cache)

    key = (credentials, cache)
    with _LOCK:
        database = _DATABASES.get(key)
        if database is None:
            database = (
                duckdb_connection(
                    s3_options=credentials,
                    memory_cache=cache.memory_cache,
                    disk_cache=cache.disk_cache,
                    cache_key=cache.cache_key,
                    disk_cache_volume_limit=cache.disk_cache_volume_limit,
                )
                if credentials is not None
                else duckdb_connection(memory_cache=cache.memory_cache)
            )
            _DATABASES[key] = database

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
        s3_options: S3Options | None,
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
            connection(s3_options, remote=uri.startswith("s3://"))
            if shared is None
            else shared
        )
        try:
            table = path(uri)
            # Not through `connected`: see the module docstring.
            hint = read(f"{uri}/metadata/version-hint.text", s3_options).decode()
            metadata = f"{table}/metadata/{hint.strip()}.metadata.json"
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
