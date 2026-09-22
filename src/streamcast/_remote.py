"""Shipping a consumer's cursor to object storage, so another box can resume.

A cursor on local disk recovers a consumer that restarted. It does not recover
one whose machine is gone — which is the case `wal_replication` exists for on
the server side, and this is the same idea one layer out: a periodic copy of
one integer, somewhere that survives the host.

    async with streamcast.connect(
        uri,
        cursor=".trades.offset",
        cursor_uri="s3://streamcast/consumer1/",
    ) as stream:
        ...

**A daemon thread, not the event loop.** The upload is a blocking S3 PUT and
it is best-effort, so it has no business on the loop or in the shared
`to_thread` pool — that pool is `min(32, cpu + 4)` and is what every replay
scan uses. A daemon thread also does not hold the interpreter open: a
consumer that exits with an upload in flight simply exits.

**pyarrow, not boto3 or s3fs.** litelink already brings pyarrow and reaches S3
through `PyArrowFileIO` itself, so this needs no dependency streamcast does not
already have. Credentials resolve the way litelink's do — from the ordinary AWS
chain, overridable with `S3Options` — so a profile, instance metadata or SSO
all work untouched.

**What this is not.** It is a periodic copy, so it lags by up to
`upload_every`; recovering on another box re-delivers whatever happened in that
window. Two consumers sharing one key is last-writer-wins and is not supported.
Neither is worth solving here: re-delivery is the safe direction (`_cursor`),
and a consumer that needs exactly-once wants its cursor in the same transaction
as its work, which is its own database and not this.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Final
from urllib.parse import urlsplit

import pyarrow.fs as pafs
from litelink import S3Options

if TYPE_CHECKING:
    from streamcast._cursor import Cursor

log: Final = logging.getLogger("streamcast.cursor")

UPLOAD_EVERY: Final = 30.0
"""Seconds between uploads.

The window a cross-box recovery re-delivers, and it is a bound on RE-DELIVERY
rather than on loss. Thirty seconds rather than the local cursor's one: this
one costs a network round trip, and the local file is what an ordinary restart
uses.
"""


def _filesystem(uri: str, s3: S3Options | None) -> tuple[pafs.S3FileSystem, str]:
    """A pyarrow S3 filesystem for `uri`, and the path inside it.

    This used to import `pyarrow.fs` inside the function, on the grounds that
    a consumer with no `cursor_uri` should not pay for pyarrow's S3 stack.
    **Measured, that saved 0.0 ms**: litelink imports `pyarrow.fs` itself, so
    it is already in `sys.modules` before `import streamcast` returns. The
    deferral bought nothing and hid the dependency from anything reading the
    imports.
    """
    split = urlsplit(uri)
    resolved = (s3 or S3Options()).resolved()
    options: dict[str, object] = {}
    if resolved.access_key and resolved.secret_key:
        options["access_key"] = resolved.access_key
        options["secret_key"] = resolved.secret_key

    if resolved.region:
        options["region"] = resolved.region

    if resolved.endpoint:
        # `scheme` as well as the override, or pyarrow assumes https and a
        # local endpoint on plain http fails with a TLS error that names
        # nothing about the scheme.
        endpoint = urlsplit(resolved.endpoint)
        options["endpoint_override"] = resolved.endpoint
        options["scheme"] = endpoint.scheme or "https"

    return pafs.S3FileSystem(**options), f"{split.netloc}{split.path}"


def _key(path: str, cursor: Cursor) -> str:
    """Where this cursor lives in the bucket.

    A `cursor_uri` ending in `/` is a PREFIX and the local file's name is
    appended, which is what makes one prefix per consumer read naturally
    (`s3://bucket/consumer1/`). Anything else is the full key.
    """
    return f"{path}{cursor.path.name}" if path.endswith("/") else path


class RemoteCursor:
    """Uploads a cursor on an interval, and reads it back when asked."""

    __slots__ = ("_cursor", "_every", "_key", "_s3", "_stop", "_thread", "_uri")

    def __init__(
        self,
        cursor: Cursor,
        uri: str,
        *,
        s3: S3Options | None = None,
        upload_every: float = UPLOAD_EVERY,
    ) -> None:
        # Checked HERE, not in `load`. Everything that reaches the network is
        # best-effort and swallowed — a bucket that is briefly unreachable
        # must not become the consumer's problem — and a mistyped scheme would
        # have been swallowed with it, leaving a consumer that believed it was
        # shipping a cursor and never had. A configuration error fails at
        # `connect`; only I/O is best-effort.
        if urlsplit(uri).scheme != "s3":
            msg = f"cursor_uri must be an s3:// URI, not {uri!r}"
            raise ValueError(msg)

        self._cursor = cursor
        self._uri = uri
        self._s3 = s3
        self._every = upload_every
        self._key = _key(urlsplit(uri).netloc + urlsplit(uri).path, cursor)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def load(self) -> int | None:
        """The offset in the bucket, or None.

        **Best effort, and a failure is not fatal.** A consumer that cannot
        reach the bucket should start from its local cursor or from live
        rather than refuse to run — this is a disaster-recovery convenience,
        not a dependency of the stream.
        """
        try:
            filesystem, _path = _filesystem(self._uri, self._s3)
            with filesystem.open_input_stream(self._key) as stream:
                found = int(stream.read().decode().strip())

        except Exception as exc:  # noqa: BLE001
            log.debug("no remote cursor at %s: %s", self._key, exc)

            return None

        log.debug("remote cursor at %s is %d", self._key, found)

        return found

    def _upload(self) -> None:
        offset = self._cursor.load()
        if offset is None:
            return

        try:
            filesystem, _path = _filesystem(self._uri, self._s3)
            with filesystem.open_output_stream(self._key) as stream:
                stream.write(str(offset).encode())

        except Exception as exc:  # noqa: BLE001
            # Logged, never raised. The consumer is mid-stream and the local
            # cursor is intact; a bucket that is briefly unreachable must not
            # become the consumer's problem.
            log.debug("could not upload cursor to %s: %s", self._key, exc)
            return

        log.debug("uploaded cursor %d to %s", offset, self._key)

    def _run(self) -> None:
        while not self._stop.wait(self._every):
            self._upload()

        # One last push on the way out, so an orderly shutdown leaves the
        # bucket holding what the consumer actually finished with rather than
        # what it held `upload_every` ago.
        self._upload()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="streamcast-cursor", daemon=True
        )
        self._thread.start()
        log.debug(
            "uploading %s to %s every %gs", self._cursor.path, self._key, self._every
        )

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None


__all__ = ["UPLOAD_EVERY", "RemoteCursor"]
