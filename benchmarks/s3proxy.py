"""A counting HTTP proxy in front of an S3 endpoint: requests and bytes, per query.

    python benchmarks/s3proxy.py --upstream http://127.0.0.1:9002 --port 9102

or, in a benchmark's own process:

    with CountingProxy("http://127.0.0.1:9002") as proxy:
        options = S3Options(endpoint=proxy.endpoint, ...)
        with proxy.measure() as seen:
            ...                     # a query reading through `options`
        seen.requests, seen.bytes_out, seen.by_object()

A reader pointed at `proxy.endpoint` instead of the store's own sends its
requests here, and each one is forwarded unchanged and recorded: method, path,
`Range`, status, and the body bytes that came back. What it measures is what
an object store bills and what a network carries, which is the cost the
question "does the reader avoid fetching column X" is about. Wall time through
it is NOT the store's: every byte crosses one more Python hop.

**The `Host` header is forwarded as the client sent it.** A SigV4 signature
covers `Host`, and the client signed the proxy's address; the store checks the
signature against the header it receives, so rewriting it to the store's own
address would fail every request. Path-style addressing, which a custom
endpoint uses, keeps the bucket in the path, so nothing else needs it.

**One upstream connection per client connection**, kept alive for as long as
the client keeps its own. DuckDB's httpfs keeps connections alive and issues
ranged GETs in parallel; a connection per request would add a TCP handshake
per range and change the timing more than it needs to.
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import threading
import time
import urllib.parse
from collections import Counter
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

CHUNK = 1 << 20
# Hop-by-hop headers (RFC 9110 §7.6.1) are the connection's, not the message's.
HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
    }
)


@dataclass(frozen=True)
class Request:
    """One forwarded request, as the store answered it."""

    method: str
    path: str
    range: str | None
    status: int
    bytes_out: int
    """Response body bytes sent back to the client."""
    bytes_in: int
    """Request body bytes sent to the store."""
    seconds: float


@dataclass
class Seen:
    """What went through the proxy inside one `measure()`."""

    log: list[Request] = field(default_factory=list)

    @property
    def requests(self) -> int:
        return len(self.log)

    @property
    def bytes_out(self) -> int:
        return sum(r.bytes_out for r in self.log)

    def methods(self) -> dict[str, int]:
        return dict(Counter(r.method for r in self.log))

    def by_object(self) -> dict[str, tuple[int, int]]:
        """`path -> (requests, bytes)`, largest first."""
        totals: dict[str, list[int]] = {}
        for r in self.log:
            entry = totals.setdefault(urllib.parse.urlsplit(r.path).path, [0, 0])
            entry[0] += 1
            entry[1] += r.bytes_out

        ordered = sorted(totals.items(), key=lambda kv: -kv[1][1])
        return {path: (n, b) for path, (n, b) in ordered}

    def summary(self) -> dict[str, object]:
        return {
            "requests": self.requests,
            "bytes": self.bytes_out,
            "methods": self.methods(),
        }


class CountingProxy:
    """A threaded forwarding proxy, started on `__enter__` and stopped on exit."""

    def __init__(
        self, upstream: str, *, host: str = "127.0.0.1", port: int = 0
    ) -> None:
        parsed = urllib.parse.urlsplit(upstream)
        if parsed.scheme != "http" or parsed.hostname is None:
            msg = f"upstream must be an http:// endpoint, not {upstream!r}"
            raise ValueError(msg)

        self._upstream = (parsed.hostname, parsed.port or 80)
        self._address = (host, port)
        self._lock = threading.Lock()
        self._current: Seen | None = None
        self._total = Seen()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        assert self._server is not None, "start the proxy first"
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def total(self) -> Seen:
        """Everything since the proxy started."""
        return self._total

    @contextlib.contextmanager
    def measure(self) -> Iterator[Seen]:
        """Record what goes through until the block ends. Not reentrant."""
        seen = Seen()
        with self._lock:
            if self._current is not None:
                msg = "a measurement is already running"
                raise RuntimeError(msg)

            self._current = seen

        try:
            yield seen
        finally:
            with self._lock:
                self._current = None

    def _record(self, request: Request) -> None:
        with self._lock:
            self._total.log.append(request)
            if self._current is not None:
                self._current.log.append(request)

    def start(self) -> CountingProxy:
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            upstream: http.client.HTTPConnection | None = None

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                # Quiet: the measurement is the record.
                pass

            def _connection(self) -> http.client.HTTPConnection:
                if self.upstream is None:
                    self.upstream = http.client.HTTPConnection(
                        *proxy._upstream, timeout=300
                    )

                return self.upstream

            def _forward(self) -> None:
                started = time.perf_counter()
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                headers = {
                    k: v for k, v in self.headers.items() if k.lower() not in HOP
                }
                try:
                    response = self._send(body, headers)
                except (ConnectionError, http.client.HTTPException):
                    # A kept-alive upstream connection the store closed; retry
                    # once on a fresh one, as any HTTP client would.
                    self.upstream = None
                    response = self._send(body, headers)

                self.send_response(response.status, response.reason)
                declared = response.getheader("Content-Length")
                payload = None if declared is not None else response.read()
                for k, v in response.getheaders():
                    if k.lower() not in HOP and k.lower() != "content-length":
                        self.send_header(k, v)

                size = int(declared) if declared is not None else len(payload or b"")
                self.send_header("Content-Length", str(size))
                self.end_headers()
                sent = 0
                if self.command != "HEAD":
                    if payload is not None:
                        self.wfile.write(payload)
                        sent = len(payload)
                    else:
                        while chunk := response.read(CHUNK):
                            self.wfile.write(chunk)
                            sent += len(chunk)
                else:
                    response.read()

                proxy._record(
                    Request(
                        self.command,
                        self.path,
                        self.headers.get("Range"),
                        response.status,
                        sent,
                        len(body or b""),
                        time.perf_counter() - started,
                    )
                )

            def _send(
                self, body: bytes | None, headers: dict[str, str]
            ) -> http.client.HTTPResponse:
                connection = self._connection()
                connection.request(self.command, self.path, body=body, headers=headers)
                return connection.getresponse()

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = _forward

        self._server = ThreadingHTTPServer(self._address, Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def __enter__(self) -> CountingProxy:
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--upstream", default="http://127.0.0.1:9002")
    parser.add_argument("--port", type=int, default=9102)
    args = parser.parse_args()
    with CountingProxy(args.upstream, port=args.port) as proxy:
        print(f"proxying {args.upstream} on {proxy.endpoint}; Ctrl-C prints totals")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            print(json.dumps(proxy.total.summary(), indent=2))


if __name__ == "__main__":
    main()
