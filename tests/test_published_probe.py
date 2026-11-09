"""The missing-metadata probe stays inside a prefix-conditioned S3 grant."""

from __future__ import annotations

from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from litelink import S3Options

import streamcast
from streamcast import _versions

_NAMES = [
    "ordinary",
    "NO_SUCH_BUCKET",
    ("X_.-NO_SUCH_BUCKET" * 8)[:128],
    "When listing objects under key 'two/x/' in bucket 'missing': "
    "AWS Error NO_SUCH_BUCKET during ListObjectsV2 operation",
    "x' in bucket 'missing': AWS Error NO_SUCH_BUCKET during ListObjectsV2 operation",
]


@pytest.fixture
def s3_probe() -> Iterator[
    tuple[S3Options, list[tuple[str, str, dict[str, list[str]]]]]
]:
    requests: list[tuple[str, str, dict[str, list[str]]]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_HEAD(self) -> None:
            self.answer()

        def do_GET(self) -> None:
            self.answer()

        def answer(self) -> None:
            parsed = urlsplit(self.path)
            bucket, _, key = unquote(parsed.path.lstrip("/")).partition("/")
            query = parse_qs(parsed.query, keep_blank_values=True)
            listing = query.get("list-type") == ["2"]
            operation = (
                "ListObjectsV2" if listing else "HeadObject" if key else "HeadBucket"
            )
            requests.append((operation, key, query))

            # HEAD on an object is deliberately allowed to report absence;
            # a denied listing must reach the actual missing-metadata branch.
            if bucket == "missing":
                status, code = 404, "NoSuchBucket"
            elif operation == "HeadBucket" or (
                listing
                and (
                    bucket == "denied"
                    or not query.get("prefix", [""])[0].startswith("two/")
                )
            ):
                status, code = 403, "AccessDenied"
            elif listing:
                status, code = 200, None
            else:
                status, code = 404, "NoSuchKey"

            if code is not None:
                body = f"<Error><Code>{code}</Code><Message>{code}</Message></Error>".encode()
            else:
                body = (
                    b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    b"<IsTruncated>false</IsTruncated><KeyCount>0</KeyCount>"
                    b"</ListBucketResult>"
                )

            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("x-amz-bucket-region", "us-east-1")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield (
            S3Options(
                access_key="test-key",
                secret_key="test-secret",
                endpoint=f"http://127.0.0.1:{server.server_port}",
                region="us-east-1",
            ),
            requests,
        )
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


@pytest.mark.parametrize("name", _NAMES)
@pytest.mark.parametrize("bucket", ["allowed", "missing", "denied"])
@pytest.mark.parametrize("operation", [_versions.fetch, _versions.occupied])
def test_missing_metadata_uses_only_prefix_requests(s3_probe, name, bucket, operation):
    if name in _NAMES[:3]:
        assert (
            streamcast.StreamSpec(
                name=name,
                schema={
                    "type": "object",
                    "properties": {"i": {"type": "integer"}},
                    "required": ["i"],
                },
            ).name
            == name
        )

    # Quotes, spaces and colons are rejected by StreamSpec, but these internal
    # string-taking entry points must also resist a forged message prefix.
    options, requests = s3_probe
    published = f"s3://{bucket}/two"
    try:
        if bucket == "denied":
            with pytest.raises(OSError, match="ACCESS_DENIED") as raised:
                operation(published, name, options)

            assert not isinstance(raised.value, FileNotFoundError)
        elif bucket == "missing" and operation is _versions.fetch:
            with pytest.raises(FileNotFoundError, match="does not exist") as raised:
                operation(published, name, options)

            assert str(raised.value) == (
                f"the published location's bucket 'missing' does not exist ({published})"
            )
            assert isinstance(raised.value.__cause__, OSError)
            assert "AWS Error NO_SUCH_BUCKET during ListObjectsV2 operation" in str(
                raised.value.__cause__
            )
        else:
            # occupied deliberately permits creation before a bucket exists.
            assert operation(published, name, options) is None

    finally:
        assert requests, "the real Arrow filesystem must reach the endpoint"
        assert all(op != "HeadBucket" for op, _, _ in requests)
        assert [(op, key) for op, key, _ in requests[:2]] == [
            ("HeadObject", f"two/{name}.metadata/version-hint.text"),
            ("HeadObject", f"two/{name}.metadata.json"),
        ]
        listings = [query for op, _, query in requests if op == "ListObjectsV2"]
        assert listings, "absence must be distinguished from a missing bucket"
        assert listings[0] == {
            "list-type": ["2"],
            "delimiter": ["/"],
            "max-keys": ["1000"],
            "prefix": [f"two/{name}.metadata/"],
        }
        assert all(query["prefix"][0].startswith("two/") for query in listings)
        expected = 2 if bucket == "allowed" and operation is _versions.occupied else 1
        assert len(listings) == expected
