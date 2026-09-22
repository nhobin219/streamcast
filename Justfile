# streamcast development commands
# Install just: uv tool install rust-just

set dotenv-load := true

# The local S3-compatible endpoint the replication tier is tested against.
# Matches tests/conftest.py; change both together. Ported from litelink, whose
# archive tier needs the same thing — same image, same port, same shape — so a
# developer moving between the two repos configures nothing.
# Port 9002, not litelink's 9000. That is the one deliberate difference from
# the recipe this is copied from: a developer with both repos checked out runs
# `just rustfs` in each, and the same port means the second one fails to bind
# with a docker error that says nothing about why.
RUSTFS_ENDPOINT := "http://127.0.0.1:9002"
RUSTFS_KEY := "streamcast"
RUSTFS_SECRET := "streamcast-secret"
RUSTFS_BUCKET := "streamcast-demo"

# Default recipe: list available commands
default:
    @just --list

# Create the dev environment. Idempotent — safe to re-run after a pull.
bootstrap:
    uv sync
    uv run pre-commit install --hook-type pre-commit --hook-type commit-msg

# Repo-wide, matching the pre-commit hook's `pass_filenames: false`: a recipe
# scoped to src/ lets tests/ and examples/ drift out of compliance while CI
# stays green. (Blank line above the doc comment on purpose — `just --list`
# shows only the last contiguous comment line.)

# Lint
lint:
    uv run ruff check .

# Run ruff formatter + blank line fixer
format path=".":
    uv run ruff format {{path}}
    python scripts/check_blank_lines.py --fix {{path}}

# Check formatting without modifying files
format-check path=".":
    uv run ruff format --check {{path}}
    python scripts/check_blank_lines.py {{path}}

# Static type check
typecheck:
    uv run ty check

# Run the test suite. No network, no container, no credentials — every test
# binds port 0 on loopback and every log is a temp directory.
test *args:
    uv run pytest {{args}}

# Everything except the slow tier, for a fast inner loop. The slow ones are the
# backpressure tests: forcing a real overflow means filling a real socket
# buffer, which is wall-clock bound and cannot be hurried.
test-fast *args:
    uv run pytest -m "not slow" {{args}}

# All pre-push gates. Mirrors CI, so a green `just check` should mean a green PR.
check: lint format-check typecheck test

# Build the wheel + sdist into dist/
build:
    uv build

# WAL replication needs somewhere to ship to. `wal_replication` is opt-in on a
# log and the tests that exercise it SKIP without an endpoint — which is how
# a whole tier goes unchecked, so `just check-all` sets STREAMCAST_REQUIRE_S3
# and turns that skip into a failure.
#
#   just rustfs        bring it up (idempotent)
#   just check-all     every gate, with replication actually run
#   just rustfs-stop   tear it down, discarding its data

# Bring up a local S3-compatible object store for the replication tier.
rustfs:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps -q -f name=^streamcast-rustfs$)" ]; then
        echo "rustfs already running on {{RUSTFS_ENDPOINT}}"
        exit 0
    fi
    docker rm -f streamcast-rustfs >/dev/null 2>&1 || true
    # Pinned, not `:latest`. A floating tag that renamed its credential env
    # vars would still answer on the port — the readiness check below only
    # asks for any HTTP response — so the bucket call would fail, the fixture
    # would skip, and the tier would vanish green.
    docker run -d --name streamcast-rustfs -p 9002:9000 \
        -e RUSTFS_ACCESS_KEY={{RUSTFS_KEY}} \
        -e RUSTFS_SECRET_KEY={{RUSTFS_SECRET}} \
        rustfs/rustfs:1.0.0-rc.4 >/dev/null
    for _ in $(seq 1 40); do
        # Any HTTP answer means it is listening. NOT `curl -f`: an
        # unauthenticated S3 root answers 403, which is a healthy server
        # refusing an anonymous request, and -f reads that as a failure.
        if curl -s -o /dev/null "{{RUSTFS_ENDPOINT}}" 2>/dev/null; then
            just _rustfs-bucket
            echo "rustfs up on {{RUSTFS_ENDPOINT}}"
            echo
            echo "  just check-all    # every gate, replication included"
            exit 0
        fi
        sleep 0.25
    done
    echo "rustfs did not answer on {{RUSTFS_ENDPOINT}}" >&2
    docker logs streamcast-rustfs 2>&1 | tail -20 >&2
    exit 1

# Create the test bucket. Idempotent, and through the same s3fs the tests use.
_rustfs-bucket:
    #!/usr/bin/env bash
    set -euo pipefail
    AWS_ENDPOINT_URL={{RUSTFS_ENDPOINT}} \
    AWS_ACCESS_KEY_ID={{RUSTFS_KEY}} \
    AWS_SECRET_ACCESS_KEY={{RUSTFS_SECRET}} \
    AWS_REGION=us-east-1 \
    uv run python -c "
    import os, s3fs
    fs = s3fs.S3FileSystem(
        key=os.environ['AWS_ACCESS_KEY_ID'],
        secret=os.environ['AWS_SECRET_ACCESS_KEY'],
        client_kwargs={'endpoint_url': os.environ['AWS_ENDPOINT_URL'],
                       'region_name': os.environ['AWS_REGION']},
    )
    if not fs.exists('{{RUSTFS_BUCKET}}'):
        fs.mkdir('{{RUSTFS_BUCKET}}')
    "

# Stop rustfs and discard its data. The container is disposable on purpose.
rustfs-stop:
    @docker rm -f streamcast-rustfs >/dev/null 2>&1 && echo "rustfs stopped" || echo "not running"

# Every gate, with the replication tier REQUIRED rather than skipped. Needs
# `just rustfs` first. This is what CI runs.
check-all: lint format-check typecheck
    STREAMCAST_REQUIRE_S3=1 uv run pytest

# START HERE. A live public feed through a server, in one process: Bitstamp
# publishes BTC/USD trades over an unauthenticated websocket, so there is
# nothing to configure and no credentials to set.
#
#   just demo              terminal 1: the server, logging every trade
#   just demo-consumer     terminal 2 (and 3, and 4): a resuming subscriber
#
# Stop a consumer, leave it stopped for a while, start it again, and watch it
# replay what it missed before it goes live.

# Run the server against a live public websocket feed.
demo *args:
    uv run python examples/server.py {{args}}

# Subscribe to the demo server, resuming from where it stopped.
demo-consumer *args:
    uv run python examples/consumer.py {{args}}

# A live-only server — no log, no litelink, no replay. The other end of the
# range, and the shape to reach for when the stream is a cache nobody resumes.
demo-live *args:
    uv run python examples/server.py --no-log {{args}}

# Delete what the demo captured.
demo-clean root="streamcast-data":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -e "{{root}}" ]; then
        echo "nothing at {{root}}"
    else
        echo "removing {{root}} ($(du -sh "{{root}}" | cut -f1))"
        rm -rf "{{root}}"
    fi

# What the fan-out costs per subscriber, and where it stops being free.
bench *args:
    uv run python benchmarks/fanout.py {{args}}

# What a replay costs, and which layer it is spent in. The numbers SPEC §4
# sizes max_replay against come from here.
bench-replay *args:
    uv run python benchmarks/replay.py {{args}}

# What permessage-deflate costs per subscriber against what it saves. This is
# the measurement `compression`'s default rests on — rerun it before arguing
# with the default.
bench-deflate *args:
    uv run python benchmarks/deflate.py {{args}}
