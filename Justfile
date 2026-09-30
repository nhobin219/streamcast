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
#
# It requires Node 22+ as well: `tests/test_node.py` runs a browser-style
# JavaScript client against a real server, and skips without Node. Put `node`
# on PATH, or point STREAMCAST_NODE at a binary.
check-all: lint format-check typecheck
    STREAMCAST_REQUIRE_S3=1 STREAMCAST_REQUIRE_NODE=1 uv run pytest

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

# The same stream mounted in a FastAPI service, on the port it already has.
demo-fastapi *args:
    uv run uvicorn examples.fastapi_app:app --port 8000 {{args}}

# A live-only server — no log, no litelink, no replay. The other end of the
# range, and the shape to reach for when the stream is a cache nobody resumes.
demo-live *args:
    uv run python examples/server.py --no-log {{args}}

# Two simulated services log and trace through the OTel SDK; the records and
# spans are published to two streams; `examples/otel/export.py` follows both
# and re-exports them as OTLP to
# otel-gui (https://github.com/metafab/otel-gui), a local dashboard. The first
# run downloads otel-gui's release for this platform, checks its SHA-256, and
# caches it. Everything stays on this machine; Ctrl-C stops all three.
#
# OpenTelemetry logs and traces through streams, live in otel-gui's dashboard.
demo-otel host="127.0.0.1" port="4318":
    #!/usr/bin/env bash
    set -euo pipefail
    version=2.1.0
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64)             asset=otel-gui-linux-x64 ;;
        Linux-aarch64|Linux-arm64) asset=otel-gui-linux-arm64 ;;
        Darwin-x86_64)            asset=otel-gui-macos-x64 ;;
        Darwin-arm64)             asset=otel-gui-macos-arm64 ;;
        *) echo "no otel-gui build for $(uname -s)-$(uname -m); see https://github.com/metafab/otel-gui" >&2; exit 1 ;;
    esac
    cache="${XDG_CACHE_HOME:-$HOME/.cache}/streamcast/otel-gui-$version"
    gui="$cache/$asset/otel-gui"
    if [ ! -x "$gui" ]; then
        echo "downloading otel-gui $version ($asset)"
        mkdir -p "$cache"
        url="https://github.com/metafab/otel-gui/releases/download/v$version/$asset.tar.gz"
        curl -fsSL -o "$cache/$asset.tar.gz" "$url"
        curl -fsSL -o "$cache/$asset.tar.gz.sha256" "$url.sha256"
        if command -v sha256sum >/dev/null; then
            (cd "$cache" && sha256sum -c "$asset.tar.gz.sha256")
        else
            (cd "$cache" && shasum -a 256 -c "$asset.tar.gz.sha256")
        fi
        tar -xzf "$cache/$asset.tar.gz" -C "$cache"
    fi
    log="$(mktemp -t streamcast-otel-XXXXXX.log)"
    # HOST and SHUTDOWN_TIMEOUT are not in otel-gui's README, but its server
    # honours both (SvelteKit's node adapter). 127.0.0.1 keeps the dashboard
    # off the network. On a signal it waits SHUTDOWN_TIMEOUT seconds (30 by
    # default) for open connections to close, and an open dashboard's live
    # stream never does: 1 lets Ctrl-C return at once.
    HOST="{{host}}" PORT="{{port}}" SHUTDOWN_TIMEOUT=1 "$gui" >"$log" 2>&1 &
    gui_pid=$!
    uv run python -m examples.otel.demo --serve >>"$log" 2>&1 &
    broker=$!
    uv run python -m examples.otel.export --receiver "http://127.0.0.1:{{port}}" >>"$log" 2>&1 &
    exporter=$!
    # SIGTERM, not SIGINT: bash starts background jobs with SIGINT ignored, so
    # Ctrl-C reaches only this script. Both Python pieces unwind on SIGTERM as
    # on Ctrl-C, and the broker stops its maintainer rather than orphaning it.
    trap 'kill "$exporter" "$broker" "$gui_pid" 2>/dev/null || true; wait' EXIT
    echo "dashboard: http://{{host}}:{{port}}   (logs: $log)   Ctrl-C to stop"
    wait "$broker"

# The OTel example once, start to finish, printing what each part saw.
demo-otel-once:
    uv run python -m examples.otel.demo

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
