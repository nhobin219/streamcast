# streamcast development commands
# Install just: uv tool install rust-just

set dotenv-load := true

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

# START HERE. A live public feed through a broker, in one process: Bitstamp
# publishes BTC/USD trades over an unauthenticated websocket, so there is
# nothing to configure and no credentials to set.
#
#   just demo              terminal 1: the broker, logging every trade
#   just demo-consumer     terminal 2 (and 3, and 4): a resuming subscriber
#
# Stop a consumer, leave it stopped for a while, start it again, and watch it
# replay what it missed before it goes live.

# Run the broker against a live public websocket feed.
demo *args:
    uv run python examples/broker.py {{args}}

# Subscribe to the demo broker, resuming from where it stopped.
demo-consumer *args:
    uv run python examples/consumer.py {{args}}

# A live-only broker — no log, no litelink, no replay. The other end of the
# range, and the shape to reach for when the stream is a cache nobody resumes.
demo-live *args:
    uv run python examples/broker.py --no-log {{args}}

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
