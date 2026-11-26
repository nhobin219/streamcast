# Container stack

The local stack runs one registry-managed broker, five independent maintenance
services and a pinned RustFS object store. The broker uses `ExternalMaintain` and
spawns no maintenance processes. Each role follows `/run/streamcast` as group
`default`; the shared data volume carries SQLite databases and their WAL files.

Build and start with Docker Engine, the Compose v2 plugin, Buildx and Just:

```sh
just stack-up 1
just stack-ps
just stack-smoke
just stack-logs broker
just stack-down
```

`stack-up` waits for health and prints `ws://127.0.0.1:8765`. Set `BROKER_PORT`
to change the loopback port. The project name is derived from the worktree path;
`COMPOSE_PROJECT_NAME` overrides it. Separate projects also need distinct broker
ports when they run together.

`just image [tag]` builds and loads the host architecture without publishing.
`STREAMCAST_IMAGE` selects another image. `just stack-up '' 1` pulls it before
starting, and `just stack-up 1 1` builds, then pulls. The default `streamcast:local`
is built from this checkout; it is not a published release channel.

The image uses Python 3.12, the frozen lockfile without development packages,
`tini -- streamcast`, and UID/GID 10001. Streamcast services have read-only root
filesystems, writable data and run mounts, and a 256 MiB `/tmp` tmpfs. The run
volume is a 16 MiB tmpfs. Broker readiness checks the broker alone; each role has
its own heartbeat probe. A failed role does not remove the broker's ready status.
Compose restarts stopped role processes according to `unless-stopped`; an
unhealthy probe reports failure but does not itself restart a container.

The memory defaults are 1 GiB for the broker, 512 MiB for seal and clean,
768 MiB for publish and compact, and 1 GiB for clean-published. Override them with
`BROKER_MEM`, `SEAL_MEM`, `CLEAN_MEM`, `PUBLISH_MEM`, `COMPACT_MEM` and
`CLEAN_PUBLISHED_MEM`. The example policy bounds each DuckDB pool at 96 MiB and
one thread, with 128 MiB retention spill under `/tmp`. These are local defaults;
size limits for your workload before using them elsewhere.

Initialization creates the object-store namespace, generates a private token
file, and explicitly initializes the registry. Re-running init preserves the
token and accepts an existing compatible registry. The `control` volume's
directory is mode 0700 and owned by UID 10001; its token is mode 0600. Only init,
broker and the smoke client mount it. Broker and smoke mount it read-only.
`TokenFileAccess` grants control authority to that bearer; requests without it
retain public data and stats access. The five maintenance services have no token
mount. No token is printed or passed in command arguments.

The object store is internal to the Compose network. Its local credentials can
be overridden with `STACK_S3_KEY` and `STACK_S3_SECRET`; they are passed through
the environment, never written in `broker.toml`.

`stack-smoke` claims one exactly-once publisher through an outbox and processes
rows through an inbox, keeping client state in SQLite. It commits ten rows,
kills clean-published, restarts the broker, waits for health, and resumes both
clients for ten more rows. It checks offset order, unique transactional work,
the publisher sequence and a complete 20-row replay. Each successful run adds
20 log rows. The fixed smoke database retains one run marker and at most 20 work
rows; incomplete runs retain evidence and can require an explicit wipe.

`stack-down` keeps every named volume, including the object store and token.
Roles and broker have a 30-second stop grace. Compose does not order role shutdown
among siblings: committed buffer rows survive and seal on the next start.
Only `just stack-wipe` removes volumes, after a confirmation prompt; `just --yes
stack-wipe` explicitly confirms it for disposable automation.

## Verification

The separate `Container stack` workflow builds on native amd64 and arm64 hosted
runners without pushing an image. Its `Compose smoke` job requires the real Docker
tier in `tests/test_stack.py`; missing Docker fails that job. The ordinary Python
suite skips those cases unless `STREAMCAST_REQUIRE_DOCKER=1` is set. The Just job
runs image, up, ps, logs, smoke twice, down, up and the explicitly confirmed wipe.
Both dependent jobs load the image artifact built at the PR head. Temporary image
artifacts expire after one day; stack resources are removed on failure too.

For a focused local check against an already-built image:

```sh
STREAMCAST_IMAGE=streamcast:local STREAMCAST_REQUIRE_DOCKER=1 \
  uv run pytest tests/test_stack.py -q --durations=10
```

`CI success` remains the repository's existing required check. Require the separate
`Container stack success` result as evidence for container changes too. Image
publication, release channels and the Helm chart use separate workflows.
