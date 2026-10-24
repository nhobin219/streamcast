# Managed broker application

`streamcast.broker.create_app` builds one FastAPI application serving data
WebSockets, `/stats`, and the HTTP control API on one ASGI listener. Install
`streamcast[broker]`; the host supplies the ASGI server, storage policy and
authorization. Construction opens no files or sockets.

```python
from pathlib import Path
from streamcast.broker import (
    BrokerConfig, StorageProfile, create_app, initialize_registry,
)

# authorization implements the Authorization protocol below.
config = BrokerConfig(
    broker_id="primary",
    registry=Path("state/broker.db"),
    profiles={"local": StorageProfile("1", Path("data"))},
    authorization=authorization,
)
# Provision once, before the service starts. Existing files are refused.
initialize_registry(config)
app = create_app(config)
```

Run **one ASGI worker**, with lifespan enabled. Disable reload and pre-fork
workers. For Uvicorn set `lifespan="on"` and `ws_max_size` to
`config.transport_max_bytes` (default 1 MiB). The application also checks
each frame, but an ASGI server can allocate it before delivering it to the
application. Apply ingress connection/rate limits and TLS in the host.

A mounting host explicitly composes the child application's
`app.router.lifespan_context(app)` in its own top-level lifespan. A mount
alone does not start a broker. The existing `serve` and `asgi` APIs remain
available for static configurations. Do not serve the same log through
both a managed broker and either static API.

## Ownership and recovery

The top-level lifespan owns one runtime: registry, operation worker, readiness
monitor, stream generations, connections, writer threads, maintenance roles,
and replication sidecars. It acquires an exclusive registry lock and holds
canonical logical-stream and physical-log locks even for stopped definitions.
Profile/layout/symlink aliases cannot create a second managed owner. These
are local cooperative locks, **not a distributed writer lease**; static
writers and other hosts must be stopped by the deployment owner first.
Hard-link aliases, lock-file deletion, and external mutation of managed logs
are unsupported. Keep the registry and log directories on durable local
storage that supports SQLite fsync and advisory locks.

Startup requires the explicitly provisioned registry with the configured
broker identity. It replays pending intent before reconciling other desired
states. It never initializes a missing registry or recreates a bound missing
log. A new definition starts stopped and creates no log. First serving may
create only an unbound `create_if_absent` declaration. Once a stream identity
is bound, disk and authoritative version-hint metadata must match it.

Each accepted mutation atomically commits the declaration/desired state,
operation and idempotency receipt before returning 202. HTTP disconnect or
cancellation does not cancel accepted work. Operations are serialized; reads
and data traffic do not wait on the control serialization lock. Blocking
storage work runs off the event loop. Subprocess creation runs on the
lifespan thread so its parent-death relationship survives worker completion.

Stop fences new reads and writes, closes connections, joins replay workers
including cancelled scans, drains the writer, stops and reaps children,
seals/publishes, closes handles, then releases ownership. A failed final
publication is an operation failure; handles and locks are retained rather
than handed to another owner. The process must exit and recover. Repeated
ASGI cancellation joins owned startup/shutdown tasks before returning.

Maintenance roles have fixed group membership. A stream transition replaces
the shared group: all writes are fenced while old children are reaped and
new children become ready. Neighbor stream objects and connections stay in
place; their write admission resumes after readiness. A maintainer reports
its PID only after opening its whole group. A replication sidecar must own
its logs and accept a connection on its control socket. This establishes
local readiness, **not remote object-store durability**. Startup/replacement
has a bounded readiness timeout (30 seconds); later child unavailability
fences writes after that deadline. Reads may continue. There is no promise
of uninterrupted writes during membership changes.

The host owns shutdown deadlines. Killing a process before draining may
leave accepted intent to recover on the next boot. An unavailable or failed
operation never proves a destructive transition had no effect.

## Host policy and authorization

`StorageProfile(revision, root, published=None, config=None, s3_options=None,
replicate=False)` holds host-only locations, litelink configuration and
credentials. Its immutable revision is bound into each declaration; changing
a profile revision requires a reviewed host handoff. HTTP never supplies
paths, URLs, credentials, WAL settings, or maintenance policy. Public profile
data contains revision, replication/maintenance flags and replay scope only.
WAL-replicated logs require a replication-enabled profile.

`BrokerConfig` also accepts `history_guards={}`, `transport_max_bytes=1<<20`,
`control_max_bytes=256*1024`, `max_pending=32`, `readiness_timeout=30.0` and
`readiness_every=0.25`. Bounds must be positive; a per-stream frame limit
cannot exceed the transport ceiling. The runtime bounds pending operations
and in-flight control tasks. It returns 429 with `Retry-After: 1` rather than
growing an unbounded queue. Durable operation and receipt history is retained;
there is no automatic eviction.

The injected `Authorization` protocol has two asynchronous methods:

```python
async def authenticate(scope) -> str | None: ...
async def allows(principal: str, action: str, resource: str) -> bool: ...
```

The principal is a stable nonsecret identity. Actions are `data.read`,
`data.publish`, `stats.read`, `control.read`, `control.mutate` and
`control.migrate`. Resources are exact stream names, with `"*"` for collection
and operation discovery. Discovery also filters each returned stream by its
individual grant. Operation lookup requires both the discovery grant and
the target stream grant.

Authentication and the applicable grant precede body parsing, stream lookup
and WebSocket upgrade. Unauthenticated requests receive 401 with a Basic
challenge; authenticated denials receive 403. A URI's percent-escaped user
information supplies ordinary Basic authorization to WebSocket clients and
to HTTP stats clients; credentials in query parameters are not interpreted.
The host validates Basic credentials or any other authentication mechanism,
trusted ingress, Origin and proxy headers. No default allow-all policy is
provided. Do not log credential-bearing URIs.

## HTTP contract, version 1

All paths below are relative to the application mount. Requests and
responses are JSON. Mutations require `Content-Type: application/json` and
`Idempotency-Key` (1–128 printable ASCII characters, no spaces).
Missing keys receive 428; malformed keys receive 400. Duplicate
JSON keys, nonfinite numbers and malformed JSON receive 400. Bodies over the
configured control limit receive 413 before the application accumulates them.
Framework validation is supplemented by the existing schema compiler;
body reading is explicit so authorization and byte limits precede parsing.
The application exposes no Swagger, ReDoc or unauthenticated OpenAPI route.

Declarations describe the physical schema supported by `to_arrow`, rather
than a general JSON Schema validator. Unknown keywords, including `enum`,
`minimum` and `maximum`, are rejected with 422. A controller adopting an
existing application schema can explicitly declare
`streamcast.from_arrow(streamcast.to_arrow(application_schema))`. This
preserves column order, widths, nullability and nested encoding; it does not
enforce discarded application constraints. The application remains responsible
for those constraints, as with the static stream API. Review that conversion
before adoption rather than silently stripping keywords in the broker.

| Method and path | Result |
| --- | --- |
| `GET /control/v1` | Contract version, broker/boot identities, bounds, public profiles, protected names, wire versions. |
| `GET /control/v1/streams?limit=100&cursor=...` | Name-ordered page: `revision`, `streams`, nullable `next_cursor`. Limit is 1–1000. |
| `GET /control/v1/streams/{name}` | Definition, desired/observed state, revision, identity, pending operation and public profile; `ETag`. |
| `PUT /control/v1/streams/{name}` | Create or replace the full declaration. Create requires `If-None-Match: *`; replace requires `If-Match`. |
| `PUT /control/v1/streams/{name}/desired` | `{"state":"serving"}`, `"stopped"`, or terminal `"retired"`; requires `If-Match`. |
| `POST /control/v1/streams/{name}/migrations` | `stream_id`, target `schema` and target `sort_by`; requires `If-Match` and `control.migrate`. |
| `GET /control/v1/operations/{id}` | Operation `id`, `name`, `kind`, `revision`, `target`, `status`, nullable `error`. |
| `GET /stats` | The existing stats payload, filtered by `stats.read`. |

Names match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`. The ETag is the exact quoted
decimal revision, for example `"3"`. Missing preconditions receive 428; a
stale or malformed precondition receives 412. Both precondition headers
together are refused. Collection cursors bind the collection revision and
last name; a mutation invalidates an older cursor with 409. Restart from the
first page. A no-op does not change collection or stream revision.

A declaration is a JSON object with no unknown keys:

```json
{
  "schema": {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"]
  },
  "sort_by": [],
  "storage_profile": "local",
  "initialization": "open_existing",
  "exactly_once": false,
  "replay_published": false,
  "max_replay": 100000,
  "group_commit": true,
  "limits": {
    "max_backlog": 8192,
    "max_inbound": 65536,
    "max_in_flight": 64,
    "max_message_bytes": 1048576
  }
}
```

`schema` and `storage_profile` are required. Other keys default as shown
except `limits`, whose authoritative defaults are `_limits` and the host's
transport ceiling. `initialization` may also be `create_if_absent`;
`max_replay` may be null (unbounded) or a nonnegative integer. Limits are
positive integers. Boolean fields accept JSON booleans only.

Schema property order is significant. The schema compiler in
[`API.md`](API.md#stream) defines supported types, nullability, integer and
float formats, binary, arrays, structs and maps. The control API rejects
unsupported constraints and unknown keywords instead of accepting ignored
validation. `title` and `description` are annotations. Explicit
`additionalProperties: true` is refused. Reserved `streamcast_ts` and
`litelink_offset` columns are refused. Sort columns must be distinct declared
column names. Row validation and protocol framing remain the existing core's.

Non-schema definition changes require observed `stopped`. A bound stream's
storage profile cannot change. Schema/order/sort changes require an explicit
migration, with the current stream identity and both desired and observed
stopped (or a failed stopped transition). Migration keeps the active
definition separate from the durable target until metadata and target
schema are verified. Failed irreversible intent cannot be overwritten with
a different target. Retrying that exact intent uses a new idempotency key
and current ETag; reusing the original key returns its original receipt.
Retirement is terminal and leaves replay available without writes.

A mutation returns 202 with `status`, `operation_id`, `revision`, an ETag and
`Location: /control/v1/operations/{id}`. Poll the operation: `accepted` →
`running` → `succeeded` or `failed`. A satisfied no-op returns 200 and a
succeeded operation without a new generation. A receipt is scoped to the
principal and broker registry and hashes method, resource, ordered JSON body
and preconditions. An identical retry returns the original status/body/
Location **before checking today's ETag**. Different intent under that key
receives 409. The controller persists keys across network retries and polls
to completion; 202 alone does not establish readiness.

Observed state carries `state`, `boot_id`, `generation`, `revision`, `at`
(Unix seconds), `read_admission`, `write_admission`, and `children_ready`.
These are observations of this boot, not durable proof of current health.
Errors use `{"error":{"code":...,"message":...,"retryable":false,
"operation_id":null}}`; messages contain no storage paths, credentials or
rows. Statuses include 400 malformed input, 401 authentication, 403 grants
or history guard, 404 unknown resources, 409 conflict, 412 preconditions,
413 size, 415 media type, 422 unsupported declaration, 428 preconditions
required, 429 capacity, and 503 unavailable. 429/503 are retryable and include
`Retry-After: 1`. A failed operation reports `transition_failed`; inspect
host-side evidence before retrying an irreversible operation.

## Data consumer contract and protected history

Data remains `ws[s]://host/{name}`; publishing uses `?publish` and, for
exactly-once streams, `&publisher={stable_id}`. Subscriptions support the
existing `offset`, projection and filtering query semantics. Wire readers
accept versions 4 and 5; greetings still write 4. Rows are text JSON
`[offset, timestamp_us, application_row]`. Existing publisher sequence,
duplicate ACK, refusal, retirement, replay and per-subscriber backpressure
rules are unchanged. There is no new client database or archive dependency.
Unknown/inactive streams close 4404 after authorized upgrade, malformed data
requests 4400, and oversized frames 1009. A stop closes connections with
1001; clients reconnect with their durable cursor and publisher identity.

For an application whose thread/history reader starts at `offset=0`, current
log replay is insufficient after migration or retention. Such a deployment
**must inject** `history_guards={name: HistoryGuard(stream_id, known_offset,
known_row)}` from a handoff audit. Before data admission the broker verifies
the authoritative identity, no migration/retention/pending metadata, live
origin at offset one, exactly-once publishing, published replay, unbounded
replay, a dense complete prefix through the current tail, and the captured
old row. It scans history through the same broker replay machinery; this can
make startup expensive for large histories. No latency claim is made for
that audit. Protected migration and retirement are rejected **before
journaling** even with control grants. An already migrated/trimmed stream fails admission;
it needs a separately reviewed multi-log history design. Removing a host
guard or mutating storage outside this owner invalidates that assurance.

## Integration responsibilities

The library owns runtime, protocol, durable intent and local lifecycle
invariants. A deployment controller owns declaration reconciliation,
idempotency keys, ETags and operation polling. The host operator owns storage
profiles, authentication, ingress, shutdown budgets, retained-state handoff,
and stopping previous writers. Each consuming application owns its stream
bindings, stable producer identities, durable cursors, frame sizing and
reconnect behavior. Release and adoption require those owners' acceptance,
supported-version interoperability, real replication checks and a reviewed
consumer change. This application is an additive API intended for a minor
release; this change does not alter the version or authorize deployment.
