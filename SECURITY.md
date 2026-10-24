# Security

## Reporting

Report a vulnerability privately through GitHub's
[private vulnerability reporting](https://github.com/nhobin219/streamcast/security/advisories/new)
rather than in a public issue — the **Security** tab, then *Report a
vulnerability*. It opens an advisory visible only to the maintainers, and it
is deliberately not an email address: the thread, the fix, the CVE and the
disclosure all live in one place, and nothing depends on a mailbox being
watched.

Include what you did, what happened, and what you expected; a reproduction is
worth more than a description.

Expect an acknowledgement within a week.

## Scope, and what is a known limit instead

**streamcast authenticates and authorizes through a policy the host injects,
and has no transport security of its own.** `create_app` returns a FastAPI
application; TLS, the bind address, proxy trust and connection limits belong to
the ASGI server running it, or the proxy in front of that, and passing them
through is the whole design.

- **Authorization** is `BrokerConfig(authorization=...)`, an object with
  `authenticate(scope)` and `allows(principal, action, resource)`. It runs
  before an HTTP body is read, a stream is looked up or a WebSocket is
  upgraded, for WebSocket scopes as well as HTTP ones. The actions are
  `data.read`, `data.publish` and `stats.read`, plus the management grants in
  [`docs/API.md`](docs/API.md#host-policy-and-authorization).
- **A broker of static streams alone is public by default**: its policy allows
  every read, every publish and `/stats` to anyone who can reach the port, as
  a server before this one did. That is a deployment choice, not a defect; pass
  a policy when the port is reachable by anyone who should not have them. A
  broker with a registry has no default and refuses to start without one.
- **TLS** is the ASGI server's (`uvicorn --ssl-certfile`) or the proxy's.
- **Binding** is the ASGI server's. Pass `127.0.0.1` for a broker that should
  only serve its own box, which is the case this library is built for.

### Mounted in an ASGI app

A mounted broker is the same application, so its policy still runs for every
request that reaches the mount. The consequence worth stating is the reverse:
**mounting a stream inside an authenticated app does not authenticate it with
the host's middleware.** Middleware that guards your routes guards the mount
only if it runs for WebSocket scopes too, which is a property of how it was
written rather than a given; the broker's own `authorization` is the one that
is certain to run.

### `/stats`

The broker answers `GET /stats` by default with each stream's name, offsets,
subscriber count and last-send time, filtered to the streams the caller's
`stats.read` grant allows. It discloses strictly less than the socket beside
it — a wrong-path connect to a broker without a registry is already answered
with the names of every stream served, the greeting already carries
`end_offset`, and anyone allowed to subscribe can read every row in full.
`BrokerConfig(stats=False)` turns it off.

**A subscriber can ask for a replay, and a replay costs the server a scan.**
`max_replay` bounds how far back one may ask; it does not bound how *often*.
A server reachable by untrusted clients wants a connection limit in front of it.

**Close reasons carry stream names.** Without a registry, a 4404 lists what
the broker serves, so a client that cannot reach a stream still learns that it
exists. If a stream's existence is itself sensitive, run it on a separate port.
A broker with a registry lists nothing.

**The greeting publishes the stream's metadata location** — the URI of its
metadata file, and the stream's id. That is deliberate — it is what lets
`catch_up=True`, `Stream.live` and anyone holding a greeting find the
published tables without being told where to look — but it means a subscriber
learns the bucket and prefix the log publishes to, or, for a log that
publishes locally, a path on the server's disk. Refusals carry no location.
Reading the tables still needs credentials the server never sends and never
has to: the client resolves its own from the ordinary AWS chain, so a bucket
policy is what decides who may read them, not this library. There is no
switch to leave the location out of the greeting; if it is itself sensitive,
the port should not be reachable by anyone who should not learn it.

**A public broker accepts writes from anyone who can reach it.** Every served
stream takes publishers — only a retired one refuses them — so a policy is not
optional on a reachable port: a published row is durable, every subscriber
sees it, and no consumer cursor undoes it. Grant `data.publish` separately from
`data.read` to allow reads and refuse writes on the same port.

What IS in scope: anything that lets a subscriber see messages from a stream it
did not subscribe to, receive a stream that silently differs from another
subscriber's, make the server exhaust memory through a path `max_backlog` is
supposed to bound, publish into a retired stream, reach a stream or an
action the injected policy refused, or disclose a storage profile's credentials.
