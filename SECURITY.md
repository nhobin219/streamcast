# Security

## Reporting

Report a vulnerability privately through GitHub's
[security advisories](https://github.com/nhobin219/streamcast/security/advisories/new)
rather than in a public issue. Include what you did, what happened, and what you
expected; a reproduction is worth more than a description.

Expect an acknowledgement within a week.

## Scope, and what is a known limit instead

**streamcast has no authentication, no authorisation and no transport security
of its own**, and adding any of them is not on this list because `websockets`
already has them and passing its keywords through is the whole design. A broker
bound to a public interface with no `ssl=` and no `process_request=` is serving
its stream to anyone who can reach the port — and that is a deployment choice,
not a defect.

- **TLS** is `serve(..., ssl=context)`.
- **Authentication** is `serve(..., process_request=...)`, which sees the
  request before the WebSocket opens and can answer `401`.
- **Binding** defaults to nothing: `serve(stream)` with no host binds to all
  interfaces, exactly as `websockets` does. Pass `"127.0.0.1"` for a broker that
  should only serve its own box, which is the case this library is built for.

**A subscriber can ask for a replay, and a replay costs the broker a scan.**
`max_replay` bounds how far back one may ask; it does not bound how *often*.
A broker reachable by untrusted clients wants a connection limit in front of it.

**Close reasons carry stream names.** A 4404 lists what the broker serves, so a
client that cannot reach a stream still learns that it exists. If a stream's
existence is itself sensitive, run it on a separate port.

What IS in scope: anything that lets a subscriber see messages from a stream it
did not subscribe to, receive a stream that silently differs from another
subscriber's, or make the broker exhaust memory through a path `max_backlog` is
supposed to bound.
