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
already has them and passing its keywords through is the whole design. A server
bound to a public interface with no `ssl=` and no `process_request=` is serving
its stream to anyone who can reach the port — and that is a deployment choice,
not a defect.

- **TLS** is `serve(..., ssl=context)`.
- **Authentication** is `serve(..., process_request=...)`, which sees the
  request before the WebSocket opens and can answer `401`.
- **Binding** defaults to nothing: `serve(stream)` with no host binds to all
  interfaces, exactly as `websockets` does. Pass `"127.0.0.1"` for a server that
  should only serve its own box, which is the case this library is built for.

**A subscriber can ask for a replay, and a replay costs the server a scan.**
`max_replay` bounds how far back one may ask; it does not bound how *often*.
A server reachable by untrusted clients wants a connection limit in front of it.

**Close reasons carry stream names.** A 4404 lists what the server serves, so a
client that cannot reach a stream still learns that it exists. If a stream's
existence is itself sensitive, run it on a separate port.

**The greeting publishes the archive's location**, and the `too_old` refusal
carries it when it fits in 123 bytes. That is deliberate — it is what lets
`catch_up=True` find the gap without being told where to look — but it means a
subscriber learns the bucket and prefix the log is archived to. Reading it
still needs credentials the server never sends and never has to: the client
resolves its own from the ordinary AWS chain, so a bucket policy is what
decides who may read the archive, not this library. If the URI itself is
sensitive, leave `archive=` off the log and give catching-up consumers the
location out of band.

**A server that allows publishing accepts writes from anyone who can reach
it.** `serve(..., publish=True)` is opt-in for that reason, and off by default
so an upgrade cannot make a server writable on its own. With it on, the
authentication above stops being optional: a published row is durable, every
subscriber sees it, and no consumer cursor undoes it. `process_request` is the
hook — it sees the request before the WebSocket opens, including the
`?publish` query that distinguishes a publisher from a subscriber, so a policy
can allow reads and refuse writes on the same port.

What IS in scope: anything that lets a subscriber see messages from a stream it
did not subscribe to, receive a stream that silently differs from another
subscriber's, make the server exhaust memory through a path `max_backlog` is
supposed to bound, or publish into a stream on a server where `publish=True`
was not set.
