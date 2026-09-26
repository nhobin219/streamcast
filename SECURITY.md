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

**streamcast has no authentication, no authorisation and no transport security
of its own**, and adding any of them is not on this list because whatever owns
the socket already has them and passing through is the whole design. A server
bound to a public interface with no `ssl=` and no `process_request=` is serving
its stream to anyone who can reach the port — and that is a deployment choice,
not a defect.

- **TLS** is `serve(..., ssl=context)`.
- **Authentication** is `serve(..., process_request=...)`, which sees the
  request before the WebSocket opens and can answer `401`.
- **Binding** defaults to nothing: `serve(stream)` with no host binds to all
  interfaces, exactly as `websockets` does. Pass `"127.0.0.1"` for a server that
  should only serve its own box, which is the case this library is built for.

### Mounted in an ASGI app

`streamcast.asgi` owns no socket, so **none of the three is streamcast's and
none of them is `websockets`' either** — they belong to the host application
and the server running it. `ssl=` and `process_request=` do not exist on a
mount; TLS is the ASGI server's or the proxy in front of it, and
authentication is the host app's middleware, which sees the request before the
WebSocket handshake exactly as `process_request` did.

The consequence worth stating: **mounting a stream inside an authenticated app
does not authenticate the stream.** Middleware that guards your routes guards
the mount only if it runs for WebSocket scopes too, which is a property of how
it was written rather than a given.

### `/stats`

`serve` answers `GET /stats` by default with each stream's name, offsets,
subscriber count and last-send time. It discloses strictly less than the
socket beside it — a wrong-path connect is already answered with the names of
every stream served, the greeting already carries `end_offset`, and anyone who
can reach the port can subscribe and read every row in full. A server that
needs it private needs the port private; `serve(..., stats=False)` turns it
off.

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
