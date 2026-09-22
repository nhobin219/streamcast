# benchmarks

```
just bench
just bench --subscribers 500 --messages 100000
```

Three numbers, because three different things could be the bottleneck and only one
of them is streamcast's.

**encode** — building one frame. Done once per message however many subscribers
there are. If this scaled with subscriber count, the library's central claim would
be wrong.

**send** — the queue insert per subscriber, with no sockets. Linear in subscribers
by construction; the question is the constant.

**send / send_many, durable** — the same call with a litelink log attached. One
SQLite transaction at `synchronous=FULL`, dominating everything above it by two
orders of magnitude. That gap is the entire argument for `send_many`.

Then an end-to-end pass over loopback with real subscribers, reporting p50 and p99
delivery latency. p99 rather than a mean, because the interesting failure is a
tail: a fan-out that is fast on average and occasionally parks is a fan-out that
is awaiting something it should not be.

Numbers move with hardware. Measure before and after **in the same session on the
same machine** — a comparison across two runs on two boxes says nothing. If a
change costs throughput, say so in the commit with the figures rather than leaving
it to be discovered.
