# benchmarks

```
just bench                       # fan-out and publish
just bench --subscribers 500 --messages 100000
just bench-replay                # replay, and which layer it is spent in
just bench-replay --rows 200000
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

`bench-replay` is separate because a replay has a different shape: real fixed
setup per scan and then a cheap per-row walk, so a single us/row figure taken at
one size is meaningless. It reports cold against warm, fixed against marginal,
and the split across DuckDB, Arrow and the encoder — and then the arithmetic
that sizes `max_replay` against `max_backlog` for your hardware. The numbers in
[`docs/SPEC.md`](../docs/SPEC.md) §4 come from it, after an earlier version of
that section stated two guesses as measurements and had both about 2x wrong.

Then an end-to-end pass over loopback with real subscribers, reporting p50 and p99
delivery latency. p99 rather than a mean, because the interesting failure is a
tail: a fan-out that is fast on average and occasionally parks is a fan-out that
is awaiting something it should not be.

Numbers move with hardware. Measure before and after **in the same session on the
same machine** — a comparison across two runs on two boxes says nothing. If a
change costs throughput, say so in the commit with the figures rather than leaving
it to be discovered.
