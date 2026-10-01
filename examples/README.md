# examples

Every example has the same three roles, each a separate process, because
that is the shape to copy:

- a **producer** is a client that publishes rows (`streamcast.publish`);
- the **broker** serves the streams: it holds each stream's log and fans
  rows out (`streamcast.serve`, or `asgi` mounted in an app);
- a **subscriber** is a client that reads them (`streamcast.connect`).

The broker has no application code. What a row means is the producer's
business, and what to do with it is the subscriber's. `broker.py` is one
generic broker that every demo uses: `--stream NAME=SCHEMA` declares a stream
from its JSON Schema file (or a `module:ATTRIBUTE` built in Python), and that
is all it is told.

There are two kinds of example:
- **Demos you run**, with `just demo`, which starts every process a demo has.
  Most run against live public feeds from Bitstamp, so there is nothing to
  configure and no credentials to set.
- **Patterns you read**, where the code is the point: `keyed_table/`,
  `migration/` and the one-shot `otel/demo.py`. They put the three roles in
  one process, talking over sockets as separate processes would, so a test
  can run each end to end. `uv run python -m examples.<path>` runs one by
  hand.

## Running a demo

```
just demo                  # every runnable demo, and what it shows
just demo NAME [ARGS]      # run one, with all its processes
```

| `just demo` | its processes | what it shows |
|---|---|---|
| `trades` | broker, `trades/producer.py`, `trades/consumer.py` | Bitstamp's BTC/USD trades, with a log to replay from |
| `consumer` | `trades/consumer.py` | one more subscriber for the trades demo, in a second terminal |
| `live` | as `trades`, with a live-only broker | no log: nothing to replay |
| `fastapi` | `fastapi_app.py` as the broker, then as `trades` | the broker mounted in a FastAPI app |
| `book` | broker, `book/producer.py`, `book/index.html` | Bitstamp's live order book as a keyed table log, kept by a browser page |
| `otel` | otel-gui, broker, `otel/services.py`, `otel/export.py` | OpenTelemetry logs and traces through streams, in a dashboard |
| `clean` | | delete what the demos stored |

`just demo` starts a demo's processes in order, waiting for each one that
listens to answer, and prints their output in one terminal with each line
labelled by its role. Ctrl-C stops them all, last started first. ARGS go to
the demo's subscriber (`just demo trades --label two`), and `just demo NAME
--help` prints what they can be. Every process is a module
with its own `--help`, so any one runs alone with `uv run python -m`.
`examples/__main__.py` holds the list.

## Start here

```
just demo trades                 # terminal 1: broker, producer, consumer
just demo consumer --label two   # terminal 2: one more consumer, and 3, and 4
```

`trades/producer.py` holds **one** connection to Bitstamp and publishes every
trade to the broker, which serves every consumer on the box from its log.
That is the whole idea, and the reason it is not one connection to Bitstamp
per consumer. The producer's loop is a parse and a publish:

```python
frame = json.loads(message)
if frame.get("event") == "trade":    # acks and heartbeats are not rows
    await publication.send(row(frame["data"]))
```

**The parse happens once, in the producer.** Consumers receive the row, not
the frame. The schema (`trades/schema.json`) is the demo's, not
streamcast's: every field worth a column gets one, which is what makes the
log a table rather than a pile of frames.

`publish` returns once the broker has the row durably, and the broker never
awaits a consumer, so a dashboard that stops reading cannot slow the strategy
sitting beside it.

## The thing worth watching

Start a second consumer with `just demo consumer --label two`, stop it with
Ctrl-C, leave it stopped while trades keep arriving, and start it again:

```
[two] connected at offset 4192, replaying 137 missed
[two] replay     4055     85,565.00  0.15000000  buy
...
[two]  live      4192     85,571.50  0.02410000  sell
```

It asked for the offset after the last one it processed, the broker replayed the
gap out of the log, and then it went live — with no gap and no duplicate at the
join. The cursor is a file here; in a real consumer it is whatever you already
persist.

`--from-start` ignores the cursor and replays everything the log still holds.

`--catch-up` is the case one step past that — a consumer so far behind that the
broker refuses, and the rows it wants are only in the stream's published tables.
Here the broker publishes to a directory beside its log, which a consumer on the
same machine reads directly. On another machine the tables have to be on S3
(`published="s3://…"`) and the consumer needs credentials for them; the script
is the same.

## Live-only, for contrast

```
just demo live
```

The same demo with a broker that keeps no litelink log. Fan-out works
identically; `?offset=` is refused outright with a message saying why, so the
consumer follows from now, and one that restarts starts from now again. Right when the stream is a cache nobody resumes — wrong the first time
a consumer restarts and you wanted the last ten minutes.

## Run several consumers

```
just demo consumer --label a     # in one terminal
just demo consumer --label b     # and another
```

Each keeps its own cursor (`.a.offset`, `.b.offset`) and each receives the
identical bytes. Stop one, let it fall behind, start it: it catches up while the
other never notices.

## Cleaning up

```
just demo clean        # the logs the demos stored
rm .*.offset           # the consumer cursors
```

The log is kept on purpose while the demo runs — it is what the replay reads, and
it is there to poke at afterwards. It is a **table**, so ask it real questions:

```python
import litelink
with litelink.open("streamcast-data", "trades", read_only=True) as log:
    print(log.sql("""
        SELECT count(*) trades, min(price) low, max(price) high, sum(amount) btc
        FROM log
    """).read_all())
    # and this prunes on Iceberg statistics rather than scanning payloads
    print(log.scan(columns=["event_ts", "price"], where="side = 1").read_all())
```


## Mounted in a FastAPI service

`fastapi_app.py` is the broker role played by an app you already have,
instead of by `serve()` on a port of its own. The trades producer publishes
to it and a consumer subscribes from it, both at
`ws://127.0.0.1:8770/streams/trades`; nothing in the app writes rows.

```
just demo fastapi
```

**You do not call `serve()`.** `serve` and `asgi` are two transports for one `Stream`,
and a mounted app uses one of them:

| | owns a socket | you get |
|---|---|---|
| `serve(stream, host, port)` | yes | a standalone server |
| `asgi(stream)` | no | an app to mount in yours |

The `Stream` is the thing either way — it holds the offsets, the log and the fan-out,
and the transport only carries frames. Which is why `/health` in that file reads
`trades.end_offset` directly without asking the websocket layer anything.

Three lines in the file are the whole of it: build the stream with `Stream.new`, wrap it
with `asgi(..., publish=True)`, and `app.mount("/streams", streams)`. The fourth thing to know is that
`async with streams` in the lifespan is **not** optional — Starlette does not run a
mounted sub-app's lifespan, so that block is what starts the maintainer which seals the
log, and what closes the log on the way out.

Needs the extra: `pip install 'streamcast[asgi]'`.

## A keyed table log, and branches

```
just demo book                                   # live, in a browser
uv run python -m examples.keyed_table.orders     # to read: a view in SQLite
uv run python -m examples.keyed_table.branches   # to read: branches
```

What state a stream holds is the application's to define. These examples
use a common shape, the **keyed table log**: each row is the whole state of
one record, keyed by an id, with a `deleted` flag. Written that way, the
table the log stands for is "the last row by id, where not deleted", and a
subscriber can keep it as it reads. streamcast knows nothing of keys or
deletes; this schema defines both. `keyed_table/view.py` keeps that table
for a log of orders, in SQLite, one statement per row.

**`just demo book` runs it live.** Bitstamp's BTC/USD order book changes
about a hundred times a second, and its feed is already a keyed table log:
an order created or changed is its whole current state, and an order deleted
is a tombstone. `book/producer.py` publishes it to the broker's `orders`
stream, and `book/index.html`, a static page, is the subscriber: it keeps
"the last row by id, where not deleted" in AG Grid as rows arrive, with no
server code building the view. A reload replays the stream and rebuilds the
same book, and a dropped connection resumes after the last row the page
applied. It shows the orders placed since the demo started, since one resting
before then appears only if it changes.

**The view is its own cursor.** It writes the offset it has applied in the
same transaction as the row, so a view reopened after a crash resumes at
exactly the next row. `keyed_table/orders.py` stops one partway, publishes
more, and reopens it. It then asks the log the same question, as one window
function over `litelink_offset`, and gets the same book.

**Branches** (`keyed_table/branches.py`) add a `branch_id` column. Production
writes to `main`. A branch is a client's private database:

1. `View.fork()` copies main's view at the offset it has applied.
2. A live subscription with `where={"branch_id": "<branch>"}` keeps the copy
   current with the branch's own rows.
3. The client writes to its branch freely, and no other view moves.
4. **A commit is one `send_many`**: the branch's rows again, with
   `branch_id: "main"`. One transaction, so main's view gets the whole change
   as one contiguous run of offsets, or none of it.

There is no merge engine: last row by id wins in offset order, as for any
other write.

A branch can also **track main**. With `where={"branch_id": ["main",
"<branch>"]}` it follows production live and keeps its own writes on top.
That is how to try a new system against live data: a migration or a new
service reads everything production does, writes only to its branch, and
production never sees a row of it.

## A migration, tested against live production data

A pattern to read, run by its test. By hand:

```
uv run python -m examples.migration.demo          # a correct migration: old and new agree
uv run python -m examples.migration.demo --bug    # a wrong one: the diff names every order it breaks
```

Production is a pipeline of streams: A (`orders`) feeds B (`positions`),
which feeds C (`alerts`). Each node subscribes to one stream and publishes
to the next (`migration/node.py`). The migration changes B's output schema.
Rather than change B in place, a shadow runs beside it:

- **D** is B's code, changed to write the new schema to its own stream. It
  subscribes to A from `EARLIEST`, so it rebuilds B's state from production's
  whole history, then follows production live.
- **E** is C's code, changed to read the new schema.

Production never knows. D and E are just more subscribers, and a stream never
waits on a subscriber: the demo makes D slow on purpose, and production
finishes long before it does.

**The test is a join.** Every output row carries `order_offset`, the offset
in A it came from, so old and new compare row for row rather than by time. A
comparator watches the four outputs live, and DuckDB joins B against D and C
against E. An empty diff means the migration does what production does. Once
it's empty, the cutover is C reading D's stream, and B retires with its
history still a queryable table.

## OpenTelemetry logs and traces, in a dashboard

```
just demo otel                        # otel-gui, a broker, the services, and the OTLP exporter
uv run python -m examples.otel.demo   # the same pipeline once, printing what each part saw
```

Two simulated services, checkout and payments, handle traced orders and log
through Python's `logging` and the OpenTelemetry SDK. Their log records and
spans are published to two streams, `logs` and `spans`. `otel/export.py`
follows both and re-exports every row as OTLP to [otel-gui](https://github.com/metafab/otel-gui),
a local dashboard where logs, traces and the service map fill in live. A
failed order is one trace across both services: payments' `POST /charge`
span with a `card declined` event and error log, and checkout's request
marked failed with an `order failed` warning.

| file | what it holds |
|---|---|
| `otel/common.py` | what both signals share: `AnyValue`, ids, scope, and publishing from OTel's export thread |
| `otel/logs.py` | the log record's schema, its row conversion, and `StreamLogExporter` |
| `otel/spans.py` | the span's schema, its row conversion, and `StreamSpanExporter` |
| `otel/demo.py` | the two services, the broker, and the one-shot demo |
| `otel/export.py` | rows back to OTel records and spans, out through OTel's OTLP exporters |
| `otel/services.py` | the producer: two services logging and tracing through OTel, publishing to the broker |
| `otel/gui.py` | otel-gui, downloaded, checked and run: where `export.py` sends what it reads |

**None of this is in streamcast.** The schemas and conversions are built from
the column types any stream can declare: trace and span ids as hex binary,
attributes as a map, OTel's `AnyValue` as a struct, and a span's events and
links as lists of structs. The OTel packages are dev dependencies, for this
example only.

**`otel/export.py` is an ordinary OTLP exporter.** OTel viewers are
*receivers*: telemetry is pushed to them, and none subscribes to a WebSocket.
So this subscribes to the streams, turns each row back into an SDK log record
or span, and hands it to OpenTelemetry's own batch processors and OTLP/HTTP
exporters. The batching, the protobuf encoding and the retries are OTel's,
and it works with any OTLP/HTTP receiver. Point `--receiver` at an OTel
Collector and it feeds whatever the Collector does.

**Nothing leaves the machine.** The first `just demo otel` downloads otel-gui's
release for your platform, checks its SHA-256 and caches it. The dashboard
listens on `127.0.0.1:4318`, which is OTLP/HTTP's standard port.

The one-shot run shows what a subscriber can do beyond a dashboard:

- a live tail filtered to `severity_text` in `["ERROR", "WARN"]`;
- one failed request replayed by its trace id from both streams, with the id
  given as hex text in `where=`;
- SQL over the stored tables: errors per service, the slowest request from
  its root span, and ingest lag from `streamcast_ts`.
