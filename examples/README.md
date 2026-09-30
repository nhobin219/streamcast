# examples

A server and a consumer, against a live public feed. Nothing to configure and no
credentials to set: Bitstamp publishes BTC/USD trades over an unauthenticated
websocket.

## Start here

```
just demo              # terminal 1: the server
just demo-consumer     # terminal 2, and 3, and 4
```

`server.py` holds **one** connection to Bitstamp and serves every consumer on the
box from it. That is the whole idea, and the reason it is not one connection per
consumer. The loop is a parse and a send:

```python
frame = json.loads(await feed.recv())
if frame.get("event") != "trade":
    continue                       # acks and heartbeats are not rows

await stream.send(row(frame["data"]))
```

**The parse happens once, here.** Consumers receive the row, not the frame — six
consumers used to mean six JSON parses of the same bytes, and now it means none.
The schema in `server.py` is the demo's, not streamcast's: every field worth a
column gets one, which is what makes the log a table rather than a pile of
frames.

`send` returns once the row is durable, and it never awaits a consumer — so a
dashboard that stops reading cannot slow the strategy sitting beside it.

## The thing worth watching

Stop a consumer with Ctrl-C. Leave it stopped while trades keep arriving. Start it
again:

```
[one] connected at offset 4192, replaying 137 missed
[one] replay     4055     85,565.00  0.15000000  buy
...
[one]  live      4192     85,571.50  0.02410000  sell
```

It asked for the offset after the last one it processed, the server replayed the
gap out of the log, and then it went live — with no gap and no duplicate at the
join. The cursor is a file here; in a real consumer it is whatever you already
persist.

`--from-start` ignores the cursor and replays everything the log still holds.

`--catch-up` is the case one step past that — a consumer so far behind that the
server refuses, and the rows it wants are only in the log's archive. This demo
cannot show it: the log has no `archive=`, because that would mean credentials,
and the point here is that there are none. `consumer.py` takes the flag anyway,
so a real deployment is the same script.

## Live-only, for contrast

```
just demo-live
```

The same server with no litelink log. Fan-out works identically; `?offset=` is
refused outright with a message saying why, and a consumer that restarts starts
from now. Right when the stream is a cache nobody resumes — wrong the first time
a consumer restarts and you wanted the last ten minutes.

## Run several consumers

```
just demo-consumer --label a
just demo-consumer --label b
```

Each keeps its own cursor (`.a.offset`, `.b.offset`) and each receives the
identical bytes. Stop one, let it fall behind, start it: it catches up while the
other never notices.

## Cleaning up

```
just demo-clean        # the captured log
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

`fastapi_app.py` is the same stream served by an app you already have, instead of by
`serve()` on a port of its own.

```
just demo-fastapi                                                  # terminal 1
just demo-consumer --uri ws://127.0.0.1:8000/streams/trades        # terminal 2
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
with `asgi(...)`, and `app.mount("/streams", streams)`. The fourth thing to know is that
`async with streams` in the lifespan is **not** optional — Starlette does not run a
mounted sub-app's lifespan, so that block is what starts the maintainer which seals the
log, and what closes the log on the way out.

Needs the extra: `pip install 'streamcast[asgi]'`.

## OpenTelemetry logs, in a dashboard

```
just demo-otel         # the broker, an OTLP exporter, and otel-gui's dashboard
just demo-otel-once    # the same pipeline once, printing what each part saw
```

`otel_logs.py` runs two simulated services (checkout and payments) that log
through Python's `logging` and the OpenTelemetry SDK. A small exporter
publishes each log record to a stream as a row. `otel_export.py` subscribes to
the stream and re-exports every row as OTLP to [otel-gui](https://github.com/metafab/otel-gui),
a local dashboard for traces, logs and metrics, where the logs arrive live.
Failed orders show up as a `card declined` error and an `order failed`
warning, sharing one trace id.

**None of this is in streamcast.** The OTel record's schema and the
record-to-row conversion live in `otel_logs.py`, built from the column types
any stream can declare: trace and span ids as hex binary, attributes as a
map, and OTel's `AnyValue` as a struct. The OTel packages are dev
dependencies, for this example only.

**`otel_export.py` is an ordinary OTLP exporter.** OTel viewers are
*receivers*: telemetry is pushed to them, and none subscribes to a WebSocket.
So this subscribes to the stream, turns each row back into an SDK log record,
and hands it to OpenTelemetry's own `BatchLogRecordProcessor` and
`OTLPLogExporter`. The batching, the protobuf encoding and the retries are
OTel's, and it works with any OTLP/HTTP receiver. Point `--receiver` at an
OTel Collector and it feeds whatever the Collector does.

**Nothing leaves the machine.** The first `just demo-otel` downloads otel-gui's
release for your platform, checks its SHA-256 and caches it. The dashboard
listens on `127.0.0.1:4318`, which is OTLP/HTTP's standard port.

`just demo-otel-once` shows what a subscriber can do beyond a dashboard:

- a live tail filtered to `severity_text` in `["ERROR", "WARN"]`;
- one failed request replayed by its trace id, with the id given as hex text
  in `where=`;
- SQL over the stored table: errors per service, and ingest lag from
  `streamcast_ts`.
