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
