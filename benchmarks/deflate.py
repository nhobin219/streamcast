"""What permessage-deflate costs and saves on a streamcast fan-out.

`compression` defaults to None where `websockets` defaults to "deflate", and
this is the measurement that picks the default. The asymmetry is the point:
deflate is per CONNECTION while the encode is shared, so its cost scales with
subscribers and the encode's does not.

    just bench-deflate
"""

import time
import zlib

import msgspec

ROW = {
    "event_ts": 1790038800123456,
    "price": 85565.0,
    "amount": 0.015,
    "side": 0,
    "trade_id": 624438572,
    "kind": "trade",
}
ENCODER = msgspec.json.Encoder()
FRAMES = [
    ENCODER.encode((1861 + i, {**ROW, "price": 85565.0 + i, "trade_id": 624438572 + i}))
    for i in range(2000)
]


def encode_cost():
    t0 = time.perf_counter()
    for i in range(2000):
        ENCODER.encode((1861 + i, ROW))

    return (time.perf_counter() - t0) / 2000 * 1e6


def deflate_stream():
    """permessage-deflate with context takeover: one compressor, many frames."""
    comp = zlib.compressobj(wbits=-15)
    raw = out = 0
    t0 = time.perf_counter()
    for frame in FRAMES:
        chunk = comp.compress(frame) + comp.flush(zlib.Z_SYNC_FLUSH)
        raw += len(frame)
        out += len(chunk)

    micros = (time.perf_counter() - t0) / len(FRAMES) * 1e6

    return micros, raw, out


def deflate_nocontext():
    """No context takeover: a fresh compressor per frame."""
    raw = out = 0
    t0 = time.perf_counter()
    for frame in FRAMES:
        comp = zlib.compressobj(wbits=-15)
        chunk = comp.compress(frame) + comp.flush(zlib.Z_SYNC_FLUSH)
        raw += len(frame)
        out += len(chunk)

    micros = (time.perf_counter() - t0) / len(FRAMES) * 1e6

    return micros, raw, out


enc = encode_cost()
ctx_us, raw, ctx_out = deflate_stream()
no_us, _, no_out = deflate_nocontext()

print(f"  frame size                  {raw / len(FRAMES):.0f} bytes")
print(f"  msgspec encode              {enc:.3f} us   (ONCE per message)")
print(f"  deflate, context takeover   {ctx_us:.3f} us   (per SUBSCRIBER per message)")
print(
    f"       -> {raw / ctx_out:.1f}x smaller  ({ctx_out / len(FRAMES):.0f} bytes/frame)"
)
print(f"  deflate, no context         {no_us:.3f} us")
print(
    f"       -> {raw / no_out:.1f}x smaller  ({no_out / len(FRAMES):.0f} bytes/frame)"
)
print()
print("  CPU per message at N subscribers (encode once + compress per subscriber):")
for n in (1, 6, 50, 200):
    print(
        f"    N={n:<4} off: {enc:7.2f} us    on: {enc + ctx_us * n:8.2f} us"
        f"   ({(enc + ctx_us * n) / enc:5.1f}x)"
    )

print()
print("  Bandwidth at 30,000 msg/s:")
mbps_off = 30_000 * (raw / len(FRAMES)) * 8 / 1e6
mbps_on = 30_000 * (ctx_out / len(FRAMES)) * 8 / 1e6
print(f"    off: {mbps_off:6.1f} Mbit/s per subscriber")
print(f"    on:  {mbps_on:6.1f} Mbit/s per subscriber")
