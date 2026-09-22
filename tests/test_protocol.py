"""The wire format, without a socket.

Every frame is JSON text now: the greeting, then one object per row. These are
pure functions, which is the point of `_protocol` being a module of them —
the frame layout, the greeting and the refusal codec are the three things both
ends have to agree about, and none of them needs a connection to check.
"""

from __future__ import annotations

import json

import pytest

from streamcast._errors import Close, ProtocolError
from streamcast._protocol import (
    CLOSE_REASON_LIMIT,
    EARLIEST,
    decode,
    encode,
    greeting,
    parse_greeting,
    parse_refusal,
    parse_subscribe,
    refusal,
    subscribe_path,
)

COLUMNS = ("event_ts", "price", "amount", "side", "tag")
ROW = {
    "event_ts": 1_790_038_800_123_456,
    "price": 85_565.0,
    "amount": 0.015,
    "side": 0,
    "tag": "t0",
}


class TestFrames:
    def test_a_frame_is_a_positional_pair(self):
        """`[offset, msg]`, and the halves are different kinds of thing.

        The offset is the broker's framing; `msg` is the publisher's row. Two
        earlier versions put the offset INSIDE the object — first as
        `litelink_offset`, then as `offset` — and both were wrong the same
        way: a subscriber takes the offset positionally, so the key name was a
        contract nobody wanted, and injecting it meant `msg` was never quite
        the row that was sent.
        """
        pair = json.loads(encode(7, ROW, COLUMNS))
        assert isinstance(pair, list)
        assert len(pair) == 2
        assert pair[0] == 7
        assert pair[1] == ROW
        assert "offset" not in pair[1]
        assert "litelink_offset" not in pair[1]

    def test_the_message_is_exactly_what_was_published(self):
        offset, message = decode(encode(1861, ROW, COLUMNS))
        assert offset == 1861
        assert message == ROW

    def test_a_frame_is_json_text_any_client_can_read(self):
        # The affordance the format exists for: `wscat ws://broker/trades`
        # prints the stream readably, and `const [offset, msg] =
        # JSON.parse(frame)` is the whole client in another language.
        frame = encode(1861, ROW, COLUMNS)
        assert isinstance(frame, bytes)
        assert json.loads(frame.decode()) == [1861, ROW]

    def test_the_column_order_comes_from_the_schema_not_the_dict(self):
        """**This is what makes a replay byte-identical to the live send.**

        A live row arrives in whatever order the caller built it; a replayed
        row arrives from Arrow in schema order. Both are projected through the
        declared columns, so the bytes match and a subscriber resuming across
        the join cannot tell where it happened.
        """
        shuffled = {k: ROW[k] for k in reversed(COLUMNS)}
        assert list(shuffled) != list(ROW)  # same data, opposite key order
        assert encode(1861, shuffled, COLUMNS) == encode(1861, ROW, COLUMNS)

        # And the projection is doing the work: without it the two orders
        # produce different bytes, which is the bug this prevents.
        assert encode(1861, shuffled, None) != encode(1861, ROW, None)

    def test_a_column_the_caller_omitted_becomes_null(self):
        # And comes back as None — which is what the table stores for it, and
        # therefore what a replay of the same row will send.
        without = {k: v for k, v in ROW.items() if k != "tag"}
        _offset, back = decode(encode(1, without, COLUMNS))
        assert back["tag"] is None

    def test_a_stream_with_no_schema_uses_the_rows_own_keys(self):
        # A live-only stream has no declared columns and nothing replays from
        # it, so there is no second encoding to match.
        _offset, back = decode(encode(None, {"b": 2, "a": 1}, None))
        assert back == {"b": 2, "a": 1}

    def test_a_stream_with_no_log_sends_a_null_offset(self):
        # Null rather than a counter: `null` cannot be mistaken for a resume
        # cursor, where an integer from a process-local sequence can.
        frame = encode(None, {"price": 1.0}, ("price",))
        assert json.loads(frame)[0] is None
        offset, message = decode(frame)
        assert offset is None
        assert message == {"price": 1.0}

    @pytest.mark.parametrize(
        ("frame", "match"),
        [
            (b"not json at all", "not JSON"),
            (b"[1, 2, 3]", r"not an \[offset, msg\] pair"),
            (b'{"price": 1.0}', r"not an \[offset, msg\] pair"),
            (b'["eight", {}]', "not an integer or null"),
            (b"[1, 2]", "not an object"),
        ],
    )
    def test_anything_that_is_not_a_frame_says_so(self, frame, match):
        with pytest.raises(ProtocolError, match=match):
            decode(frame)


class TestGreeting:
    def test_it_round_trips(self):
        info = parse_greeting(
            greeting(
                stream="trades", end_offset=1861, replay=(1200, 1861), durable=True
            )
        )
        assert info.stream == "trades"
        assert info.end_offset == 1861
        assert info.replay == (1200, 1861)
        assert info.durable is True

    def test_a_live_only_subscribe_has_no_replay_range(self):
        info = parse_greeting(
            greeting(stream="", end_offset=1, replay=None, durable=False)
        )
        assert info.replay is None
        assert info.durable is False

    @pytest.mark.parametrize(
        ("frame", "match"),
        [
            (b"\x00\x01", "binary"),
            ("<html>502 Bad Gateway</html>", "not JSON"),
            ('{"hello": 1}', "not a streamcast greeting"),
            ('{"streamcast": 99, "end_offset": 1}', "speaks streamcast 99"),
        ],
    )
    def test_anything_that_is_not_a_greeting_says_so(self, frame, match):
        with pytest.raises(ProtocolError, match=match):
            parse_greeting(frame)


class TestRefusals:
    def test_it_round_trips(self):
        error, fields = parse_refusal(
            refusal("not_replayable", why="too_old", behind=9)
        )
        assert error == "not_replayable"
        assert fields == {"why": "too_old", "behind": 9}

    def test_it_fits_the_close_frame_by_dropping_the_least_useful_field(self):
        reason = refusal(
            "no_such_stream", serves=[f"stream-{i:03}" for i in range(200)]
        )
        assert len(reason.encode()) <= CLOSE_REASON_LIMIT
        error, fields = parse_refusal(reason)
        assert error == "no_such_stream"
        # Trimmed to nothing rather than truncated to something wrong: a
        # partial list would read as "these are all the streams".
        assert fields == {}

    def test_it_keeps_the_fields_that_fit(self):
        reason = refusal("not_replayable", why="evicted", offset=100, earliest=5000)
        assert len(reason.encode()) <= CLOSE_REASON_LIMIT
        assert parse_refusal(reason)[1] == {
            "why": "evicted",
            "offset": 100,
            "earliest": 5000,
        }

    def test_a_reason_from_something_that_is_not_a_broker_never_raises(self):
        assert parse_refusal("connection reset by peer") == ("", {})
        assert parse_refusal("") == ("", {})
        assert parse_refusal("[1, 2, 3]") == ("", {})

    def test_the_close_codes_echo_their_http_cousins(self):
        assert Close.BAD_REQUEST == 4400
        assert Close.NO_SUCH_STREAM == 4404
        assert Close.NOT_REPLAYABLE == 4416
        assert Close.TOO_SLOW == 4429


class TestSubscribePath:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/", ("", None)),
            ("/trades", ("trades", None)),
            ("/trades?offset=1200", ("trades", 1200)),
            ("/trades?offset=0", ("trades", EARLIEST)),
        ],
    )
    def test_it_reads_a_subscribe(self, path, expected):
        assert parse_subscribe(path) == expected

    @pytest.mark.parametrize(
        ("path", "match"),
        [
            ("/trades?offset=abc", "not an integer"),
            ("/trades?offset=-1", "negative"),
            ("/trades?from=12", "unknown query parameter"),
        ],
    )
    def test_it_refuses_what_it_cannot_read(self, path, match):
        with pytest.raises(ValueError, match=match):
            parse_subscribe(path)

    @pytest.mark.parametrize(
        ("name", "offset"),
        [("", None), ("trades", None), ("trades", 1200), ("t", 0)],
    )
    def test_the_builder_and_the_parser_are_inverses(self, name, offset):
        assert parse_subscribe(subscribe_path(name, offset)) == (name, offset)
