"""The wire format, without a socket.

Everything here is pure functions, which is the point of `_protocol` being a
module of them: the frame layout, the greeting and the refusal codec are the
three things both ends have to agree about, and none of them needs a
connection to check.
"""

from __future__ import annotations

import json

import pytest

from streamcast._errors import Close, ProtocolError
from streamcast._protocol import (
    BINARY,
    CLOSE_REASON_LIMIT,
    EARLIEST,
    TEXT,
    decode,
    encode,
    frame_offset,
    greeting,
    kind_of,
    parse_greeting,
    parse_refusal,
    parse_subscribe,
    refusal,
    subscribe_path,
)


class TestFrames:
    @pytest.mark.parametrize(
        "message",
        [
            "",
            "hello",
            '{"event":"trade","price":78501.62}',
            "ünïcödé — a multi-byte payload",
            b"",
            b"\x00\x01\xff\xfe binary that is not UTF-8",
        ],
    )
    @pytest.mark.parametrize("offset", [1, 2**31, 2**62])
    def test_a_message_survives_the_round_trip_unchanged(self, message, offset):
        kind = kind_of(message)
        back_offset, back = decode(encode(offset, kind, message))
        assert back_offset == offset
        assert back == message
        # And the TYPE, which is the whole reason `kind` is on the row: a
        # subscriber handed `str` where the publisher sent `bytes` has been
        # handed different data, not a different encoding.
        assert type(back) is type(message)

    def test_the_offset_is_readable_without_decoding_the_payload(self):
        frame = encode(1861, TEXT, "x" * 10_000)
        assert frame_offset(frame) == 1861

    def test_kind_of_refuses_anything_that_is_not_a_message(self):
        assert kind_of("a") == TEXT
        assert kind_of(b"a") == BINARY
        # Narrower than `websockets` on purpose: a buffer would be copied
        # to `bytes` inside `encode` anyway, and refusing it makes the copy
        # the caller's to see.
        with pytest.raises(TypeError, match="not bytearray"):
            kind_of(bytearray(b"a"))  # ty: ignore[invalid-argument-type]

        with pytest.raises(TypeError, match="str or bytes, not dict"):
            kind_of({"not": "a message"})  # ty: ignore[invalid-argument-type]

    def test_a_frame_too_short_to_hold_a_header_is_a_protocol_error(self):
        with pytest.raises(ProtocolError, match="too short"):
            decode(b"\x00\x00\x00")

    def test_an_unknown_payload_kind_is_a_protocol_error(self):
        # A future build's third kind, reaching this one. It must not be
        # guessed at: handing back the raw bytes would be a silent reinterpret.
        with pytest.raises(ProtocolError, match="unknown payload kind 7"):
            decode(encode(1, 7, b"payload"))

    def test_bytes_marked_text_that_are_not_utf8_are_a_protocol_error(self):
        with pytest.raises(ProtocolError, match="not valid UTF-8"):
            decode(encode(1, TEXT, b"\xff\xfe"))


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
        # A broker serving hundreds of streams: the name list is unbounded and
        # everything else in the refusal is not.
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
        # A proxy or a load balancer closing the connection with its own
        # reason. The caller is already handling a closed connection; losing
        # the detail is fine, raising inside the handler is not.
        assert parse_refusal("connection reset by peer") == ("", {})
        assert parse_refusal("") == ("", {})
        assert parse_refusal("[1, 2, 3]") == ("", {})

    def test_the_close_codes_echo_their_http_cousins(self):
        # Not decoration: a `4416` in an operator's log should be readable as
        # "range not satisfiable" without this repo open.
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
            ("/", ("", None)),
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
        ("name", "offset"), [("", None), ("trades", None), ("trades", 1200), ("t", 0)]
    )
    def test_the_builder_and_the_parser_are_inverses(self, name, offset):
        # They live in one module so they cannot drift; this is the assertion
        # that says so.
        assert parse_subscribe(subscribe_path(name, offset)) == (name, offset)


def test_the_greeting_is_json_an_unrelated_client_can_read():
    # The affordance the protocol is shaped around: `wscat ws://broker/trades`
    # is a working subscriber, and the first thing it prints has to be legible.
    fields = json.loads(
        greeting(stream="trades", end_offset=7, replay=None, durable=True)
    )
    assert fields["streamcast"] == 1
    assert fields["stream"] == "trades"
