"""The two atomicity claims, checked against the source rather than believed.

`_stream` opens by saying that `send` contains no `await`, and that neither
does the pair of statements that attaches a subscriber. Both are correctness
arguments — the first is why two concurrent senders cannot deliver offset 8
before offset 7, the second is why a resume is exactly-once — and **both fail
silently if broken.** Adding an `await` inside either compiles, passes every
other test in this suite, and produces a reordering or a duplicate only under
a race nobody will reproduce on purpose.

So they are read out of the AST. A test that says "this function has no await
in it" looks like a strange thing to write until you have chased the bug it
prevents.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from streamcast import _log, _stream, _subscriber


def function(module, *path: str) -> ast.AsyncFunctionDef | ast.FunctionDef:
    """The definition named by `path`, e.g. ("Stream", "send")."""
    source = inspect.getsourcefile(module)
    assert source is not None, f"{module} has no source file"
    tree = ast.parse(Path(source).read_text())
    node: ast.AST = tree
    for name in path:
        found = [
            child
            for child in ast.iter_child_nodes(node)
            if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and child.name == name
        ]
        assert len(found) == 1, f"{name} is not unique in {path}"
        node = found[0]

    return node  # ty: ignore[invalid-return-type]


def awaits(node: ast.AST) -> list[int]:
    """Line numbers of every `await` under `node`, excluding nested defs."""
    lines = []
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        if isinstance(child, ast.Await):
            lines.append(child.lineno)

        lines.extend(awaits(child))

    return lines


@pytest.mark.parametrize("name", ["send", "send_many", "_fan_out"])
def test_publishing_never_awaits(name):
    """The ordering guarantee.

    The offset is assigned, the row is made durable and the frame is offered
    to every subscriber with nothing able to interleave. An `await` anywhere
    in here lets a second sender run between the assignment and the fan-out,
    and a subscriber then receives offsets out of order.
    """
    found = awaits(function(_stream, "Stream", name))
    assert found == [], (
        f"Stream.{name} awaits at line(s) {found} — see the module docstring"
    )


def test_attaching_reads_the_frontier_with_nothing_in_between():
    """The replay partition.

    The subscriber joins the fan-out set and the frontier is read as one
    step. Anything between them — an await, or even another statement that
    could grow to contain one — is a message that is in neither the replay
    range nor the queue, or in both.
    """
    body = function(_stream, "Stream", "serve_subscriber").body
    joins = [
        index
        for index, statement in enumerate(body)
        if "_subscribers.add" in ast.unparse(statement)
    ]
    assert len(joins) == 1, "the fan-out set is joined in more than one place"

    following = body[joins[0] + 1]
    assert ast.unparse(following) == "frontier = self._end_offset", (
        "the statement after joining the fan-out set must be the frontier read; "
        f"it is {ast.unparse(following)!r}"
    )


def test_offering_a_frame_never_awaits_and_never_raises_queue_full():
    """`Stream.send` depends on all of it: `offer` is called in a loop over
    every subscriber, and one that blocked or raised would break the loop
    partway — leaving some subscribers with the message and some without."""
    assert awaits(function(_subscriber, "Subscriber", "offer")) == []

    source = ast.unparse(function(_subscriber, "Subscriber", "offer"))
    # The overflow path is a size check, not a caught exception: catching
    # `QueueFull` would mean the queue had already refused the put, and the
    # reserved sentinel slot is what makes that unnecessary.
    assert "QueueFull" not in source
    assert "qsize" in source


def test_the_overflow_slot_is_reserved_rather_than_taken_from_a_message():
    """The queue is one deeper than `max_backlog` so the sentinel has a home.

    The first version evicted the oldest queued frame to make room, which
    punches a hole in the middle of what the subscriber receives — and a hole
    it cannot see, because the offsets on either side are still increasing.
    """
    source = inspect.getsource(_subscriber.Subscriber.__init__)
    assert "maxsize=max_backlog + 1" in source


def test_a_replay_is_read_in_a_thread():
    """A replay is DuckDB reading Parquet: 2.11 us/row warm and ~0.5 s cold
    for the first scan in a process. On the event loop that is the whole
    server stopped — no live message fanned out, no other subscriber served,
    no keepalive answered."""
    # `rows`, not `replay`: the batch reader was split out so the CLIENT can
    # reuse it for a catch-up without an encode-then-decode round trip, and
    # the thread hops went with the reader.
    source = inspect.getsource(_log.rows)
    assert source.count("asyncio.to_thread") == 2, (
        "both the scan and each batch read must cross into a thread"
    )
    assert "rows(" in inspect.getsource(_log.replay), "replay delegates to rows"


def test_the_wire_key_order_comes_from_one_place():
    """A replayed message must encode to the same bytes as the live one.

    The live path projects the caller's dict through `Stream._columns`; the
    replay path gets its order from the scan's projection and pops the offset
    off the front. Both must resolve to the log's declared column order.
    """
    from streamcast import _protocol

    encode_src = inspect.getsource(_protocol.encode)
    # The frame is a PAIR, so the offset is never a key in the message.
    assert "_ENCODER.encode((offset, message))" in encode_src
    assert "{name: row.get(name) for name in columns}" in encode_src

    stream_src = inspect.getsource(_stream.Stream.__init__)
    assert "_log.columns(log)" in stream_src

    replay_src = inspect.getsource(_log.rows)
    assert "names = (COLUMN, *declared)" in replay_src
    # The order check is what makes popping the front sound.
    assert "tuple(batch.schema.names) != names" in replay_src


def test_the_offset_is_never_a_key_in_the_message():
    """Non-negotiable: the server sends `offset, msg`, and `msg` is the row.

    Checked against the source as well as behaviour, because a future
    convenience — "let us put the offset back in so subscribers can store one
    object" — would be an easy change to make and a silent contract break for
    every consumer that iterates the message's keys.
    """
    from streamcast import _log as log_module
    from streamcast import _protocol

    for source in (
        inspect.getsource(_protocol.encode),
        inspect.getsource(_protocol.encode_projected),
    ):
        assert "OFFSET" not in source, source

    # And the reader pops litelink's column rather than passing it through.
    assert "message.pop(COLUMN)" in inspect.getsource(log_module.rows)


# Anything here reaches the world: it opens a file, hits SQLite or S3, spawns
# a process, or builds a litelink handle. An `__init__` that calls one is
# building a collaborator rather than receiving one.
_REACHES_THE_WORLD = frozenset(
    {
        "new",
        "open",
        "restore",
        "snapshot",
        "load",
        "read_text",
        "write_replication_config",
        "which",
        "Popen",
        "_open_or_create",
    }
)

# `Stream.__init__` reads the offset counter off the log it was handed. That
# is a read on an INJECTED collaborator, not the construction of one — a fake
# log with an `end_offset()` substitutes cleanly, which is the property the
# rule exists to protect. Written down here rather than left as a silent gap.
_ALLOWED = {("Stream", "end_offset")}


def test_no_initialiser_builds_its_own_collaborators():
    """litelink's rule, applied here: `__init__` takes what it is given.

    *"The initialiser takes already built collaborators and does no I/O, so a
    test can substitute any of them."* Three classes here were breaking it —
    `Stream` called `litelink.new`/`open`, `connect` read a cursor file and
    fetched one from S3, and `Sidecar` wrote a config file and probed PATH —
    so construction touched the disk and, in one case, the network before
    anything had been awaited.

    Each of those moved to a factory (`Stream.new`, `Sidecar.new`,
    `connect._resolve`). This is the audit that found them, kept as a test,
    because the pull back the other way is constant: doing the work in
    `__init__` always looks like one less call at the call site.
    """
    offenders = []
    for path in sorted(Path("src/streamcast").glob("*.py")):
        tree = ast.parse(path.read_text())
        for cls in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
            for fn in cls.body:
                if not isinstance(fn, ast.FunctionDef) or fn.name != "__init__":
                    continue

                for call in (n for n in ast.walk(fn) if isinstance(n, ast.Call)):
                    name = (
                        getattr(call.func, "attr", None)
                        or getattr(call.func, "id", None)
                        or ""
                    )
                    if name in _REACHES_THE_WORLD and (cls.name, name) not in _ALLOWED:
                        offenders.append(
                            f"{path.name}:{call.lineno} {cls.name} -> {name}()"
                        )

    assert not offenders, (
        "an initialiser builds a collaborator or does I/O; move it to a "
        f"factory (see `Stream.new`): {offenders}"
    )
