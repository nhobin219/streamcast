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

from streamcast import _stream, _subscriber


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
    """A replay is DuckDB reading Parquet: milliseconds to seconds. On the
    event loop that is the whole broker stopped — no live message fanned out,
    no other subscriber served, no keepalive answered."""
    from streamcast import _log

    source = inspect.getsource(_log.replay)
    assert source.count("asyncio.to_thread") == 2, (
        "both the scan and each batch read must cross into a thread"
    )
