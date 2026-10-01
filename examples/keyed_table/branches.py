"""Branches: every client its own database, committed with one `send_many`.

    uv run python -m examples.keyed_table.branches

One stream of order events, with a `branch_id` column. `main` is production.
A branch is a client's private database: a copy of main's view at the offset
it had applied (`View.fork`), kept current by a live subscription to its own
rows only, `where={"branch_id": "<branch>"}`. A client writes to its branch
as freely as it likes, and nobody else's view moves.

**A commit is one `send_many`.** The branch's rows are published again with
`branch_id: "main"`, in one call. `send_many` is one transaction, so main's
view receives the whole change as one contiguous run of offsets, or none of
it. There is no merge engine: last row by id wins, in offset order, exactly as
for any other write. A commit overwrites what main changed since the fork for
the same orders; checking that first is the client's choice to make.

**A branch can track main instead.** Add `"main"` to the filter —
`where={"branch_id": ["main", "<branch>"]}` — and the branch follows
production live while keeping its own writes. That is how to try a new system
against live data: a new schema, a new service or a migration reads
everything production does, writes only to its branch, and production never
sees a row of it.

The demo, in order: production takes three orders; alice and bob fork; a
shadow branch, carol, forks tracking main; production takes a fourth order;
each branch writes; alice commits.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import streamcast
from examples.keyed_table.orders import gone, order
from examples.keyed_table.view import ORDER, View

if TYPE_CHECKING:
    from collections.abc import Sequence

SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {**ORDER, "branch_id": {"type": "string"}},
    "required": ["order_id", "deleted", "branch_id"],
}

MAIN = "main"


class Branch:
    """A client's private database: main as of the fork, plus its own rows."""

    def __init__(
        self,
        name: str,
        main: View,
        publication: streamcast.Publication,
        *,
        tracking_main: bool = False,
    ) -> None:
        self.name = name
        self.view = main.fork()
        self.forked_at = self.view.offset
        self.where = {"branch_id": [MAIN, name] if tracking_main else name}
        self._publication = publication
        self._written: list[dict[str, Any]] = []

    async def write(self, rows: Sequence[dict[str, Any]]) -> None:
        """Publish to this branch, and wait until its own view has them."""
        offsets = await self._publication.send_many(
            [{**row, "branch_id": self.name} for row in rows]
        )
        self._written.extend(rows)
        assert offsets[-1] is not None
        await self.view.reached(offsets[-1])

    async def commit(self, main: View) -> list[int | None]:
        """Everything this branch wrote, onto main, in one transaction."""
        offsets = await self._publication.send_many(
            [{**row, "branch_id": MAIN} for row in self._written]
        )
        assert offsets[-1] is not None
        await main.reached(offsets[-1])
        return offsets


def book(view: View) -> list[int]:
    return [row["order_id"] for row in view.orders()]


async def main(root: Path) -> dict[str, Any]:
    stream = streamcast.Stream.new("orders", root=root, schema=SCHEMA)
    server = await streamcast.serve(stream, "127.0.0.1", 0, publish=True)
    uri = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/orders"
    following: list[asyncio.Task[None]] = []

    def follow(view: View, where: dict[str, Any]) -> None:
        following.append(asyncio.create_task(view.follow(uri, where)))

    try:
        async with streamcast.publish(uri) as publication:

            async def production(rows: Sequence[dict[str, Any]]) -> int:
                offsets = await publication.send_many(
                    [{**row, "branch_id": MAIN} for row in rows]
                )
                assert offsets[-1] is not None
                await production_view.reached(offsets[-1])
                return offsets[-1]

            production_view = View.open()
            follow(production_view, {"branch_id": MAIN})
            await production(
                [
                    order(1, "AAPL", "buy", 100, 190.0),
                    order(2, "MSFT", "sell", 50, 410.0),
                    order(3, "NVDA", "buy", 200, 120.0),
                ]
            )

            # Forked from main's view as it stands, each at the same offset.
            alice = Branch("alice", production_view, publication)
            bob = Branch("bob", production_view, publication)
            carol = Branch("carol", production_view, publication, tracking_main=True)
            for branch in (alice, bob, carol):
                follow(branch.view, branch.where)

            # Production moves on. Only the branch tracking main sees it.
            fourth = await production([order(4, "TSLA", "buy", 10, 250.0)])
            await carol.view.reached(fourth)

            await alice.write([gone(2), order(10, "AMZN", "buy", 5, 180.0)])
            await bob.write([order(1, "AAPL", "buy", 150, 189.5)])
            await carol.write([order(20, "TEST", "buy", 1, 1.0)])

            before = {
                "main": book(production_view),
                "alice": book(alice.view),
                "bob": book(bob.view),
                "carol": book(carol.view),
            }

            committed = await alice.commit(production_view)
            # carol tracks main, so the commit reaches her too.
            assert committed[-1] is not None
            await carol.view.reached(committed[-1])
            after = {
                "main": book(production_view),
                "alice": book(alice.view),
                "bob": book(bob.view),
                "carol": book(carol.view),
            }
            forked_at = alice.forked_at
            bobs_order = bob.view.orders()[0]

    finally:
        for task in following:
            task.cancel()

        for task in following:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        server.close()
        await server.wait_closed()

    print(f"three branches forked from main at offset {forked_at}")
    print("open orders, before alice commits:")
    for name, ids in before.items():
        print(f"  {name:<6} {ids}")

    print(f"alice commits {len(committed)} rows at offsets {committed}:")
    for name, ids in after.items():
        print(f"  {name:<6} {ids}")

    return {
        "forked_at": forked_at,
        "before": before,
        "after": after,
        "committed": committed,
        "bobs_order": bobs_order,
    }


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        asyncio.run(main(Path(directory)))
