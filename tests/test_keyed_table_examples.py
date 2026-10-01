"""`examples/keyed_table/`, run rather than read.

The keyed table log's view and the branches built on it are patterns a reader
copies, so each runs end to end here, with every view's book pinned.
"""

from __future__ import annotations

from examples.keyed_table import branches, orders
from examples.keyed_table.view import View


class TestTheKeyedTable:
    async def test_the_book_is_the_last_row_per_order_without_tombstones(
        self, tmp_path
    ):
        seen = await orders.main(tmp_path)

        assert seen["book"] == [
            {"order_id": 1, "symbol": "AAPL", "side": "buy", "qty": 60, "price": 191.0},
            {"order_id": 4, "symbol": "TSLA", "side": "buy", "qty": 10, "price": 250.0},
        ]
        # The window function over the log is the same question, answered
        # with no state of its own.
        assert seen["from_log"] == seen["book"]

    async def test_a_reopened_view_resumes_from_its_own_offset(self, tmp_path):
        seen = await orders.main(tmp_path)

        # Written in the same transaction as the rows, so what the view read
        # back on reopening is exactly where it had stopped.
        assert seen["resumed_after"] == seen["stopped_at"]


class TestAFork:
    def test_is_its_own_database_at_the_same_offset(self):
        main = View.open()
        main.apply(1, orders.order(1, "AAPL", "buy", 100, 190.0))
        fork = main.fork()

        assert fork.offset == main.offset == 1
        main.apply(2, orders.gone(1))
        fork.apply(3, orders.order(2, "MSFT", "sell", 5, 410.0))

        assert [o["order_id"] for o in main.orders()] == []
        assert [o["order_id"] for o in fork.orders()] == [1, 2]


class TestBranches:
    async def test_each_branch_sees_main_at_its_fork_and_only_its_own_rows(
        self, tmp_path
    ):
        seen = await branches.main(tmp_path)

        assert seen["before"] == {
            "main": [1, 2, 3, 4],
            # Cancelled 2, added 10; forked before 4, and does not track main.
            "alice": [1, 3, 10],
            # Amended 1 in place.
            "bob": [1, 2, 3],
            # Tracks main, so has 4, plus its own test order.
            "carol": [1, 2, 3, 4, 20],
        }
        assert seen["bobs_order"]["qty"] == 150

    async def test_a_commit_is_one_send_many_onto_main(self, tmp_path):
        seen = await branches.main(tmp_path)

        # One transaction: contiguous offsets, all after the fork.
        committed = seen["committed"]
        assert committed == list(range(committed[0], committed[0] + 2))
        assert committed[0] > seen["forked_at"]

        assert seen["after"] == {
            "main": [1, 3, 4, 10],
            "alice": [1, 3, 10],
            # Untouched by alice's commit: bob does not track main.
            "bob": [1, 2, 3],
            # Tracks main, so the commit reaches it too.
            "carol": [1, 3, 4, 10, 20],
        }
