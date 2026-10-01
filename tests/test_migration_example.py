"""`examples/migration/`, run rather than read.

The claim is that a shadow pipeline tests a migration against production's
data, live and historical, without production waiting on it. Each half is
checked: a correct migration diffs empty, a wrong one does not, and
production finishes while the shadow is still behind.
"""

from __future__ import annotations

from examples.migration import demo


class TestAShadowMigration:
    async def test_a_correct_migration_matches_production_row_for_row(self, tmp_path):
        seen = await demo.main(tmp_path, count=20)

        assert seen["positions_diff"] == []
        assert seen["alerts_diff"] == []
        # Not vacuously: there were alerts to compare, and the new column is
        # filled for every order, history included.
        assert seen["alerts"] > 0
        assert len(seen["notional"]) == seen["orders"]
        assert all(value is not None for value in seen["notional"])
        # And production did not wait for it: production had caught up while
        # the deliberately slow shadow was still behind.
        assert seen["shadow_behind"] > 0

    async def test_a_wrong_migration_is_named_order_by_order(self, tmp_path):
        seen = await demo.main(tmp_path, bug=True, count=20)

        # The bug counts sells as buys: every differing position is one that
        # has seen a sell, and the alerts downstream differ with it.
        assert seen["positions_diff"]
        assert seen["alerts_diff"]
        assert all(row["new"] > row["old"] for row in seen["positions_diff"])
