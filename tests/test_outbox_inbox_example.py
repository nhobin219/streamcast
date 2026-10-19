"""`examples/outbox_inbox/`, run rather than read.

The claim is exactly-once end to end across two crashes: a producer that dies
with orders committed and unshipped, and an enricher that dies in the middle
of its transaction. Every order must reach the enriched stream's log once, the
consumer once, and the enricher's totals must be the orders summed.
"""

from __future__ import annotations

from examples.outbox_inbox import demo


class TestTheExample:
    async def test_every_order_arrives_exactly_once_across_both_crashes(self, tmp_path):
        seen = await demo.main(tmp_path, count=20, crash_at=7)

        # Not vacuously: both crashes happened.
        assert seen["stranded"] == 10, "the producer died with rows unshipped"
        assert seen["crashed"], "the enricher died mid-transaction"
        assert sorted(seen["in_log"]) == list(range(20))
        assert len(seen["in_log"]) == 20, "no order enriched twice"
        assert seen["seen"] == list(range(20))
        assert seen["totals"] == seen["expected_totals"]

    async def test_it_runs_without_the_enricher_crash(self, tmp_path):
        seen = await demo.main(tmp_path, count=6, crash_at=None)

        assert not seen["crashed"]
        assert sorted(seen["in_log"]) == list(range(6))
        assert seen["totals"] == seen["expected_totals"]
