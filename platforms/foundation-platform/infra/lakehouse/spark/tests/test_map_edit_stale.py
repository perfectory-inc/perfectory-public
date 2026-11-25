"""An edit applies only while its target is as it was pinned; anything else is stale (root ADR-0621 §3-§5).

Synthetic ids only: the repository is public.
"""
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import served_gold_common as common  # noqa: E402

P, P1, P2, Q = "99999-p", "99999-p1", "99999-p2", "99999-q"
OLD, NEW = "a" * 64, "b" * 64


def edit(seq, feature_id, op="upsert", *, base=OLD, pinned=True):
    return {
        "change_seq": seq,
        "feature_id": feature_id,
        "op": op,
        "base_source_snapshot_id": "9000000000000000001" if pinned else None,
        "base_geometry_checksum_sha256": base,
    }


class JudgeTest(unittest.TestCase):
    def test_an_edit_over_an_unchanged_target_applies(self):
        applied, stale = common.judge([edit(1, P)], {P: OLD})
        self.assertEqual(([row["change_seq"] for row in applied], stale), ([1], {}))

    def test_the_same_id_with_a_redrawn_source_is_stale(self):
        self.assertEqual(common.judge([edit(1, P)], {P: NEW})[1], {1: "source_changed"})

    def test_a_split_target_is_stale_and_the_successors_stay_official(self):
        # The source split P into P1 and P2: the edit on P is not applied and moves to neither.
        applied, stale = common.judge([edit(1, P)], {P1: NEW, P2: NEW})
        self.assertEqual((applied, stale), ([], {1: "source_removed_id"}))

    def test_a_merged_or_deleted_target_is_stale(self):
        self.assertEqual(common.judge([edit(1, P, "delete")], {Q: NEW})[1], {1: "source_removed_id"})

    def test_an_added_feature_applies_until_the_source_has_that_id(self):
        added = edit(1, Q, base=None)
        self.assertEqual(common.judge([added], {P: OLD})[1], {})
        self.assertEqual(common.judge([added], {P: OLD, Q: NEW})[1], {1: "source_gained_id"})

    def test_a_delete_of_an_id_the_source_lacked_is_stale_not_only_counted(self):
        self.assertEqual(common.judge([edit(1, Q, "delete", base=None)], {P: OLD})[1], {1: "absent_at_base"})

    def test_an_edit_whose_pin_could_not_be_read_is_stale(self):
        self.assertEqual(common.judge([edit(1, P, pinned=False)], {P: OLD})[1], {1: "pin_unreadable"})

    def test_a_redraw_over_the_new_source_applies_while_the_old_edit_stays_stale(self):
        applied, stale = common.judge([edit(2, P, base=NEW), edit(1, P, base=OLD)], {P: NEW})
        self.assertEqual(([row["change_seq"] for row in applied], stale), ([2], {1: "source_changed"}))

    def test_every_reason_is_declared(self):
        reasons = set()
        for ledger, current in (
            ([edit(1, P)], {P: NEW}), ([edit(1, P)], {}), ([edit(1, Q, base=None)], {Q: OLD}),
            ([edit(1, Q, "delete", base=None)], {}), ([edit(1, P, pinned=False)], {P: OLD}),
        ):
            reasons.update(common.judge(ledger, current)[1].values())
        self.assertEqual(reasons, set(common.STALE_REASONS))

    def test_judging_the_same_inputs_twice_gives_the_same_outcome(self):
        ledger, current = [edit(3, P), edit(1, Q, base=None), edit(2, P1)], {P: NEW, Q: OLD}
        self.assertEqual(common.judge(ledger, current), common.judge(list(reversed(ledger)), current))


class OutcomeChangesTest(unittest.TestCase):
    def test_an_edit_that_always_applied_writes_no_row(self):
        self.assertEqual(common.outcome_changes({}, [edit(1, P)], {}), [])

    def test_a_row_is_written_only_when_the_outcome_changes(self):
        ledger = [edit(1, P), edit(2, Q)]
        first = common.outcome_changes({}, ledger, {1: "source_changed"})
        self.assertEqual([(row["change_seq"], row["outcome"], row["reason"]) for row in first],
                         [(1, "stale", "source_changed")])
        self.assertEqual(common.outcome_changes({1: "stale"}, ledger, {1: "source_changed"}), [], "still stale")
        back = common.outcome_changes({1: "stale"}, ledger, {})
        self.assertEqual([(row["change_seq"], row["outcome"], row["reason"]) for row in back], [(1, "applied", None)])

    def test_the_counts_name_every_stale_edit_and_the_new_ones(self):
        changes = common.outcome_changes({1: "stale"}, [edit(1, P), edit(2, Q)], {1: "source_changed", 2: "source_changed"})
        self.assertEqual(common.stale_counts({1: "source_changed", 2: "source_changed"}, changes),
                         {"edits_stale": 2, "edits_newly_stale": 1})


class ResolvePinsTest(unittest.TestCase):
    """The pin's lookups are Spark reads; here the one function that does them is replaced."""

    def setUp(self):
        self.asked = []
        self.saved = common._pin_of

        def pin_of(spark, served_table, silver_table, gold, base_checksums):
            self.asked.append(gold)
            return None if gold == "9000000000000000009" else ("8000000000000000001", {P: OLD})

        common._pin_of = pin_of

    def tearDown(self):
        common._pin_of = self.saved

    def test_each_release_snapshot_is_resolved_once_and_an_unknown_one_leaves_the_pin_empty(self):
        fresh = [
            {"change_seq": 1, "feature_id": P, "base_served_snapshot_id": "9000000000000000001"},
            {"change_seq": 2, "feature_id": Q, "base_served_snapshot_id": "9000000000000000001"},
            {"change_seq": 3, "feature_id": P, "base_served_snapshot_id": "9000000000000000009"},
            {"change_seq": 4, "feature_id": P, "base_served_snapshot_id": None},
        ]
        pinned = common.resolve_pins(None, served_table="s", silver_table="t", fresh=fresh, base_checksums=dict)
        self.assertEqual(self.asked, ["9000000000000000001", "9000000000000000009"])
        self.assertEqual(
            [(row["base_source_snapshot_id"], row["base_geometry_checksum_sha256"]) for row in pinned],
            [("8000000000000000001", OLD), ("8000000000000000001", None), (None, None), (None, None)],
        )


if __name__ == "__main__":
    unittest.main()
