import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_attribute_carry as carry  # noqa: E402
from parcel_lineage import Link  # noqa: E402

OLD, MID, NEW = "9999910100", "9999920100", "9999930100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


def link(old, new, relation="code_change", grade="code_derived"):
    return Link(old, new, relation, grade, "t")


class CarryCandidatesTest(unittest.TestCase):
    def test_a_renumbered_parcel_takes_its_old_numbers_value(self):
        candidates, _ = carry.carry_candidates([link(pnu(OLD, 1), pnu(NEW, 1))])
        result = carry.attach({}, {pnu(OLD, 1): 52000}, candidates)
        self.assertEqual(result, {pnu(NEW, 1): (52000, "lineage:code_derived")})

    def test_its_own_value_wins(self):
        candidates, _ = carry.carry_candidates([link(pnu(OLD, 1), pnu(NEW, 1))])
        result = carry.attach({pnu(NEW, 1): 61000}, {pnu(OLD, 1): 52000, pnu(NEW, 1): 61000}, candidates)
        self.assertEqual(result[pnu(NEW, 1)], (61000, "same_pnu"))

    def test_a_chain_takes_the_nearest_holder_and_the_weakest_grade(self):
        links = [
            link(pnu(OLD, 1), pnu(MID, 1), grade="official"),
            link(pnu(MID, 1), pnu(NEW, 1), relation="jurisdiction_transfer", grade="evidence_strong"),
        ]
        candidates, _ = carry.carry_candidates(links)
        self.assertEqual(carry.attach({}, {pnu(OLD, 1): 1}, candidates)[pnu(NEW, 1)], (1, "lineage:evidence_strong"))
        self.assertEqual(carry.attach({}, {pnu(OLD, 1): 1, pnu(MID, 1): 2}, candidates)[pnu(NEW, 1)], (2, "lineage:evidence_strong"))

    def test_a_split_child_carries_the_original_parcels_value_labelled_as_such(self):
        links = [link(pnu(OLD, 1), pnu(NEW, 1), "split", "official"), link(pnu(OLD, 1), pnu(NEW, 2), "split", "official")]
        candidates, _ = carry.carry_candidates(links)
        result = carry.attach({}, {pnu(OLD, 1): 7}, candidates)
        self.assertEqual(result, {pnu(NEW, 1): (7, "origin:split:official"), pnu(NEW, 2): (7, "origin:split:official")})

    def test_a_merge_is_never_crossed(self):
        links = [link(pnu(OLD, 1), pnu(NEW, 1), "merge", "official"), link(pnu(OLD, 2), pnu(NEW, 1), "merge", "official")]
        candidates, stopped = carry.carry_candidates(links)
        self.assertEqual(candidates, [])
        self.assertEqual(stopped["merged"], 1)
        _, stopped = carry.carry_candidates([link(pnu(OLD, 1), pnu(NEW, 1), "merge", "official")])
        self.assertEqual(stopped["merged"], 1, "a merge with one known predecessor is still a merge")

    def test_links_not_in_effect_are_never_crossed(self):
        for grade in ("needs_review", "pending"):
            candidates, stopped = carry.carry_candidates([link(pnu(OLD, 1), pnu(NEW, 1), grade=grade)])
            self.assertEqual((candidates, stopped["not_in_effect"]), ([], 1))

    def test_a_rerun_with_a_better_grade_wins(self):
        links = [link(pnu(OLD, 1), pnu(NEW, 1), grade="needs_review"), link(pnu(OLD, 1), pnu(NEW, 1), grade="official")]
        candidates, _ = carry.carry_candidates(links)
        self.assertEqual([c.path for c in candidates], ["lineage:official"])

    def test_a_cycle_stops(self):
        _, stopped = carry.carry_candidates([link(pnu(OLD, 1), pnu(NEW, 1)), link(pnu(NEW, 1), pnu(OLD, 1))])
        self.assertEqual(stopped["cycle"], 2)


if __name__ == "__main__":
    unittest.main()
