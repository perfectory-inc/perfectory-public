import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_identity as pi  # noqa: E402
from parcel_lineage import Link  # noqa: E402

# Synthetic PNUs in the repository-reserved 99999 range.
OLD, NEW = "9999910100", "9999920100"


def pnu(dong, main, sub=0):
    return f"{dong}1{main:04d}{sub:04d}"


def link(pred, succ, relation, grade):
    return Link(pred, succ, relation, grade, "test")


class ParcelIdTest(unittest.TestCase):
    def test_ids_are_deterministic_and_depend_on_the_first_seen_date(self):
        self.assertEqual(pi.parcel_id_for(pnu(OLD, 1), "2099-06-01"), pi.parcel_id_for(pnu(OLD, 1), "2099-06-01"))
        self.assertNotEqual(pi.parcel_id_for(pnu(OLD, 1), "2099-06-01"), pi.parcel_id_for(pnu(OLD, 1), "2100-06-01"))

    def test_bootstrap_gives_every_parcel_one_current_id(self):
        rows = pi.bootstrap([pnu(OLD, 1), pnu(OLD, 2), pnu(OLD, 1)], "2099-06-01")
        self.assertEqual(len(rows), 2)
        self.assertEqual({r.status for r in rows}, {pi.CURRENT})


class AdvanceTest(unittest.TestCase):
    def setUp(self):
        self.boot = {r.pnu: r.parcel_id for r in pi.bootstrap([pnu(OLD, n) for n in range(1, 7)], "2099-06-01")}

    def test_a_renumbered_parcel_keeps_its_id_and_a_merge_or_weak_link_gets_a_new_one(self):
        after = [pnu(NEW, 1), pnu(NEW, 2), pnu(NEW, 3), pnu(NEW, 50), pnu(NEW, 51)]
        effective = {
            pnu(NEW, 1): link(pnu(OLD, 1), pnu(NEW, 1), "code_change", "code_derived"),
            pnu(NEW, 2): link(pnu(OLD, 2), pnu(NEW, 2), "jurisdiction_transfer", "evidence_strong"),
            pnu(NEW, 3): link(pnu(OLD, 3), pnu(NEW, 3), "jurisdiction_transfer", "evidence_weak"),
            pnu(NEW, 50): link(pnu(OLD, 4), pnu(NEW, 50), "merge", "official"),
        }
        t = pi.advance(self.boot, after, effective, "2099-09-01")
        current = {r.pnu: r.parcel_id for r in t.rows if r.status == pi.CURRENT}
        self.assertEqual(current[pnu(NEW, 1)], self.boot[pnu(OLD, 1)], "same land under a new code keeps its id")
        self.assertEqual(current[pnu(NEW, 2)], self.boot[pnu(OLD, 2)], "evidence_strong carries")
        self.assertNotEqual(current[pnu(NEW, 3)], self.boot[pnu(OLD, 3)], "evidence_weak does not carry")
        self.assertNotIn(current[pnu(NEW, 50)], self.boot.values(), "a merge makes a new parcel")
        historic = {r.pnu for r in t.rows if r.status == pi.HISTORIC}
        self.assertEqual(historic, {pnu(OLD, n) for n in range(1, 7)}, "nothing is deleted; every old number closes")
        self.assertEqual((t.carried, t.issued, t.retired), (2, 3, 4))

    def test_a_later_stronger_link_redirects_the_id_issued_before_it(self):
        after = [pnu(NEW, 3)]
        weak = {pnu(NEW, 3): link(pnu(OLD, 3), pnu(NEW, 3), "jurisdiction_transfer", "evidence_weak")}
        first = pi.advance(self.boot, after, weak, "2099-09-01")
        fresh = next(r.parcel_id for r in first.rows if r.status == pi.CURRENT)
        strong = {pnu(NEW, 3): link(pnu(OLD, 3), pnu(NEW, 3), "jurisdiction_transfer", "official")}
        second = pi.advance(self.boot, after, strong, "2099-10-01", {pnu(NEW, 3): fresh})
        redirect = [r for r in second.rows if r.status == pi.REDIRECTED]
        self.assertEqual(len(redirect), 1)
        self.assertEqual((redirect[0].parcel_id, redirect[0].redirect_to), (fresh, self.boot[pnu(OLD, 3)]))
        self.assertEqual(second.issued, 0, "ids are never issued twice for the same parcel")

    def test_a_one_to_many_identity_claim_does_not_carry(self):
        after = [pnu(NEW, 1), pnu(NEW, 9)]
        effective = {
            pnu(NEW, 1): link(pnu(OLD, 1), pnu(NEW, 1), "jurisdiction_transfer", "evidence_strong"),
            pnu(NEW, 9): link(pnu(OLD, 1), pnu(NEW, 9), "jurisdiction_transfer", "evidence_strong"),
        }
        t = pi.advance(self.boot, after, effective, "2099-09-01")
        self.assertEqual(t.carried, 0)

    def test_folding_the_appended_rows_gives_the_registry_now_and_an_upgrade_redirects(self):
        boot = pi.bootstrap([pnu(OLD, 3), pnu(OLD, 4)], "2099-06-01")
        weak = {pnu(NEW, 3): link(pnu(OLD, 3), pnu(NEW, 3), "jurisdiction_transfer", "evidence_weak")}
        step = pi.advance({r.pnu: r.parcel_id for r in boot}, [pnu(NEW, 3), pnu(OLD, 4)], weak, "2099-09-01")
        state = pi.fold(boot + step.rows)
        original = next(r.parcel_id for r in boot if r.pnu == pnu(OLD, 3))
        self.assertEqual(set(state.current), {pnu(NEW, 3), pnu(OLD, 4)})
        self.assertNotEqual(state.current[pnu(NEW, 3)], original)
        self.assertEqual(state.last_id_of_closed[pnu(OLD, 3)], original)

        official = {pnu(NEW, 3): link(pnu(OLD, 3), pnu(NEW, 3), "jurisdiction_transfer", "official")}
        up = pi.upgrade(state, official, "2099-10-01")
        after = pi.fold(boot + step.rows + up.rows)
        self.assertEqual(after.current[pnu(NEW, 3)], original, "the land gets its original id back")
        self.assertEqual(up.redirected, 1)
        self.assertEqual(pi.upgrade(after, official, "2099-11-01").rows, [], "a second run changes nothing")

    def test_the_gate_finds_a_parcel_without_a_current_id(self):
        problems = pi.check_every_parcel_has_one_current_id({pnu(NEW, 1): "a"}, [pnu(NEW, 1), pnu(NEW, 2)])
        self.assertEqual(problems, [f"no current id: {pnu(NEW, 2)}"])
        self.assertEqual(pi.check_every_parcel_has_one_current_id({pnu(NEW, 1): "a", pnu(NEW, 2): "a"}, [pnu(NEW, 1), pnu(NEW, 2)]), ["id on two current PNUs: a"])


if __name__ == "__main__":
    unittest.main()
