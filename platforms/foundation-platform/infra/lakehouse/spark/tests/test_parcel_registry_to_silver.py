import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_identity as pi  # noqa: E402
from parcel_lineage import Link  # noqa: E402
from parcel_registry_to_silver import parse_args, plan, validate_args  # noqa: E402

OLD, NEW = "9999910100", "9999920100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


class PlanTest(unittest.TestCase):
    def test_the_first_registration_needs_the_explicit_flag(self):
        with self.assertRaisesRegex(ValueError, "--bootstrap"):
            plan([], {pnu(OLD, 1)}, [], "2099-06-01", False)
        mode, rows, summary = plan([], {pnu(OLD, 1), pnu(OLD, 2)}, [], "2099-06-01", True)
        self.assertEqual((mode, len(rows), summary["current_ids"]), ("bootstrap", 2, 2))

    def test_a_new_snapshot_advances_and_a_rerun_with_a_stronger_lineage_upgrades(self):
        registry = pi.bootstrap([pnu(OLD, n) for n in range(1, 4)], "2099-06-01")
        after = {pnu(NEW, n) for n in range(1, 4)}
        weak = [Link(pnu(OLD, n), pnu(NEW, n), "jurisdiction_transfer", "evidence_weak", "t") for n in range(1, 4)]
        mode, rows, summary = plan(registry, after, weak, "2099-09-01", False)
        self.assertEqual(mode, "advance")
        self.assertEqual((summary["carried"], summary["issued"]), (0, 3))
        strong = [Link(pnu(OLD, 1), pnu(NEW, 1), "jurisdiction_transfer", "official", "t")] + weak[1:]
        mode, up, summary = plan(registry + rows, after, strong, "2099-10-01", False)
        self.assertEqual((mode, summary["redirected"]), ("upgrade", 1))

    def test_identity_conflicts_block_registration(self):
        registry = pi.bootstrap([pnu(OLD, 1), pnu(OLD, 2)], "2099-06-01")
        conflicting = [
            Link(pnu(OLD, 1), pnu(NEW, 1), "jurisdiction_transfer", "official", "t"),
            Link(pnu(OLD, 2), pnu(NEW, 1), "jurisdiction_transfer", "evidence_strong", "t"),
        ]
        with self.assertRaisesRegex(ValueError, "identity conflicts"):
            plan(registry, {pnu(NEW, 1)}, conflicting, "2099-09-01", False)

    def test_real_tables_need_the_flag_and_advance_needs_the_lineage_snapshot(self):
        args = parse_args(["--to-snapshot-id", "b", "--allow-non-served-edition", "--to-date", "2099-09-01", "--to-sido", "99"])
        with self.assertRaisesRegex(ValueError, "--from-snapshot-id"):
            validate_args(args)


if __name__ == "__main__":
    unittest.main()
