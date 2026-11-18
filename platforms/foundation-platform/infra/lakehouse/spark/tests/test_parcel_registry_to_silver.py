import os
import sys
import unittest
from unittest import mock
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from parcel_registry_to_silver import missing_regions, mode, parse_args, validate_args  # noqa: E402


class ModeTest(unittest.TestCase):
    # The mode is what the registry as of the snapshot holds (root ADR-0490 §4a), not a share of new
    # parcels: an edition that changes under 0.1% of a region's parcels was taken for a re-run before.
    def test_the_first_registration_needs_the_explicit_flag(self):
        with self.assertRaisesRegex(ValueError, "--bootstrap"):
            mode(False, False, False, None)
        self.assertEqual(mode(False, False, True, None), "bootstrap")

    def test_a_snapshot_no_row_registers_advances_from_its_lineage_and_a_registered_one_upgrades(self):
        self.assertEqual(mode(True, False, False, "a"), "advance")
        with self.assertRaisesRegex(ValueError, "--from-snapshot-id"):
            mode(True, False, False, None)
        self.assertEqual(mode(True, True, False, "a"), "upgrade")
        self.assertEqual(mode(True, True, False, None), "upgrade")

    def test_real_tables_need_the_flag_and_advance_needs_the_lineage_snapshot(self):
        args = parse_args(["--to-snapshot-id", "b", "--allow-non-served-edition", "--to-date", "2099-09-01", "--to-sido", "99"])
        with self.assertRaisesRegex(ValueError, "--from-snapshot-id"):
            validate_args(args)

    def test_an_advance_probe_takes_only_the_snapshot(self):
        base = ["--to-snapshot-id", "b", "--allow-non-served-edition", "--probe-advance"]
        for extra in (["--to-sido", "99"], ["--to-date", "2099-09-01"], ["--bootstrap"], ["--from-snapshot-id", "a"],
                      ["--probe-missing"]):
            with self.subTest(extra=extra), self.assertRaisesRegex(ValueError, "--probe-advance takes|two questions"):
                validate_args(parse_args(base + extra))
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(ValueError, "lakehouse catalog"):
            validate_args(parse_args(base))


class FirstRegistration(unittest.TestCase):
    # The scheduled job registers a region at a time within its time limit (root ADR-0243).
    def test_only_regions_with_nothing_registered_are_missing_and_the_smallest_come_first(self):
        counts = {"97": 300, "98": 10, "99": 20}
        self.assertEqual(missing_regions(counts, {"97"}), [("98", 10), ("99", 20)])
        self.assertEqual(missing_regions(counts, set(counts)), [])

    def test_a_probe_takes_no_region_date_or_lineage(self):
        base = ["--to-snapshot-id", "b", "--allow-non-served-edition", "--probe-missing"]
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(ValueError, "lakehouse catalog"):
            validate_args(parse_args(base))
        for extra in (["--to-sido", "99"], ["--to-date", "2099-09-01"], ["--bootstrap"], ["--from-snapshot-id", "a"]):
            with self.subTest(extra=extra), self.assertRaisesRegex(ValueError, "--probe-missing takes only"):
                validate_args(parse_args(base + extra))

    def test_a_registration_still_needs_its_region_and_date(self):
        with self.assertRaisesRegex(ValueError, "--to-date and --to-sido are required"):
            validate_args(parse_args(["--to-snapshot-id", "b", "--allow-non-served-edition", "--bootstrap"]))


if __name__ == "__main__":
    unittest.main()
