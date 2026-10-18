"""What `silver_release_compare` refuses before it reads a row (root ADR-0275)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import silver_release_compare as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

ARGS = ["--contract", "silver.land_transfer_history", "--branch", "load_0123456789abcdef",
        "--source-snapshot-id", "vworldkr-land-transfer-history:20990901", "--objects", "a.zip",
        "--content-columns", "reason"]


class CompareRuleTest(unittest.TestCase):
    def test_the_land_transfer_history_rule_is_its_key_and_removal_column(self):
        contract = load_lakehouse_contract("silver.land_transfer_history")
        self.assertEqual(job.compare_rule(contract, ["reason", "moved_at"]),
                         (("pnu", "transfer_history_seq"), "row_state"))

    def test_content_must_be_source_columns_outside_the_key(self):
        contract = load_lakehouse_contract("silver.land_transfer_history")
        for content in ([], ["no_such_column"], ["ingested_at_utc"], ["source_snapshot_id"], ["row_state"]):
            with self.subTest(content=content):
                with self.assertRaisesRegex(ValueError, "content columns"):
                    job.compare_rule(contract, content)

    def test_a_table_without_a_removal_column_cannot_append_changes(self):
        with self.assertRaisesRegex(ValueError, "removal_column"):
            job.compare_rule(load_lakehouse_contract("silver.industrial_complexes"), ["sido_code"])

    def test_the_release_is_read_from_a_load_branch_never_main(self):
        self.assertEqual(job.parse_args(ARGS).branch, "load_0123456789abcdef")
        for branch in ("main", "Load-1", "x;drop"):
            with self.subTest(branch=branch):
                with self.assertRaises(ValueError):
                    job.parse_args([*ARGS[:2], "--branch", branch, *ARGS[4:]])


if __name__ == "__main__":
    unittest.main()
