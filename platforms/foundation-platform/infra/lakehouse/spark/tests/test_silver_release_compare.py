"""What `silver_release_compare` refuses before it reads a row (root ADR-0275)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import silver_release_compare as job  # noqa: E402
from platform_contracts import load_lakehouse_contract  # noqa: E402

LANES = Path(__file__).resolve().parents[2] / "contracts"
ARGS = ["--contract", "silver.land_transfer_history", "--branch", "load_0123456789abcdef",
        "--source-snapshot-id", "vworldkr-land-transfer-history:20990901", "--objects", "a.zip",
        "--content-columns", "reason", "--changes-branch", "changes_0123456789abcdef"]


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

    def test_the_changes_are_staged_on_their_own_branch_never_main(self):
        self.assertEqual(job.parse_args(ARGS).changes_branch, "changes_0123456789abcdef")
        for branch in ("main", "load_0123456789abcdef", "x;drop"):
            with self.subTest(branch=branch):
                with self.assertRaises(ValueError):
                    job.parse_args([*ARGS[:-1], branch])


class VersionOrderTest(unittest.TestCase):
    """A compared table orders a key's versions by the edition, then the commit's instant (ADR-0328)."""

    def test_every_compared_lane_orders_its_versions_by_edition_then_instant(self):
        import json

        from platform_contracts import current_row_latest

        editions = {}
        for path in sorted(LANES.glob("*-source-objects.json")):
            block = json.loads(path.read_text(encoding="utf-8")).get("silver_refresh") or {}
            if "compare" in block:
                latest = current_row_latest(load_lakehouse_contract(block["table"]))
                editions[block["table"]] = job.edition_column(latest)
        self.assertEqual(editions, {
            "silver.building_permit_basis_outline": "vintage",
            "silver.building_register_exclusive_unit": "vintage",
            "silver.building_register_master": "vintage",
            "silver.building_register_titles": "valid_from_utc",
            "silver.building_register_unit_areas": "valid_from_utc",
            "silver.building_register_units": "valid_from_utc",
            "silver.land_characteristic": "source_snapshot_id",
            "silver.land_forest_ledger": "source_snapshot_id",
            "silver.land_individual_price": "source_snapshot_id",
            "silver.land_ownership": "source_snapshot_id",
            "silver.land_right_registration": "source_snapshot_id",
            "silver.land_transfer_history": "source_snapshot_id",
            "silver.land_use_plan": "source_snapshot_id",
            "silver.land_use_zone_area": "source_snapshot_id",
            "silver.land_use_zone_code": None,
        })

    def test_an_order_a_release_loaded_again_would_tie_on_is_refused(self):
        # 2026-10-21: land_transfer_history ordered by the release id alone; vintage 20260930 loaded
        # again, and its 1,585 removal markers tied with the rows they removed.
        from platform_contracts import CurrentRowLatest

        for order in (("source_snapshot_id",), ("vintage",), ("ingested_at_utc", "vintage"),
                      ("vintage", "valid_from_utc", "ingested_at_utc")):
            with self.subTest(order=order):
                with self.assertRaisesRegex(ValueError, "edition, ingested_at_utc"):
                    job.edition_column(CurrentRowLatest(key=("k",), newest_first=order, removal_column="row_state"))

    def test_the_edition_is_not_content(self):
        contract = load_lakehouse_contract("silver.building_register_exclusive_unit")
        with self.assertRaisesRegex(ValueError, "content columns"):
            job.compare_rule(contract, ["ho_name", "vintage"])

    def test_a_release_names_its_objects(self):
        # An empty list read as sealed (`all([])`): the run reported "staged" having compared nothing.
        for objects in ("", ",", ",,"):
            with self.subTest(objects=objects):
                with self.assertRaisesRegex(ValueError, "--objects"):
                    job.parse_args([*ARGS[:6], "--objects", objects, *ARGS[8:]])


class BucketTest(unittest.TestCase):
    """The compare's buckets (ADR-0318): disjoint, named the same on every run, resumable."""

    def test_a_deadline_names_its_zone(self):
        self.assertEqual(job.parse_args([*ARGS, "--deadline-utc", "2099-01-01T00:00:00Z"]).deadline_utc.year, 2099)
        with self.assertRaisesRegex(ValueError, "no zone"):
            job.parse_args([*ARGS, "--deadline-utc", "2099-01-01T00:00:00"])
        with self.assertRaises(ValueError):
            job.parse_args([*ARGS, "--bucket-prefix-length", "-1"])

    def test_the_null_bucket_and_the_empty_bucket_are_two_names(self):
        self.assertEqual(job.bucket_label(None), "null")
        self.assertEqual(job.bucket_label(""), '""')
        self.assertEqual(job.bucket_label("41"), '"41"')

    def test_buckets_are_compared_null_first_then_in_order(self):
        self.assertEqual(job.ordered_buckets({"47", None, "11", ""}), [None, "", "11", "47"])

    def test_only_this_releases_buckets_on_the_branch_lineage_are_done(self):
        mine = "vworldkr-land-use-plan:20990901"
        snapshots = [
            {"snapshot_id": 1, "summary": {job.COMPARE_RELEASE_KEY: mine, job.COMPARE_BUCKET_KEY: '"11"'}},
            {"snapshot_id": 2, "summary": {job.COMPARE_RELEASE_KEY: "vworldkr-land-use-plan:20990601",
                                           job.COMPARE_BUCKET_KEY: '"26"'}},
            {"snapshot_id": 3, "summary": {job.COMPARE_RELEASE_KEY: mine, job.COMPARE_BUCKET_KEY: '"41"'}},
            {"snapshot_id": 4, "summary": {job.COMPARE_RELEASE_KEY: mine}},
            {"snapshot_id": 5, "summary": None},
        ]
        # Snapshot 3 is on another lineage (a dropped branch), so its bucket is compared again.
        self.assertEqual(job.recorded_buckets(snapshots, {1, 2, 4, 5}, mine), {'"11"'})


if __name__ == "__main__":
    unittest.main()
