"""What `silver_release_compare` refuses before it reads a row (root ADR-0275)."""
import json
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
            "silver.building_energy_monthly_electricity": "vintage",
            "silver.building_energy_monthly_gas": "vintage",
            "silver.building_permit_basis_outline": "vintage",
            "silver.building_permit_demolition_loss": "vintage",
            "silver.building_permit_parking": "vintage",
            "silver.building_permit_annex_parking": "vintage",
            "silver.building_permit_building_outline": "vintage",
            "silver.building_permit_major_repair": "vintage",
            "silver.building_permit_structure_ledger": "vintage",
            "silver.building_permit_floor_overview": "vintage",
            "silver.building_permit_temporary_building": "vintage",
            "silver.building_permit_site_location": "vintage",
            "silver.building_maintenance_regular_inspection_history": "vintage",
            "silver.building_register_exclusive_unit": "vintage",
            "silver.building_register_master": "vintage",
            "silver.building_register_sub_parcel": "vintage",
            "silver.building_register_titles": "valid_from_utc",
            "silver.housing_permit_basis_outline": "vintage",
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
            "silver.real_estate_broker_member": "source_snapshot_id",
            "silver.real_estate_broker_office": "source_snapshot_id",
        })

    def test_every_hub_family_lane_orders_its_versions_by_vintage(self):
        # The family lanes (root ADR-0551) are entries of a family file, not lane files; each table is
        # the HUB part lane shape, its versions ordered by the provider month.
        from platform_contracts import current_row_latest

        tables = [f"silver.{lane.replace('-', '_')}"
                  for path in sorted(LANES.glob("hub-lane-family-*.v1.json"))
                  for lane in json.loads(path.read_text(encoding="utf-8"))["lanes"]]
        self.assertTrue(tables)
        for table in tables:
            latest = current_row_latest(load_lakehouse_contract(table))
            self.assertEqual(job.edition_column(latest), "vintage", table)

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

    def test_a_release_scoped_compare_reads_only_the_release_buckets(self):
        # 월별 에너지: a 202606 release says nothing of 202605's keys, so main is not a side.
        staged, main = object(), object()
        self.assertEqual(job.compared_sides("release", staged, main), [staged])
        self.assertEqual(job.compared_sides("table", staged, main), [staged, main])

    def test_a_release_scope_needs_buckets(self):
        base = [*ARGS, "--bucket-scope", "release"]
        with self.assertRaisesRegex(ValueError, "bucket-prefix-length"):
            job.parse_args(base)
        self.assertEqual(job.parse_args([*base, "--bucket-prefix-length", "6"]).bucket_scope, "release")

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


class GeometryToleranceTest(unittest.TestCase):
    """A checksum-only change within the lane's tolerance is no change (`compare.geometry_tolerance`)."""

    def test_the_tolerance_is_geometry_checksum_and_bounded_metres(self):
        self.assertIsNone(job.parse_args(ARGS).geometry_tolerance)
        parsed = job.parse_args([*ARGS, "--geometry-tolerance", "geometry_wkb,geometry_checksum_sha256,0.1"])
        self.assertEqual(parsed.geometry_tolerance, ("geometry_wkb", "geometry_checksum_sha256", 0.1))
        for raw in ("geometry_wkb,geometry_checksum_sha256,0", "geometry_wkb,geometry_checksum_sha256,1.5",
                    "geometry_wkb,geometry_checksum_sha256,-1", "geometry_wkb,0.1", ",geometry_checksum_sha256,0.1"):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    job.parse_args([*ARGS, "--geometry-tolerance", raw])

    def test_the_checksum_is_content_and_the_geometry_is_not(self):
        contract = load_lakehouse_contract("silver.land_use_zone_area")
        content = ["zone_name", "geometry_checksum_sha256"]
        job.check_tolerance(contract, content, ("geometry_wkb", "geometry_checksum_sha256", 0.1))
        for tolerance in (("geometry_wkb", "zone_code", 0.1), ("zone_name", "geometry_checksum_sha256", 0.1),
                          ("no_such_column", "geometry_checksum_sha256", 0.1)):
            with self.subTest(tolerance=tolerance):
                with self.assertRaisesRegex(ValueError, "geometry tolerance"):
                    job.check_tolerance(contract, content, tolerance)

    def test_only_the_zone_lane_tolerates_and_its_contract_parses_here(self):
        import json

        tolerances = {}
        for path in sorted(LANES.glob("*-source-objects.json")):
            block = json.loads(path.read_text(encoding="utf-8")).get("silver_refresh") or {}
            tolerance = (block.get("compare") or {}).get("geometry_tolerance")
            if tolerance:
                tolerances[block["table"]] = tolerance
                job.check_tolerance(load_lakehouse_contract(block["table"]), block["compare"]["content_columns"],
                                    job.geometry_tolerance(f"{tolerance['geometry_column']},"
                                                           f"{tolerance['checksum_column']},{tolerance['metres']}"))
        self.assertEqual(list(tolerances), ["silver.land_use_zone_area"])
        self.assertEqual(tolerances["silver.land_use_zone_area"]["metres"], 0.1)


class PartialMainTest(unittest.TestCase):
    """main 이 판의 일부만 기록하면, 그것이 load branch 의 첫 묶음일 때만 비교한다."""

    def test_only_a_main_the_branch_grew_from_is_compared(self):
        # main 1 <- 2 (the first batch); the branch 1 <- 2 <- 3 <- 4 grew from it.
        self.assertTrue(job.part_of_the_staged_release({1, 2}, {1, 2, 3, 4}))
        # main moved on past the branch's base: its part came from elsewhere.
        self.assertFalse(job.part_of_the_staged_release({1, 2, 5}, {1, 2, 3, 4}))
        self.assertFalse(job.part_of_the_staged_release(set(), {1, 2}))


if __name__ == "__main__":
    unittest.main()
