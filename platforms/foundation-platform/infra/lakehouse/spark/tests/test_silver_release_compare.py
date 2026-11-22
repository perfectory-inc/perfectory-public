"""What `silver_release_compare` refuses before it reads a row (root ADR-0275)."""
import json
import os
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


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class ChangedOnlySparkTest(unittest.TestCase):
    """Root ADR-0311: a compared table appends only the keys that are new or whose content changed."""

    def test_only_new_and_changed_keys_are_appended(self) -> None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("changed-only").getOrCreate()
        try:
            schema = "onbid_pbanc_no bigint, pbct_no bigint, org_nm string, rspb_sbr_nm string"
            current = spark.createDataFrame([(1, 1, "a", None), (1, 2, "b", "x"), (2, 1, "c", "")], schema)
            staged = spark.createDataFrame([(1, 1, "a", None), (1, 2, "b", "y"), (2, 1, "c", None), (3, 1, "d", None)],
                                           schema)
            changed = job.changed_only(staged, current, ["onbid_pbanc_no", "pbct_no"], ["org_nm", "rspb_sbr_nm"])
            self.assertEqual(sorted((r.onbid_pbanc_no, r.pbct_no) for r in changed.collect()),
                             [(1, 2), (2, 1), (3, 1)])  # a null where an empty string was is a change
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
