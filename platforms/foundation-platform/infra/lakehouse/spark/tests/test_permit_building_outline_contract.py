"""The HUB 건축인허가 동별개요 lane keys each building by its provider key and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_building_outline"
SOURCE = SPARK.parent / "contracts/hub-building-permit-building-outline-source-objects.json"


class PermitBuildingOutlineContractTest(unittest.TestCase):
    def test_measured_building_outline_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 8,546,999 rows of the 2026-10 edition has 29 columns and a
        # distinct 관리동별개요PK in column 0, so that column is the key.
        self.assertEqual((source["column_count"], source["inner_file"], source.get("row_key")),
                         (29, "mart_kcy_02.txt", None))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "permit_mgmt_key": 1, "sigungu_cd": 4, "bjdong_cd": 5, "san_gubun": 6, "bonbeon": 7,
            "bubeon": 8, "main_annex_code": 12, "main_annex_name": 13, "main_purpose_code": 14,
            "main_purpose_name": 15, "unit_count": 16, "family_count": 17, "household_count": 18,
            "structure_code": 19, "structure_name": 20, "roof_code": 21, "roof_name": 22, "building_area_m2": 23,
            "total_floor_area_m2": 24, "far_floor_area_m2": 25, "passenger_elevator_count": 27,
            "emergency_elevator_count": 28,
        })
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 2))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]),
                         (set(source["columns"]) - {"mgmt_key"}) | {"pnu", "raw_columns"})
        latest = current_row_latest(contract)
        self.assertEqual((latest.key, latest.keep_ties), (("mgmt_key",), True))

    def test_building_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
