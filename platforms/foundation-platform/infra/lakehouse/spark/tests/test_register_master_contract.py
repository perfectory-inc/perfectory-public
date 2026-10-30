"""The HUB 총괄표제부 lane keeps each site's totals and its raw row (root ADR-0357)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_register_master"


class RegisterMasterContractTest(unittest.TestCase):
    def test_measured_master_layout(self):
        source = json.loads((SPARK.parent / "contracts/hub-building-register-master-source-objects.json")
                            .read_text(encoding="utf-8"))
        # Measured 2026-10-30: every one of 621,193 rows of the 2026-10 edition has 64 columns.
        self.assertEqual((source["column_count"], source["inner_file"]), (64, "mart_djy_02.txt"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "sigungu_cd": 10, "bjdong_cd": 11, "san_gubun": 12, "bonbeon": 13, "bubeon": 14,
            "plot_area_m2": 24, "building_area_m2": 25, "total_floor_area_m2": 27, "main_purpose_code": 30,
            "main_purpose_name": 31, "main_building_count": 35, "annex_building_count": 36, "parking_count": 38,
            "permit_date": 47, "start_date": 48, "use_approval_date": 49,
        })
        # Column 9 (building name) is shown nowhere: it stays in raw_columns only.
        self.assertNotIn(9, {column["index"] for column in source["columns"].values()})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["roles"]), (TABLE, {"source": "hubgokr__building_register_master"}))
        contract = load_lakehouse_contract(TABLE)
        columns = {c["name"]: c for c in contract["columns"]}
        self.assertEqual(set(columns), set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]), set(source["columns"]) - {"mgmt_key"} | {"pnu", "raw_columns"})

    def test_master_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
