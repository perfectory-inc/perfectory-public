"""The HUB 건축인허가 기본개요 lane keeps each permit's dates and its raw row (root ADR-0356)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_basis_outline"


class PermitBasisOutlineContractTest(unittest.TestCase):
    def test_measured_permit_layout(self):
        source = json.loads((SPARK.parent / "contracts/hub-building-permit-basis-outline-source-objects.json")
                            .read_text(encoding="utf-8"))
        # Measured 2026-10-30: every one of 6,527,521 rows of the 2026-10 edition has 41 columns.
        self.assertEqual((source["column_count"], source["inner_file"]), (41, "mart_kcy_01.txt"))
        self.assertEqual((source["encoding"], source["csv_delimiter"], source["has_header"]), ("utf-8", "|", False))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "sigungu_cd": 3, "bjdong_cd": 4, "san_gubun": 5, "bonbeon": 6, "bubeon": 7,
            "arch_kind_code": 19, "arch_kind_name": 20, "plot_area_m2": 21, "building_area_m2": 22,
            "total_floor_area_m2": 24, "main_purpose_code": 29, "main_purpose_name": 30,
            "start_scheduled_date": 35, "start_delayed_date": 36, "start_actual_date": 37,
            "permit_date": 38, "use_approval_date": 39,
        })
        # Column 2 (building name) carries people's names on some rows: it stays in raw_columns only.
        self.assertNotIn(2, {column["index"] for column in source["columns"].values()})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["roles"]), (TABLE, {"source": "hubgokr__building_permit_basis_outline"}))
        self.assertLessEqual(source["rows_per_part"] * block["spark"]["input_file_batch_size"], 40_000_000)
        contract = load_lakehouse_contract(TABLE)
        columns = {c["name"]: c for c in contract["columns"]}
        self.assertEqual(set(columns), set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertTrue(all(columns[name]["logical_type"] == "string" for name in source["columns"]))
        # The compare reads every consumed column, the PNU and the raw row, never the load's columns.
        self.assertEqual(set(block["compare"]["content_columns"]), set(source["columns"]) - {"mgmt_key"} | {"pnu", "raw_columns"})
        self.assertEqual(contract["current_row_latest"]["key"], ["mgmt_key"])

    def test_permit_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
