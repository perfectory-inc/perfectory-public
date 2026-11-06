"""The HUB 가설건축물 lane keys each row by its content and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_temporary_building"
SOURCE = SPARK.parent / "contracts/hub-building-permit-temporary-building-source-objects.json"


class PermitTemporaryBuildingContractTest(unittest.TestCase):
    def test_measured_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 2,308,148 rows of the 2026-10 edition has 23 columns; column 0
        # (the permit key) holds only 1,361,608 values, so the key is the row's own SHA-256.
        self.assertEqual((source["column_count"], source["inner_file"], source["row_key"]),
                         (23, "mart_kcy_08.txt", "sha256_of_row"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {"permit_mgmt_key": 0, "sigungu_cd": 3, "bjdong_cd": 4, "san_gubun": 5, "bonbeon": 6, "bubeon": 7, "whole_building_area_m2": 11, "whole_floor_area_m2": 12, "plot_area_m2": 13, "existence_expiry_date": 14, "structure_code": 15, "structure_name": 16, "main_purpose_code": 17, "main_purpose_name": 18, "building_area_m2": 19, "total_floor_area_m2": 20, "ground_floor_count": 21})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 0))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "mgmt_key", "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]), set(source["columns"]) | {"pnu", "raw_columns"})
        latest = current_row_latest(contract)
        self.assertEqual((latest.key, latest.keep_ties), (("mgmt_key",), True))

    def test_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
