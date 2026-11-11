"""The HUB 오수정화시설 lane keys each row by its content and keeps its raw row (root ADR-0490)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_register_sewage_facility"
SOURCE = SPARK.parent / "contracts/hub-building-register-sewage-facility-source-objects.json"


class RegisterSewageFacilityContractTest(unittest.TestCase):
    def test_measured_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-11: every one of 6,616,909 rows of the 2026-10 edition has 29 columns; column 0
        # (the building key) holds only 6,567,455 values, so the key is the row's own SHA-256.
        self.assertEqual((source["column_count"], source["inner_file"], source["row_key"]),
                         (29, "mart_djy_07.txt", "sha256_of_row"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "building_mgmt_key": 0, "register_kind_code": 3, "register_kind_name": 4, "sigungu_cd": 8,
            "bjdong_cd": 9, "san_gubun": 10, "bonbeon": 11, "bubeon": 12, "treatment_method_code": 21,
            "treatment_method_name": 22, "other_method": 23, "capacity_unit_name": 25, "capacity_persons": 26,
            "capacity_cubic_m": 27, "created_date": 28})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 1))
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
