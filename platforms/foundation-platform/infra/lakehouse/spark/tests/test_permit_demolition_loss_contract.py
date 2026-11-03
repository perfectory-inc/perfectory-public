"""The HUB 철거멸실관리대장 lane keys each row by its content and keeps its raw row (root ADR-0397)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_demolition_loss"
SOURCE = SPARK.parent / "contracts/hub-building-permit-demolition-loss-source-objects.json"


class PermitDemolitionLossContractTest(unittest.TestCase):
    def test_measured_demolition_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 462,774 rows of the 2026-10 edition has 34 columns and an
        # empty column 0, so the key is the row's own SHA-256.
        self.assertEqual((source["column_count"], source["inner_file"], source["row_key"]),
                         (34, "mart_kcy_07.txt", "sha256_of_row"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "sigungu_cd": 3, "bjdong_cd": 4, "san_gubun": 5, "bonbeon": 6, "bubeon": 7,
            "demolition_kind_code": 11, "demolition_kind_name": 12, "demolition_start_date": 13, "demolition_end_date": 14,
            "demolition_loss_date": 15, "total_floor_area_m2": 16, "building_count": 17, "main_purpose_code": 18,
            "main_purpose_name": 19, "structure_code": 20, "structure_name": 21,
        })
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

    def test_demolition_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
