"""The HUB 건축물 정기점검이력 lane keeps each row by its content and reaches no parcel by itself (root ADR-0404)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_maintenance_regular_inspection_history"
SOURCE = SPARK.parent / "contracts/hub-building-maintenance-regular-inspection-history-source-objects.json"


class MaintenanceRegularInspectionContractTest(unittest.TestCase):
    def test_measured_regular_inspection_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 64,328 rows of the 2026-10 edition has 11 columns; the
        # 점검실시PK in column 6 repeats over the buildings one inspection covers, so the row is the key.
        self.assertEqual((source["column_count"], source["inner_file"], source["row_key"]),
                         (11, "mart_ll_02.txt", "sha256_of_row"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "sigungu_cd": 0, "bjdong_cd": 1, "bonbeon": 2, "bubeon": 3, "dong_name": 5,
            "inspection_key": 6, "building_register_key": 7, "inspection_agency_name": 8,
            "inspection_start_date": 9, "processed_date": 10,
        })
        # No 대지구분 column, so no PNU: the parcel comes from the titles at read (root ADR-0404).
        self.assertNotIn("san_gubun", source["columns"])
        link = source["parcel_link"]
        self.assertEqual((link["column"], link["register"], link["register_column"]),
                         ("building_register_key", "silver.building_register_titles", "mgm_bldrgst_pk"))
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 0))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "mgmt_key", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]), set(source["columns"]) | {"raw_columns"})
        latest = current_row_latest(contract)
        self.assertEqual((latest.key, latest.keep_ties), (("mgmt_key",), True))
        register = load_lakehouse_contract(link["register"])
        self.assertTrue({"pnu", link["register_column"]} <= {c["name"] for c in register["columns"]})

    def test_inspection_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
