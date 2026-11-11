"""The HUB 주택인허가 기본개요 lane keys each approval by the provider's key and keeps its raw row (root ADR-0511)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.housing_permit_basis_outline"
SOURCE = SPARK.parent / "contracts/hub-housing-permit-basis-outline-source-objects.json"


class HousingPermitBasisOutlineContractTest(unittest.TestCase):
    def test_measured_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-11: every one of 381,382 rows of the 2026-10 edition has 29 columns and a
        # distinct column 0 (관리주택대장PK), so the provider's key is the row key.
        self.assertEqual((source["column_count"], source["inner_file"], source.get("row_key")),
                         (29, "mart_jty_01.txt", None))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "sigungu_cd": 3, "bjdong_cd": 4, "san_gubun": 5, "bonbeon": 6, "bubeon": 7,
            "purpose_code": 11, "purpose_name": 12, "structure_code": 13, "structure_name": 14,
            "main_building_count": 15, "total_floor_area_m2": 16, "total_household_count": 17,
            "approval_date": 23, "start_scheduled_date": 24, "start_actual_date": 25,
            "use_inspection_scheduled_date": 26, "use_inspection_date": 27})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 0))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]), set(source["columns"]) - {"mgmt_key"} | {"pnu", "raw_columns"})
        latest = current_row_latest(contract)
        self.assertEqual((latest.key, latest.keep_ties), (("mgmt_key",), True))

    def test_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
