"""The HUB 건축인허가 주차장 lane keys each permit by 관리허가대장PK and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_parking"
SOURCE = SPARK.parent / "contracts/hub-building-permit-parking-source-objects.json"


class PermitParkingContractTest(unittest.TestCase):
    def test_measured_parking_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 2,835,072 rows of the 2026-10 edition has 24 columns and a
        # filled, distinct column 0, so the key is the provider's 관리허가대장PK.
        self.assertEqual((source["column_count"], source["inner_file"]), (24, "mart_kcy_10.txt"))
        self.assertNotIn("row_key", source)
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "sigungu_cd": 2, "bjdong_cd": 3, "san_gubun": 4, "bonbeon": 5, "bubeon": 6,
            "indoor_self_parking_count": 10, "indoor_self_parking_area_m2": 11,
            "outdoor_self_parking_count": 12, "outdoor_self_parking_area_m2": 13,
            "indoor_mechanical_parking_count": 14, "indoor_mechanical_parking_area_m2": 15,
        })
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["compare"]["bucket_prefix_length"]), (TABLE, 2))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]),
                         set(source["columns"]) - {"mgmt_key"} | {"pnu", "raw_columns"})
        latest = current_row_latest(contract)
        self.assertEqual(latest.key, ("mgmt_key",))

    def test_parking_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
