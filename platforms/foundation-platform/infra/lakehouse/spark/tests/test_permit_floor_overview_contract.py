"""The HUB 건축인허가 층별개요 lane keys each floor by its provider key and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_floor_overview"
SOURCE = SPARK.parent / "contracts/hub-building-permit-floor-overview-source-objects.json"


class PermitFloorOverviewContractTest(unittest.TestCase):
    def test_measured_floor_overview_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 15,041,597 rows of the 2026-10 edition has 24 columns and a
        # distinct 관리층별개요PK in column 0, so that column is the key.
        self.assertEqual((source["column_count"], source["inner_file"], source.get("row_key")),
                         (24, "mart_kcy_03.txt", None))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "building_mgmt_key": 1, "permit_mgmt_key": 2, "sigungu_cd": 5, "bjdong_cd": 6,
            "san_gubun": 7, "bonbeon": 8, "bubeon": 9, "structure_code": 13, "structure_name": 14,
            "main_purpose_code": 15, "main_purpose_name": 16, "floor_no": 17, "floor_area_m2": 18,
            "floor_kind_code": 19, "floor_kind_name": 20, "arch_kind_code": 21, "arch_kind_name": 22,
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

    def test_floor_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
