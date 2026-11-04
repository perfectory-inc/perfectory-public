"""The HUB 건축인허가 대지위치 lane keys each site row by its provider key and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_site_location"
SOURCE = SPARK.parent / "contracts/hub-building-permit-site-location-source-objects.json"


class PermitSiteLocationContractTest(unittest.TestCase):
    def test_measured_site_location_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 13,639,544 rows of the 2026-10 edition has 20 columns and a
        # distinct 관리대지위치관리번호 in column 0, so that column is the key. 법정동 is column 9, not the
        # 행정동 column 8: a sample composed from column 8 matches no parcel.
        self.assertEqual((source["column_count"], source["inner_file"], source.get("row_key")),
                         (20, "mart_kcy_16.txt", None))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "permit_mgmt_key": 1, "building_mgmt_key": 2, "representative_flag": 3,
            "sigungu_cd": 5, "main_building_code": 6, "main_building_name": 7, "admin_dong_cd": 8,
            "bjdong_cd": 9, "san_gubun": 10, "bonbeon": 11, "bubeon": 12, "land_category_code": 16,
            "land_category_name": 17,
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

    def test_site_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
