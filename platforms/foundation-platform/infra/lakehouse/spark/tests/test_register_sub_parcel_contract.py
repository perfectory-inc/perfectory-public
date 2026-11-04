"""The HUB 부속지번 lane keeps both parcels of each row and its raw row (root ADR-0395)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_register_sub_parcel"
SOURCE = SPARK.parent / "contracts/hub-building-register-sub-parcel-source-objects.json"


class RegisterSubParcelContractTest(unittest.TestCase):
    def test_measured_sub_parcel_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 5,492,042 rows of the 2026-10 edition has 33 columns.
        self.assertEqual((source["column_count"], source["inner_file"]), (33, "mart_djy_05.txt"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {
            "mgmt_key": 0, "register_kind_code": 3, "register_kind_name": 4,
            "sigungu_cd": 8, "bjdong_cd": 9, "san_gubun": 10, "bonbeon": 11, "bubeon": 12,
            "sub_sigungu_cd": 23, "sub_bjdong_cd": 24, "sub_san_gubun": 25, "sub_bonbeon": 26, "sub_bubeon": 27,
        })
        # Column 7 (building name) is shown nowhere: it stays in raw_columns only.
        self.assertNotIn(7, {column["index"] for column in source["columns"].values()})
        # The second parcel is composed from the row's other 지번, like `pnu` from its own.
        self.assertEqual(source["extra_pnus"], {"sub_pnu": {
            "sigungu_cd": "sub_sigungu_cd", "bjdong_cd": "sub_bjdong_cd", "san_gubun": "sub_san_gubun",
            "bonbeon": "sub_bonbeon", "bubeon": "sub_bubeon"}})
        block = source["silver_refresh"]
        self.assertEqual((block["table"], block["roles"]), (TABLE, {"source": "hubgokr__building_register_sub_parcel"}))
        contract = load_lakehouse_contract(TABLE)
        self.assertEqual({c["name"] for c in contract["columns"]}, set(source["columns"]) | {
            "pnu", "sub_pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state",
        })
        self.assertEqual(set(block["compare"]["content_columns"]),
                         set(source["columns"]) - {"mgmt_key"} | {"pnu", "sub_pnu", "raw_columns"})

    def test_a_register_is_its_whole_set_of_sub_parcels(self):
        # One register has many rows (measured up to 774); its version is all of its newest rows.
        latest = current_row_latest(load_lakehouse_contract(TABLE))
        self.assertEqual((latest.key, latest.keep_ties, latest.removal_column), (("mgmt_key",), True, "row_state"))

    def test_sub_parcel_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract(TABLE)
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)


if __name__ == "__main__":
    unittest.main()
