"""The HUB 공작물관리대장 lane keys each row by its content and keeps its raw row (root ADR-0402)."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import current_row_latest, load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg

TABLE = "silver.building_permit_structure_ledger"
SOURCE = SPARK.parent / "contracts/hub-building-permit-structure-ledger-source-objects.json"


class PermitStructureLedgerContractTest(unittest.TestCase):
    def test_measured_layout(self):
        source = json.loads(SOURCE.read_text(encoding="utf-8"))
        # Measured 2026-11-04: every one of 559,438 rows of the 2026-10 edition has 28 columns; column 0
        # (the permit key) holds only 298,096 values, so the key is the row's own SHA-256.
        self.assertEqual((source["column_count"], source["inner_file"], source["row_key"]),
                         (28, "mart_kcy_06.txt", "sha256_of_row"))
        self.assertEqual({name: column["index"] for name, column in source["columns"].items()}, {"permit_mgmt_key": 0, "sigungu_cd": 3, "bjdong_cd": 4, "san_gubun": 5, "bonbeon": 6, "bubeon": 7, "land_category_code": 11, "land_category_name": 12, "zone_code": 13, "district_code": 14, "precinct_code": 15, "zone_name": 16, "district_name": 17, "precinct_name": 18, "facility_kind_code": 19, "facility_kind_name": 20, "structure_code": 21, "structure_name": 22, "length": 23, "height": 24, "area_m2": 25, "building_coverage_ratio": 26})
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
