"""The HUB bridge retains unit labels and raw observations before any projection."""

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import unittest

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import load_lakehouse_contract
from silver_scalar_handoff_to_lakehouse import write_silver_iceberg


class ExclusiveUnitContractTest(unittest.TestCase):
    def test_measured_exclusive_unit_layout_and_objects(self):
        path = SPARK.parent / "contracts/hub-building-register-exclusive-unit-source-objects.json"
        assert path.is_file(), "the exclusive-unit source contract is missing"
        source = json.loads(path.read_text(encoding="utf-8"))
        assert source["column_count"] == 27
        assert source["inner_file"] == "mart_djy_09.txt"
        assert (source["encoding"], source["csv_delimiter"], source["has_header"]) == ("utf-8", "|", False)
        assert {name: column["index"] for name, column in source["columns"].items()} == {
            "mgmt_key": 0, "sigungu_cd": 8, "bjdong_cd": 9, "san_gubun": 10,
            "bonbeon": 11, "bubeon": 12, "dong_name": 21, "ho_name": 22,
            "floor_kind": 24, "floor_no": 25,
        }
        assert source["selected_vintage"] == "202608"
        assert {o["vintage"]: o["bytes"] for o in source["objects"]} == {
            "202604": 966116523, "202605": 967624253, "202606": 968762635,
            "202607": 969813866, "202608": 917907836,
        }
        assert source["granularity_counts"] == {"national": 1, "vintages": 5, "objects": 5}
        assert source["handoff_layout"] == "manifest_parts"
        assert source["rows_per_part"] == 10_000_000
        contract = load_lakehouse_contract("silver.building_register_exclusive_unit")
        columns = {c["name"]: c for c in contract["columns"]}
        assert set(columns) == set(source["columns"]) | {
            "pnu", "raw_columns", "vintage", "source_record_id", "source_part_id",
            "source_line_number", "source_snapshot_id", "ingested_at_utc",
        }
        assert all(columns[name]["logical_type"] == "string" for name in source["columns"])
        assert columns["raw_columns"] == {"name": "raw_columns", "logical_type": "array<string>", "required": True}
        assert not columns["pnu"]["required"]
        assert contract["partition_spec"] == ["vintage"]
        assert contract["load"]["column"] == "source_part_id"
        assert all(columns[name]["required"] for name in (
            "vintage", "source_record_id", "source_part_id", "source_line_number",
            "source_snapshot_id", "ingested_at_utc",
        ))


    def test_exclusive_observations_refuse_overwrite_before_opening_spark(self):
        contract = load_lakehouse_contract("silver.building_register_exclusive_unit")
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), contract, None, override)
