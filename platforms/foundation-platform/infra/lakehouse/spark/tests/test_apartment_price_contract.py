"""The measured headerless HUB layout and its Silver consumer must agree."""

import json
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace

SPARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK / "jobs"))
from platform_contracts import load_lakehouse_contract, spark_sql_type
from silver_scalar_handoff_to_lakehouse import spark_type, write_silver_iceberg


class ApartmentPriceContractTest(unittest.TestCase):
    def test_measured_objects_and_consumed_positions(self):
        path = SPARK.parent / "contracts/hub-building-register-apartment-price-source-objects.json"
        self.assertTrue(path.is_file(), "the HUB layout contract is missing")
        c = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(c["schema_version"], 1)
        self.assertEqual(c["column_count"], 25)
        self.assertEqual(c["inner_file"], "mart_djy_08.txt")
        self.assertEqual((c["encoding"], c["csv_delimiter"], c["has_header"]), ("utf-8", "|", False))
        self.assertEqual({k: v["index"] for k, v in c["columns"].items()}, {
            "mgmt_key": 0, "sigungu_cd": 8, "bjdong_cd": 9, "san_gubun": 10,
            "bonbeon": 11, "bubeon": 12, "base_date": 22, "price_won": 23, "notice_date": 24,
        })
        self.assertTrue(all(v["meaning"] for v in c["columns"].values()))
        self.assertEqual(len(c["objects"]), 5)
        self.assertEqual({o["vintage"] for o in c["objects"]}, {"202604", "202605", "202606", "202607", "202608"})
        self.assertEqual(c["selected_vintage"], max(o["vintage"] for o in c["objects"]))
        self.assertEqual(c["granularity_counts"], {"national": 1, "vintages": 5, "objects": 5})
        self.assertEqual(len({o["object_key"] for o in c["objects"]}), 5)
        self.assertTrue(all(o["bytes"] > 0 and o["object_key"].startswith(c["source"] + "/") for o in c["objects"]))
        self.assertEqual(c["handoff_suffix"], ".jsonl.gz")
        self.assertEqual(c["rows_per_part"], 10_000_000)

    def test_silver_retains_raw_array_and_loads_each_part_once(self):
        c = load_lakehouse_contract("silver.building_register_apartment_price")
        columns = {x["name"]: x for x in c["columns"]}
        self.assertEqual(columns["raw_columns"]["logical_type"], "array<string>")
        self.assertTrue(columns["raw_columns"]["required"])
        self.assertFalse(columns["pnu"]["required"])
        self.assertEqual(c["partition_spec"], ["vintage"])
        self.assertEqual(c["load"]["column"], "source_part_id")
        self.assertTrue(columns["source_record_id"]["required"])
        self.assertTrue(columns["source_line_number"]["required"])
        self.assertEqual(spark_sql_type("array<string>"), "ARRAY<STRING>")

    def test_spark_maps_string_arrays_without_stringifying_them(self):
        class Types:
            @staticmethod
            def StringType():
                return "string"

            @staticmethod
            def ArrayType(element, containsNull=True):
                return ("array", element, containsNull)

        self.assertEqual(spark_type("array<string>", Types), ("array", "string", True))

    def test_append_only_observations_refuse_overwrite_before_opening_spark(self):
        c = load_lakehouse_contract("silver.building_register_apartment_price")
        self.assertIn("append_only", c["quality_gates"])
        for configured, override in [("overwrite", None), ("append", "overwrite")]:
            with self.assertRaisesRegex(ValueError, "append.only"):
                write_silver_iceberg(None, None, SimpleNamespace(iceberg_write_mode=configured), c, None, override)


if __name__ == "__main__":
    unittest.main()
