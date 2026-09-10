"""The building bake preserves identity, absent facts, links and content-only digests."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from pyspark.sql import SparkSession, types as T
import building_panel_silver_to_gold as job
from platform_contracts import load_lakehouse_contract

PNU = "9999900000100000000"


class BuildingPanelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("building-panel-test")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, name, rows):
        types = {"string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
                 "double": T.DoubleType(), "timestamp": T.TimestampType()}
        schema = T.StructType([T.StructField(c["name"], types[c["logical_type"]], True)
                               for c in load_lakehouse_contract(name)["columns"]])
        return self.spark.createDataFrame([
            {**{"source_snapshot_id": "snapshot-fixture"}, **row} for row in rows
        ], schema)

    def sources(self):
        return {
            job.TITLE_SOURCE: self.frame(job.TITLE_SOURCE, [{
                "mgm_bldrgst_pk": "BLDG-1", "pnu": PNU, "ground_floor_count": None,
                "basement_floor_count": 0, "purpose_code_raw": " 03000 ",
            }]),
            job.FLOOR_SOURCE: self.frame(job.FLOOR_SOURCE, [{
                "floor_row_id": "FLOOR-1", "mgm_bldrgst_pk": "BLDG-1", "floor_kind": "above_ground",
                "floor_number": 1, "floor_index": 1, "floor_display_ko": "1층",
            }]),
            job.UNIT_SOURCE: self.frame(job.UNIT_SOURCE, [
                {"mgm_bldrgst_pk": pk, "pnu": PNU, "building_mgm_bldrgst_pk": link,
                 "dong_name_raw": "101동", "unit_label_ko": ho, "floor_kind": "above_ground", "floor_number": 1}
                for pk, link, ho in [("UNIT-1", "BLDG-1", "101호"), ("UNIT-2", None, "102호")]
            ]),
            job.AREA_SOURCE: self.frame(job.AREA_SOURCE, [
                {"area_row_id": aid, "mgm_bldrgst_pk": "UNIT-1", "pnu": PNU,
                 "area_kind": "exclusive", "area_m2": area, "usage_name_raw": usage}
                for aid, area, usage in [("AREA-1", 30.0, "공장"), ("AREA-2", 20.0, "창고")]
            ]),
            job.PRICE_SOURCE: self.frame(job.PRICE_SOURCE, []),
        }

    def test_digest_columns_come_from_the_contract(self):
        self.assertEqual(set(job.GOLD_COLUMNS) - set(job.CONTENT_DIGEST_COLUMNS),
                         {"row_digest", "source_snapshot_id", "published_at_utc"})
        self.assertEqual(job.CONTENT_DIGEST_COLUMNS, ("pnu", "buildings_json", "unlinked_units_json"))

    def test_nested_sections_keep_unlinked_units_and_absent_facts(self):
        gold = job.build_gold_panel_frame(self.sources(), "source-a", "2026-01-01T00:00:00Z", {})
        row = gold.first()
        fixture_path = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "building_panel_gold_row.json"
        self.assertEqual(row.asDict(), json.loads(fixture_path.read_text(encoding="utf-8")))
        buildings = json.loads(row.buildings_json)
        self.assertEqual(len(buildings), 1)
        self.assertEqual(buildings[0]["register_pk"], "BLDG-1")
        self.assertEqual(buildings[0]["purpose_code"], "03000")
        self.assertIsNone(buildings[0]["stories"])
        self.assertIsNone(buildings[0]["floor_area_m2"])
        self.assertNotIn("updated_at", buildings[0])
        self.assertEqual(buildings[0]["floors"][0]["floor_number"], 1)
        unit = buildings[0]["units"][0]
        self.assertEqual(unit["building_id"], buildings[0]["id"])
        self.assertEqual(unit["exclusive_area_m2"], 50.0)
        self.assertEqual(unit["usage_name"], "공장")
        unlinked = json.loads(row.unlinked_units_json)
        self.assertEqual(len(unlinked), 1)
        self.assertIsNone(unlinked[0]["building_id"])

    def test_digest_ignores_lineage_and_shuffle_order(self):
        sources = self.sources()
        first = job.build_gold_panel_frame(sources, "source-a", "2026-01-01T00:00:00Z", {}).first()
        second = job.build_gold_panel_frame({k: v.repartition(2) for k, v in sources.items()},
                                           "source-b", "2026-01-02T00:00:00Z", {}).first()
        self.assertEqual(first.row_digest, second.row_digest)
        self.assertEqual(first.buildings_json, second.buildings_json)

    def test_duplicate_title_identity_and_multi_snapshot_refuse(self):
        sources = self.sources()
        sources[job.TITLE_SOURCE] = sources[job.TITLE_SOURCE].union(sources[job.TITLE_SOURCE])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            job.build_gold_panel_frame(sources, "source-a", "2026-01-01T00:00:00Z", {})
        mixed = self.frame(job.TITLE_SOURCE, [
            {"mgm_bldrgst_pk": "A", "source_snapshot_id": "a"},
            {"mgm_bldrgst_pk": "B", "source_snapshot_id": "b"},
        ])
        with self.assertRaisesRegex(ValueError, "source_snapshot_id"):
            job.assert_single_snapshot(mixed, job.TITLE_SOURCE)


if __name__ == "__main__":
    unittest.main()
