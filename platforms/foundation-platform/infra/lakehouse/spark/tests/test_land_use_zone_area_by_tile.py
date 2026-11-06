"""규제 구역 타일 색인(루트 ADR-0375 §2): 타일 계산이 굽기의 Rust 계산과 같고, 바뀐 구역 목록이 맞다.

Spark 없이 도는 순수 부분이다. Spark 위의 유지 계산은 `test_land_use_zone_area_by_tile_spark.py` 가 본다.
"""

import json
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))
GOLDEN = Path(__file__).resolve().parent / "fixtures" / "tile_range_golden.json"

import land_use_zone_area_by_tile as by_tile  # noqa: E402


class TileMathTest(unittest.TestCase):
    def test_the_golden_tile_ranges_are_what_the_bake_computes(self):
        golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
        # The fixture is computed with the bake's own buffer: the contract's, which Rust also checks.
        self.assertEqual(golden["buffer_pixels"], by_tile.patch_buffer_pixels())
        self.assertEqual(golden["zoom"], by_tile.ZOOM)
        for case in golden["cases"]:
            got = by_tile.tile_range(tuple(case["bbox"]), golden["zoom"], golden["buffer_pixels"])
            self.assertEqual(list(got) if got else None, case["tiles"], case["case"])

    def test_a_zone_is_filed_under_at_least_every_tile_the_bake_buffer_reaches(self):
        golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
        self.assertGreater(by_tile.MEMBERSHIP_BUFFER_PIXELS, golden["buffer_pixels"])
        for case in golden["cases"]:
            filed = set(by_tile.membership_tiles(*case["bbox"]))
            if case["tiles"] is None:
                self.assertEqual(filed, set(), case["case"])
                continue
            x0, y0, x1, y1 = case["tiles"]
            needed = {by_tile.tile_key(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)}
            self.assertLessEqual(needed, filed, case["case"])

    def test_a_box_with_a_missing_side_is_filed_nowhere(self):
        self.assertEqual(by_tile.membership_tiles(None, 10.0, 10.1, 10.1), [])
        self.assertEqual(by_tile.membership_tiles(float("nan"), 10.0, 10.1, 10.1), [])

    def test_a_tile_key_names_its_tile(self):
        self.assertEqual(by_tile.tile_xy(by_tile.tile_key(540, 483)), (540, 483))
        self.assertEqual(by_tile.tile_key(0, 1023), 1023)
        self.assertEqual(by_tile.tile_key(1, 0), 1024)


class ChangeSetTest(unittest.TestCase):
    def test_the_maintained_snapshot_is_the_newest_recorded_one_on_silvers_lineage(self):
        # Silver 1 <- 2 <- 3 <- 4, and 5 a rolled-back branch off 2.
        silver = [{"snapshot_id": 1, "parent_id": None}, {"snapshot_id": 2, "parent_id": 1},
                  {"snapshot_id": 3, "parent_id": 2}, {"snapshot_id": 4, "parent_id": 3},
                  {"snapshot_id": 5, "parent_id": 2}]
        self.assertEqual(by_tile.maintained_through({"1", "3"}, silver, "4"), "3")
        self.assertEqual(by_tile.maintained_through({"4"}, silver, "4"), "4")
        # A maintenance of a snapshot a rollback left behind is not on the lineage.
        self.assertEqual(by_tile.maintained_through({"1", "5"}, silver, "4"), "1")
        self.assertIsNone(by_tile.maintained_through(set(), silver, "4"))
        # An expired lineage ends the walk.
        self.assertIsNone(by_tile.maintained_through({"1"}, silver[2:], "4"))

    def test_a_changed_zone_is_an_upsert_when_current_and_a_delete_when_not(self):
        lines = by_tile.change_list({"D999:b", "D998:a", "D999:c"}, {"D998:a", "D999:c", "D999:z"})
        self.assertEqual(lines, [
            {"feature_id": "D998:a", "op": "upsert"},
            {"feature_id": "D999:b", "op": "delete"},
            {"feature_id": "D999:c", "op": "upsert"},
        ])

    def test_compactions_are_walked_over_and_anything_else_stops_the_change_set(self):
        from changelog import chain

        snapshots = [
            {"snapshot_id": 1, "parent_id": None, "operation": "append"},
            {"snapshot_id": 2, "parent_id": 1, "operation": "append"},
            {"snapshot_id": 3, "parent_id": 2, "operation": "replace"},
            {"snapshot_id": 4, "parent_id": 3, "operation": "append"},
            {"snapshot_id": 5, "parent_id": 4, "operation": "overwrite"},
        ]
        steps = by_tile.own_appends(snapshots)
        self.assertEqual(chain(snapshots, steps, 1, 4), [("1", "2"), ("3", "4")])
        self.assertEqual(chain(snapshots, steps, 4, 4), [])
        self.assertIsNone(chain(snapshots, steps, 4, 5))

    def test_the_table_is_partitioned_by_its_tile_and_keyed_by_tile_and_zone(self):
        self.assertEqual(by_tile.CONTRACT["partition_spec"], ["tile_key"])
        self.assertEqual(by_tile.CONTRACT["current_row_latest"]["key"], ["tile_key", "zone_id"])
        self.assertEqual(by_tile.CONTRACT["load"], {"unit": "run", "column": "silver_iceberg_snapshot_id"})
        for name in (*by_tile.TILE_PROPERTIES, *by_tile.BBOX, "geometry_wkb", "geometry_srid",
                     "geometry_checksum_sha256", "source_snapshot_id"):
            self.assertIn(name, by_tile.COLUMNS)


if __name__ == "__main__":
    unittest.main()
