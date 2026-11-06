"""규제 구역 타일 색인의 유지(루트 ADR-0375 §2)를 Spark 위에서: 바뀐 구역만 덧붙고, 타일을 떠난 구역은
그 타일에 삭제 표시를 받고, 그대로인 구역은 한 줄도 덧붙지 않는다. 이웃 읽기는 현재 구역을 한 번씩 낸다.

Skipped where pyspark is missing; the CI spark-runtime lane runs it. Zones are synthetic (layers D998,
D999).
"""
import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import land_use_zone_area_by_tile as by_tile  # noqa: E402
from platform_contracts import spark_sql_type  # noqa: E402

# Tile (540, 483) at z10 and its right neighbour (541, 483), near 10°E 10°N (synthetic).
LEFT = (10.05, 10.02, 10.06, 10.06)
RIGHT = (10.40, 10.02, 10.41, 10.06)


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def at(day):
    return datetime(2026, 11, day, tzinfo=timezone.utc)


def silver(code, area, name, box, checksum, vintage, day, removed=False):
    return {"layer_code": code, "zone_feature_id": area, "layer_name": "SYN", "sigungu_code": None,
            "zone_code": "UQ999", "zone_name": name, "field_a7": None, "field_a8": None, "field_a9": None,
            "geometry_wkb": bytearray(b"\x01"), "geometry_srid": 4326,
            "bbox_min_x": box[0], "bbox_min_y": box[1], "bbox_max_x": box[2], "bbox_max_y": box[3],
            "geometry_checksum_sha256": checksum, "source_record_id": f"obj-{vintage}",
            "source_snapshot_id": vintage, "ingested_at_utc": at(day), "row_state": "removed" if removed else None}


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class ByTileMaintenanceSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("land-use-zone-area-by-tile")
                     .config("spark.sql.shuffle.partitions", "2")
                     # The tile UDF runs on the executors, which import it from the jobs directory.
                     .config("spark.executorEnv.PYTHONPATH", str(Path(by_tile.__file__).parent))
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows, contract):
        schema = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])
        return self.spark.createDataFrame([tuple(row[c["name"]] for c in contract["columns"]) for row in rows], schema)

    def maintain(self, table, silver_rows, snapshot, day):
        zones = by_tile.latest_zones(self.frame(silver_rows, by_tile.SILVER_CONTRACT))
        previous = (by_tile.previous_memberships(table, zones) if table is not None
                    else self.frame([], by_tile.CONTRACT).select("tile_key", "zone_id", "layer_code",
                                                                  "content_sha256", "source_snapshot_id"))
        changes = by_tile.membership_changes(zones, previous, snapshot, at(day))
        return changes if table is None else table.unionByName(changes)

    def test_only_what_changed_is_appended_and_a_zone_that_leaves_a_tile_is_marked_there(self):
        month_1 = [
            silver("D998", "a", "stays", LEFT, "c-a", "20260609", 1),
            silver("D998", "b", "moves", LEFT, "c-b", "20260609", 1),
            silver("D999", "c", "goes", RIGHT, "c-c", "20260609", 1),
        ]
        table = self.maintain(None, month_1, "101", 1)
        current = {(r.tile_key, r.zone_id) for r in table.collect()}
        left, right = by_tile.membership_tiles(*LEFT), by_tile.membership_tiles(*RIGHT)
        self.assertEqual(left, [by_tile.tile_key(540, 483)])
        self.assertEqual(right, [by_tile.tile_key(541, 483)])
        self.assertEqual(current, {(left[0], "D998:a"), (left[0], "D998:b"), (right[0], "D999:c")})
        # Month 2 appends only b (moved right) and c's removal marker; a is not in the change.
        month_2 = [
            silver("D998", "b", "moves", RIGHT, "c-b2", "20261009", 9),
            silver("D999", "c", None, RIGHT, "c-c", "20261009", 9, removed=True),
        ]
        appended = self.maintain(table, month_2, "202", 9).subtract(table)
        got = sorted((r.tile_key, r.zone_id, r.row_state, r.silver_iceberg_snapshot_id) for r in appended.collect())
        self.assertEqual(got, sorted([
            (right[0], "D998:b", None, "202"),
            (left[0], "D998:b", "removed", "202"),
            (right[0], "D999:c", "removed", "202"),
        ]))
        after = table.unionByName(appended)
        self.assertEqual(
            sorted((r.tile_key, r.zone_id) for r in by_tile.current_memberships(after).collect()),
            sorted([(left[0], "D998:a"), (right[0], "D998:b")]))
        # The neighbourhood of both tiles: every current zone once, with its geometry.
        zones = by_tile.neighbourhood(after, left + right)
        self.assertEqual(sorted(r.zone_id for r in zones.collect()), ["D998:a", "D998:b"])
        # The same Silver rows maintained again append nothing.
        self.assertEqual(self.maintain(after, month_2, "203", 10).subtract(after).count(), 0)

    def test_an_older_vintage_than_the_filed_one_is_refused(self):
        table = self.maintain(None, [silver("D998", "a", "x", LEFT, "c-a", "20261009", 1)], "101", 1)
        with self.assertRaisesRegex(ValueError, "older Silver vintage"):
            self.maintain(table, [silver("D998", "a", "x", LEFT, "c-old", "20260609", 2)], "202", 2).count()

    def test_two_different_current_rows_of_one_zone_are_refused(self):
        rows = [silver("D998", "a", "x", LEFT, "c-1", "20261009", 1),
                silver("D998", "a", "x", LEFT, "c-2", "20261009", 1)]
        with self.assertRaisesRegex(ValueError, "two different current rows"):
            by_tile.latest_zones(self.frame(rows, by_tile.SILVER_CONTRACT))


if __name__ == "__main__":
    unittest.main()
