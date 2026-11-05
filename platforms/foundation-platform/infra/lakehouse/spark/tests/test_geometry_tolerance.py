"""The compare's geometry tolerance: a polygon whose vertices all moved less than the tolerance is
the same polygon (`geometry_tolerance`), and a compare with it appends nothing for such a key
(`silver_release_compare.compared_changes`).

The kernel is checked without PySpark; the compare runs with `RUN_SPARK_TESTS=1` in the pinned Spark
image. Shapes are synthetic squares near 10°E 10°N; codes are synthetic.
"""

import math
import os
import struct
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import geometry_tolerance as kernel  # noqa: E402

LAT = 10.0
# Degrees per metre along each axis at LAT, the kernel's own scaling.
NORTH = 1 / kernel.METRES_PER_DEGREE
EAST = 1 / (kernel.METRES_PER_DEGREE * math.cos(math.radians(LAT)))


def ring(lon, lat, side=0.001):
    return [(lon, lat), (lon + side, lat), (lon + side, lat + side), (lon, lat + side), (lon, lat)]


def polygon_body(rings):
    body = struct.pack("<I", len(rings))
    for points in rings:
        body += struct.pack("<I", len(points)) + b"".join(struct.pack("<dd", x, y) for x, y in points)
    return body


def polygon(*rings):
    return struct.pack("<BI", 1, 3) + polygon_body(rings)


def multipolygon(*polygons):
    return struct.pack("<BII", 1, 6, len(polygons)) + b"".join(struct.pack("<BI", 1, 3) + polygon_body(p)
                                                              for p in polygons)


def moved(rings, metres_east=0.0, metres_north=0.0):
    return [[(x + metres_east * EAST, y + metres_north * NORTH) for x, y in points] for points in rings]


SQUARE = [ring(10.0, LAT)]
HOLED = [ring(10.0, LAT), ring(10.0002, LAT + 0.0002, 0.0002)]


class KernelTest(unittest.TestCase):
    def test_a_five_centimetre_shift_is_the_same_polygon(self):
        self.assertTrue(kernel.within_tolerance(polygon(*SQUARE), polygon(*moved(SQUARE, 0.03, 0.04)), 0.1))
        self.assertTrue(kernel.within_tolerance(polygon(*SQUARE), polygon(*SQUARE), 0.1))

    def test_a_twenty_centimetre_shift_is_a_change(self):
        self.assertFalse(kernel.within_tolerance(polygon(*SQUARE), polygon(*moved(SQUARE, 0.2)), 0.1))
        self.assertFalse(kernel.within_tolerance(polygon(*SQUARE), polygon(*moved(SQUARE, 0.0, 0.2)), 0.1))

    def test_one_vertex_moved_beyond_the_tolerance_is_a_change(self):
        rings = moved(SQUARE, 0.05)
        rings[0][2] = (rings[0][2][0] + 0.5 * EAST, rings[0][2][1])
        self.assertFalse(kernel.within_tolerance(polygon(*SQUARE), polygon(*rings), 0.1))

    def test_a_different_vertex_count_is_a_change(self):
        extra = [SQUARE[0][:2] + [(10.0005, LAT)] + SQUARE[0][2:]]
        self.assertFalse(kernel.within_tolerance(polygon(*SQUARE), polygon(*extra), 0.1))

    def test_a_hole_is_compared_ring_by_ring(self):
        self.assertTrue(kernel.within_tolerance(polygon(*HOLED), polygon(*moved(HOLED, 0.05)), 0.1))
        self.assertFalse(kernel.within_tolerance(polygon(*HOLED), polygon(*SQUARE), 0.1))
        hole_moved = [HOLED[0], moved([HOLED[1]], 0.3)[0]]
        self.assertFalse(kernel.within_tolerance(polygon(*HOLED), polygon(*hole_moved), 0.1))

    def test_a_multipolygon_is_compared_part_by_part(self):
        other = [ring(10.01, LAT)]
        self.assertTrue(kernel.within_tolerance(multipolygon(SQUARE, other),
                                                multipolygon(moved(SQUARE, 0.05), moved(other, 0, -0.05)), 0.1))
        self.assertFalse(kernel.within_tolerance(multipolygon(SQUARE, other), multipolygon(SQUARE), 0.1))
        # The same rings as one Polygon are another type.
        self.assertFalse(kernel.within_tolerance(multipolygon(SQUARE), polygon(*SQUARE), 0.1))

    def test_a_missing_or_unreadable_geometry_is_a_change(self):
        point = struct.pack("<BIdd", 1, 1, 10.0, LAT)
        for old, new in ((None, polygon(*SQUARE)), (polygon(*SQUARE), None), (point, point),
                         (polygon(*SQUARE), polygon(*SQUARE)[:-4])):
            with self.subTest(old=old, new=new):
                self.assertFalse(kernel.within_tolerance(old, new, 0.1))

    def test_ewkb_with_an_srid_reads_as_its_polygon(self):
        ewkb = struct.pack("<BII", 1, 3 | 0x20000000, 4326) + polygon_body(SQUARE)
        self.assertTrue(kernel.within_tolerance(polygon(*SQUARE), ewkb, 0.1))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires pyspark")
class CompareSparkTest(unittest.TestCase):
    """A zone whose geometry moved 5 cm appends nothing; 1 m, or an attribute, appends."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from platform_contracts import column_names, create_table_columns_sql, load_lakehouse_contract

        cls.spark = (SparkSession.builder.master("local[2]").appName("geometry-tolerance")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
        contract = load_lakehouse_contract("silver.land_use_zone_area")
        cls.columns = list(column_names(contract))
        cls.schema = " ".join(create_table_columns_sql(contract, indent=0).split())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, zones):
        """`zones`: (feature id, zone name, polygon rings); the checksum names the exact bytes."""
        import hashlib

        at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        rows = []
        for feature, name, rings in zones:
            wkb = polygon(*rings)
            row = {"layer_code": "D999", "zone_feature_id": feature, "zone_name": name, "geometry_wkb": bytearray(wkb),
                   "geometry_checksum_sha256": hashlib.sha256(wkb).hexdigest(), "source_record_id": "bronze/a.zip",
                   "source_snapshot_id": "vworldkr-land-use-zone:20990101", "ingested_at_utc": at}
            rows.append(tuple(row.get(c) for c in self.columns))
        return self.spark.createDataFrame(rows, self.schema)

    def compare(self, old, new, tolerance=("geometry_wkb", "geometry_checksum_sha256", 0.1)):
        from pyspark.sql import functions as F

        import json

        import silver_release_compare as job

        lane = Path(__file__).resolve().parents[2] / "contracts" / "vworld-land-use-zone-area-source-objects.json"
        content = json.loads(lane.read_text(encoding="utf-8"))["silver_refresh"]["compare"]["content_columns"]
        changes, kept = job.compared_changes(
            self.frame(new), self.frame(old), key=("layer_code", "zone_feature_id"), content=content,
            removal="row_state", source_snapshot_id="vworldkr-land-use-zone:20990601",
            ingested_at=F.current_timestamp(), columns=self.columns, tolerance=tolerance)
        appended = sorted(row.zone_feature_id for row in changes.collect())
        return appended, (None if kept is None else sorted(row["_kept1"] for row in kept.collect()))

    def test_only_a_shift_within_the_tolerance_appends_nothing(self):
        old = [("z1", "a", SQUARE), ("z2", "a", SQUARE), ("z3", "a", SQUARE), ("z4", "a", SQUARE)]
        new = [("z1", "a", moved(SQUARE, 0.05)), ("z2", "a", moved(SQUARE, 1.0)),
               ("z3", "b", moved(SQUARE, 0.05)), ("z4", "a", SQUARE)]
        self.assertEqual(self.compare(old, new), (["z2", "z3"], ["z1"]))

    def test_without_a_tolerance_every_moved_geometry_appends(self):
        old = [("z1", "a", SQUARE), ("z2", "a", SQUARE)]
        new = [("z1", "a", moved(SQUARE, 0.05)), ("z2", "a", SQUARE)]
        self.assertEqual(self.compare(old, new, tolerance=None), (["z1"], None))


if __name__ == "__main__":
    unittest.main()
