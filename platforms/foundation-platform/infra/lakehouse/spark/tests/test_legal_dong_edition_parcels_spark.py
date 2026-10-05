"""The pairing's parcel read runs on Spark executors (root ADR-0144, ADR-0148; owner decision 2026-10-05).

`legal_dong_code_change_pairs._snapshot_parcels` reads one edition's parcels with their 지목 (from
`jibun`) and area (from `geometry_wkb`), computed on the executors by `parcel_land`, which it ships
to them. The Python workers do not have the jobs directory on their path; a function they cannot
import fails only when a task runs, so this test runs one.
"""

import os
import struct
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))


def square(lon, lat, side=0.0001):
    ring = [(lon, lat), (lon + side, lat), (lon + side, lat + side), (lon, lat + side), (lon, lat)]
    return struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(ring)) + b"".join(struct.pack("<dd", *p) for p in ring)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class EditionParcelsSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("legal-dong-edition-parcels").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_parcels_come_back_with_their_jimok_and_area(self):
        import legal_dong_code_change_pairs as job
        import parcel_land
        from pyspark.sql import functions as F

        rows = [
            ("9999910100100010000", "vworldkr__parcel-209906", "1대", bytearray(square(127.1231, 36.1231))),
            ("9999910100100020000", "vworldkr__parcel-209906", "2전", bytearray(square(127.1234, 36.1231, 0.0002))),
            ("9999910200100010000", "vworldkr__parcel-209906", "1대", bytearray(square(127.1237, 36.1231))),
            ("9999910100100010000", "vworldkr__parcel-209902", "1임", bytearray(square(127.1231, 36.1231))),
        ]
        self.spark.createDataFrame(rows, "pnu string, source_snapshot_id string, jibun string, geometry_wkb binary") \
            .createOrReplaceTempView("parcel_boundaries")
        got = job._snapshot_parcels(self.spark, F, "parcel_boundaries", "vworldkr__parcel-209906",
                                    {"9999910100"}, {"100010000", "100020000"})
        self.assertEqual(sorted((pnu, jimok) for pnu, jimok, _ in got),
                         [("9999910100100010000", "대"), ("9999910100100020000", "전")])
        areas = {pnu: area for pnu, _, area in got}
        self.assertAlmostEqual(areas["9999910100100010000"], parcel_land.wkb_area_m2(square(127.1231, 36.1231)))
        self.assertAlmostEqual(areas["9999910100100020000"] / areas["9999910100100010000"], 4.0, places=2)
        self.assertIsNone(job._snapshot_parcels(self.spark, F, "parcel_boundaries", "vworldkr__parcel-209910",
                                                {"9999910100"}, None), "an edition not loaded is not 'no parcels'")


if __name__ == "__main__":
    unittest.main()
