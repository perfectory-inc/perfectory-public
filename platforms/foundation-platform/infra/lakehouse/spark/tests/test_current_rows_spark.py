"""`current_rows` on a Spark runtime (root ADR-0265): an append-only `silver.industrial_complexes`
that holds several versions of each complex yields exactly the contract's current row per code, and
the boundary export joins one complex per boundary rather than one per version.

Skipped where pyspark is missing; the CI spark-runtime lane runs it. Rows are synthetic.
"""
import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from platform_contracts import current_rows, load_lakehouse_contract  # noqa: E402

SCHEMA = (
    "official_complex_code string, complex_id string, sido_code string, "
    "valid_from_utc timestamp, valid_to_utc timestamp, ingested_at_utc timestamp"
)


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


def at(day):
    return datetime(2026, 10, day, tzinfo=timezone.utc)


SOURCE_MONTH = datetime(2025, 6, 1, tzinfo=timezone.utc)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class CurrentRowsSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("current-rows")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.contract = load_lakehouse_contract("silver.industrial_complexes")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def test_the_newest_version_of_each_code_is_the_one_current_row(self):
        rows = [
            # Three loads of the same source month, as production holds them: the last load wins.
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(1)),
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(5)),
            ("C1", "id-c1", "41", SOURCE_MONTH, None, at(9)),
            # A newer source month beats a later re-load of an older month.
            ("C2", "id-c2", "26", datetime(2025, 9, 1, tzinfo=timezone.utc), None, at(2)),
            ("C2", "id-c2", "27", SOURCE_MONTH, None, at(8)),
            # A closed row is not current however new it is; the predicate applies first.
            ("C3", "id-c3", "30", SOURCE_MONTH, None, at(3)),
            ("C3", "id-c3", "31", SOURCE_MONTH, at(7), at(7)),
        ]

        current = {
            row["official_complex_code"]: (row["sido_code"], row["ingested_at_utc"])
            for row in current_rows(self.frame(rows), self.contract).collect()
        }

        self.assertEqual(len(current), 3)
        self.assertEqual(current["C1"][0], "41")
        self.assertEqual(current["C2"][0], "26")
        self.assertEqual(current["C3"][0], "30")

    def test_a_tie_on_every_ordering_column_is_refused(self):
        rows = [
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(4)),
            ("C1", "id-c1", "41", SOURCE_MONTH, None, at(4)),
        ]

        with self.assertRaisesRegex(ValueError, "tie"):
            current_rows(self.frame(rows), self.contract)

    def test_the_boundary_export_joins_one_complex_per_boundary(self):
        from pyspark.sql import functions as F

        from industrial_complex_boundaries_silver_to_postgis_handoff import project

        complexes = self.frame([
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(1)),
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(5)),
            ("C1", "id-c1", "11", SOURCE_MONTH, None, at(9)),
        ])
        boundaries = self.spark.createDataFrame(
            [("b1", "id-c1", "official", 5186, bytearray(b"\x01"), 1.0)],
            "boundary_id string, complex_id string, boundary_kind string, geometry_srid int, "
            "geometry_wkb binary, area_sqm_calculated double",
        )

        projected = project(boundaries, complexes, F).collect()

        self.assertEqual(len(projected), 1, "three versions of one complex must not triple a boundary")
        self.assertEqual(projected[0]["official_complex_code"], "C1")


if __name__ == "__main__":
    unittest.main()
