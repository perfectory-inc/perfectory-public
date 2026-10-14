"""A year-end snapshot is kept forever; a 13-month-old month-end tag is not (root ADR-0233).

Grandfather-father-son: 30 days of snapshots, the month end for 12 months, the year end forever. The
maintenance job tags month and year ends, drops month-end tags past their window, then expires.
This proves both edges against the pinned Iceberg runtime and the job's own expiry step: the
year-end tag keeps its snapshot long after the monthly window, and a snapshot only a month-end tag
13 months back names loses the tag and is expired. A real local Iceberg catalog; no R2.

The job takes `now` as an argument, so the clock is moved instead of the commits: every snapshot
here is committed today, and the job is run as if months had passed.
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import lakehouse_maintenance as maintenance  # noqa: E402

TABLE = "silver.parcel_boundaries"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class MonthEndTagsIceberg(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("month-end-tags")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.proof.type", "hadoop")
            .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE proof.silver")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def snapshots(self):
        return {str(row.snapshot_id) for row in
                self.spark.sql(f"SELECT snapshot_id FROM proof.{TABLE}.snapshots").collect()}

    def head(self):
        return str(self.spark.sql(f"SELECT snapshot_id FROM proof.{TABLE}.refs WHERE name = 'main'").first()[0])

    def tag(self, name):
        rows = self.spark.sql(f"SELECT snapshot_id FROM proof.{TABLE}.refs WHERE name = '{name}'").collect()
        return str(rows[0].snapshot_id) if rows else None

    def test_a_year_end_tag_survives_and_a_13_month_old_month_end_tag_is_expired(self):
        expiry = maintenance.load_maintenance_contract()["snapshot_expiry"]
        monthly = expiry["month_end_tag"]["name_format"]
        yearly = expiry["year_end_tag"]["name_format"]
        window = timedelta(days=expiry["retain_days"])

        self.spark.sql(f"CREATE TABLE proof.{TABLE} (pnu STRING, row_digest STRING) USING iceberg")
        self.spark.sql(f"INSERT INTO proof.{TABLE} VALUES ('9999900000100000001', 'a')")
        month_end_only = self.head()
        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'b')")
        year_end = self.head()
        committed = datetime.now(timezone.utc)
        year = committed.year

        # The run just after this year ends tags `year_end` as every remaining month's end and as
        # the year's end, before anything else is committed.
        maintenance.run_snapshot_expiry(
            self.spark, "proof", TABLE, expiry, maintenance.month_start(year + 1, 1), False)
        year_tag = maintenance.month_start(year, 12).strftime(yearly)
        self.assertEqual(self.tag(year_tag), year_end)
        self.assertEqual(self.tag(maintenance.month_start(year, 12).strftime(monthly)), year_end)

        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'c')")
        current = self.head()

        # A run 14 months after that December. `month_end_only` is pinned by nothing but a
        # month-end tag naming the month 13 months back, as an earlier run would have left it.
        later = maintenance.month_start(year + 2, 2)
        old_month_tag = maintenance.month_start(year + 1, 1).strftime(monthly)
        self.assertEqual(maintenance.months_ago(year + 1, 1, later), 13)
        self.assertGreater(maintenance.months_ago(year + 1, 1, later),
                           expiry["month_end_tag"]["keep_months"])
        self.spark.sql(f"ALTER TABLE proof.{TABLE} CREATE TAG `{old_month_tag}` AS OF VERSION {month_end_only}")
        self.assertGreaterEqual(later - committed, window + timedelta(days=1))
        maintenance.run_snapshot_expiry(self.spark, "proof", TABLE, expiry, later, False)

        self.assertIsNone(self.tag(old_month_tag), "a 13-month-old month-end tag was kept")
        self.assertNotIn(month_end_only, self.snapshots(),
                         "the snapshot only a 13-month-old month-end tag named survived")
        self.assertEqual(self.tag(year_tag), year_end, "the year-end tag was dropped or moved")
        self.assertEqual(self.snapshots(), {year_end, current},
                         "expiry removed the year-end snapshot, or kept a snapshot nothing pins")
        refs = {row.name for row in self.spark.sql(f"SELECT name FROM proof.{TABLE}.refs").collect()}
        old_year = {maintenance.month_start(year, month).strftime(monthly) for month in range(1, 13)}
        self.assertFalse(refs & old_year, "a month-end tag of the old year outlived the monthly window")
        rows = self.spark.sql(f"SELECT row_digest FROM proof.{TABLE} VERSION AS OF {year_end}").collect()
        self.assertEqual([row.row_digest for row in rows], ["b"])


if __name__ == "__main__":
    unittest.main()
