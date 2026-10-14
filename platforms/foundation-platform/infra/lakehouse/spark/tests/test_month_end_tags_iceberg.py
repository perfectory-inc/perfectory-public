"""A month-end snapshot outlives the 30-day window; an untagged older one does not (root ADR-0233).

The maintenance job tags the snapshot `main` pointed at when each month ended, then expires what is
past the window. This proves, against the pinned Iceberg runtime and the job's own expiry step, the
two facts the retention rule rests on: the tag the job creates keeps its snapshot past the window,
and a snapshot no tag names is expired once it is older than the window. A real local Iceberg
catalog; no R2.

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


def month_after(moment: datetime, months: int) -> datetime:
    """The first instant of the month `months` after the one `moment` falls in (UTC)."""
    year, month = moment.year, moment.month
    for _ in range(months):
        year, month = maintenance.next_month(year, month)
    return maintenance.month_start(year, month)


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

    def test_the_month_end_snapshot_survives_the_window_and_an_untagged_one_expires(self):
        expiry = maintenance.load_maintenance_contract()["snapshot_expiry"]
        name_format = expiry["month_end_tag"]["name_format"]
        window = timedelta(days=expiry["retain_days"])

        self.spark.sql(f"CREATE TABLE proof.{TABLE} (pnu STRING, row_digest STRING) USING iceberg")
        self.spark.sql(f"INSERT INTO proof.{TABLE} VALUES ('9999900000100000001', 'a')")
        untagged = self.head()
        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'b')")
        month_end = self.head()
        committed = datetime.now(timezone.utc)
        tag = maintenance.month_start(committed.year, committed.month).strftime(name_format)

        # The run just after this month ends tags `month_end`, before anything else is committed.
        maintenance.run_snapshot_expiry(self.spark, "proof", TABLE, expiry, month_after(committed, 1), False)
        self.assertEqual(self.tag(tag), month_end)

        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'c')")
        current = self.head()

        # A run months later: every snapshot is past the window by more than a day.
        later = month_after(committed, 3)
        self.assertGreaterEqual(later - committed, window + timedelta(days=1))
        maintenance.run_snapshot_expiry(self.spark, "proof", TABLE, expiry, later, False)

        self.assertNotIn(untagged, self.snapshots(), "an untagged snapshot past the window survived")
        self.assertEqual(self.snapshots(), {month_end, current},
                         "expiry removed the month-end snapshot, or kept an untagged old one")
        self.assertEqual(self.tag(tag), month_end, "a month-end tag was moved")
        rows = self.spark.sql(f"SELECT row_digest FROM proof.{TABLE} VERSION AS OF {month_end}").collect()
        self.assertEqual([row.row_digest for row in rows], ["b"])


if __name__ == "__main__":
    unittest.main()
