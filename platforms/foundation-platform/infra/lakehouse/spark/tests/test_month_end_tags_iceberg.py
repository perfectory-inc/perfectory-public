"""A year-end snapshot is kept forever; a 13-month-old month-end tag is not (root ADR-0237).

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
        # `month_end_only` is pinned by nothing but a month-end tag naming January of next year,
        # as a run would have left it; 14 months after December that month is 13 months back.
        old_month_tag = maintenance.month_start(year + 1, 1).strftime(monthly)
        self.spark.sql(f"ALTER TABLE proof.{TABLE} CREATE TAG `{old_month_tag}` AS OF VERSION {month_end_only}")

        # The run just after this year ends tags `year_end` as every remaining month's end and as
        # the year's end, before anything else is committed.
        maintenance.run_snapshot_expiry(
            self.spark, "proof", TABLE, expiry, maintenance.month_start(year + 1, 1), False)
        year_tag = maintenance.month_start(year, 12).strftime(yearly)
        self.assertEqual(self.tag(year_tag), year_end)
        self.assertEqual(self.tag(maintenance.month_start(year, 12).strftime(monthly)), year_end)
        self.assertIn(month_end_only, self.snapshots(), "a month-end tag inside its window did not pin")

        self.spark.sql(f"INSERT OVERWRITE proof.{TABLE} VALUES ('9999900000100000001', 'c')")
        current = self.head()

        # A run 14 months after that December.
        later = maintenance.month_start(year + 2, 2)
        self.assertEqual(maintenance.months_ago(year + 1, 1, later), 13)
        self.assertGreater(maintenance.months_ago(year + 1, 1, later),
                           expiry["month_end_tag"]["keep_months"])
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

    def commit(self, table, digest, **summary):
        """One append through the DataFrame writer, with the summary properties a loader records."""
        writer = self.spark.createDataFrame([("9999900000100000001", digest)], "pnu STRING, row_digest STRING") \
            .writeTo(f"proof.{table}")
        for key, value in summary.items():
            writer = writer.option(f"snapshot-property.{key}", value)
        writer.append()
        return str(self.spark.sql(f"SELECT snapshot_id FROM proof.{table}.refs WHERE name = 'main'").first()[0])

    def ids(self, table):
        return {str(row.snapshot_id) for row in
                self.spark.sql(f"SELECT snapshot_id FROM proof.{table}.snapshots").collect()}

    def test_expiry_keeps_what_a_loader_or_the_gold_rebuild_reads_back(self):
        """Past the window, an ingest record and the newest Gold pins survive; what nobody reads does not.

        A part lane counts its release loaded only while every part's record is in `main`
        (silver_refresh `decide_parts`), and the Gold rebuild plans from the newest pins.
        """
        expiry = maintenance.load_maintenance_contract()["snapshot_expiry"]
        later = maintenance.month_start(datetime.now(timezone.utc).year + 1, 6)

        silver = "silver.building_register_unit_areas"
        self.spark.sql(f"CREATE TABLE proof.{silver} (pnu STRING, row_digest STRING) USING iceberg")
        unread = self.commit(silver, "a")
        part_1 = self.commit(silver, "b", **{maintenance.INGEST_BATCH_OBJECTS_KEY: "part-1"})
        part_2 = self.commit(silver, "c", **{maintenance.INGEST_BATCH_OBJECTS_KEY: "part-2"})
        maintenance.run_snapshot_expiry(self.spark, "proof", silver, expiry, later, False)
        self.assertNotIn(unread, self.ids(silver), "a snapshot nothing reads back outlived the window")
        self.assertLessEqual({part_1, part_2}, self.ids(silver), "expiry removed an ingest record")

        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS proof.gold")
        gold = "gold.building_panel"
        self.spark.sql(f"CREATE TABLE proof.{gold} (pnu STRING, row_digest STRING) USING iceberg")
        old_pins = self.commit(gold, "a", **{maintenance.SOURCE_SNAPSHOTS_PROPERTY: "{}"})
        new_pins = self.commit(gold, "b", **{maintenance.SOURCE_SNAPSHOTS_PROPERTY: "{}"})
        after = self.commit(gold, "c")
        maintenance.run_snapshot_expiry(self.spark, "proof", gold, expiry, later, False)
        self.assertNotIn(old_pins, self.ids(gold), "superseded Gold pins kept the table from expiring")
        self.assertLessEqual({new_pins, after}, self.ids(gold), "expiry removed the pins the rebuild plans from")

    def test_the_compaction_call_runs_on_the_pinned_iceberg(self):
        """The options the job passes are ones this Iceberg accepts (partial progress, root ADR-0238)."""
        table = "silver.compaction_probe"
        self.spark.sql(f"CREATE TABLE proof.{table} (pnu STRING, row_digest STRING) USING iceberg")
        for digest in "abcd":
            self.commit(table, digest)
        before = self.spark.sql(f"SELECT count(*) AS n FROM proof.{table}.files").first().n
        compaction = maintenance.load_maintenance_contract()["compaction"]
        row = self.spark.sql(maintenance.rewrite_data_files_sql("proof", table, compaction)).first()
        self.assertEqual(row[0], before, "the compaction did not read every small file")
        self.assertEqual(self.spark.sql(f"SELECT count(*) AS n FROM proof.{table}.files").first().n, 1)


if __name__ == "__main__":
    unittest.main()
