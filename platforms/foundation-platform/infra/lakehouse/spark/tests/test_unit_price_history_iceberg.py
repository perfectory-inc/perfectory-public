"""gold.unit_price_history on a real local Iceberg catalog (root ADR-0189).

The producer runs its own `run` against Silver tables made from the lakehouse contracts. The
first run creates the table; a run over the same inputs records the pins only; a new reference
date is one append that rewrites no data file; a changed price, a unit that moved PNU and a unit
gone are one merge. Each state equals the rows the build makes of its inputs, and every row sits
under the PNU the building panel serves its unit from. PNUs are synthetic (`99999…`).
"""
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


def pnu(main: int) -> str:
    return f"99999000001{main:04d}0000"


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class UnitPriceHistoryIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("unit-price-history")
                     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                     .config("spark.sql.catalog.proof", "org.apache.iceberg.spark.SparkCatalog")
                     .config("spark.sql.catalog.proof.type", "hadoop")
                     .config("spark.sql.catalog.proof.warehouse", cls.directory.name)
                     .config("spark.sql.shuffle.partitions", "4")
                     .config("spark.sql.adaptive.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        for namespace in ("silver", "gold"):
            cls.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS proof.{namespace}")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def put(self, name, rows):
        from pyspark.sql import functions as F
        from platform_contracts import create_table_columns_sql, load_lakehouse_contract

        table = f"proof.{name}"
        if not self.spark.catalog.tableExists(table):
            self.spark.sql(f"CREATE TABLE {table} ({create_table_columns_sql(load_lakehouse_contract(name))}) USING iceberg")
        schema = self.spark.table(table).schema
        frame = self.spark.createDataFrame([tuple(row.get(f.name) for f in schema.fields) for row in rows], schema)
        frame.writeTo(table).overwrite(F.lit(True))
        return str(self.spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()[0])

    def run_job(self, pins, published):
        import unit_price_history_silver_to_gold as job

        work = Path(self.directory.name) / f"run-{published[:13]}"
        work.mkdir(parents=True, exist_ok=True)
        args = job.parse_args(["--input-mode", "iceberg", "--iceberg-catalog-name", "proof",
                               "--iceberg-snapshot-id", "1", "--published-at-utc", published,
                               "--summary-output", str(work / "summary.json")])
        with redirect_stdout(StringIO()):
            self.assertEqual(job.run(self.spark, args, pins), 0)
        return json.loads((work / "summary.json").read_text(encoding="utf-8"))

    def gold(self):
        return {(r.register_pk, r.base_date): (r.pnu, r.price_won)
                for r in self.spark.table("proof.gold.unit_price_history").collect()}

    def files(self):
        return {r.file_path for r in self.spark.sql("SELECT file_path FROM proof.gold.unit_price_history.files").collect()}

    def head(self):
        return self.spark.sql("SELECT operation, summary FROM proof.gold.unit_price_history.snapshots "
                              "ORDER BY committed_at DESC LIMIT 1").first()

    def test_a_vintage_writes_only_what_differs(self):
        import unit_price_history_silver_to_gold as job

        titles = [{"mgm_bldrgst_pk": f"T{i}", "pnu": pnu(i), "source_snapshot_id": "titles-v1"} for i in range(1, 5)]
        evidence = {"building_link_method": "parent_key", "building_link_source_record_id": "synthetic#line=1",
                    "building_link_input_sha256": "a" * 64, "source_snapshot_id": "units-v1"}
        # U1..U3 hang off T1..T3 (U2's own PNU differs from its building's); U9 is unlinked.
        units = [{"mgm_bldrgst_pk": f"U{i}", "pnu": pnu(i + (10 if i == 2 else 0)), "building_mgm_bldrgst_pk": f"T{i}",
                  "unit_row_id": f"u{i}", **evidence} for i in range(1, 4)]
        units.append({"mgm_bldrgst_pk": "U9", "pnu": pnu(9), "unit_row_id": "u9", "building_link_method": "unresolved",
                      "source_snapshot_id": "units-v1"})

        def prices(rows, batch):
            return [{"mgm_bldrgst_pk": k, "pnu": pnu(1), "dong_name": "", "ho_name": "", "base_date": d,
                     "price_won": p, "sido": "99", "source_snapshot_id": batch, "source_record_id": f"{batch}/{k}/{d}"}
                    for k, d, p in rows]

        v1 = [("U1", "20240101", 100), ("U2", "20240101", 200), ("U3", "20240101", 300), ("U9", "20240101", 900),
              ("U-none", "20240101", 1)]
        pins = {job.TITLE_SOURCE: self.put(job.TITLE_SOURCE, titles), job.UNIT_SOURCE: self.put(job.UNIT_SOURCE, units),
                job.PRICE_SOURCE: self.put(job.PRICE_SOURCE, prices(v1, "batch-1"))}
        first = self.run_job(pins, "2099-01-01T00:00:00Z")
        self.assertEqual(first["write_disposition"], "iceberg_create")
        self.assertEqual(first["quality_metrics"]["unplaced_price_count"], 1)
        # A linked unit is served under its building's PNU, an unlinked one under its own.
        self.assertEqual(self.gold(), {("U1", "20240101"): (pnu(1), 100), ("U2", "20240101"): (pnu(2), 200),
                                       ("U3", "20240101"): (pnu(3), 300), ("U9", "20240101"): (pnu(9), 900)})

        same = self.run_job(pins, "2099-01-02T00:00:00Z")
        self.assertEqual(same["write_disposition"], "iceberg_pins_only")
        self.assertFalse(same["rebuild"]["merged"])

        # A new vintage that adds a reference date: one append, every earlier file kept.
        before = self.files()
        v2 = v1 + [("U1", "20250101", 110), ("U3", "20250101", 330)]
        pins[job.PRICE_SOURCE] = self.put(job.PRICE_SOURCE, prices(v2, "batch-2"))
        appended = self.run_job(pins, "2099-02-01T00:00:00Z")
        self.assertEqual(appended["write_disposition"], "iceberg_append")
        self.assertEqual(appended["rebuild"]["counts"], {"insert": 2, "update": 0, "delete": 0})
        self.assertTrue(before <= self.files(), "an append rewrote a data file")
        self.assertEqual(self.head().operation, "append")

        # A corrected price, a unit relinked to another building and a unit gone: one merge.
        v3 = [row for row in v2 if row[0] != "U9"]
        v3[0] = ("U1", "20240101", 101)
        units[2] = {**units[2], "building_mgm_bldrgst_pk": "T4"}
        pins[job.UNIT_SOURCE] = self.put(job.UNIT_SOURCE, units)
        pins[job.PRICE_SOURCE] = self.put(job.PRICE_SOURCE, prices(v3, "batch-3"))
        merged = self.run_job(pins, "2099-03-01T00:00:00Z")
        self.assertEqual(merged["write_disposition"], "iceberg_merge")
        self.assertEqual(merged["rebuild"]["counts"], {"insert": 2, "update": 1, "delete": 3})
        self.assertEqual(self.gold(), {("U1", "20240101"): (pnu(1), 101), ("U1", "20250101"): (pnu(1), 110),
                                       ("U2", "20240101"): (pnu(2), 200), ("U3", "20240101"): (pnu(4), 300),
                                       ("U3", "20250101"): (pnu(4), 330)})
        recorded = json.loads(self.head().summary["foundation.source-iceberg-snapshots"])
        self.assertEqual(recorded, pins)

    def test_the_merge_names_every_key_column_and_only_its_changes(self):
        import unit_price_history_silver_to_gold as job

        sql = job.merge_sql("t")
        for key in job.KEY:
            self.assertIn(f"t.`{key}` = s.`{key}`", sql)
        self.assertNotIn("NOT MATCHED BY SOURCE", sql)


if __name__ == "__main__":
    unittest.main()
