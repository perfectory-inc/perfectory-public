"""gold.unit_price_panel on a real local Iceberg catalog (root ADR-0189).

The producer runs its own `run` against Silver tables made from the lakehouse contracts: v1 in
full, then a new price vintage (one price corrected, a reference date added, a unit's prices
gone) and a unit relinked to another building, merged incrementally and rebuilt in full into a
second table. Both must hold the same rows; PNUs the change did not reach keep their bytes; each
unit sits under the PNU its building document carries it. PNUs are synthetic (`99999…`).
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
class UnitPricePanelIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[2]").appName("unit-price-panel")
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

    def run_job(self, pins, table, published, previous=None):
        import unit_price_panel_silver_to_gold as job

        work = Path(self.directory.name) / f"run-{table}-{published[:13]}"
        work.mkdir(parents=True, exist_ok=True)
        argv = ["--input-mode", "iceberg", "--write-mode", "iceberg", "--iceberg-catalog-name", "proof",
                "--target-iceberg-table", table, "--allow-non-smoke-overwrite", "--iceberg-snapshot-id", "1",
                "--published-at-utc", published, "--summary-output", str(work / "summary.json")]
        if previous is not None:
            (work / "previous.json").write_text(json.dumps(previous), encoding="utf-8")
            argv += ["--incremental-from-snapshots", str(work / "previous.json"),
                     "--max-changed-key-fraction", "0.9", "--parity-sample-keys", "1000"]
        with redirect_stdout(StringIO()):
            self.assertEqual(job.run(self.spark, job.parse_args(argv), pins), 0)
        return json.loads((work / "summary.json").read_text(encoding="utf-8"))

    def rows(self, table):
        return {r.pnu: (json.loads(r.units_json), r.row_digest, r.published_at_utc)
                for r in self.spark.table(f"proof.gold.{table}").collect()}

    def test_an_incremental_vintage_equals_the_full_rebuild(self):
        import building_panel_silver_to_gold as panel
        import unit_price_panel_silver_to_gold as job

        titles = [{"mgm_bldrgst_pk": f"T{i}", "pnu": pnu(i), "source_snapshot_id": "titles-v1"} for i in range(1, 21)]
        evidence = {"building_link_method": "parent_key", "building_link_source_record_id": "synthetic#line=1",
                    "building_link_input_sha256": "a" * 64, "source_snapshot_id": "units-v1"}
        # U{i} hangs off T{i}; U2's own PNU is another (it is served under its building's).
        units = [{"mgm_bldrgst_pk": f"U{i}", "pnu": pnu(i + (30 if i == 2 else 0)), "building_mgm_bldrgst_pk": f"T{i}",
                  "unit_row_id": f"u{i}", **evidence} for i in range(1, 21)]
        units.append({"mgm_bldrgst_pk": "U-orphan", "pnu": pnu(25), "unit_row_id": "u-orphan",
                      "building_link_method": "unresolved", "source_snapshot_id": "units-v1"})

        def prices(rows, batch):
            return [{"mgm_bldrgst_pk": k, "pnu": pnu(1), "dong_name": "", "ho_name": "", "base_date": d,
                     "price_won": p, "sido": "99", "source_snapshot_id": batch, "source_record_id": f"{batch}/{k}/{d}"}
                    for k, d, p in rows]

        v1 = [(f"U{i}", "20240101", 100 * i) for i in range(1, 21) if i % 4] + [("U-orphan", "20240101", 7),
                                                                                ("U-none", "20240101", 1)]
        pins1 = {job.TITLE_SOURCE: self.put(job.TITLE_SOURCE, titles), job.UNIT_SOURCE: self.put(job.UNIT_SOURCE, units),
                 job.PRICE_SOURCE: self.put(job.PRICE_SOURCE, prices(v1, "batch-1"))}
        first = self.run_job(pins1, "unit_price_panel", "2099-01-01T00:00:00Z")
        self.assertEqual(first["rebuild"]["mode"], "full")
        self.assertEqual(first["quality_metrics"]["unplaced_price_unit_count"], 1)
        before = self.rows("unit_price_panel")
        # Only priced units' PNUs; U2 under its building's PNU, the orphan under its own.
        self.assertEqual(sorted(before), sorted([pnu(i) for i in range(1, 21) if i % 4] + [pnu(25)]))
        self.assertEqual(before[pnu(2)][0], [{"id": before[pnu(2)][0][0]["id"],
                                              "official_price_history": [{"base_date": "20240101", "price_won": 200}]}])

        # The next vintage: U1 corrected, U3 gains a reference date, U5's prices gone; U6 relinked to T8.
        v2 = [row for row in v1 if row[0] != "U5"]
        v2[0] = ("U1", "20240101", 101)
        v2.append(("U3", "20250101", 330))
        units[5] = {**units[5], "building_mgm_bldrgst_pk": "T8"}
        pins2 = {**pins1, job.UNIT_SOURCE: self.put(job.UNIT_SOURCE, units),
                 job.PRICE_SOURCE: self.put(job.PRICE_SOURCE, prices(v2, "batch-2"))}
        merged = self.run_job(pins2, "unit_price_panel", "2099-02-01T00:00:00Z", previous=pins1)
        self.assertEqual(merged["rebuild"]["mode"], "incremental")
        full = self.run_job(pins2, "unit_price_panel_full", "2099-02-02T00:00:00Z")
        after, rebuilt = self.rows("unit_price_panel"), self.rows("unit_price_panel_full")
        self.assertEqual({k: v[:2] for k, v in after.items()}, {k: v[:2] for k, v in rebuilt.items()})
        self.assertNotIn(pnu(5), after)
        self.assertNotIn(pnu(6), after)
        self.assertEqual(len(after[pnu(8)][0]), 1, "U6 joined T8's PNU; U8 itself is unpriced")
        self.assertEqual(after[pnu(3)][0][0]["official_price_history"],
                         [{"base_date": "20250101", "price_won": 330}, {"base_date": "20240101", "price_won": 300}])
        reached = {pnu(1), pnu(3), pnu(5), pnu(6), pnu(8)}
        for key in set(before) - reached:
            self.assertEqual(after[key], before[key], f"{key} was not reached but its row changed")
        self.assertIsNotNone(panel.unit_placement)


if __name__ == "__main__":
    unittest.main()
