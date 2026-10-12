"""gold.parcel_zoning is the parcel panel's zonings, one kind on its own (root ADR-0193).

Builds both from the same Silver rows and holds them equal on every PNU, the parity the cut-over
gate reads (ADR-0193 §6-2), and proves the parity check refuses a drifted panel. PNUs are synthetic.
"""

import json
import os
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

OLD, NEW = "9999910100", "9999930100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class ParcelZoningSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("parcel-zoning").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def build(self):
        import parcel_attribute_carry as carry
        import parcel_panel_silver_to_gold as panel
        import parcel_zoning_silver_to_gold as job
        from parcel_lineage import Link

        spark = self.spark
        candidates, _ = carry.carry_candidates([Link(pnu(OLD, 1), pnu(NEW, 2), "code_change", "code_derived", "")])
        cand = spark.createDataFrame([(c.successor_pnu, c.source_pnu, c.hop, c.path) for c in candidates],
                                     "successor_pnu string, source_pnu string, hop int, path string")
        frames = {
            job.PARCEL_SOURCE: spark.createDataFrame([(pnu(NEW, n),) for n in (1, 2, 3)], "pnu string"),
            job.ZONING_SOURCE: spark.createDataFrame([
                (pnu(NEW, 1), "1", "UQA110", " 제1종 "),
                (pnu(NEW, 1), "2", "UQA110", "제1종"),
                (pnu(NEW, 2), "3", "UQA110", "접함"),
                (pnu(OLD, 1), "1", "UQB100", "관리"),
            ], "pnu string, inclusion_code string, zone_code string, zone_name string"),
            job.ZONE_CODE_SOURCE: spark.createDataFrame(
                [("UQA110", "UQA100"), ("UQA100", "UQA001"), ("UQB100", "UQB001")], "ucode string, parent_ucode string"),
        }
        gold = job.build_gold_frame(spark, frames, cand, {}, "snap", "2099-09-01T00:00:00Z")
        anchors = panel.resolve_zoning_anchors(frames[job.ZONE_CODE_SOURCE])
        empty = {
            "price": "pnu string, price_json string",
            "characteristics": "pnu string, characteristics_json string",
            "forest_ledger": "pnu string, forest_ledger_json string",
            "transfer_history": "pnu string, transfer_history_json string",
            "land_rights": "pnu string, land_rights_json string, land_right_total long",
        }
        sections = {name: spark.createDataFrame([], schema) for name, schema in empty.items()}
        sections["zonings"] = panel.build_zonings(frames[job.ZONING_SOURCE], anchors, spark, {})
        sections = {n: panel.carry_section(f, panel.SECTION_VIA_COLUMNS[n], cand) for n, f in sections.items()}
        panel_rows = panel.build_gold_panel_frame(frames[job.PARCEL_SOURCE], sections, "snap", "2099-09-01T00:00:00Z")
        return job, gold, panel_rows

    def test_only_parcels_with_a_verdict_have_a_row_and_a_carried_one_names_its_path(self):
        job, gold, _ = self.build()
        rows = {r.pnu: r for r in gold.collect()}
        self.assertEqual(sorted(rows), [pnu(NEW, 1), pnu(NEW, 2)], "no row for a parcel without a verdict")
        self.assertEqual(json.loads(rows[pnu(NEW, 1)].zonings_json),
                         [{"zone_code": "UQA110", "zone_name": "제1종", "anchor_code": "UQA100", "inclusion_code": "1"}])
        self.assertIsNone(rows[pnu(NEW, 1)].attached_via)
        self.assertEqual(json.loads(rows[pnu(NEW, 2)].zonings_json)[0]["zone_code"], "UQB100")
        self.assertEqual(rows[pnu(NEW, 2)].attached_via, "lineage:code_derived")
        count, _ = job.validate_gold_frame(gold, 2)
        self.assertEqual(count, 2)

    def test_the_table_equals_the_panel_section_on_every_pnu(self):
        job, gold, panel_rows = self.build()
        self.assertEqual(job.parity_mismatches(gold, panel_rows).count(), 0)

    def test_parity_refuses_a_drifted_panel(self):
        from pyspark.sql import functions as F

        job, gold, panel_rows = self.build()
        drifted = panel_rows.withColumn(
            "zonings_json", F.when(F.col("pnu") == pnu(NEW, 1), F.lit("[]")).otherwise(F.col("zonings_json")))
        self.assertEqual([r.pnu for r in job.parity_mismatches(gold, drifted).collect()], [pnu(NEW, 1)])
        unpathed = panel_rows.withColumn("attached_via_json", F.lit(None).cast("string"))
        self.assertEqual([r.pnu for r in job.parity_mismatches(gold, unpathed).collect()], [pnu(NEW, 2)])

    def test_the_digest_ignores_lineage_columns(self):
        from pyspark.sql import functions as F

        job, gold, _ = self.build()
        first = {r.pnu: r.row_digest for r in gold.collect()}
        relabeled = (gold.drop("row_digest")
                     .withColumn("source_snapshot_id", F.concat(F.col("source_snapshot_id"), F.lit("x")))
                     .withColumn("row_digest", job.row_digest_column()))
        self.assertEqual({r.pnu: r.row_digest for r in relabeled.collect()}, first)

    def test_lineage_covers_every_contract_column(self):
        import parcel_zoning_silver_to_gold as job

        for carry_lineage in (True, False):
            self.assertEqual([c["output_column"] for c in job.column_lineage(carry_lineage)], list(job.GOLD_COLUMNS))


if __name__ == "__main__":
    unittest.main()
