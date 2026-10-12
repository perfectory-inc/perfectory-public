"""Every parcel kind is the parcel panel's section on its own (root ADR-0193).

Builds the panel and each kind the contract names from the same Silver rows and holds every kind
equal to its panel section on every PNU (the parity ADR-0193 §6-2 reads), proves the parity check
refuses a drifted panel, that only PNUs with the section have a row, and that a carried section names
its lineage path. A new kind line is covered here without an edit. PNUs are synthetic.
"""

import argparse
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


# One row of each section's Silver input, on NEW 1; NEW 2 has nothing of its own and carries OLD 1's;
# NEW 3 has nothing at all.
ROWS = {
    "silver.land_use_plan": [(pnu(NEW, 1), "1", "UQA110", " 제1종 "), (pnu(OLD, 1), "1", "UQB100", "관리")],
    "silver.land_use_zone_code": [("UQA110", "UQA100"), ("UQA100", "UQA001"), ("UQB100", "UQB001")],
    "silver.land_individual_price": [(pnu(NEW, 1), "2099", "1", "1000", "20990101"),
                                     (pnu(OLD, 1), "2099", "1", "2000", None)],
    "silver.land_characteristic": [(pnu(NEW, 1), 10.0, "대", "주거", "평지", "정방형", "세로"),
                                   (pnu(OLD, 1), 20.0, "전", "농지", "평지", "부정형", None)],
    "silver.land_forest_ledger": [(pnu(NEW, 1), 30.0, 1, "임", "1"), (pnu(OLD, 1), 40.0, None, "임", "2")],
    "silver.land_transfer_history": [(pnu(NEW, 1), 1, "1", "10", "분할", "20990101", None, "대", 10.0, None),
                                     (pnu(OLD, 1), 2, "1", "20", "합병", "20980101", None, "전", 20.0, "1")],
    "silver.land_right_registration": [(pnu(NEW, 1), "1", "가", "101", "1", "101", "", "1/2", "말소", "1"),
                                       (pnu(OLD, 1), "2", "나", "", "", "", "", "1/1", None, None)],
}


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class ParcelKindSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("by-pnu-kind").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self):
        import parcel_attribute_carry as carry
        import parcel_panel_silver_to_gold as panel
        from parcel_lineage import Link
        from platform_contracts import load_lakehouse_contract, spark_sql_type

        spark = self.spark
        candidates, _ = carry.carry_candidates([Link(pnu(OLD, 1), pnu(NEW, 2), "code_change", "code_derived", "")])
        cand = spark.createDataFrame([(c.successor_pnu, c.source_pnu, c.hop, c.path) for c in candidates],
                                     "successor_pnu string, source_pnu string, hop int, path string")
        frames = {panel.PARCEL_SOURCE: spark.createDataFrame([(pnu(NEW, n),) for n in (1, 2, 3)], "pnu string")}
        for name, rows in ROWS.items():
            types = {c["name"]: c["logical_type"] for c in load_lakehouse_contract(name)["columns"]}
            schema = ", ".join(f"{c} {spark_sql_type(types[c])}" for c in panel.BUILD_COLUMNS[name])
            frames[name] = spark.createDataFrame(rows, schema)
        args = argparse.Namespace(published_at_utc="2099-09-01T00:00:00Z", allow_intra_snapshot_duplicates=False)
        panel_rows = panel.build_panel(spark, args, frames, cand, {}, "snap")
        return args, frames, cand, panel_rows

    def kinds(self):
        import by_pnu_kind_silver_to_gold as job
        import by_pnu_kinds

        lanes = [lane for lane, kind in by_pnu_kinds.kinds().items() if kind.host == "parcel_by_pnu_gateway"]
        self.assertGreaterEqual(len(lanes), 6)
        return job, [job.KindSpec(lane) for lane in lanes]

    def test_every_kind_equals_its_panel_section_on_every_pnu(self):
        args, frames, cand, panel_rows = self.frames()
        job, specs = self.kinds()
        for spec in specs:
            with self.subTest(lane=spec.kind.lane):
                gold = job.build_gold_frame(self.spark, args, spec, {n: frames[n] for n in spec.sources}, cand, {}, "snap")
                self.assertEqual(job.parity_mismatches(spec, gold, panel_rows).count(), 0)
                rows = {r.pnu: r for r in gold.collect()}
                # Only PNUs that have the section; NEW 2 carries OLD 1's and names the path.
                self.assertEqual(sorted(rows), [pnu(NEW, 1), pnu(NEW, 2)])
                self.assertIsNone(rows[pnu(NEW, 1)].attached_via)
                self.assertEqual(rows[pnu(NEW, 2)].attached_via, "lineage:code_derived")
                count, _ = job.validate_gold_frame(spec, gold, 2)
                self.assertEqual(count, 2)

    def test_parity_refuses_a_drifted_panel(self):
        from pyspark.sql import functions as F

        args, frames, cand, panel_rows = self.frames()
        job, specs = self.kinds()
        for spec in specs:
            with self.subTest(lane=spec.kind.lane):
                gold = job.build_gold_frame(self.spark, args, spec, {n: frames[n] for n in spec.sources}, cand, {}, "snap")
                first = spec.kind.columns[0]
                drifted = panel_rows.withColumn(first, F.when(F.col("pnu") == pnu(NEW, 1), F.lit("[]"))
                                                .otherwise(F.col(first)))
                self.assertEqual([r.pnu for r in job.parity_mismatches(spec, gold, drifted).collect()], [pnu(NEW, 1)])
                unpathed = panel_rows.withColumn("attached_via_json", F.lit(None).cast("string"))
                self.assertEqual([r.pnu for r in job.parity_mismatches(spec, gold, unpathed).collect()], [pnu(NEW, 2)])

    def test_the_digest_ignores_lineage_columns(self):
        from pyspark.sql import functions as F

        args, frames, cand, _ = self.frames()
        job, specs = self.kinds()
        spec = specs[0]
        gold = job.build_gold_frame(self.spark, args, spec, {n: frames[n] for n in spec.sources}, cand, {}, "snap")
        first = {r.pnu: r.row_digest for r in gold.collect()}
        relabeled = (gold.drop("row_digest")
                     .withColumn("source_snapshot_id", F.concat(F.col("source_snapshot_id"), F.lit("x")))
                     .withColumn("row_digest", job.row_digest_column(spec)))
        self.assertEqual({r.pnu: r.row_digest for r in relabeled.collect()}, first)

    def test_the_zoning_kind_keeps_the_panels_verdict(self):
        args, frames, cand, _ = self.frames()
        job, specs = self.kinds()
        spec = next(s for s in specs if s.kind.lane == "parcel-zoning")
        rows = {r.pnu: r for r in job.build_gold_frame(self.spark, args, spec, {n: frames[n] for n in spec.sources},
                                                       cand, {}, "snap").collect()}
        self.assertEqual(json.loads(rows[pnu(NEW, 1)].zonings_json),
                         [{"zone_code": "UQA110", "zone_name": "제1종", "anchor_code": "UQA100", "inclusion_code": "1"}])
        self.assertEqual(json.loads(rows[pnu(NEW, 2)].zonings_json)[0]["zone_code"], "UQB100")

    def test_lineage_covers_every_contract_column(self):
        job, specs = self.kinds()
        for spec in specs:
            for carry_lineage in (True, False):
                self.assertEqual([c["output_column"] for c in job.column_lineage(spec, carry_lineage)],
                                 list(spec.gold_columns))


if __name__ == "__main__":
    unittest.main()
