"""The panel reads the served parcel edition only, with two editions side by side (root ADR-0148 §3).

Runs `parcel_panel_silver_to_gold.read_served_parcels` against a real Spark frame of
`silver.parcel_boundaries` holding two synthetic editions, every row current by the contract's
predicate. Nothing here stands in for the filter: the job's own read, predicate and edition filter
run, and the single-snapshot check the job applies next must accept what they return. With the
edition filter removed the frame holds both editions and that check refuses it (the second test
shows the refusal on the same input).
"""

import argparse
import os
import sys
import tempfile
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

SERVED, OTHER = "209906", "209902"


def source_contract(served):
    """A parcel source contract holding both editions, `served` the map's."""

    def edition(name):
        return {"provider_base_month": f"{name[:4]}-{name[4:]}",
                "extracted_on": {"earliest": f"{name[:4]}-{name[4:]}-10", "latest": f"{name[:4]}-{name[4:]}-10"},
                "handoff_prefix": f"silver-handoff/synthetic/edition={name}",
                "granularity_counts": {"sido": 0, "sigungu": 1},
                "objects": [{"object_key": f"bronze/source=vworldkr__parcel/{name}-97110.zip", "bytes": 1,
                             "dataset_name": f"LSMD_CONT_LDREG_97110_{name}", "region_code": "97110",
                             "granularity": "sigungu"}]}

    return {"schema_version": 2, "load_granularity": "sigungu", "snapshot_id_prefix": "vworldkr__parcel-",
            "served_edition": served, "handoff_suffix": ".jsonl.gz",
            "editions": {name: edition(name) for name in (OTHER, SERVED)}}


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class PanelReadsTheServedEditionSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("parcel-panel-served-edition").getOrCreate()
        cls.tmp = tempfile.TemporaryDirectory()
        cls.write_two_editions(Path(cls.tmp.name))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp.cleanup()

    @classmethod
    def write_two_editions(cls, root):
        """silver.parcel_boundaries as parquet: three parcels in each edition, all current."""

        import parcel_panel_silver_to_gold as job
        from platform_contracts import columns, load_lakehouse_contract, spark_sql_type

        contract = load_lakehouse_contract(job.PARCEL_SOURCE)
        rows = [(f"97110101001{n + 1:04d}0000", f"vworldkr__parcel-{name}")
                for name in (OTHER, SERVED) for n in range(3)]
        given = cls.spark.createDataFrame(rows, "pnu string, source_snapshot_id string")
        select = [
            column["name"] if column["name"] in ("pnu", "source_snapshot_id")
            else f"CAST(NULL AS {spark_sql_type(column['logical_type'])}) AS {column['name']}"
            for column in columns(contract)
        ]
        given.selectExpr(*select).write.parquet(str(root / job.source_table_name(job.PARCEL_SOURCE)))

    def args(self):
        return argparse.Namespace(input_mode="parquet", input_root=self.tmp.name, region_prefix=None)

    def test_only_the_served_edition_is_read_and_the_job_accepts_it(self):
        import parcel_panel_silver_to_gold as job

        for served, other in ((SERVED, OTHER), (OTHER, SERVED)):
            with self.subTest(served=served):
                parcels = job.read_served_parcels(self.spark, self.args(), {}, source_contract(served))
                ids = {row.source_snapshot_id for row in parcels.select("source_snapshot_id").collect()}
                self.assertEqual(ids, {f"vworldkr__parcel-{served}"})
                self.assertEqual(parcels.count(), 3)
                self.assertEqual(job.assert_single_snapshot(parcels, job.PARCEL_SOURCE), f"vworldkr__parcel-{served}")

    def test_both_editions_unfiltered_are_refused_by_the_check_that_follows(self):
        import parcel_panel_silver_to_gold as job
        from platform_contracts import current_row_predicate, load_lakehouse_contract

        predicate = current_row_predicate(load_lakehouse_contract(job.PARCEL_SOURCE))
        both = job.read_source(self.spark, self.args(), job.PARCEL_SOURCE, {}).where(predicate)
        self.assertEqual(both.count(), 6, "every row of both editions is current by the predicate")
        with self.assertRaisesRegex(ValueError, "2 distinct source_snapshot_id"):
            job.assert_single_snapshot(both, job.PARCEL_SOURCE)


if __name__ == "__main__":
    unittest.main()
