"""Tiny real lineage derivations in a disposable Hadoop catalog, without backend credentials."""
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import parcel_lineage_to_silver as job
from parcel_lineage_inputs import bind_input_views


@unittest.skipUnless(os.getenv("RUN_ICEBERG_TESTS") == "1", "requires the pinned Iceberg runtime")
class LineageSnapshotIcebergTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.directory = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master("local[1]").appName("lineage-snapshot-pins")
            .config("spark.sql.catalog.lineageproof", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lineageproof.type", "hadoop")
            .config("spark.sql.catalog.lineageproof.warehouse", cls.directory.name + "/warehouse")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        for namespace in ("silver", "staging", "reference"):
            cls.spark.sql(f"CREATE NAMESPACE lineageproof.{namespace}")
        cls.before, cls.after = "9999910100100010000", "9999920100100010000"
        cls.sources = {}
        fixtures = [
            ("boundaries_from", "silver.parcel_boundaries", "pnu STRING, source_snapshot_id STRING",
             f"('{cls.before}', 'june')", "('9999999999100010000', 'june')"),
            ("boundaries_to", "staging.parcel_boundaries", "pnu STRING, source_snapshot_id STRING",
             f"('{cls.after}', 'september')", "('9999999999100020000', 'september')"),
            ("codes", "reference.legal_dong_code_snapshot", "region_cd STRING, full_name STRING, status STRING, snapshot_date DATE",
             "('9999910100', '합성도 가구 갑동', '폐지', DATE '2099-09-29'), "
             "('9999920100', '합성시 가구 갑동', '존재', DATE '2099-09-29')",
             "('9999999999', 'changed head', '존재', DATE '2099-09-30')"),
            ("history", "silver.land_transfer_history",
             "pnu STRING, reason_code STRING, reason STRING, moved_at STRING, erased_at STRING, land_category_code STRING, area_m2 DOUBLE",
             f"('{cls.before}', '10', '지목변경', '2099-05-15', '', '17', 42.0), "
             f"('{cls.after}', '52', '행정관할구역변경', '2099-07-01', '', '17', 42.0)",
             "('9999999999100030000', '52', 'changed head', '2099-07-01', '', '17', 99.0)"),
            ("buildings_from", "silver.building_register_titles", "mgm_bldrgst_pk STRING, pnu STRING, source_snapshot_id STRING",
             f"('building-1', '{cls.before}', 'building-june')", "('changed', '9999999999100040000', 'building-june')"),
            ("buildings_to", "staging.building_register_titles", "mgm_bldrgst_pk STRING, pnu STRING, source_snapshot_id STRING",
             f"('building-1', '{cls.after}', 'building-september')", "('changed', '9999999999100050000', 'building-september')"),
        ]
        cls.heads = {}
        for role, table, schema, rows, changed_rows in fixtures:
            qualified = "lineageproof." + table
            cls.spark.sql(f"CREATE TABLE {qualified} ({schema}) USING iceberg")
            cls.spark.sql(f"INSERT INTO {qualified} VALUES {rows}")
            selected = str(cls.spark.sql(f"SELECT snapshot_id FROM {qualified}.refs WHERE name='main'").first()[0])
            cls.sources[role] = {"table": table, "snapshot_id": selected}
            cls.spark.sql(f"INSERT OVERWRITE {qualified} VALUES {changed_rows}")
            cls.heads[table] = cls.spark.sql(f"SELECT snapshot_id FROM {qualified}.refs WHERE name='main'").first()[0]
        cls.path = Path(cls.directory.name) / "inputs.json"
        cls.path.write_text(json.dumps(cls.sources), encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def argv(self, *extra):
        return [
            "--from-snapshot-id", "june", "--to-snapshot-id", "september",
            "--from-date", "2099-06-01", "--to-date", "2099-10-01",
            "--from-sido", "99", "--to-sido", "99",
            "--building-from-snapshot-id", "building-june", "--building-to-snapshot-id", "building-september",
            "--source-snapshots-path", str(self.path), "--iceberg-catalog-name", "lineageproof",
            "--iceberg-table", "lineage_smoke", "--iceberg-packages", "", *extra,
        ]

    def run_job(self, *extra):
        output = io.StringIO()
        with mock.patch.object(job, "assert_catalog_env"), \
             mock.patch.object(job, "apply_catalog_settings", side_effect=lambda builder, catalog: builder), \
             mock.patch.object(self.spark, "stop"), redirect_stdout(output):
            self.assertEqual(job.main(self.argv(*extra)), 0)
        return json.loads(output.getvalue().split("parcel-lineage-summary-json ")[1])

    def test_separate_pinned_tables_drive_real_readers_and_idempotent_appends(self):
        with mock.patch.object(job, "assert_catalog_env"):
            args = job.parse_args(self.argv())
            inputs = job.validate_args(args)
        views = bind_input_views(self.spark, "lineageproof", inputs)
        self.assertEqual(job.read_pnus(self.spark, "unused", "june", "99", table=views["boundaries_from"]), {self.before})
        self.assertEqual(job.read_pnus(self.spark, "unused", "september", "99", table=views["boundaries_to"]), {self.after})
        self.assertEqual(job.latest_code_snapshot(self.spark, views["codes"], "2099-10-01"), "2099-09-29")
        self.assertEqual(len(job.read_codes(self.spark, views["codes"], "2099-09-29")), 2)
        window = job.read_window_events(self.spark, views["history"], "99", "2099-06-01", "2099-10-01")
        self.assertEqual({event.pnu for event in window}, {self.after})
        facts = job.read_facts_before(self.spark, views["history"], "2099-06-01", {self.before})
        self.assertEqual([event.pnu for event in facts], [self.before])
        self.assertEqual(job.read_buildings(self.spark, views["buildings_from"], "building-june", "99"), {"building-1": self.before})
        self.assertEqual(job.read_buildings(self.spark, views["buildings_to"], "building-september", "99"), {"building-1": self.after})

        first = self.run_job()
        retry = self.run_job()
        self.assertTrue(first["appended"])
        self.assertFalse(retry["appended"])
        self.assertEqual(first["derivation_run_id"], retry["derivation_run_id"])
        self.assertEqual(first["code_snapshot_date"], "2099-09-29")
        self.assertEqual(first["input_provenance"], job.input_provenance(args, inputs, "2099-09-29"))
        rows = self.spark.table("lineageproof.silver.lineage_smoke").collect()
        self.assertEqual([(row.predecessor_pnu, row.successor_pnu, row.grade) for row in rows], [(self.before, self.after, "code_derived")])
        changed = self.run_job("--from-date", "2099-05-01")
        self.assertTrue(changed["appended"])
        self.assertNotEqual(changed["derivation_run_id"], first["derivation_run_id"])
        self.assertEqual(self.spark.table("lineageproof.silver.lineage_smoke").count(), 2)
        for table, snapshot in self.heads.items():
            self.assertEqual(self.spark.sql(f"SELECT snapshot_id FROM lineageproof.{table}.refs WHERE name='main'").first()[0], snapshot)

        missing = {role: dict(binding) for role, binding in self.sources.items()}
        missing["history"]["snapshot_id"] = "1"
        self.path.write_text(json.dumps(missing), encoding="utf-8")
        try:
            with self.assertRaises(Exception) as caught:
                self.run_job()
            self.assertIn("snapshot", str(caught.exception).lower())
            self.assertEqual(self.spark.table("lineageproof.silver.lineage_smoke").count(), 2)
        finally:
            self.path.write_text(json.dumps(self.sources), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
