"""실제 Spark에서 원천 부모 연결과 근거 없는 과거 연결의 차이를 확인한다."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

try:
    from pyspark.sql import SparkSession, functions as F
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class BuildingParentLinksTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import building_panel_silver_to_gold as gold
        import building_register_units_parcel_handoff as handoff
        cls.gold, cls.handoff = gold, handoff
        handoff.load_pyspark()
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("synthetic-building-parent-links")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def units(self, method="parent_key"):
        frame = self.spark.createDataFrame([
            ("9999900000100000001", "UNIT-1", "TITLE-1", "101동", "101동", "101호", "101호", 1, "above_ground"),
        ], "pnu string,mgm_bldrgst_pk string,building_mgm_bldrgst_pk string,dong_join_name string,"
           "dong_name_raw string,unit_label_ko string,unit_name_raw string,floor_number int,floor_kind string")
        return (frame.withColumn("unit_row_id", F.lit("synthetic-unit-row"))
                .withColumn("building_link_method", F.lit(method))
                .withColumn("building_link_source_record_id", F.lit("synthetic-basis#line=2"))
                .withColumn("building_link_input_sha256", F.lit("a" * 64))
                .withColumn("building_link_reason", F.lit(None).cast("string"))
                .withColumn("normalization_application_id", F.lit(None).cast("string")))

    def test_catalog_handoff_does_not_promote_name_inference(self):
        units = self.units("canonical_dong").withColumnRenamed("mgm_bldrgst_pk", "register_pk")
        areas = self.spark.createDataFrame([], "register_pk string,exclusive_area_m2 double,usage_name string,structure_name string")
        row = self.handoff.build_handoff_frame(units, areas, ["register_pk", "building_register_pk"]).first()
        self.assertEqual(row.register_pk, "UNIT-1")
        self.assertIsNone(row.building_register_pk)

    def test_explicit_parent_is_not_lost_when_the_unit_states_another_parcel(self):
        titles = self.spark.createDataFrame([
            ("9999900000100000002", "TITLE-1", "synthetic-title-id"),
        ], "pnu string,mgm_bldrgst_pk string,id string")
        areas = self.spark.createDataFrame([], "area_row_id string,mgm_bldrgst_pk string,area_kind string,area_m2 double,usage_name_raw string,structure_name_raw string")
        prices = self.spark.createDataFrame([], "mgm_bldrgst_pk string,base_date string,price_won long")
        row = self.gold.build_units(self.units(), titles, areas, prices).first()
        self.assertEqual(row.building_id, "synthetic-title-id")
        self.assertEqual(row.pnu, "9999900000100000001")

    def test_verified_parent_missing_from_titles_stays_visible_as_unlinked(self):
        import json
        titles = self.spark.createDataFrame([
            (None, "TITLE-1", "03000", None, 0.0, None, 0, None),
        ], "pnu string,mgm_bldrgst_pk string,purpose_code_raw string,structure_code_raw string,"
           "floor_area_m2 double,ground_floor_count int,basement_floor_count int,approval_year int")
        areas = self.spark.createDataFrame([], "area_row_id string,mgm_bldrgst_pk string,area_kind string,area_m2 double,usage_name_raw string,structure_name_raw string,floor_kind string")
        floors = self.spark.createDataFrame([], "mgm_bldrgst_pk string,floor_row_id string,floor_kind string,floor_number int,floor_index int,floor_display_ko string")
        prices = self.spark.createDataFrame([], "mgm_bldrgst_pk string,base_date string,price_won long")
        frames = {self.gold.UNIT_SOURCE: self.units(), self.gold.TITLE_SOURCE: titles,
                  self.gold.AREA_SOURCE: areas, self.gold.FLOOR_SOURCE: floors, self.gold.PRICE_SOURCE: prices}
        row = self.gold.build_gold_panel_frame(frames, "synthetic-source", "2099-01-01T00:00:00Z", {}).first()
        self.assertEqual(json.loads(row.buildings_json), [])
        unit = json.loads(row.unlinked_units_json)[0]
        self.assertIsNone(unit["building_id"])
        self.assertEqual(unit["building_register_pk"], "TITLE-1")
        self.assertEqual(unit["building_link_evidence"]["building_link_method"], "parent_key")

    def test_ingestion_refuses_incomplete_or_mixed_evidence_and_blank_parent(self):
        from building_link_evidence import invalid_building_link_evidence, verified_building_links
        invalid = [
            ("building_link_method", "canonical_dong"),
            ("building_link_source_record_id", None),
            ("building_link_input_sha256", "not-a-sha256"),
            ("building_link_reason", "parent_conflict"),
            ("normalization_application_id", "11111111-1111-4111-8111-111111111111"),
            ("building_mgm_bldrgst_pk", ""),
            ("building_mgm_bldrgst_pk", " TITLE-1"),
        ]
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                frame = self.units().withColumn(field, F.lit(value).cast("string"))
                self.assertTrue(frame.select(invalid_building_link_evidence().alias("bad")).first().bad)
                row = verified_building_links(frame).first()
                self.assertIsNone(row.building_mgm_bldrgst_pk)
                self.assertIsNotNone(row.building_link_evidence.building_link_reason)
        self.assertFalse(self.units().select(invalid_building_link_evidence().alias("bad")).first().bad)

    def test_unresolved_and_approved_withdrawal_survive_handoff(self):
        from building_link_evidence import invalid_building_link_evidence, verified_building_links
        frame = (self.units("unresolved").withColumn("building_mgm_bldrgst_pk", F.lit(None).cast("string"))
                 .withColumn("building_link_reason", F.lit("parent_missing")))
        self.assertFalse(frame.select(invalid_building_link_evidence().alias("bad")).first().bad)
        row = verified_building_links(frame).first()
        self.assertEqual(row.building_link_evidence.building_link_reason, "parent_missing")
        approved = (frame.withColumn("building_link_reason", F.lit(None).cast("string"))
                    .withColumn("building_link_source_record_id", F.lit(None).cast("string"))
                    .withColumn("building_link_input_sha256", F.lit(None).cast("string"))
                    .withColumn("normalization_application_id", F.lit("11111111-1111-4111-8111-111111111111")))
        self.assertFalse(approved.select(invalid_building_link_evidence().alias("bad")).first().bad)
        row = verified_building_links(approved).first()
        self.assertIsNone(row.building_mgm_bldrgst_pk)
        self.assertEqual(row.building_link_evidence.normalization_application_id, "11111111-1111-4111-8111-111111111111")

    def test_silver_validation_executes_evidence_gate(self):
        import silver_scalar_handoff_to_lakehouse as ingest
        from platform_contracts import load_lakehouse_contract, spark_sql_type
        contract = load_lakehouse_contract("silver.building_register_units")
        frame = self.units("canonical_dong")
        for column in contract["columns"]:
            if column["name"] not in frame.columns:
                value = "synthetic" if column["required"] else None
                frame = frame.withColumn(column["name"], F.lit(value).cast(spark_sql_type(column["logical_type"])))
        metrics = ingest.collect_quality_metrics(frame, contract, F)
        self.assertEqual(metrics["invalid_building_link_evidence_count"], 1)
        with self.assertRaisesRegex(ValueError, "building link lacks"):
            ingest.assert_quality_metrics(frame, contract, metrics, F)

    def test_emitted_gzip_rows_keep_named_evidence_and_explicit_null_parent(self):
        import gzip
        import json
        from types import SimpleNamespace
        from unittest.mock import patch
        import building_unit_building_link_handoff as recovery
        from building_link_evidence import verified_building_links

        recovery.load_pyspark()
        units = self.units().withColumnRenamed("mgm_bldrgst_pk", "register_pk")
        null_unit = (units.withColumn("register_pk", F.lit("UNIT-2"))
                     .withColumn("building_mgm_bldrgst_pk", F.lit(None).cast("string")))
        units = units.unionByName(null_unit)
        areas = self.spark.createDataFrame([], "register_pk string,exclusive_area_m2 double,usage_name string,structure_name string")
        catalog = self.handoff.build_handoff_frame(units, areas, self.handoff.load_handoff_contract()["columns"])
        pairs = verified_building_links(units).withColumnRenamed("building_mgm_bldrgst_pk", "building_register_pk")
        for job, frame, args in [(self.handoff, catalog, []),
                                 (recovery, pairs, [recovery.load_handoff_contract()["columns"]])]:
            captured = []
            client = SimpleNamespace(put_object=lambda **kwargs: captured.append(kwargs))
            with self.subTest(job=job.__name__), patch.dict("sys.modules", {"boto3": SimpleNamespace(client=lambda *a, **k: client)}):
                manifest = job.write_objects(frame, {}, "synthetic", "synthetic", ".jsonl.gz", *args)
                self.assertEqual(sum(row["rows"] for row in manifest), 2)
                rows = [json.loads(line) for line in gzip.decompress(captured[0]["Body"]).splitlines()]
                for row in rows:
                    self.assertIsInstance(row["building_link_evidence"], dict)
                    self.assertEqual(row["building_link_evidence"]["building_link_input_sha256"], "a" * 64)
                self.assertIsNone(next(row for row in rows if row["register_pk"] == "UNIT-2")["building_register_pk"])


if __name__ == "__main__":
    unittest.main()
