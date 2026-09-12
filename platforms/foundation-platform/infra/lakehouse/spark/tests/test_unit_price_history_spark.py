"""Run with RUN_SPARK_TESTS=1 in the pinned Spark 3.5.6 container."""
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class UnitPriceHistorySparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, functions as F
        import building_panel_silver_to_gold as gold
        import unit_official_price as silver
        cls.F, cls.gold = F, gold
        cls.spark = (SparkSession.builder.master("local[2]").appName("unit-reference-date-fixture")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.pnu = "9999900000100000000"
        cls.spark.createDataFrame([
            ("A", cls.pnu, "101동", "101호"), ("B", cls.pnu, "101동", "102호"),
        ], "mgmt_key string,pnu string,dong_name string,ho_name string").createOrReplaceTempView("exclusive_source")
        cls.spark.sql(silver.DICTIONARY_SQL).createOrReplaceTempView("unit_dictionary")
        cls.spark.createDataFrame([
            ("A", " 20100101 ", "20100430", "37000000"),
            ("A", "20100101", "20100531", "36000000"),
            ("A", "20100601", "20100930", "35000000"),
            ("B", "20100601", "20100930", "20"),
            ("B", "20100601", "20100930", "100"),
        ], "mgmt_key string,base_date string,notice_date string,price_won string").createOrReplaceTempView("price_source")
        cls.spark.sql(silver.PRICES_SQL).createOrReplaceTempView("reference_prices")
        cls.prices = (cls.spark.sql(silver.JOIN_SQL).withColumn("sido", F.lit("99"))
                      .withColumn("source_snapshot_id", F.lit("synthetic-source-a"))
                      .withColumn("source_record_id", F.lit("unit-official-price/base-date-v2/99/fixture")))
        cls.units = cls.spark.createDataFrame([
            (cls.pnu, "UNIT-1", "BLDG-1", "", "101동", "101호", "101호", 1, "above_ground"),
            (cls.pnu, "UNIT-2", None, "", "101동", "102호", "102호", 1, "above_ground"),
        ], "pnu string,mgm_bldrgst_pk string,building_mgm_bldrgst_pk string,dong_join_name string,"
           "dong_name_raw string,unit_label_ko string,unit_name_raw string,floor_number int,floor_kind string")
        cls.titles = cls.spark.createDataFrame([
            (cls.pnu, "BLDG-1", "03000", None, 0.0, None, 0, None),
        ], "pnu string,mgm_bldrgst_pk string,purpose_code_raw string,structure_code_raw string,"
           "floor_area_m2 double,ground_floor_count int,basement_floor_count int,approval_year int")
        cls.areas = cls.spark.createDataFrame([
            ("AREA-1", "UNIT-1", "exclusive", 50.0, "공장", "", "above_ground"),
        ], "area_row_id string,mgm_bldrgst_pk string,area_kind string,area_m2 double,"
           "usage_name_raw string,structure_name_raw string,floor_kind string")
        cls.floors = cls.spark.createDataFrame([
            ("BLDG-1", "FLOOR-1", "above_ground", 1, 1, "1층"),
        ], "mgm_bldrgst_pk string,floor_row_id string,floor_kind string,floor_number int,"
           "floor_index int,floor_display_ko string")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_silver_parquet_to_gold_matches_shared_rust_fixture(self):
        from platform_contracts import column_names, load_lakehouse_contract, spark_sql_type
        contract = load_lakehouse_contract(self.gold.PRICE_SOURCE)
        self.assertEqual(tuple(self.prices.columns), column_names(contract))
        actual_types = dict(self.prices.dtypes)
        for column in contract["columns"]:
            self.assertEqual(actual_types[column["name"]], spark_sql_type(column["logical_type"]).lower())
        with tempfile.TemporaryDirectory() as temp:
            older = self.prices.withColumn("source_snapshot_id", self.F.lit("older-source"))
            self.prices.unionByName(older).write.parquet(temp + "/unit_official_price")
            args = SimpleNamespace(input_mode="parquet", input_root=temp, price_source_snapshot_id=None)
            mixed = self.gold.read_source(self.spark, args, self.gold.PRICE_SOURCE, {})
            with self.assertRaisesRegex(ValueError, "exactly one"):
                self.gold.assert_single_snapshot(mixed, self.gold.PRICE_SOURCE)
            args.price_source_snapshot_id = "synthetic-source-a"
            prices = self.gold.read_source(self.spark, args, self.gold.PRICE_SOURCE, {})
            self.assertEqual(self.gold.assert_single_snapshot(prices, self.gold.PRICE_SOURCE), "synthetic-source-a")
            self.assertEqual(mixed.count(), 6, "older Silver rows remain available")
            frames = {self.gold.PRICE_SOURCE: prices, self.gold.UNIT_SOURCE: self.units,
                      self.gold.TITLE_SOURCE: self.titles, self.gold.AREA_SOURCE: self.areas,
                      self.gold.FLOOR_SOURCE: self.floors}
            frame = self.gold.build_gold_panel_frame(frames, "synthetic-source-a", "2099-01-01T00:00:00Z", {})
            row = frame.first().asDict()
            fixture = Path(__file__).parent / "fixtures" / "building_panel_gold_row.json"
            if os.environ.get("UPDATE_BUILDING_PRICE_FIXTURE") == "1":
                fixture.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            self.assertEqual(row, json.loads(fixture.read_text(encoding="utf-8")))
            self.assertEqual(json.loads(row["buildings_json"])[0]["units"][0]["official_price_history"], [
                {"base_date": "20100601", "price_won": 35000000},
                {"base_date": "20100101", "price_won": 36000000},
            ])
            self.assertEqual(json.loads(row["unlinked_units_json"])[0]["official_price_history"], [
                {"base_date": "20100601", "price_won": 100},
            ])

    def test_duplicate_dates_invalid_dates_and_negative_values_fail_closed(self):
        titles = self.titles.withColumn("id", self.gold.canonical_id("building", self.F.col("mgm_bldrgst_pk")))
        invalid_frames = [
            self.prices.unionByName(self.prices),
            self.prices.withColumn("base_date", self.F.lit("2010-01-01")),
            self.prices.withColumn("base_date", self.F.lit(None).cast("string")),
            self.prices.withColumn("price_won", self.F.lit(-1)),
        ]
        for index, prices in enumerate(invalid_frames):
            with self.subTest(case=index), self.assertRaises(ValueError):
                self.gold.build_units(self.units, titles, self.areas, prices)


if __name__ == "__main__":
    unittest.main()
