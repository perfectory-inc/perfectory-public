"""Every building kind is the building panel's content in its own shape (root ADR-0193).

Builds the panel and each building kind the contract names from the same synthetic registers and
holds every kind equal to the panel's view of it on every PNU (the parity ADR-0193 §6-2 reads), and
proves the parity check refuses a drifted panel. PNUs and register keys are synthetic.
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

try:
    from pyspark.sql import SparkSession, functions as F
    HAS_SPARK = hasattr(SparkSession, "builder")
except ImportError:
    HAS_SPARK = False

A, B = "9999900000100000001", "9999900000100000002"


@unittest.skipUnless(HAS_SPARK, "requires the pinned Spark runtime")
class BuildingKindSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import building_panel_silver_to_gold as panel
        import by_pnu_kind_silver_to_gold as job
        import by_pnu_kinds
        cls.panel, cls.job = panel, job
        cls.lanes = [lane for lane, kind in by_pnu_kinds.kinds().items() if kind.host == "building_by_pnu_gateway"]
        cls.spark = (SparkSession.builder.master("local[2]").appName("synthetic-building-kinds")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self):
        spark, panel = self.spark, self.panel
        titles = spark.createDataFrame([
            (A, "TITLE-1", "03000", "21", 120.5, 3, 1, 2001),
            (A, "TITLE-2", "04000", None, 80.0, 1, 0, None),
        ], "pnu string,mgm_bldrgst_pk string,purpose_code_raw string,structure_code_raw string,"
           "floor_area_m2 double,ground_floor_count int,basement_floor_count int,approval_year int")
        floors = spark.createDataFrame([
            ("TITLE-1", "FLOOR-1", "above_ground", 1, 1, "1층"),
            ("TITLE-1", "FLOOR-2", "rooftop", None, 9, "옥탑"),
        ], "mgm_bldrgst_pk string,floor_row_id string,floor_kind string,floor_number int,floor_index int,"
           "floor_display_ko string")
        units = spark.createDataFrame([
            (A, "UNIT-1", "TITLE-1", "가동", "가동", "101호", "101호", 1, "above_ground"),
            (B, "UNIT-2", None, "", "", "201호", "201호", 2, "above_ground"),
        ], "pnu string,mgm_bldrgst_pk string,building_mgm_bldrgst_pk string,dong_join_name string,"
           "dong_name_raw string,unit_label_ko string,unit_name_raw string,floor_number int,floor_kind string")
        units = (units.withColumn("unit_row_id", F.concat(F.lit("row-"), F.col("mgm_bldrgst_pk")))
                 .withColumn("building_link_method", F.when(F.col("building_mgm_bldrgst_pk").isNotNull(), F.lit("parent_key")))
                 .withColumn("building_link_source_record_id", F.when(F.col("building_mgm_bldrgst_pk").isNotNull(),
                                                                      F.lit("synthetic-basis#line=2")))
                 .withColumn("building_link_input_sha256", F.when(F.col("building_mgm_bldrgst_pk").isNotNull(), F.lit("a" * 64)))
                 .withColumn("building_link_reason", F.lit(None).cast("string"))
                 .withColumn("normalization_application_id", F.lit(None).cast("string")))
        areas = spark.createDataFrame([
            ("AREA-1", "UNIT-1", "exclusive", 59.9, "주거", "철근", "above_ground"),
            ("AREA-2", "TITLE-1", "common", 10.0, "물탱크", "철근", "rooftop"),
        ], "area_row_id string,mgm_bldrgst_pk string,area_kind string,area_m2 double,usage_name_raw string,"
           "structure_name_raw string,floor_kind string")
        return {panel.TITLE_SOURCE: titles, panel.FLOOR_SOURCE: floors, panel.UNIT_SOURCE: units,
                panel.AREA_SOURCE: areas}

    def build(self, lane):
        import argparse
        frames = self.frames()
        spec = self.job.KindSpec(lane)
        args = argparse.Namespace(published_at_utc="2099-01-01T00:00:00Z")
        gold = self.job.build_gold_frame(self.spark, args, spec, frames, None, {}, "snap")
        panel_rows = self.panel.build_gold_panel_frame(frames, "snap", "2099-01-01T00:00:00Z", {})
        return spec, frames, gold, panel_rows

    def test_every_building_kind_equals_the_panels_view_of_it(self):
        self.assertEqual(sorted(self.lanes), ["building-floors", "building-titles", "building-units"])
        for lane in self.lanes:
            with self.subTest(lane=lane):
                spec, frames, gold, panel_rows = self.build(lane)
                types = self.job.entry_types(spec, frames, {})
                self.assertEqual(self.job.parity_mismatches(spec, gold, panel_rows, types).count(), 0)

    def test_each_kind_carries_only_its_own_content_and_only_where_it_has_some(self):
        rows = {lane: {r.pnu: r for r in self.build(lane)[2].collect()} for lane in self.lanes}
        titles = json.loads(rows["building-titles"][A].buildings_json)
        self.assertEqual([b["register_pk"] for b in titles], ["TITLE-1", "TITLE-2"])
        self.assertTrue(all("floors" not in b and "units" not in b for b in titles))
        self.assertTrue(titles[0]["has_rooftop"])
        self.assertNotIn(B, rows["building-titles"], "a PNU with only an unlinked unit has no titles")
        floors = json.loads(rows["building-floors"][A].floors_json)
        self.assertEqual([(f["register_pk"], len(f["floors"])) for f in floors], [("TITLE-1", 2)])
        # Root ADR-0252: a units row is one building of a PNU; a PNU's unlinked units are one row
        # under the nil building id. A building without units has no row.
        units = rows["building-units"]
        self.assertEqual(units[A].building_id, titles[0]["id"])
        self.assertEqual([u["register_pk"] for u in json.loads(units[A].units_json)], ["UNIT-1"])
        self.assertEqual(units[B].building_id, self.panel.NO_BUILDING_ID)
        self.assertEqual([u["register_pk"] for u in json.loads(units[B].units_json)], ["UNIT-2"])
        self.assertEqual(sorted((r.pnu, r.building_id) for r in self.build("building-units")[2].collect()),
                         sorted([(A, titles[0]["id"]), (B, self.panel.NO_BUILDING_ID)]))

    def test_a_kind_carries_the_sources_values_not_a_screens_words(self):
        # Root ADR-0193: the web writes "지하 1층"/"옥탑" and joins the rooftop usages; a kind does not.
        rows = {lane: {r.pnu: r for r in self.build(lane)[2].collect()} for lane in self.lanes}
        titles = json.loads(rows["building-titles"][A].buildings_json)
        self.assertEqual(titles[0]["rooftop_usage"], ["물탱크"])
        floors = json.loads(rows["building-floors"][A].floors_json)[0]["floors"]
        self.assertTrue(all("floor_display_ko" not in f and "floor_kind" in f for f in floors))
        unit = json.loads(rows["building-units"][A].units_json)[0]
        self.assertNotIn("floor_label", unit)
        self.assertEqual(set(unit) & {"floor_kind", "floor_number"}, {"floor_kind", "floor_number"})
        # The source's normalized unit number, as a number; the web sorts by it (no parsing of ho_name).
        self.assertEqual(unit["ho_number"], 101)

    def test_parity_refuses_a_drifted_panel(self):
        for lane in self.lanes:
            with self.subTest(lane=lane):
                spec, frames, gold, panel_rows = self.build(lane)
                types = self.job.entry_types(spec, frames, {})
                drifted = panel_rows.withColumn("buildings_json", F.lit("[]")).withColumn("unlinked_units_json", F.lit("[]"))
                self.assertGreater(self.job.parity_mismatches(spec, gold, drifted, types).count(), 0)

    def test_the_panel_is_the_kinds_put_together(self):
        # The panel composes the same parts: a building is its title fields, then floors, then units.
        _, _, _, panel_rows = self.build(self.lanes[0])
        building = {r.pnu: r for r in panel_rows.collect()}[A]
        first = json.loads(building.buildings_json)[0]
        self.assertEqual(list(first)[-2:], ["floors", "units"])
        self.assertEqual(list(first)[:len(self.panel.TITLE_FIELDS)], self.panel.TITLE_FIELDS)


if __name__ == "__main__":
    unittest.main()
