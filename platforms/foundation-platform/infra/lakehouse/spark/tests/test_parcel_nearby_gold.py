"""Parcel nearby by-PNU kind (root ADR-0465).

Synthetic PNUs (prefix 99999) and synthetic coordinates. Pins: each parcel's nearest airport, station,
school and university by Sedona's ST_KNN with the spheroid distance in metres; a coordinate outside
Korea or not a number is no candidate; a parcel counts the stores in the H3 rings around its cell and
not a store beyond them; it lists the 고속도로 출입시설 of its 시군구; a changed store names the
parcels within its rings and no other; the written JSON's paths are exactly the registry's.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import parcel_nearby_silver_to_gold as job  # noqa: E402

LANE = "parcel-nearby"
GOLD_COLUMNS = ("pnu", "nearby_json", "attached_via", "row_digest", "source_snapshot_id", "published_at_utc")
NEAR, FAR = "9999910100" + "100010000", "9999920100" + "100020000"
# About 1.1 km apart at this latitude, 0.01 degrees of latitude; FAR is 0.5 degrees north.
NEAR_AT, FAR_AT = (127.0, 37.0), (127.0, 37.5)


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_facility_files_rebuild_whole(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, *job.POINTS, job.UNIVERSITY_SOURCE, job.STORE_SOURCE,
                                         job.HIGHWAY_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor), ({}, job.PARCEL_SOURCE))
        # Only a parcel or a store maps to the PNUs it changes; every other input rebuilds whole.
        self.assertEqual(set(required) - set(whole), {job.PARCEL_SOURCE, job.STORE_SOURCE})

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns), (job.JOB_NAME, job.SECTION, ("nearby_json",)))
        self.assertFalse(kind.follows_host_panel)

    def test_the_headers_are_the_file_contract_s(self):
        contract = json.loads((Path(job.__file__).resolve().parents[2] / "contracts"
                               / "data-go-kr-file-data.contract.json").read_text(encoding="utf-8"))["files"]

        def header(source):
            file = contract[source.removeprefix("silver.")]
            return set(file.get("header") or file["member"]["header"])

        for source, (_, latitude, longitude, fields) in job.POINTS.items():
            self.assertLessEqual({latitude, longitude, *fields.values()}, header(source), source)
        self.assertLessEqual(set(job.UNIVERSITY[1].values()), header(job.UNIVERSITY_SOURCE))
        self.assertLessEqual({job.STORE_LATITUDE, job.STORE_LONGITUDE}, header(job.STORE_SOURCE))
        self.assertLessEqual(set(job.HIGHWAY_FIELDS.values()), header(job.HIGHWAY_SOURCE))

    def test_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertEqual(src.count("panel.renumbered("), 2)

    def test_a_change_maps_to_pnus_only_for_parcels_and_stores(self):
        class Frame:
            def __init__(self, name):
                self.name = name

            def select(self, *_):
                return self

            def unionByName(self, other):
                return Frame(f"{self.name}+{other.name}")

            def distinct(self):
                return self

        def run(changed):
            return job.affected_pnus(changed, parcel_change=lambda: Frame("parcels"),
                                     store_change=lambda: Frame("stores")).name

        self.assertEqual(run([job.STORE_SOURCE]), "stores")
        self.assertEqual(run([job.PARCEL_SOURCE, job.STORE_SOURCE]), "parcels+stores")
        with self.assertRaises(ValueError):
            run([job.AIRPORT_SOURCE])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        from lakehouse_engine import with_sedona

        cls.spark = with_sedona(SparkSession.builder.master("local[2]").appName("parcel-nearby-gold")).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def parcels(self, rows):
        frame = self.spark.createDataFrame(
            [(pnu, f"POLYGON(({x} {y}, {x + 0.0002} {y}, {x + 0.0002} {y + 0.0002}, {x} {y + 0.0002}, {x} {y}))")
             for pnu, (x, y) in rows], "pnu string, wkt string")
        return frame.selectExpr("pnu", "ST_AsBinary(ST_GeomFromWKT(wkt)) AS geometry_wkb")

    def points(self, fields, rows):
        return self.spark.createDataFrame(rows, ", ".join(f"{c} string" for c in ("row_key", "lat", "lon", *fields)))

    def frames(self, stores=(), highway=()):
        point = {source: self.points(fields, []) for source, (_, fields) in job.NEAREST.items()}
        point[job.AIRPORT_SOURCE] = self.points(("name",), [
            ("a1", "37.01", "127.0", "북쪽공항"), ("a2", "37.6", "127.0", "먼공항"), ("a3", "0", "0", "좌표오류"),
            ("a4", "", "127.0", "빈좌표")])
        point[job.SUBWAY_SOURCE] = self.points(("name", "route_name"), [("s1", "37.0", "127.001", "역", "1호선")])
        stores_frame = self.spark.createDataFrame(list(stores), "row_key string, lat string, lon string")
        highway_frame = self.spark.createDataFrame(list(highway), "row_key string, sigungu string, route_name string, "
                                                                  "facility_name string")
        return {job.PARCEL_SOURCE: self.parcels([(NEAR, NEAR_AT), (FAR, FAR_AT)]), **point,
                job.STORE_SOURCE: stores_frame, job.HIGHWAY_SOURCE: highway_frame}

    def documents(self, frames):
        gold = job.gold_rows(frames, "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: json.loads(r.nearby_json) for r in gold.collect()}, gold

    def test_the_nearest_airport_by_spheroid_and_bad_coordinates_are_no_candidates(self):
        documents, gold = self.documents(self.frames())
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)
        near, far = documents[NEAR], documents[FAR]
        self.assertEqual((near["airport"]["name"], far["airport"]["name"]), ("북쪽공항", "먼공항"))
        self.assertTrue(1000 < near["airport"]["distance_m"] < 1200, near["airport"])
        self.assertEqual(near["subway_station"]["route_name"], "1호선")
        self.assertIsNone(near["school"])

    def test_stores_count_within_the_rings_only(self):
        stores = [("r1", "37.0001", "127.0001"), ("r2", "37.0003", "127.0002"), ("r3", "37.2", "127.0"),
                  ("r4", "99", "127.0")]
        documents, _ = self.documents(self.frames(stores=stores))
        self.assertEqual((documents[NEAR]["commercial_store_count"], documents[FAR]["commercial_store_count"]), (2, 0))

    def test_highway_access_points_of_the_parcel_s_sigungu(self):
        highway = [("h1", "99999", "경부선", "가IC"), ("h2", "99999", "경부선", "가IC"), ("h3", "88888", "다른선", "나IC")]
        documents, _ = self.documents(self.frames(highway=highway))
        listed = documents[NEAR]["highway_access_points"]
        self.assertEqual((listed["total"], listed["records"]), (1, [{"route_name": "경부선", "facility_name": "가IC"}]))

    def test_a_changed_store_names_the_parcels_within_its_rings(self):
        points = job.parcel_points(self.parcels([(NEAR, NEAR_AT), (FAR, FAR_AT)]))
        cells = job.store_cells(self.spark.createDataFrame([("r1", "37.0001", "127.0001")],
                                                           "row_key string, lat string, lon string"))
        self.assertEqual({r.pnu for r in job.store_pnus(cells.select("cell"), points).collect()}, {NEAR})

    def test_restricting_keeps_every_facility(self):
        frames = self.frames()
        kept = job.restrict_inputs(frames, self.spark.createDataFrame([(NEAR,)], "pnu string"))
        self.assertEqual(kept[job.PARCEL_SOURCE].count(), 1)
        self.assertEqual(kept[job.AIRPORT_SOURCE].count(), frames[job.AIRPORT_SOURCE].count())

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        point = {source: self.points(fields, [("x", "37.0", "127.0", *["v"] * len(fields))])
                 for source, (_, fields) in job.NEAREST.items()}
        frames = {**self.frames(highway=[("h1", "99999", "선", "IC")]), **point}
        documents, _ = self.documents(frames)
        written = by_pnu_kinds.json_paths(documents[NEAR], "nearby")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_nearby_by_pnu") if p.startswith("nearby.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        first = job.gold_rows(self.frames(), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).orderBy("pnu").first()
        again = job.gold_rows(self.frames(), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).orderBy("pnu").first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
