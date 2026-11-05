"""Regulation zones by-PNU kind (root ADR-0362, ADR-0375).

Synthetic PNUs (prefix 99999) and layers (D998, D999). Pins: a parcel's document lists the zones
whose polygon covers part of its own, in the provider's words, with each zone row's release date; a
zone that only touches the parcel, or covers a sliver under the bound, is not listed; only served
parcels have a document; the written JSON's paths are exactly the registry's (root ADR-0336); a run
recomputes the parcels a changed zone's old or new box reaches, the parcels that changed, and the
parcels under a changed code, and nothing else.
"""

import json
import math
import os
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import regulation_zones_silver_to_gold as job  # noqa: E402

DONG = "9999910100"
LANE = "parcel-regulation-zones"
GOLD_COLUMNS = ("pnu", "regulation_zones_json", "zone_total", "attached_via", "row_digest", "source_snapshot_id",
                "published_at_utc")
# The repository-reserved synthetic coordinates (public-fixture-safety); a cell line (0.0025°)
# runs 0.0014° east and north of the origin.
X0, Y0 = 127.1236, 36.1236


def pnu(main):
    return f"{DONG}1{main:04d}0000"


def square(x, y, side):
    """A square of `side` degrees at (X0 + x, Y0 + y), as WKB, and its box."""
    ring = [(X0 + x, Y0 + y), (X0 + x + side, Y0 + y), (X0 + x + side, Y0 + y + side), (X0 + x, Y0 + y + side)]
    closed = [*ring, ring[0]]
    wkb = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(closed)) + b"".join(struct.pack("<dd", *p) for p in closed)
    return bytearray(wkb), X0 + x, Y0 + y, X0 + x + side, Y0 + y + side


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_the_zones_the_served_parcels_and_the_code_change_table(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.ZONE_SOURCE, job.CODE_CHANGE_SOURCE})
        # No input rebuilds the kind whole: a code change reaches the parcels under its codes.
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, ()))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("regulation_zones_json", "zone_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_parse_args_accepts_the_section_coverage_flags_the_harness_passes(self):
        args = job.parse_args([
            "--lane", LANE, "--iceberg-snapshot-id", "1", "--source-snapshots-path", "/x", "--validate-only",
            "--section-coverage-snapshot", "123", "--max-section-coverage-drop", "0.05",
            "--section-coverage-min-pnus", "1000"])
        self.assertEqual((args.section_coverage_snapshot, args.region_prefix), ("123", None))

    def test_the_parcel_universe_is_the_served_edition(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)

    def test_the_zone_and_parcel_shapes_go_through_the_kernel_on_the_executors(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("addPyFile(zone_overlap.__file__)", src)
        rings, area = job.parcel_shape(square(0, 0, 0.001)[0])
        self.assertEqual(len(rings), 1)
        self.assertAlmostEqual(area, (0.001 * 111195.08) ** 2 * math.cos(math.radians(Y0 + 0.0005)), delta=50)
        self.assertEqual(job.parcel_shape(struct.pack("<BIdd", 1, 1, X0, Y0)), ([], 0.0))

    def test_one_cell_s_pairs_carry_both_areas_in_square_metres(self):
        from collections import namedtuple

        import zone_overlap

        parcel = namedtuple("Parcel", "pnu rings area_m2 latitude")
        piece = namedtuple("Piece", "layer_code zone_feature_id sigungu_code rings")
        inside_rings, inside_area = job.parcel_shape(square(0.0002, 0.0002, 0.0002)[0])
        apart_rings, apart_area = job.parcel_shape(square(0.0012, 0.0002, 0.0002)[0])
        zone = zone_overlap.rings_of(square(0, 0, 0.0010)[0])
        i, j = int(X0 // job.CELL_DEGREES), int(Y0 // job.CELL_DEGREES)
        parcels = [parcel(pnu(1), inside_rings, inside_area, Y0), parcel(pnu(2), apart_rings, apart_area, Y0)]
        pairs = job.cell_pairs(i, j, parcels, [piece("D999", "99999-0001", "99999", job.flat(zone))])
        self.assertEqual(len(pairs), 1)
        found, layer, feature, sigungu, overlap, area = pairs[0]
        self.assertEqual((found, layer, feature, sigungu), (pnu(1), "D999", "99999-0001", "99999"))
        # The test hands the cell latitude Y0, the area is measured at the parcel centre.
        self.assertAlmostEqual(overlap, area, delta=area * 1e-5)

    def test_the_release_date_is_the_snapshot_s_date(self):
        import re

        self.assertEqual(re.search(job.RELEASE_DATE, "vworldkr-land-use-zone:20261009").groups(), ("2026", "10", "09"))

    def test_the_affected_pnus_come_only_from_the_inputs_that_changed(self):
        calls = []

        class Frame:
            def __init__(self, name):
                self.name = name

            def select(self, *_):
                return self

            def unionByName(self, other):
                return Frame(f"{self.name}+{other.name}")

            def distinct(self):
                return self

        def part(name):
            return lambda: calls.append(name) or Frame(name)

        def run(changed, appended):
            calls.clear()
            frame = job.affected_pnus(changed, appended, appended_zones=part("appended"), changed_zones=part("zones"),
                                      parcel_change=part("parcels"), code_change=part("codes"))
            return frame.name, list(calls)

        # A Silver release appends only the changed zones' rows: read those, never diff the table.
        self.assertEqual(run([job.ZONE_SOURCE], [job.ZONE_SOURCE]), ("appended", ["appended"]))
        self.assertEqual(run([job.ZONE_SOURCE], []), ("zones", ["zones"]))
        self.assertEqual(run([job.PARCEL_SOURCE, job.ZONE_SOURCE], [job.ZONE_SOURCE]),
                         ("appended+parcels", ["appended", "parcels"]))
        self.assertEqual(run([job.CODE_CHANGE_SOURCE], []), ("codes", ["codes"]))
        with self.assertRaises(ValueError):
            run([], [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        import zone_overlap

        cls.spark = SparkSession.builder.master("local[2]").appName("regulation-zones-gold").getOrCreate()
        cls.spark.sparkContext.addPyFile(zone_overlap.__file__)
        cls.spark.sparkContext.addPyFile(job.__file__)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    PARCELS = {
        # Across the cell line 0.0014° east of the origin: its overlap is summed over two pieces.
        "inside": (pnu(1), square(0.0013, 0.0002, 0.0002)),
        "touching": (pnu(2), square(0.0020, 0.0002, 0.0002)),
        "sliver": (pnu(3), square(0.0019999, 0.0006, 0.0002)),
        "across": (pnu(4), square(0.0007, 0.0032, 0.0006)),
    }

    def zone(self, layer, feature, shape, release="20261009", code="UQA100", name="제1종일반주거지역", sigungu="99999"):
        wkb, *box = shape
        return (layer, feature, sigungu, f"국토계획/{layer}", code, name, wkb, *box, f"vworldkr-land-use-zone:{release}")

    def frames(self, zones, parcels=None):
        zone_schema = ("layer_code string, zone_feature_id string, sigungu_code string, layer_name string, zone_code string, "
                       "zone_name string, geometry_wkb binary, bbox_min_x double, bbox_min_y double, "
                       "bbox_max_x double, bbox_max_y double, source_snapshot_id string")
        parcel_schema = ("pnu string, geometry_wkb binary, bbox_min_x double, bbox_min_y double, "
                         "bbox_max_x double, bbox_max_y double")
        parcels = parcels or list(self.PARCELS.values())
        return {job.ZONE_SOURCE: self.spark.createDataFrame(zones, zone_schema),
                job.PARCEL_SOURCE: self.spark.createDataFrame([(p, *shape) for p, shape in parcels], parcel_schema)}

    def documents(self, zones, parcels=None):
        gold = job.gold_rows(self.frames(zones, parcels), "snap-1", "2026-11-04T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.regulation_zones_json), r.zone_total) for r in gold.collect()}, gold

    def test_a_parcel_lists_the_zones_that_cover_part_of_it_and_not_those_that_only_touch_it(self):
        # One zone spans two cells (0.0025 degrees) and holds the first parcel; the second parcel only
        # touches its east edge, the third lies over that edge by a 1e-7 degree sliver (0.2 m²).
        big = self.zone("D999", "99999-0001", square(0, 0, 0.0020))
        road = self.zone("D998", "99999-0002", square(0.0008, 0.0031, 0.0010), release="20260609",
                         code="UQS110", name="도로")
        documents, gold = self.documents([big, road])
        self.assertEqual(set(documents), {pnu(1), pnu(4)})
        inside, total = documents[pnu(1)]
        self.assertEqual(total, 1)
        self.assertEqual(inside["zones"], [{
            "zone_id": "D999:99999-0001:99999", "layer_code": "D999", "layer_name": "국토계획/D999",
            "zone_code": "UQA100", "zone_name": "제1종일반주거지역", "release_date": "2026-10-09"}])
        across, _ = documents[pnu(4)]
        self.assertEqual([(z["zone_id"], z["release_date"]) for z in across["zones"]],
                         [("D998:99999-0002:99999", "2026-06-09")])
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_one_area_split_at_a_sigungu_boundary_is_two_zones(self):
        # 2026-11-03: the provider ships an area split at a 시군구 boundary as two polygons sharing
        # A1 (308 pairs in D036 of 20261009); keyed without the 시군구 they were one zone.
        west = self.zone("D999", "99999-0001", square(0, 0, 0.0020), sigungu="99998")
        east = self.zone("D999", "99999-0001", square(0, 0, 0.0020), sigungu="99999", name="합성 동쪽")
        documents, _ = self.documents([west, east])
        listed, total = documents[pnu(1)]
        self.assertEqual(total, 2)
        self.assertEqual(sorted(z["zone_id"] for z in listed["zones"]),
                         ["D999:99999-0001:99998", "D999:99999-0001:99999"])

    def test_zones_are_listed_by_layer_and_bounded(self):
        many = [self.zone("D999", f"99999-{n:04d}", square(0, 0, 0.0020)) for n in range(job.ZONE_PAGE_BOUND + 3)]
        documents, _ = self.documents([*many, self.zone("D998", "99999-9999", square(0, 0, 0.0020))])
        listed, total = documents[pnu(1)]
        self.assertEqual((len(listed["zones"]), total), (job.ZONE_PAGE_BOUND, job.ZONE_PAGE_BOUND + 4))
        self.assertEqual(listed["zones"][0]["layer_code"], "D998")

    def test_an_unserved_parcel_has_no_document(self):
        documents, _ = self.documents([self.zone("D999", "99999-0001", square(0, 0, 0.0020))],
                                      parcels=[self.PARCELS["touching"]])
        self.assertEqual(documents, {})

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents([self.zone("D999", "99999-0001", square(0, 0, 0.0020))])
        written = set()
        for document, _total in documents.values():
            written |= by_pnu_kinds.json_paths(document, "regulation_zones")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_regulation_zones_by_pnu")
                    if p.startswith("regulation_zones.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        zones = [self.zone("D999", "99999-0001", square(0, 0, 0.0020))]
        first = job.gold_rows(self.frames(zones), "snap-1", "2026-11-04T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(zones), "snap-2", "2026-11-05T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)

    def test_the_restriction_keeps_the_zones_whose_box_meets_an_affected_parcel(self):
        near = self.zone("D999", "99999-0001", square(0, 0, 0.0020))
        far = self.zone("D998", "99999-0002", square(0.05, 0.05, 0.0010))
        frames = self.frames([near, far])
        keys = self.spark.createDataFrame([(pnu(1),)], "pnu string")
        restricted = job.restrict_inputs(frames, keys)
        self.assertEqual([r.pnu for r in restricted[job.PARCEL_SOURCE].collect()], [pnu(1)])
        self.assertEqual([r.zone_feature_id for r in restricted[job.ZONE_SOURCE].collect()], ["99999-0001"])
        reached = job.boxes_meet(frames[job.PARCEL_SOURCE], self.spark.createDataFrame(
            [square(0.0021, 0.0002, 0.0001)[1:]], ", ".join(f"{c} double" for c in job.BOX)), ("pnu",))
        self.assertEqual(sorted(r.pnu for r in reached.collect()), [pnu(2)])


if __name__ == "__main__":
    unittest.main()
