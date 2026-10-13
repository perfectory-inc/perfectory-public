"""The per-kind size measurement (root ADR-0193 §6-1): how a panel row splits into lanes.

The split and the sizes are plain functions, checked without PySpark. The aggregation runs with
`RUN_SPARK_TESTS=1` in the pinned Spark image. PNUs are synthetic.
"""

import gzip
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import by_pnu_kind_sizes as sizes  # noqa: E402

PNU_A = "9999910100100010000"
PNU_B = "9999910100100020000"


def parcel_row(**overrides):
    row = {
        "pnu": PNU_A, "kind": "parcel", "area_m2": 10,
        "zonings_json": '[{"name":"z"}]', "price_json": '{"won":1}', "characteristics_json": None,
        "forest_ledger_json": None, "transfer_history_json": "[]", "land_rights_json": "[]",
        "land_right_total": 0, "attached_via_json": None,
        "row_digest": "d", "source_snapshot_id": "1", "published_at_utc": "t",
    }
    row.update(overrides)
    return row


def building_row():
    unit = {"id": "u1", "ho_name": "101", "official_price_history": [{"base_date": "20260101", "price_won": 5}]}
    bare = {"id": "u2", "ho_name": "102", "official_price_history": []}
    building = {"id": "b1", "stories": 3, "floors": [{"floor_number": 1}], "units": [unit, bare]}
    return {
        "pnu": PNU_A, "buildings_json": json.dumps([building, {"id": "b2", "floors": [], "units": []}]),
        "unlinked_units_json": "[]", "row_digest": "d", "source_snapshot_id": "1", "published_at_utc": "t",
    }


class ParcelSplit(unittest.TestCase):
    def test_only_kinds_with_content_are_carried(self):
        self.assertEqual(sorted(sizes.parcel_kinds(parcel_row())), ["parcel-price", "parcel-zoning"])

    def test_land_rights_carry_their_total(self):
        bodies = sizes.parcel_kinds(parcel_row(land_rights_json='[{"a":1}]', land_right_total=1))
        self.assertEqual(json.loads(bodies["parcel-land-rights"]), {"land_rights": [{"a": 1}], "land_right_total": 1})


class BuildingSplit(unittest.TestCase):
    def test_each_fact_lands_in_exactly_one_kind(self):
        bodies = {k: json.loads(v) for k, v in sizes.building_kinds(building_row()).items()}
        self.assertEqual([b["id"] for b in bodies["building-titles"]], ["b1", "b2"])
        self.assertTrue(all("floors" not in b and "units" not in b for b in bodies["building-titles"]))
        self.assertEqual(bodies["building-floors"], [{"id": "b1", "floors": [{"floor_number": 1}]}])
        self.assertEqual([u["id"] for u in bodies["building-units"]["units"]], ["u1", "u2"])
        self.assertTrue(all("official_price_history" not in u for u in bodies["building-units"]["units"]))
        self.assertEqual([p["id"] for p in bodies["unit-price"]], ["u1"])

    def test_a_building_without_units_has_no_unit_kinds(self):
        row = building_row()
        row["buildings_json"] = json.dumps([{"id": "b1", "floors": [], "units": []}])
        self.assertEqual(list(sizes.building_kinds(row)), ["building-titles"])


class Sizes(unittest.TestCase):
    def test_gzip_is_the_pack_member(self):
        text = "가" * 100
        self.assertEqual(sizes.gzip_size(text), len(gzip.compress(text.encode(), compresslevel=9, mtime=0)))

    def test_a_row_yields_its_kinds_and_the_bundle_in_bytes(self):
        measured = {kind: raw for _, kind, raw, *_ in sizes.measure_row("parcel", parcel_row(zonings_json='["가"]'))}
        self.assertEqual(measured["parcel-zoning"], len('["가"]'.encode()))
        self.assertIn("bundle", measured)

    def test_the_bundle_embeds_json_columns_as_json_not_as_escaped_strings(self):
        bundle = json.loads(sizes.bundle_body(parcel_row()))
        self.assertEqual(bundle["zonings"], [{"name": "z"}])
        self.assertNotIn("zonings_json", bundle)
        self.assertNotIn("\\", sizes.bundle_body(parcel_row()))
        self.assertNotIn("row_digest", bundle)


class PackParts(unittest.TestCase):
    def test_the_part_hash_is_the_contract_one(self):
        contract = json.loads(sizes.r2_contract_path().read_text(encoding="utf-8"))
        vectors = contract["by_pnu_section_packs"]["parts"]["hash_test_vectors"]
        self.assertTrue(vectors)
        for vector in vectors:
            self.assertEqual(sizes.fnv1a32(vector["input"]), vector["fnv1a32"], vector["input"])

    def test_the_rules_come_from_the_contract(self):
        bound, part_target = sizes.pack_rules()
        self.assertLess(0, part_target)
        self.assertLessEqual(part_target, bound)

    def test_one_digest_per_column_unpacks_to_its_named_quantiles(self):
        self.assertEqual(sizes.unpack_quantiles({"kind": "k", "gzip_bytes_quantiles": [10, 90], "gzip_bytes_max": 99}),
                         {"kind": "k", "gzip_bytes_p50": 10, "gzip_bytes_p95": 90, "gzip_bytes_max": 99})
        self.assertEqual(sizes.unpack_quantiles({"raw_bytes_quantiles": None}),
                         {"raw_bytes_p50": None, "raw_bytes_p95": None})
        # The national parcel run's final merge outgrew the small Spark at 10000 (2026-10-13).
        self.assertLessEqual(sizes.PERCENTILE_ACCURACY, 1000)

    def test_each_row_carries_its_pnu_part_hash(self):
        hashes = {h for *_, h in sizes.measure_row("parcel", parcel_row())}
        self.assertEqual(hashes, {sizes.fnv1a32(PNU_A)})


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class MeasureSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("by-pnu-kind-sizes").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_documents_and_dong_packs_are_counted_per_kind(self):
        rows = [parcel_row(), parcel_row(pnu=PNU_B, price_json=None)]
        columns = list(rows[0])
        frame = self.spark.createDataFrame(
            [tuple(r[c] for c in columns) for r in rows],
            "pnu string, kind string, area_m2 long, zonings_json string, price_json string, "
            "characteristics_json string, forest_ledger_json string, transfer_history_json string, "
            "land_rights_json string, land_right_total long, attached_via_json string, row_digest string, "
            "source_snapshot_id string, published_at_utc string",
        )
        kinds = sizes.measure_frame(self.spark, "parcel", frame)
        self.assertEqual(kinds["parcel-zoning"]["pnus"], 2)
        self.assertEqual(kinds["parcel-price"]["pnus"], 1)
        self.assertEqual(kinds["parcel-zoning"]["packs"], 1)
        self.assertEqual(kinds["first-paint"]["pnus"], 2)
        self.assertNotIn("packs", kinds["first-paint"])
        self.assertEqual(kinds["parcel-zoning"]["dongs"], 1)
        self.assertEqual(kinds["parcel-zoning"]["parted_dongs"], 0)
        with tempfile.TemporaryDirectory() as work:
            sizes.write_summary(f"{work}/s.json", {"kinds": kinds})
            self.assertIn("bundle", json.loads(Path(work, "s.json").read_text(encoding="utf-8"))["kinds"])

    def test_a_dong_over_the_part_target_is_measured_as_its_parts(self):
        rows = [parcel_row(pnu=f"99999101001{i:04d}0000") for i in range(40)]
        columns = list(rows[0])
        frame = self.spark.createDataFrame(
            [tuple(r[c] for c in columns) for r in rows],
            "pnu string, kind string, area_m2 long, zonings_json string, price_json string, "
            "characteristics_json string, forest_ledger_json string, transfer_history_json string, "
            "land_rights_json string, land_right_total long, attached_via_json string, row_digest string, "
            "source_snapshot_id string, published_at_utc string",
        )
        member = sizes.gzip_size('[{"name":"z"}]')
        # A part target of four members splits the one dong into ten parts.
        kinds = sizes.measure_frame(self.spark, "parcel", frame, (8 * member, 4 * member))
        zoning = kinds["parcel-zoning"]
        self.assertEqual((zoning["dongs"], zoning["parted_dongs"]), (1, 1))
        self.assertEqual(zoning["dong_bytes_max"], 40 * member)
        self.assertGreater(zoning["packs"], 1)
        self.assertLess(zoning["pack_bytes_max"], zoning["dong_bytes_max"])


if __name__ == "__main__":
    unittest.main()
