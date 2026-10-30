"""Parcel ownership by-PNU kind (root ADR-0360, ADR-0361).

Synthetic PNUs (prefix 99999). Pins: a parcel's document lists its own co-owners in the provider's
order, at most the page bound, and counts them all; unit owners of a collective building are only
counted by 소유구분; only served parcels have a document; the written JSON's paths are exactly the
registry's (root ADR-0336); a run recomputes the PNUs of appended owner rows and of parcels that
entered or left the served edition, and nothing else.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import land_ownership_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


FACTORY, COMPLEX, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-ownership"
GOLD_COLUMNS = ("pnu", "ownership_json", "owner_total", "attached_via", "row_digest", "source_snapshot_id",
                "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.OWNERSHIP_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("ownership_json", "owner_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_parse_args_accepts_the_section_coverage_flags_the_harness_passes(self):
        args = job.parse_args([
            "--lane", LANE, "--iceberg-snapshot-id", "1", "--source-snapshots-path", "/x", "--validate-only",
            "--section-coverage-snapshot", "123", "--max-section-coverage-drop", "0.05",
            "--section-coverage-min-pnus", "1000"])
        self.assertEqual((args.section_coverage_snapshot, args.region_prefix), ("123", None))

    def test_the_parcel_universe_is_the_served_edition(self):
        # As the 공매 kind (2026-10-21): every edition's parcels would place documents under PNUs the
        # served map does not hold, which the by-PNU serving gate refuses.
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)

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
            frame = job.affected_pnus(changed, appended, appended_rows=part("appended"),
                                      changed_rows=part("changed"), parcel_change=part("parcels"))
            return frame.name, list(calls)

        # A Silver release appends only the changed parcels' rows: read those, never diff the table.
        self.assertEqual(run([job.OWNERSHIP_SOURCE], [job.OWNERSHIP_SOURCE]), ("appended", ["appended"]))
        self.assertEqual(run([job.OWNERSHIP_SOURCE], []), ("changed", ["changed"]))
        self.assertEqual(run([job.PARCEL_SOURCE, job.OWNERSHIP_SOURCE], [job.OWNERSHIP_SOURCE]),
                         ("appended+parcels", ["appended", "parcels"]))
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE], [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("land-ownership-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def owner(self, parcel, serial, collective="0000", kind="법인", changed="2019-11-26"):
        return (parcel, collective, "2024-12", "2025-08-26", serial, "03" if kind == "법인" else "01", kind,
                "03", "읍면동내", "ZZ", "구분없음", "03", "소유권이전", changed, 1)

    def frames(self, rows):
        schema = ("pnu string, collective_building_serial_no string, base_year_month string, "
                  "data_reference_date string, co_owner_serial_no string, ownership_kind_code string, "
                  "ownership_kind_name string, residence_kind_code string, residence_kind_name string, "
                  "state_agency_kind_code string, state_agency_kind_name string, "
                  "ownership_change_reason_code string, ownership_change_reason string, "
                  "ownership_changed_on string, co_owner_count int")
        parcels = self.spark.createDataFrame([(FACTORY,), (COMPLEX,)], "pnu string")
        return {job.OWNERSHIP_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, rows):
        gold = job.gold_rows(self.frames(rows), "snap-1", "2026-10-30T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.ownership_json), r.owner_total) for r in gold.collect()}, gold

    def test_owners_in_provider_order_units_counted_and_unserved_parcels_left_out(self):
        rows = [self.owner(FACTORY, "000002", kind="개인", changed="2024-01-02"), self.owner(FACTORY, "000001"),
                self.owner(COMPLEX, "000001", collective="0012", kind="개인"),
                self.owner(COMPLEX, "000002", collective="0012", kind="개인"),
                self.owner(COMPLEX, "000003", collective="0012", kind="법인"),
                self.owner(UNSERVED, "000001")]
        documents, gold = self.documents(rows)
        self.assertEqual(set(documents), {FACTORY, COMPLEX})
        factory, total = documents[FACTORY]
        self.assertEqual(total, 2)
        self.assertEqual([o["co_owner_serial_no"] for o in factory["owners"]], ["000001", "000002"])
        self.assertEqual((factory["unit_owner_total"], factory["unit_owner_kinds"]), (0, []))
        self.assertEqual(factory["latest_changed_on"], "2024-01-02")
        complex_, total = documents[COMPLEX]
        self.assertEqual((total, complex_["owners"], complex_["unit_owner_total"]), (0, [], 3))
        self.assertEqual(complex_["unit_owner_kinds"], [{"ownership_kind_name": "개인", "count": 2},
                                                        {"ownership_kind_name": "법인", "count": 1}])
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_a_parcel_with_more_owners_than_the_bound_keeps_the_first_and_counts_all(self):
        rows = [self.owner(FACTORY, f"{n:06d}") for n in range(1, job.OWNER_PAGE_BOUND + 6)]
        documents, _ = self.documents(rows)
        owners, total = documents[FACTORY]
        self.assertEqual((len(owners["owners"]), total), (job.OWNER_PAGE_BOUND, job.OWNER_PAGE_BOUND + 5))

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        rows = [self.owner(FACTORY, "000001"), self.owner(COMPLEX, "000001", collective="0012")]
        documents, _ = self.documents(rows)
        written = set()
        for document, _total in documents.values():
            written |= by_pnu_kinds.json_paths(document, "ownership")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_ownership_by_pnu") if p.startswith("ownership.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [self.owner(FACTORY, "000001")]
        first = job.gold_rows(self.frames(rows), "snap-1", "2026-10-30T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2026-10-31T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
