"""Parcel building-register-master by-PNU kind (root ADR-0357, ADR-0450).

Synthetic PNUs (prefix 99999). Pins: a parcel's document lists its 총괄표제부 newest first by the latest
of 사용승인일·착공일·허가일, at most the bound, and counts them all; a tied repeated row is one site; an
empty provider value is null; a row without a PNU and an unserved parcel have no document; a row under
a renumbered code lands on the served code (root ADR-0329 §3); the written JSON's paths are exactly
the registry's (root ADR-0336); a run recomputes only the PNUs whose current rows changed (both
directions) and of parcels that entered or left.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import building_register_master_silver_to_gold as job  # noqa: E402

DONG, OLD_DONG = "9999910100", "9999910200"


def pnu(main, dong=DONG):
    return f"{dong}1{main:04d}0000"


FACTORY, WAREHOUSE, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-register-masters"
GOLD_COLUMNS = ("pnu", "register_masters_json", "register_master_total", "attached_via", "row_digest",
                "source_snapshot_id", "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.MASTER_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("register_masters_json", "register_master_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_the_parcel_universe_is_the_served_edition_and_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertIn("panel.renumbered(panel.read_source(spark, args, MASTER_SOURCE, at), renumbering)", src)

    def test_the_record_fields_are_the_silver_contract_s_named_columns(self):
        from platform_contracts import load_lakehouse_contract

        named = {c["name"] for c in load_lakehouse_contract(job.MASTER_SOURCE)["columns"]}
        self.assertLessEqual({"mgmt_key", *job.RECORD_FIELDS}, named)

    def test_the_source_is_compared_not_read_as_an_append(self):
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
            return job.affected_pnus(changed, changed_rows=lambda: Frame("changed"),
                                     parcel_change=lambda: Frame("parcels")).name

        self.assertEqual(run([job.MASTER_SOURCE]), "changed")
        self.assertEqual(run([job.PARCEL_SOURCE, job.MASTER_SOURCE]), "changed+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE])
        self.assertNotIn("append_inputs", Path(job.__file__).read_text(encoding="utf-8"))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("building-register-master-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def site(self, parcel, key, approved="20990101", started="", permitted="", purpose="공장", plot="1000.5"):
        return (parcel, key, plot, "400.25", "800", purpose, "2", "1", "10", permitted, started, approved)

    def frames(self, rows):
        schema = "pnu string, mgmt_key string, " + ", ".join(f"{f} string" for f in job.RECORD_FIELDS)
        parcels = self.spark.createDataFrame([(FACTORY,), (WAREHOUSE,)], "pnu string")
        return {job.MASTER_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, frames):
        gold = job.gold_rows(frames, "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.register_masters_json), r.register_master_total) for r in gold.collect()}, gold

    def test_sites_newest_first_ties_once_blanks_null_and_unplaced_left_out(self):
        rows = [self.site(FACTORY, "k1", approved="20980101"),
                self.site(FACTORY, "k2", approved="", started="20990505", purpose="창고시설", plot=" "),
                self.site(FACTORY, "k2", approved="", started="20990505", purpose="창고시설", plot=" "),
                self.site(WAREHOUSE, "k3"), self.site(UNSERVED, "k4"), self.site(None, "k5")]
        documents, gold = self.documents(self.frames(rows))
        self.assertEqual(set(documents), {FACTORY, WAREHOUSE})
        factory, total = documents[FACTORY]
        self.assertEqual(total, 2)
        newest, older = factory["records"]
        self.assertEqual((newest["start_date"], newest["main_purpose_name"]), ("20990505", "창고시설"))
        self.assertIsNone(newest["plot_area_m2"])
        self.assertIsNone(newest["use_approval_date"])
        self.assertEqual((older["use_approval_date"], older["total_floor_area_m2"], older["parking_count"]),
                         ("20980101", "800", "10"))
        self.assertNotIn("mgmt_key", older)
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_a_parcel_with_more_sites_than_the_bound_keeps_the_newest_and_counts_all(self):
        rows = [self.site(FACTORY, f"k{n}", approved=f"2099{1 + n // 28:02d}{1 + n % 28:02d}")
                for n in range(job.RECORD_BOUND + 4)]
        documents, _ = self.documents(self.frames(rows))
        sites, total = documents[FACTORY]
        self.assertEqual((len(sites["records"]), total), (job.RECORD_BOUND, job.RECORD_BOUND + 4))
        dates = [r["use_approval_date"] for r in sites["records"]]
        self.assertEqual(dates, sorted(dates, reverse=True))

    def test_a_row_under_a_renumbered_code_lands_on_the_served_parcel(self):
        import parcel_panel_silver_to_gold as panel

        frames = self.frames([self.site(pnu(1, OLD_DONG), "k1")])
        renumbering = self.spark.createDataFrame([(OLD_DONG, DONG)], "_renumbered_from string, _renumbered_to string")
        frames[job.MASTER_SOURCE] = panel.renumbered(frames[job.MASTER_SOURCE], renumbering)
        documents, _ = self.documents(frames)
        self.assertEqual(set(documents), {FACTORY})

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents(self.frames([self.site(FACTORY, "k1")]))
        written = set()
        for document, _total in documents.values():
            written |= by_pnu_kinds.json_paths(document, "register_masters")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_register_masters_by_pnu")
                    if p.startswith("register_masters.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [self.site(FACTORY, "k1")]
        first = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
