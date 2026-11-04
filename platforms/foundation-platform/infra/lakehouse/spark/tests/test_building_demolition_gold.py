"""Parcel demolition-history by-PNU kind (root ADR-0397, ADR-0399).

Synthetic PNUs (prefix 99999). Pins: a parcel's document lists its distinct reports newest first by
the latest of their three dates, at most the bound, and counts them all; an identical repeated row is
one report; an empty provider value is null; only served parcels have a document; the written JSON's
paths are exactly the registry's (root ADR-0336); a run recomputes only the PNUs of appended rows and
of parcels that entered or left the served edition.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import building_demolition_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


FACTORY, EMPTY_LOT, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-demolitions"
GOLD_COLUMNS = ("pnu", "demolitions_json", "demolition_total", "attached_via", "row_digest", "source_snapshot_id",
                "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.DEMOLITION_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("demolitions_json", "demolition_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_the_parcel_universe_is_the_served_edition_and_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertIn("panel.renumbered(panel.read_source(spark, args, DEMOLITION_SOURCE, at), renumbering)", src)

    def test_the_record_fields_are_the_silver_contract_s_named_columns(self):
        from platform_contracts import load_lakehouse_contract

        named = {c["name"] for c in load_lakehouse_contract(job.DEMOLITION_SOURCE)["columns"]}
        self.assertLessEqual(set(job.RECORD_FIELDS), named)

    def test_the_affected_pnus_come_only_from_the_inputs_that_changed(self):
        class Frame:
            def __init__(self, name):
                self.name = name

            def select(self, *_):
                return self

            def unionByName(self, other):
                return Frame(f"{self.name}+{other.name}")

            def distinct(self):
                return self

        def run(changed, appended):
            return job.affected_pnus(changed, appended, appended_rows=lambda: Frame("appended"),
                                     changed_rows=lambda: Frame("changed"), parcel_change=lambda: Frame("parcels")).name

        self.assertEqual(run([job.DEMOLITION_SOURCE], [job.DEMOLITION_SOURCE]), "appended")
        self.assertEqual(run([job.DEMOLITION_SOURCE], []), "changed")
        self.assertEqual(run([job.PARCEL_SOURCE, job.DEMOLITION_SOURCE], [job.DEMOLITION_SOURCE]), "appended+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE], [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("building-demolition-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def report(self, parcel, kind="철거", start="20990101", end="20990110", loss="", area="305.08", purpose="공장"):
        return (parcel, kind, start, end, loss, area, "1", purpose, "철근콘크리트구조")

    def frames(self, rows):
        schema = "pnu string, " + ", ".join(f"{f} string" for f in job.RECORD_FIELDS)
        parcels = self.spark.createDataFrame([(FACTORY,), (EMPTY_LOT,)], "pnu string")
        return {job.DEMOLITION_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, rows):
        gold = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.demolitions_json), r.demolition_total) for r in gold.collect()}, gold

    def test_reports_newest_first_repeats_once_blanks_null_and_unserved_left_out(self):
        rows = [self.report(FACTORY, start="20980101", end="20980301"),
                self.report(FACTORY, kind="멸실", start="", end="", loss="20990505", area=" ", purpose="창고시설"),
                self.report(FACTORY, kind="멸실", start="", end="", loss="20990505", area=" ", purpose="창고시설"),
                self.report(EMPTY_LOT), self.report(UNSERVED)]
        documents, gold = self.documents(rows)
        self.assertEqual(set(documents), {FACTORY, EMPTY_LOT})
        factory, total = documents[FACTORY]
        self.assertEqual(total, 2)
        newest, older = factory["records"]
        self.assertEqual((newest["demolition_kind_name"], newest["demolition_loss_date"], newest["main_purpose_name"]),
                         ("멸실", "20990505", "창고시설"))
        self.assertIsNone(newest["demolition_start_date"])
        self.assertIsNone(newest["total_floor_area_m2"])
        self.assertEqual((older["demolition_end_date"], older["total_floor_area_m2"], older["structure_name"]),
                         ("20980301", "305.08", "철근콘크리트구조"))
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_a_parcel_with_more_reports_than_the_bound_keeps_the_newest_and_counts_all(self):
        rows = [self.report(FACTORY, start=f"2099{1 + n // 28:02d}{1 + n % 28:02d}", end="") for n in range(job.RECORD_BOUND + 4)]
        documents, _ = self.documents(rows)
        reports, total = documents[FACTORY]
        self.assertEqual((len(reports["records"]), total), (job.RECORD_BOUND, job.RECORD_BOUND + 4))
        starts = [r["demolition_start_date"] for r in reports["records"]]
        self.assertEqual(starts, sorted(starts, reverse=True))

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents([self.report(FACTORY), self.report(EMPTY_LOT, loss="20990101")])
        written = set()
        for document, _total in documents.values():
            written |= by_pnu_kinds.json_paths(document, "demolitions")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_demolitions_by_pnu") if p.startswith("demolitions.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [self.report(FACTORY)]
        first = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
