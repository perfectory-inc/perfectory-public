"""Parcel registered-factory by-PNU kind (root ADR-0280 §4, ADR-0449).

Synthetic PNUs (prefix 99999). Pins: a parcel's document lists its distinct factories by 회사명, at
most the bound, and counts them all; a repeated factory is one; an empty provider value is null; a row
without a PNU and an unserved parcel have no document; a row under a renumbered code lands on the
served code (root ADR-0329 §3); the written JSON's paths are exactly the registry's (root ADR-0336);
a run recomputes only the PNUs whose edition rows changed and of parcels that entered or left.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import registered_factory_silver_to_gold as job  # noqa: E402

DONG, OLD_DONG = "9999910100", "9999910200"


def pnu(main, dong=DONG):
    return f"{dong}1{main:04d}0000"


FACTORY, WAREHOUSE, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-factories"
GOLD_COLUMNS = ("pnu", "factories_json", "factory_total", "attached_via", "row_digest", "source_snapshot_id",
                "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.FACTORY_SOURCE, job.CODE_CHANGE_SOURCE,
                                         *job.place_id_join.INPUTS})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("factories_json", "factory_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_the_parcel_universe_is_the_served_edition_and_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertIn("panel.renumbered(panel.read_source(spark, args, FACTORY_SOURCE, at), renumbering)", src)

    def test_the_record_fields_are_the_silver_contract_s_named_columns(self):
        from platform_contracts import load_lakehouse_contract

        named = {c["name"] for c in load_lakehouse_contract(job.FACTORY_SOURCE)["columns"]}
        self.assertLessEqual(set(job.RECORD_FIELDS), named)

    def test_the_editions_source_is_compared_not_read_as_an_append(self):
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

        self.assertEqual(run([job.FACTORY_SOURCE]), "changed")
        self.assertEqual(run([job.PARCEL_SOURCE, job.FACTORY_SOURCE]), "changed+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE])
        # The one append input is the place-id changelog (root ADR-0483), never the editions table.
        self.assertIn("append_inputs={place_id_join.CHANGELOG_SOURCE: place_id_join.changelog_table(args)})",
                      Path(job.__file__).read_text(encoding="utf-8"))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("registered-factory-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, rows):
        schema = "pnu string, " + ", ".join(f"{f} string" for f in job.RECORD_FIELDS)
        parcels = self.spark.createDataFrame([(FACTORY,), (WAREHOUSE,)], "pnu string")
        return {job.FACTORY_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, frames):
        gold = job.gold_rows(frames, "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.factories_json), r.factory_total) for r in gold.collect()}, gold

    def test_factories_by_name_repeats_once_blanks_null_and_unplaced_left_out(self):
        rows = [(FACTORY, "나공업", "가산업단지", "볼트"), (FACTORY, "가정밀", " ", "금형"),
                (FACTORY, "가정밀", " ", "금형"), (WAREHOUSE, "다물류", None, "창고"),
                (UNSERVED, "라기계", None, "기계"), (None, "마화학", None, "도료")]
        documents, gold = self.documents(self.frames(rows))
        self.assertEqual(set(documents), {FACTORY, WAREHOUSE})
        factory, total = documents[FACTORY]
        self.assertEqual(total, 2)
        first, second = factory["records"]
        self.assertEqual((first["company_name"], first["complex_name"], first["products"]), ("가정밀", None, "금형"))
        self.assertEqual(second["company_name"], "나공업")
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_a_parcel_with_more_factories_than_the_bound_keeps_the_first_and_counts_all(self):
        rows = [(FACTORY, f"회사{n:03d}", None, "부품") for n in range(job.RECORD_BOUND + 4)]
        documents, _ = self.documents(self.frames(rows))
        factories, total = documents[FACTORY]
        self.assertEqual((len(factories["records"]), total), (job.RECORD_BOUND, job.RECORD_BOUND + 4))
        names = [r["company_name"] for r in factories["records"]]
        self.assertEqual(names, sorted(names))

    def test_a_row_under_a_renumbered_code_lands_on_the_served_parcel(self):
        import parcel_panel_silver_to_gold as panel

        frames = self.frames([(pnu(1, OLD_DONG), "가정밀", None, "금형")])
        renumbering = self.spark.createDataFrame([(OLD_DONG, DONG)], "_renumbered_from string, _renumbered_to string")
        frames[job.FACTORY_SOURCE] = panel.renumbered(frames[job.FACTORY_SOURCE], renumbering)
        documents, _ = self.documents(frames)
        self.assertEqual(set(documents), {FACTORY})

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents(self.frames([(FACTORY, "가정밀", "가산업단지", "금형")]))
        written = set()
        for document, _total in documents.values():
            written |= by_pnu_kinds.json_paths(document, "factories")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_factories_by_pnu") if p.startswith("factories.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [(FACTORY, "가정밀", None, "금형")]
        first = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
