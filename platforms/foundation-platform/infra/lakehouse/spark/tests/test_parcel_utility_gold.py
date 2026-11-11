"""Parcel utility by-PNU kind (root ADR-0496, ADR-0497).

Synthetic PNUs (prefix 99999). Pins: a parcel's document counts its facilities per 오수정화방식, most
first, and says whether one is connected to a treatment plant (300·400); a row without a method says
nothing; a tied repeated row is one facility; a row without a PNU and an unserved parcel have no
document; a row under a renumbered code lands on the served code (root ADR-0329 §3); the written JSON's
paths are exactly the registry's (root ADR-0336); a run recomputes only the PNUs whose current rows
changed (both directions) and of parcels that entered or left.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import parcel_utility_silver_to_gold as job  # noqa: E402

DONG, OLD_DONG = "9999910100", "9999910200"


def pnu(main, dong=DONG):
    return f"{dong}1{main:04d}0000"


CONNECTED, SEPTIC, SILENT, UNSERVED = pnu(1), pnu(2), pnu(3), pnu(4)
LANE = "parcel-utility"
GOLD_COLUMNS = ("pnu", "utility_json", "attached_via", "row_digest", "source_snapshot_id", "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.SEWAGE_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns), (job.JOB_NAME, job.SECTION, ("utility_json",)))
        self.assertFalse(kind.follows_host_panel)

    def test_the_parcel_universe_is_the_served_edition_and_codes_follow_reorganizations(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertIn("panel.renumbered(panel.read_source(spark, args, SEWAGE_SOURCE, at), renumbering)", src)

    def test_the_read_columns_are_the_silver_contract_s_named_columns(self):
        from platform_contracts import load_lakehouse_contract

        named = {c["name"] for c in load_lakehouse_contract(job.SEWAGE_SOURCE)["columns"]}
        self.assertLessEqual(set(job.BUILD_COLUMNS[job.SEWAGE_SOURCE]), named)

    def test_the_treatment_plant_codes_are_the_lane_contract_s(self):
        contract = Path(job.__file__).resolve().parents[2] / "contracts/hub-building-register-sewage-facility-source-objects.json"
        meaning = json.loads(contract.read_text(encoding="utf-8"))["columns"]["treatment_method_code"]["meaning"]
        self.assertIn("300 하수종말처리장연결", meaning)
        self.assertIn("400 마을하수처리장연결", meaning)

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

        self.assertEqual(run([job.SEWAGE_SOURCE]), "changed")
        self.assertEqual(run([job.PARCEL_SOURCE, job.SEWAGE_SOURCE]), "changed+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE])
        self.assertNotIn("append_inputs", Path(job.__file__).read_text(encoding="utf-8"))


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("parcel-utility-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, rows):
        schema = "pnu string, mgmt_key string, treatment_method_code string, treatment_method_name string"
        parcels = self.spark.createDataFrame([(CONNECTED,), (SEPTIC,), (SILENT,)], "pnu string")
        return {job.SEWAGE_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, frames):
        gold = job.gold_rows(frames, "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: json.loads(r.utility_json)["sewage"] for r in gold.collect()}, gold

    def test_methods_most_first_ties_once_silence_and_unplaced_left_out(self):
        rows = [(CONNECTED, "k1", "300", "하수종말처리장연결"), (CONNECTED, "k2", "201", "부패탱크방법"),
                (CONNECTED, "k3", " 300 ", "하수종말처리장연결"),
                (SEPTIC, "k4", "201", "부패탱크방법"), (SEPTIC, "k4", "201", "부패탱크방법"),
                (SILENT, "k5", "", ""), (SILENT, "k6", " ", None),
                (UNSERVED, "k7", "300", "하수종말처리장연결"), (None, "k8", "300", "하수종말처리장연결")]
        documents, gold = self.documents(self.frames(rows))
        self.assertEqual(set(documents), {CONNECTED, SEPTIC})
        connected = documents[CONNECTED]
        self.assertEqual((connected["treatment_plant_connection"], connected["facility_total"]), (True, 3))
        self.assertEqual(connected["methods"], [{"code": "300", "name": "하수종말처리장연결", "facilities": 2},
                                                {"code": "201", "name": "부패탱크방법", "facilities": 1}])
        septic = documents[SEPTIC]
        self.assertEqual((septic["treatment_plant_connection"], septic["facility_total"]), (False, 1))
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_a_parcel_with_more_methods_than_the_bound_keeps_the_most_and_counts_all(self):
        rows = [(CONNECTED, f"k{n}", f"{100 + n}", f"방법{n}") for n in range(job.METHOD_BOUND + 3)]
        documents, _ = self.documents(self.frames(rows))
        self.assertEqual((len(documents[CONNECTED]["methods"]), documents[CONNECTED]["facility_total"]),
                         (job.METHOD_BOUND, job.METHOD_BOUND + 3))

    def test_a_row_under_a_renumbered_code_lands_on_the_served_parcel(self):
        import parcel_panel_silver_to_gold as panel

        frames = self.frames([(pnu(1, OLD_DONG), "k1", "300", "하수종말처리장연결")])
        renumbering = self.spark.createDataFrame([(OLD_DONG, DONG)], "_renumbered_from string, _renumbered_to string")
        frames[job.SEWAGE_SOURCE] = panel.renumbered(frames[job.SEWAGE_SOURCE], renumbering)
        documents, _ = self.documents(frames)
        self.assertEqual(set(documents), {CONNECTED})

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        gold = job.gold_rows(self.frames([(CONNECTED, "k1", "300", "하수종말처리장연결")]), "snap-1",
                             "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        written = set()
        for row in gold.collect():
            written |= by_pnu_kinds.json_paths(json.loads(row.utility_json), "utility")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_utility_by_pnu") if p.startswith("utility.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [(CONNECTED, "k1", "300", "하수종말처리장연결")]
        first = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
