"""Parcel sub-parcel by-PNU kind (root ADR-0395, ADR-0399).

Synthetic PNUs (prefix 99999). Pins: a 부속지번 row reaches both of its parcels' documents, as
`on_other_parcels` on its 대지 and as `from_other_parcels` on its 부속 지번; the other parcel's 지번 is
the provider's even when its PNU is null; a row naming the parcel itself is no link; only served parcels
have a document; the written JSON's paths are exactly the registry's (root ADR-0336); a run recomputes
the PNUs at both ends of the rows that changed, and nothing else.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import building_sub_parcel_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


SITE, ANNEX, UNSERVED = pnu(1), pnu(2), pnu(3)
LANE = "parcel-sub-parcels"
GOLD_COLUMNS = ("pnu", "sub_parcels_json", "attached_via", "row_digest", "source_snapshot_id", "published_at_utc")


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_no_lineage(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.SUB_PARCEL_SOURCE, job.CODE_CHANGE_SOURCE})
        self.assertEqual((optional, anchor, whole), ({}, job.PARCEL_SOURCE, (job.CODE_CHANGE_SOURCE,)))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns), (job.JOB_NAME, job.SECTION, ("sub_parcels_json",)))
        self.assertFalse(kind.follows_host_panel)

    def test_both_ends_follow_reorganizations_and_the_universe_is_the_served_edition(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)
        self.assertIn("renumbered_both(panel.read_source(spark, args, SUB_PARCEL_SOURCE, at)", src)
        self.assertIn('both_ends(renumbered_both(appended.select("pnu", "sub_pnu"), renumbering))', src)

    def test_the_build_columns_are_the_silver_contract_s(self):
        from platform_contracts import load_lakehouse_contract

        named = {c["name"] for c in load_lakehouse_contract(job.SUB_PARCEL_SOURCE)["columns"]}
        self.assertLessEqual(set(job.BUILD_COLUMNS[job.SUB_PARCEL_SOURCE]), named)

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

        self.assertEqual(run([job.SUB_PARCEL_SOURCE], [job.SUB_PARCEL_SOURCE]), "appended")
        self.assertEqual(run([job.SUB_PARCEL_SOURCE], []), "changed")
        self.assertEqual(run([job.PARCEL_SOURCE, job.SUB_PARCEL_SOURCE], [job.SUB_PARCEL_SOURCE]), "appended+parcels")
        with self.assertRaises(ValueError):
            run([job.CODE_CHANGE_SOURCE], [])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("building-sub-parcel-gold").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def row(self, site, sub, key="synthetic-1", sub_bon="0002"):
        return (site, sub, key, "일반건축물", "99999", "10100", "0", sub_bon, "0000", "99999", "10100", "0", "0001", "0000")

    def frames(self, rows):
        schema = ", ".join(f"{c} string" for c in job.BUILD_COLUMNS[job.SUB_PARCEL_SOURCE])
        parcels = self.spark.createDataFrame([(SITE,), (ANNEX,)], "pnu string")
        return {job.SUB_PARCEL_SOURCE: self.spark.createDataFrame(rows, schema), job.PARCEL_SOURCE: parcels}

    def documents(self, rows):
        gold = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS)
        return {r.pnu: json.loads(r.sub_parcels_json) for r in gold.collect()}, gold

    def test_a_link_reaches_both_parcels_and_unserved_ends_have_no_document(self):
        rows = [self.row(SITE, ANNEX), self.row(SITE, UNSERVED, sub_bon="0003"), self.row(SITE, SITE, sub_bon="0001"),
                self.row(SITE, None, key="synthetic-2", sub_bon="0099")]
        documents, gold = self.documents(rows)
        self.assertEqual(set(documents), {SITE, ANNEX})
        site = documents[SITE]
        self.assertEqual(site["on_other_total"], 3)
        others = {(e["mgmt_key"], e["other_pnu"], e["other_jibun"]) for e in site["on_other_parcels"]}
        self.assertEqual(others, {("synthetic-1", ANNEX, "99999-10100-0-0002-0000"),
                                  ("synthetic-1", UNSERVED, "99999-10100-0-0003-0000"),
                                  ("synthetic-2", None, "99999-10100-0-0099-0000")})
        self.assertEqual((site["from_other_parcels"], site["from_other_total"]), ([], 0))
        annex = documents[ANNEX]
        self.assertEqual((annex["on_other_parcels"], annex["from_other_total"]), ([], 1))
        self.assertEqual([(e["other_pnu"], e["other_jibun"], e["register_kind_name"]) for e in annex["from_other_parcels"]],
                         [(SITE, "99999-10100-0-0001-0000", "일반건축물")])
        self.assertEqual(tuple(gold.columns), GOLD_COLUMNS)

    def test_more_links_than_the_bound_keep_the_bound_and_count_all(self):
        rows = [self.row(SITE, f"{DONG}1{9000 + n:04d}0000", key=f"synthetic-{n:03d}", sub_bon=f"{9000 + n}")
                for n in range(job.ENTRY_BOUND + 3)]
        documents, _ = self.documents(rows)
        self.assertEqual((len(documents[SITE]["on_other_parcels"]), documents[SITE]["on_other_total"]),
                         (job.ENTRY_BOUND, job.ENTRY_BOUND + 3))

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        documents, _ = self.documents([self.row(SITE, ANNEX)])
        written = set()
        for document in documents.values():
            written |= by_pnu_kinds.json_paths(document, "sub_parcels")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_sub_parcels_by_pnu") if p.startswith("sub_parcels.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        rows = [self.row(SITE, ANNEX)]
        first = job.gold_rows(self.frames(rows), "snap-1", "2099-08-01T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(self.frames(rows), "snap-2", "2099-09-01T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
