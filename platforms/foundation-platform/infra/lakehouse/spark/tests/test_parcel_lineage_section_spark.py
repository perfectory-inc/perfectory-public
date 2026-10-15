"""The panel's lineage section (root ADR-0113 §1, ADR-0115 §9): each parcel's edges as values, both
directions, read through the steward's standing decisions without collecting the table."""

import json
import os
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


A, B, C, D, E, F, G, H, I = (pnu(n) for n in range(1, 10))
SCHEMA = ("predecessor_pnu string, successor_pnu string, relation string, cardinality string, "
          "effective_date string, grade string, evidence_kind string, evidence_ref string")


def edge(pred, succ, relation, grade, kind="land_transfer_history", ref=None, cardinality="1:1", when="2099-08-01"):
    return (pred, succ, relation, cardinality, when, grade, kind, ref)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class LineageSectionSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("parcel-lineage-section").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def section(self, rows):
        from parcel_lineage_section import build_lineage

        frame = self.spark.createDataFrame(rows, SCHEMA)
        return {r.pnu: json.loads(r.lineage_json) for r in build_lineage(frame, self.spark).collect()}

    def test_an_edge_lands_on_both_parcels_once_with_values_only(self):
        rows = [
            edge(A, B, "split", "official", cardinality="1:N"),
            edge(C, B, "merge", "official", cardinality="N:1"),
            edge(None, D, "other", "pending", cardinality=None, when=None),
            # The same edge from two derivation runs is one entry.
            edge(E, F, "code_change", "code_derived"),
            edge(E, F, "code_change", "code_derived"),
        ]
        got = self.section(rows)
        self.assertEqual([e["predecessor_pnu"] for e in got[B]], [A, C])
        self.assertEqual(got[A], [{"predecessor_pnu": A, "successor_pnu": B, "relation": "split",
                                   "cardinality": "1:N", "effective_date": "2099-08-01", "grade": "official"}])
        self.assertEqual(got[D], [{"successor_pnu": D, "relation": "other", "grade": "pending"}])
        self.assertEqual(len(got[E]), 1)
        self.assertEqual(got[E], got[F])
        for entries in got.values():
            for entry in entries:
                self.assertLessEqual(set(entry), {"predecessor_pnu", "successor_pnu", "relation", "cardinality",
                                                  "effective_date", "grade"})

    def test_a_standing_steward_decision_replaces_the_derived_edges_of_its_parcel(self):
        from lineage_review_queue import STEWARD, fingerprint

        derived = [edge(H, G, "other", "evidence_weak"), edge(I, G, "other", "needs_review")]
        keys = ("predecessor_pnu", "successor_pnu", "relation", "cardinality", "effective_date", "grade",
                "evidence_kind", "evidence_ref")
        etag = fingerprint([dict(zip(keys, row)) for row in derived])[2]
        decision = edge(I, G, "merge", "official", kind=STEWARD,
                        ref=json.dumps({"evidence_etag": etag, "decided_at": "2099-09-01", "decision_id": "d1"}))
        got = self.section([*derived, decision, edge(A, B, "split", "official")])
        self.assertEqual([(e["predecessor_pnu"], e["relation"]) for e in got[G]], [(I, "merge")])
        self.assertNotIn(H, got, "the edge the decision replaced is gone from its other end too")
        self.assertIn(A, got)

        # A decision made on other evidence has lapsed: the derived edges stand.
        lapsed = edge(I, G, "merge", "official", kind=STEWARD, ref=json.dumps({"evidence_etag": "old"}))
        got = self.section([*derived, lapsed])
        self.assertEqual(sorted(e["predecessor_pnu"] for e in got[G]), [H, I])

    def test_a_run_without_the_lineage_has_an_empty_section(self):
        from parcel_lineage_section import build_lineage

        self.assertEqual(build_lineage(None, self.spark).collect(), [])


if __name__ == "__main__":
    unittest.main()
