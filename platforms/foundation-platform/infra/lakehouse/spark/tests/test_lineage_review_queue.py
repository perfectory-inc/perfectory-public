import json
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import lineage_review_queue as q  # noqa: E402

OLD, NEW = "9999910100", "9999930100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


def row(old, new, grade, kind="area+category", to="s2"):
    return {"predecessor_pnu": old, "successor_pnu": new, "relation": "jurisdiction_transfer",
            "grade": grade, "evidence_kind": kind, "evidence_ref": "", "from_snapshot_id": "s1", "to_snapshot_id": to}


class ReviewQueueTest(unittest.TestCase):
    def test_only_needs_review_and_pending_reach_a_person(self):
        items, counts = q.review_queue([
            row(pnu(OLD, 1), pnu(NEW, 1), "code_derived"),
            row(pnu(OLD, 2), pnu(NEW, 2), "evidence_weak"),
            row(pnu(OLD, 3), pnu(NEW, 3), "needs_review", "area+category, ownership differs"),
            row("", pnu(NEW, 4), "pending", "no unique attribute match"),
        ])
        self.assertEqual({(i.subject_code, i.status) for i in items}, {(pnu(NEW, 3), "needs_review"), (pnu(NEW, 4), "pending")})
        self.assertEqual((counts["open_needs_review"], counts["open_pending"]), (1, 1))

    def test_the_reviewer_sees_the_candidates_and_their_evidence(self):
        items, _ = q.review_queue([row(pnu(OLD, 3), pnu(NEW, 3), "needs_review", "area+category, ownership differs")])
        self.assertEqual(json.loads(items[0].candidates_json), [{
            "predecessor_pnu": pnu(OLD, 3), "relation": "jurisdiction_transfer", "grade": "needs_review",
            "evidence_kind": "area+category, ownership differs", "evidence_ref": "", "in_effect": False,
        }])
        self.assertEqual(items[0].evidence_etag, q.evidence_etag("needs_review", items[0].candidates_json))

    def test_the_etag_matches_the_rust_twin(self):
        # stewardship_domain::tests::the_etag_matches_the_python_twin pins the same vector.
        self.assertEqual(q.evidence_etag("pending", "[]"), "012053e5d2845d033fa0c2799a9646e3367fc51fa516d45bb0e2f6b9a5a61a8f")

    def test_a_later_stronger_row_closes_the_item(self):
        items, counts = q.review_queue([
            row("", pnu(NEW, 4), "pending", "no unique attribute match"),
            row(pnu(OLD, 4), pnu(NEW, 4), "official", "30527", to="s3"),
        ])
        self.assertEqual((items, counts["closed_by_evidence"]), ([], 1))

    def test_a_steward_decision_closes_the_item_even_if_it_says_no_match(self):
        items, counts = q.review_queue([
            row("", pnu(NEW, 4), "pending", "no unique attribute match"),
            row("", pnu(NEW, 4), "pending", q.STEWARD, to="s3"),
        ])
        self.assertEqual((items, counts["closed_by_steward"]), ([], 1))

    def test_the_item_id_is_stable_across_runs(self):
        first, _ = q.review_queue([row("", pnu(NEW, 4), "pending", "x", to="s2")])
        second, _ = q.review_queue([row("", pnu(NEW, 4), "pending", "x", to="s3")])
        self.assertEqual(first[0].item_id, second[0].item_id)
        self.assertEqual(second[0].to_snapshot_id, "s3")


class HandoffTest(unittest.TestCase):
    def test_the_handoff_carries_every_item_and_its_count(self):
        items, _ = q.review_queue([row("", pnu(NEW, 4), "pending", "x"), row(pnu(OLD, 3), pnu(NEW, 3), "needs_review")])
        document = q.handoff_document(items, "parcel", "2099-09-30T00:00:00Z")
        self.assertEqual(document["schema_version"], q.HANDOFF_SCHEMA_VERSION)
        self.assertEqual(document["item_count"], 2)
        self.assertEqual({i["subject_code"] for i in document["items"]}, {pnu(NEW, 3), pnu(NEW, 4)})
        first = document["items"][0]
        self.assertEqual(first["evidence_etag"], q.evidence_etag(first["status"], first["candidates_json"]))


class ReviewQueueJobArgsTest(unittest.TestCase):
    def test_a_one_sido_run_cannot_rewrite_the_national_queue(self):
        from lineage_review_queue_to_gold import parse_args, validate_args

        with self.assertRaisesRegex(ValueError, "national queue"):
            validate_args(parse_args(["--sido", "28", "--allow-non-smoke-write"]))


if __name__ == "__main__":
    unittest.main()
