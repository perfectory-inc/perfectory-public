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


def steward(new, derived, predecessor, decided_at):
    """A steward row as the fold writes it, made on the evidence `derived` shows."""

    etag = q.fingerprint(derived)[2]
    ref = {"decision_id": f"d-{decided_at}", "decided_at": decided_at, "evidence_etag": etag}
    return {"predecessor_pnu": predecessor, "successor_pnu": new, "relation": "jurisdiction_transfer" if predecessor else "other",
            "grade": "official" if predecessor else "pending", "evidence_kind": q.STEWARD, "evidence_ref": json.dumps(ref),
            "from_snapshot_id": "s1", "to_snapshot_id": "s2"}


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

    def test_a_steward_decision_on_the_current_evidence_closes_the_item(self):
        derived = [row("", pnu(NEW, 4), "pending", "no unique attribute match")]
        items, counts = q.review_queue(derived + [steward(pnu(NEW, 4), derived, "", "2099-10-01")])
        self.assertEqual((items, counts["closed_by_steward"]), ([], 1))

    def test_new_evidence_after_a_decision_puts_the_parcel_back(self):
        before = [row("", pnu(NEW, 4), "pending", "no unique attribute match")]
        decision = steward(pnu(NEW, 4), before, "", "2099-10-01")
        after = [row(pnu(OLD, 4), pnu(NEW, 4), "needs_review", "area+category, ownership differs", to="s3")]
        items, counts = q.review_queue(before + after + [decision])
        self.assertEqual([i.subject_code for i in items], [pnu(NEW, 4)])
        self.assertEqual(counts["reopened_by_new_evidence"], 1)
        self.assertEqual(q.steward_resolved(before + after + [decision]), before + after, "a lapsed decision is not read")

    def test_a_standing_decision_replaces_the_derived_rows_and_the_latest_wins(self):
        derived = [
            row(pnu(OLD, 3), pnu(NEW, 3), "needs_review", "area+category, ownership differs"),
            row(pnu(OLD, 5), pnu(NEW, 3), "needs_review", "area+category, ownership differs"),
        ]
        first = steward(pnu(NEW, 3), derived, pnu(OLD, 3), "2099-10-01")
        correction = steward(pnu(NEW, 3), derived, pnu(OLD, 5), "2099-10-02")
        self.assertEqual(q.steward_resolved(derived + [first]), [first])
        self.assertEqual(q.steward_resolved(derived + [first, correction]), [correction])
        untouched = row(pnu(OLD, 1), pnu(NEW, 1), "code_derived")
        self.assertIn(untouched, q.steward_resolved([untouched] + derived + [first]))

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

    def test_a_probe_writes_nothing_so_it_needs_no_write_permission(self):
        from unittest import mock

        from lineage_review_queue_to_gold import parse_args, validate_args

        with mock.patch("lineage_review_queue_to_gold.assert_catalog_env"):
            validate_args(parse_args(["--probe-only"]))
            with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
                validate_args(parse_args([]))


if __name__ == "__main__":
    unittest.main()
