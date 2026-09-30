import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import lineage_review_queue as q  # noqa: E402
import lineage_steward_fold_to_silver as fold  # noqa: E402

NEW, OLD = "9999930100100010000", "9999910100100010000"
NOW = datetime(2099, 9, 30, tzinfo=timezone.utc)


def exported(rows):
    """A handoff as `export-lineage-steward-fold` writes it (stewardship_domain::fold)."""

    return {"schema_version": fold.SCHEMA_VERSION, "row_count": len(rows), "rows": rows}


def link_row(decision_id="d1", etag="e" * 64, predecessor=OLD, grade="official"):
    ref = {"decision_id": decision_id, "decided_by": "u", "decided_at": "2099-09-30T00:00:00+00:00",
           "reason_code": "building_register", "evidence_etag": etag, "idempotency_key": "k" * 8}
    return {"decision_id": decision_id, "predecessor_pnu": predecessor, "successor_pnu": NEW, "relation": "jurisdiction_transfer",
            "grade": grade, "evidence_kind": "steward", "evidence_ref": json.dumps(ref),
            "from_snapshot_id": "s1", "to_snapshot_id": "s2", "effective_date": "2099-09-30"}


class FoldRowsTest(unittest.TestCase):
    def test_rows_become_lineage_rows_under_one_repeatable_run_id(self):
        run_id, rows, ids = fold.fold_rows(exported([link_row()]), NOW)
        self.assertEqual(ids, ["d1"])
        self.assertEqual(rows[0]["derivation_run_id"], run_id)
        self.assertEqual((rows[0]["grade"], rows[0]["evidence_kind"], rows[0]["rules_version"]), ("official", "steward", "steward-v1"))
        self.assertEqual(fold.fold_rows(exported([link_row()]), NOW)[0], run_id, "the same decisions make the same run")

    def test_a_folded_row_is_read_back_as_a_standing_decision(self):
        derived = [{"predecessor_pnu": OLD, "successor_pnu": NEW, "relation": "jurisdiction_transfer", "grade": "needs_review",
                    "evidence_kind": "area+category, ownership differs", "evidence_ref": ""}]
        _, rows, _ = fold.fold_rows(exported([link_row(etag=q.fingerprint(derived)[2])]), NOW)
        self.assertEqual(q.steward_resolved(derived + rows), rows)

    def test_anything_the_exporter_would_not_write_is_refused(self):
        bad = [
            {**exported([link_row()]), "row_count": 2},
            {**exported([link_row()]), "schema_version": "v0"},
            exported([{**link_row(), "evidence_kind": "area+category"}]),
            exported([link_row(predecessor=None)]),
            exported([link_row(grade="pending")]),
            exported([link_row(), link_row()]),
        ]
        for handoff in bad:
            with self.assertRaises(ValueError):
                fold.fold_rows(handoff, NOW)


if __name__ == "__main__":
    unittest.main()
