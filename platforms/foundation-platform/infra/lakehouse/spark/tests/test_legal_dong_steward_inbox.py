"""The pairing run's half of the one steward path for 법정동 pairs (root ADR-0263), on synthetic data.

The run publishes its steward list as a review handoff the steward database loads, reads the
standing approvals the database exports, judges each again against the table and its own list, and
names the decisions it recorded. Every refusal is proven by planting what it must refuse.
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import legal_dong_code_change_pairs as pairs_job  # noqa: E402
import lineage_review_queue as review_queue  # noqa: E402
from test_code_go_kr_legal_dong import (  # noqa: E402
    CADASTRAL, CONTRACT, NO_PARCELS_BEFORE, NOW, merged_table, renamed_table, row, table_html, DAY,
)

DOMAIN = SPARK_DIR.parents[2] / "crates" / "stewardship" / "stewardship-domain" / "src"
DECIDER = "00000000-0000-0000-0000-000000000001"


def export(*approvals: tuple[str, str, str], note: str = "합성 공문") -> dict:
    """The database's standing approvals as `export-legal-dong-steward-decisions` writes them."""

    decisions = [
        {"decision_id": decision_id, "old_code": old, "new_code": new, "decided_by": DECIDER,
         "decided_at": "2099-01-02T00:00:00Z", "reason_code": "official_document" if note else None, "note": note}
        for decision_id, old, new in approvals
    ]
    return {"schema_version": pairs_job.DECISION_EXPORT_SCHEMA, "review_items": 1,
            "decision_count": len(decisions), "decisions": decisions}


def rust_fields(struct: str, module: str = "lib.rs") -> set[str]:
    """The fields of a `stewardship-domain` struct, read from its source: the reader the handoff must fit."""

    body = re.search(rf"pub struct {struct} \{{(.*?)\n\}}", (DOMAIN / module).read_text(encoding="utf-8"), re.S)
    assert body, struct
    return set(re.findall(r"pub (\w+):", body.group(1)))


class DatabaseDecisionTest(unittest.TestCase):
    def setUp(self):
        self.table = renamed_table()
        self.codes = {r["region_cd"] for r in self.table}
        self.plan = pairs_job.plan_derivation(self.table, [], CONTRACT, CADASTRAL, "run", NOW, NO_PARCELS_BEFORE)

    def test_a_standing_approval_is_recorded_as_a_steward_row_and_named_for_the_fold(self):
        decisions = pairs_job.db_decisions(export(("d-1", "9811010100", "9911010100")))
        rows, verdicts = pairs_job.fold_steward_decisions(decisions, self.plan["review"], self.codes, NOW)
        self.assertEqual(verdicts, {"db:d-1": "recorded"})
        self.assertEqual((rows[0]["source"], rows[0]["old_code"], rows[0]["new_code"], rows[0]["detail"]),
                         (f"steward:{DECIDER}", "9811010100", "9911010100", "합성 공문"))
        self.assertEqual(pairs_job.fold_summary(verdicts, "run-1"), {"derivation_run_id": "run-1", "decision_ids": ["d-1"]})

    def test_an_approval_with_neither_reason_nor_note_is_recorded(self):
        decisions = pairs_job.db_decisions(export(("d-1", "9811010100", "9911010100"), note=""))
        rows, verdicts = pairs_job.fold_steward_decisions(decisions, self.plan["review"], self.codes, NOW)
        self.assertEqual(verdicts, {"db:d-1": "recorded"})
        self.assertEqual((rows[0]["source"], rows[0]["new_code"], rows[0]["detail"]), (f"steward:{DECIDER}", "9911010100", None))

    def test_the_run_refuses_again_what_the_list_or_the_table_does_not_hold(self):
        # 심은 위반: an approval of a change that is not listed (it was paired by the rule), and one of a
        # code the full table does not hold. The API checked them once; the run checks them again.
        decisions = pairs_job.db_decisions(export(("d-unlisted", "9811010200", "9911010200"),
                                                  ("d-no-code", "9811010100", "9911019900")))
        rows, verdicts = pairs_job.fold_steward_decisions(decisions, self.plan["review"], self.codes, NOW)
        self.assertEqual(rows, [])
        self.assertRegex(verdicts["db:d-unlisted"], "^rejected: .*not on the steward list")
        self.assertRegex(verdicts["db:d-no-code"], "^rejected: .*not a candidate")
        self.assertEqual(pairs_job.fold_summary(verdicts, "run-1")["decision_ids"], [], "a refused decision is not marked done")

    def test_a_split_can_never_be_approved(self):
        review = [dict(self.plan["review"][0], status="split")]
        decisions = pairs_job.db_decisions(export(("d-split", "9811010100", "9911010100")))
        _, verdicts = pairs_job.fold_steward_decisions(decisions, review, self.codes, NOW)
        self.assertRegex(verdicts["db:d-split"], "the data decides it")

    def test_a_partial_or_foreign_export_is_refused(self):
        whole = export(("d-1", "9811010100", "9911010100"))
        with self.assertRaisesRegex(ValueError, "not whole"):
            pairs_job.db_decisions(dict(whole, decision_count=2))
        with self.assertRaisesRegex(ValueError, "legal_dong_steward_decisions"):
            pairs_job.db_decisions(dict(whole, schema_version="v0"))


class ReviewHandoffTest(unittest.TestCase):
    def test_the_steward_list_is_a_handoff_the_steward_database_reads(self):
        rows = merged_table()
        # 을동 has two new codes of its name on DAY: the rule cannot choose, a person does.
        rows.append(row("9911010300", "합성특별시 가구 을동", parent="9911000000", created=DAY))
        table = pairs_job.cg.parse_full_table_html(table_html(rows), CONTRACT)
        plan = pairs_job.plan_derivation(table, [], CONTRACT, CADASTRAL, "run", NOW, NO_PARCELS_BEFORE)
        handoff = pairs_job.review_handoff(plan["review"], table, False, {"9811010200": 3}, NOW)

        self.assertEqual((handoff["schema_version"], handoff["unit"]), (review_queue.HANDOFF_SCHEMA_VERSION, "legal_dong"))
        self.assertEqual(handoff["item_count"], len(handoff["items"]))
        item = next(i for i in handoff["items"] if i["subject_code"] == "9811010200")
        self.assertEqual(item["item_id"], review_queue.item_id("legal_dong", "9811010200"))
        self.assertEqual(item["evidence_etag"], review_queue.evidence_etag(item["status"], item["candidates_json"]))
        candidates = json.loads(item["candidates_json"])
        self.assertEqual({c["code"] for c in candidates}, {"9911010200", "9911010300"})
        self.assertEqual({(c["name"], c["created_date"]) for c in candidates}, {("합성특별시 가구 을동", DAY)})
        evidence = json.loads(item["evidence_json"])
        self.assertEqual((evidence["name"], evidence["abolished_date"], evidence["rule"]),
                         ("합성도 가구 을동", DAY, "ambiguous_name_match"))
        self.assertEqual((evidence["official_history"], evidence["official_partial_moves"]), ("not_collected", 3))
        # The Rust loader refuses unknown and missing fields: the documents must have exactly its fields.
        self.assertEqual(set(evidence), rust_fields("LegalDongEvidence"))
        self.assertEqual(set(candidates[0]), rust_fields("LegalDongCandidate"))
        self.assertEqual(set(item), rust_fields("HandoffItem", "handoff.rs"))

    def test_only_pairs_are_published_for_a_decision(self):
        review = [{"kind": "official_off_window", "old_code": "9811010100", "status": "official_disagrees_off_window"}]
        self.assertEqual(pairs_job.review_handoff(review, [], True, {}, NOW)["items"], [])


class ValidateOnlyRunTest(unittest.TestCase):
    def test_a_run_writes_the_list_handoff_and_names_the_decisions_it_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            page = tmp / "table.html"
            rows = merged_table()
            rows[5] = row("9911010100", "합성특별시 가구 새갑동", parent="9911000000", created=DAY)
            page.write_text(table_html(rows), encoding="utf-8")
            decisions = tmp / "decisions.json"
            decisions.write_text(json.dumps(export(("d-1", "9811010100", "9911010100"))), encoding="utf-8")
            argv = ["--snapshot-date", "2099-07-02", "--table-source-record-id", "bronze/k", "--validate-only",
                    "--table-html", str(page), "--jibun-evidence", "editions", "--steward-decision-export", str(decisions),
                    "--review-handoff-output", str(tmp / "handoff.json"),
                    "--steward-fold-summary-output", str(tmp / "fold.json"), "--summary-output", str(tmp / "summary.json")]
            # The 지번 step ran and found nothing to weigh, so the change is a person's (ADR-0144 §3.4).
            with mock.patch.object(pairs_job, "edition_evidence", return_value=NO_PARCELS_BEFORE):
                self.assertEqual(pairs_job.main(argv), 0)
            fold = json.loads((tmp / "fold.json").read_text(encoding="utf-8"))
            summary = json.loads((tmp / "summary.json").read_text(encoding="utf-8"))
            handoff = json.loads((tmp / "handoff.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["steward_decisions"], {"db:d-1": "recorded"})
        self.assertEqual(fold, {"derivation_run_id": summary["run_id"], "decision_ids": ["d-1"]})
        # The approved change is paired now, so it is no longer on the list the database loads.
        self.assertNotIn("9811010100", {i["subject_code"] for i in handoff["items"]})



class RulesVersionTest(unittest.TestCase):
    """The load owes a re-pairing when this value changes (scripts/ops/legal-dong-code-load.sh)."""

    def test_the_host_and_the_spark_run_compute_the_same_version(self):
        before = pairs_job.rules_version()
        import lakehouse_ingest  # noqa: F401,PLC0415 - a sibling the Spark path loads and the host does not
        self.assertEqual(pairs_job.rules_version(), before)
        self.assertRegex(before, "^[0-9a-f]{64}$")

    def test_a_changed_rule_input_changes_the_version(self):
        before = pairs_job.rules_version()
        with tempfile.TemporaryDirectory() as tmp:
            changed = Path(tmp) / "vworld-parcel-source-objects.json"
            changed.write_bytes(pairs_job.editions.contract_path().read_bytes() + b" ")
            with mock.patch.object(pairs_job.editions, "contract_path", return_value=changed):
                self.assertNotEqual(pairs_job.rules_version(), before)

    def test_the_rules_reach_every_module_the_pairing_imports(self):
        # The walk follows `from x import y` too: the 지번 step's land rules live in parcel_land.
        parcel_land = Path(pairs_job.__file__).with_name("parcel_land.py")
        original = parcel_land.read_bytes()
        before = pairs_job.rules_version()
        real_read = Path.read_bytes
        with mock.patch.object(Path, "read_bytes", lambda path: real_read(path) + (b"#" if path.name == "parcel_land.py" else b"")):
            self.assertNotEqual(pairs_job.rules_version(), before)
        self.assertEqual(parcel_land.read_bytes(), original)

if __name__ == "__main__":
    unittest.main()
