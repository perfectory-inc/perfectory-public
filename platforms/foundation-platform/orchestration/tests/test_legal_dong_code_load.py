"""The 법정동 load half owes a pairing until one succeeds (scripts/ops/legal-dong-code-load.sh, root ADR-0143).

On 2026-10-06 a 30527 handoff was loaded and moved to loaded/, then the pairing failed; the next run saw no
pending handoff and no steward decision, so it never paired again. These tests run the real script, sourced
the way lineage-stewardship-cycle.sh sources it, with `spark` replaced by a stand-in that writes the outputs
the real jobs write (or fails the pairing on demand), and the publisher replaced by a stand-in for the
steward database (root ADR-0263), and read what it leaves in the state roots.
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
LOAD = ORCHESTRATION.parent / "scripts/ops/legal-dong-code-load.sh"

# Stands in for the Spark jobs: argv is (work, container_work, job, args...). Container paths map to work.
SPARK = textwrap.dedent("""
    import json, os, pathlib, sys
    work, container, job, args = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
    opt = {args[i]: args[i + 1] for i in range(len(args) - 1) if args[i].startswith("--")}
    out = lambda key: pathlib.Path(opt[key].replace(container, work, 1))
    if job == "legal_dong_code_snapshot_to_reference.py":
        out("--latest-marker-output").write_text(json.dumps(
            {"snapshot_date": opt["--snapshot-date"], "source_record_id": opt["--source-record-id"]}))
        out("--summary-output").write_text("{}")
    elif job == "legal_dong_code_change_pairs.py":
        if os.environ.get("FAIL_PAIRING"):
            sys.exit("PairingConflict")
        out("--projection-output").write_text('{"projection": true}')
        out("--review-output").write_text('{"review": []}')
        export = json.loads(out("--steward-decision-export").read_text())
        recorded = [d["decision_id"] for d in export["decisions"]]
        out("--review-handoff-output").write_text('{"items": []}')
        out("--steward-fold-summary-output").write_text(json.dumps({"derivation_run_id": "r", "decision_ids": recorded}))
        verdicts = {f"db:{i}": "recorded" for i in recorded}
        if "--steward-decisions" in opt:
            verdicts.update({p.name: "recorded" for p in out("--steward-decisions").glob("*.json")})
        summary = {"steward_decisions": verdicts, "rules_version": os.environ["RULES_VERSION"]}
        if os.environ.get("OFFICIAL_DECIDED"):
            summary["superseded_pairs"] = [{"old_code": "9999910100", "new_code": "9999910300",
                                            "source": "derived:code-go-kr:date+name:20990701",
                                            "official_targets": ["9999910500"], "kind": "dong_level"}]
            summary["steward_contradicted_by_official"] = {"9999910700": {
                "steward_pair": "9999910800", "steward_source": "steward:steward-a", "official_targets": ["9999910900"],
                "kind": "dong_level", "changed_on": ["20990701", "20990702"]}}
        out("--summary-output").write_text(json.dumps(summary))
    elif job == "vworld_parcel_number_change_history.py":
        out("--summary-output").write_text("{}")
    else:
        sys.exit("unexpected job " + job)
""")

# Stands in for the publisher and the steward database: `db-decisions.json` holds the ids of the standing
# approvals no run recorded yet; recording them removes them, as the fold record does.
PUBLISHER = textwrap.dedent("""
    import json, os, pathlib, sys
    db = pathlib.Path(os.environ["DB_DECISIONS"])
    standing = json.loads(db.read_text()) if db.exists() else []
    with open(os.environ["PUBLISHER_CALLS"], "a") as calls:
        calls.write(sys.argv[1] + "\\n")
    if sys.argv[1] == "export-legal-dong-steward-decisions":
        pathlib.Path(os.environ["FOUNDATION_PLATFORM_LEGAL_DONG_STEWARD_DECISIONS_OUTPUT"]).write_text(json.dumps(
            {"review_items": 0, "decision_count": len(standing), "decisions": [{"decision_id": i} for i in standing]}))
    elif sys.argv[1] == "record-lineage-steward-folds":
        done = json.loads(pathlib.Path(os.environ["FOUNDATION_PLATFORM_LINEAGE_STEWARD_FOLD_SUMMARY"]).read_text())
        db.write_text(json.dumps([i for i in standing if i not in done["decision_ids"]]))
    elif sys.argv[1] == "load-lineage-review-items":
        json.loads(pathlib.Path(os.environ["FOUNDATION_PLATFORM_LINEAGE_REVIEW_HANDOFF_INPUT"]).read_text())
    else:
        sys.exit("unexpected command " + sys.argv[1])
""")

DRIVER = textwrap.dedent("""
    set -euo pipefail
    work="${WORK_ROOT}/${RUN_ID}"
    container_work="/workspace/target/lakehouse/runs/${RUN_ID}"
    mkdir -p "${work}"
    run_log="${work}/run.log"
    publisher() { "${PUBLISHER_BIN}" "$@"; }
    notify_slack() { printf '%s\\n---\\n' "$1" >> "${WORK_ROOT}/slack.log"; }
    spark() { printf '%s\\n' "$1" >> "${CALLS}"; python3 "${SPARK_STUB}" "${work}" "${container_work}" "$@"; }
    source "${LOAD}"
    load_legal_dong_code_handoffs
    echo "wrote=${legal_dong_code_wrote}"
""")


class Harness(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="legal-dong-load-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        self.legal = self.root / "legal-dong-code"
        self.parcel = self.root / "parcel-number-change"
        self.legal.mkdir()
        self.parcel.mkdir()
        (self.root / "spark.py").write_text(SPARK)
        (self.root / "driver.sh").write_text(DRIVER)
        (self.root / "publisher.py").write_text(PUBLISHER)
        # Stands in for the pairing job's `rules-version`: the deployed rules are whatever RULES_VERSION says.
        (self.root / "jobs").mkdir()
        (self.root / "jobs/legal_dong_code_change_pairs.py").write_text("import os\nprint(os.environ['RULES_VERSION'])\n")
        self.rules = "v1"
        publisher = self.root / "publisher.sh"
        publisher.write_text(f'#!/usr/bin/env bash\nexec python3 "{(self.root / "publisher.py").as_posix()}" "$@"\n')
        publisher.chmod(0o755)
        self.db = self.root / "db-decisions.json"
        self.publisher_calls = self.root / "publisher-calls.log"
        self.journal = self.root / "journal.log"
        self.calls = self.root / "calls.log"
        self.runs = 0

    def run_cycle(self, fail_pairing=False, official_decided=False):
        self.runs += 1
        self.calls.write_text("")
        self.publisher_calls.write_text("")
        env = {"PATH": os.environ["PATH"], "LOAD": str(LOAD), "SPARK_STUB": str(self.root / "spark.py"),
               "WORK_ROOT": str(self.root / "runs"), "RUN_ID": f"run{self.runs}", "CALLS": str(self.calls),
               "journal": str(self.journal), "run_id": f"run{self.runs}",
               "FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT": str(self.legal),
               "FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT": str(self.parcel),
               "PUBLISHER_BIN": str(self.root / "publisher.sh"), "DB_DECISIONS": str(self.db),
               "PUBLISHER_CALLS": str(self.publisher_calls), "RULES_VERSION": self.rules,
               "FOUNDATION_LEGAL_DONG_JOBS_DIR": str(self.root / "jobs")}
        if fail_pairing:
            env["FAIL_PAIRING"] = "1"
        if official_decided:
            env["OFFICIAL_DECIDED"] = "1"
        return subprocess.run(["bash", str(self.root / "driver.sh")], env=env, capture_output=True, text=True,
                              timeout=120)

    def jobs(self):
        return self.calls.read_text().split()

    def publisher(self):
        return self.publisher_calls.read_text().split()

    def published(self):
        (self.legal / "steward-list-published.json").write_text("{}")

    def journal_lines(self):
        return self.journal.read_text(encoding="utf-8").splitlines() if self.journal.exists() else []

    def stage_legal_dong(self, name, snapshot_date):
        handoff = self.legal / "pending" / name
        handoff.mkdir(parents=True)
        (handoff / "handoff.json").write_text(json.dumps(
            {"snapshot_date": snapshot_date, "table_object_key": f"bronze/{name}.html"}))
        (handoff / "manifest.json").write_text(json.dumps({"objects": [{"role": "full_table", "local_path": "t.html"}]}))
        (handoff / "t.html").write_text("<table></table>")

    def stage_parcel_number_change(self, name):
        handoff = self.parcel / "pending" / name
        handoff.mkdir(parents=True)
        (handoff / "handoff.json").write_text("{}")

    def latest_snapshot(self, snapshot_date="20990701"):
        (self.legal / "latest-legal-dong-snapshot.json").write_text(json.dumps(
            {"snapshot_date": snapshot_date, "source_record_id": "bronze/table.html"}))

    def marker(self):
        path = self.legal / "pairing-owed.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

class PairingOwed(Harness):
    def test_a_failed_pairing_after_official_history_is_owed_and_the_next_run_pays_it(self):
        self.latest_snapshot()
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))
        self.stage_parcel_number_change("pnch-20990702T000000Z")

        failed = self.run_cycle(fail_pairing=True)
        self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
        self.assertTrue((self.parcel / "loaded/pnch-20990702T000000Z").is_dir())
        owed = self.marker()
        self.assertEqual(owed["owed_since_run"], "run1")
        self.assertEqual(owed["handoffs"]["parcel_number_change"], ["pnch-20990702T000000Z"])
        self.assertEqual(owed["snapshot_date"], "20990701")

        repaired = self.run_cycle()
        self.assertEqual(repaired.returncode, 0, repaired.stdout + repaired.stderr)
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        self.assertIsNone(self.marker())
        self.assertEqual(json.loads((self.legal / "last-pairing.json").read_text())["paid"]["owed_since_run"], "run1")
        run2 = [line for line in self.journal_lines() if line.endswith("run=run2")]
        self.assertTrue(any("pairing owed since run1" in line for line in run2), run2)
        self.assertTrue(any("re-paired (pairing owed) on 20990701" in line for line in run2), run2)
        self.assertEqual(repaired.stdout.splitlines()[-1], "wrote=1", "the cycle's evidence of a write")

        self.run_cycle()
        self.assertEqual(self.jobs(), [])
        self.assertIn("no pending handoff run=run3", self.journal_lines()[-1])

    def test_a_failed_pairing_of_a_new_table_stays_owed_across_runs(self):
        self.stage_legal_dong("table-20990701", "20990701")
        self.assertNotEqual(self.run_cycle(fail_pairing=True).returncode, 0)
        self.assertNotEqual(self.run_cycle(fail_pairing=True).returncode, 0)
        self.assertEqual(self.marker()["owed_since_run"], "run1")
        self.assertEqual(self.marker()["handoffs"]["legal_dong_code"], ["table-20990701"])
        self.assertTrue(any("pairing owed since run1" in line and line.endswith("run=run2")
                            for line in self.journal_lines()))

    def test_a_successful_pairing_clears_what_it_owed(self):
        self.stage_parcel_number_change("pnch-20990702T000000Z")
        self.stage_legal_dong("table-20990701", "20990701")
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["vworld_parcel_number_change_history.py",
                                       "legal_dong_code_snapshot_to_reference.py", "legal_dong_code_change_pairs.py"])
        self.assertIsNone(self.marker())
        paid = json.loads((self.legal / "last-pairing.json").read_text())
        self.assertEqual(paid["run"], "run1")
        self.assertEqual(paid["paid"]["handoffs"], {"legal_dong_code": ["table-20990701"],
                                                    "parcel_number_change": ["pnch-20990702T000000Z"]})
        self.assertTrue((self.legal / "loaded/table-20990701").is_dir())
        self.assertEqual(result.stdout.splitlines()[-1], "wrote=1")

    def test_nothing_owed_runs_nothing(self):
        self.latest_snapshot()
        self.published()
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))
        (self.legal / "loaded/table-20990701").mkdir(parents=True)
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), [])
        self.assertIsNone(self.marker())
        self.assertEqual(len(self.journal_lines()), 1)
        self.assertIn("legal-dong-code no pending handoff run=run1", self.journal_lines()[0])
        # The unit's journal carries the same line (root ADR-0174).
        self.assertIn("legal-dong-code no pending handoff run=run1", result.stdout.splitlines())
        # Nothing loaded or paired: the cycle has no write to report (lineage-stewardship-cycle.sh).
        self.assertEqual(result.stdout.splitlines()[-1], "wrote=0")

    def test_an_empty_host_owes_nothing(self):
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), [])
        self.assertIsNone(self.marker())
        self.assertFalse((self.legal / "last-pairing.json").exists())

    def test_handoffs_loaded_before_any_recorded_pairing_are_paired_once(self):
        # The host as it stood on 2026-10-06: tables and official history in loaded/, no record of a pairing after them.
        self.latest_snapshot()
        (self.legal / "loaded/table-20990701").mkdir(parents=True)
        (self.parcel / "loaded/pnch-20990702T000000Z").mkdir(parents=True)
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        paid = json.loads((self.legal / "last-pairing.json").read_text())["paid"]
        self.assertEqual(paid["reason"], "loaded handoffs with no pairing recorded after them")
        self.assertEqual(paid["handoffs"]["parcel_number_change"], ["pnch-20990702T000000Z"])
        self.run_cycle()
        self.assertEqual(self.jobs(), [])

    def test_a_debt_with_no_table_to_pair_on_waits_for_one(self):
        self.stage_parcel_number_change("pnch-20990702T000000Z")
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["vworld_parcel_number_change_history.py"])
        self.assertEqual(self.marker()["owed_since_run"], "run1")
        self.assertIn("no legal-dong snapshot loaded yet", self.journal_lines()[-1])


class StewardDatabase(Harness):
    """The steward list and decisions go through the steward database (root ADR-0263)."""

    def setUp(self):
        super().setUp()
        self.latest_snapshot()
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))

    def test_a_list_the_database_never_received_is_paired_and_published_once(self):
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        self.assertEqual(self.publisher(), ["export-legal-dong-steward-decisions", "load-lineage-review-items"])
        self.assertTrue((self.legal / "steward-list-published.json").exists())
        self.assertTrue(any("re-paired (steward database)" in line for line in self.journal_lines()))
        self.run_cycle()
        self.assertEqual(self.jobs(), [], "a published list with no approval waiting owes nothing")

    def test_a_standing_approval_is_paired_recorded_and_not_paired_again(self):
        self.published()
        self.db.write_text(json.dumps(["d-1"]))
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        self.assertEqual(self.publisher(), ["export-legal-dong-steward-decisions", "export-legal-dong-steward-decisions",
                                            "record-lineage-steward-folds", "load-lineage-review-items"])
        self.assertEqual(json.loads(self.db.read_text()), [], "the run recorded the approval it appended")
        self.run_cycle()
        self.assertEqual(self.jobs(), [])

    def test_a_staged_file_and_a_database_approval_reach_the_same_run(self):
        self.published()
        self.db.write_text(json.dumps(["d-1"]))
        (self.legal / "steward/pending").mkdir(parents=True)
        (self.legal / "steward/pending/20990102T000000Z-steward-a.json").write_text("{}")
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(json.loads(self.db.read_text()), [])
        self.assertTrue((self.legal / "steward/recorded/20990102T000000Z-steward-a.json").exists())

    def test_a_failed_pairing_records_no_approval(self):
        self.published()
        self.db.write_text(json.dumps(["d-1"]))
        self.assertNotEqual(self.run_cycle(fail_pairing=True).returncode, 0)
        self.assertNotIn("record-lineage-steward-folds", self.publisher())
        self.assertEqual(json.loads(self.db.read_text()), ["d-1"], "an approval the run did not append stays standing")


class OfficialEvidenceDecided(Harness):
    """Root ADR-0329: a superseded inference is journalled; a contradicted steward pair reaches a person."""

    def test_a_supersession_is_journalled_and_a_contradicted_steward_pair_is_one_alert(self):
        self.latest_snapshot()
        self.published()
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))
        self.stage_parcel_number_change("pnch-20991101T000000Z")
        result = self.run_cycle(official_decided=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        run1 = [line for line in self.journal_lines() if line.endswith("run=run1")]
        self.assertTrue(any("legal-dong-code superseded 9999910100->9999910300 (derived:code-go-kr:date+name:20990701) "
                            "by official dong_level 9999910500" in line for line in run1), run1)
        self.assertTrue(any("legal-dong-code steward-contradicted 9999910700->9999910800" in line for line in run1), run1)
        slack = (self.root / "runs" / "slack.log").read_text(encoding="utf-8").split("---\n")
        self.assertEqual(len([m for m in slack if m.strip()]), 1, "one message per run")
        self.assertIn("9999910700->9999910800", slack[0])
        self.assertNotIn("9999910100", slack[0], "a supersession is the record working, not an alert")

    def test_a_pairing_without_either_says_nothing(self):
        self.latest_snapshot()
        self.published()
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))
        self.stage_parcel_number_change("pnch-20991101T000000Z")
        self.assertEqual(self.run_cycle().returncode, 0)
        self.assertFalse((self.root / "runs" / "slack.log").exists())
        self.assertFalse(any("superseded" in line for line in self.journal_lines()))


class RulesVersion(Harness):
    """A deploy that changes the pairing rules owes one re-pairing (the 2026-10-16 fix waited for data)."""

    def setUp(self):
        super().setUp()
        self.latest_snapshot()
        self.published()

    def test_new_rules_are_paired_once_and_recorded(self):
        (self.legal / "last-pairing.json").write_text(json.dumps({"rules_version": "v1"}))
        self.run_cycle()
        self.assertEqual(self.jobs(), [], "the rules the last pairing ran with: nothing owed")
        self.rules = "v2"
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        self.assertTrue(any("re-paired (pairing rules changed)" in line for line in self.journal_lines()))
        self.assertEqual(json.loads((self.legal / "last-pairing.json").read_text())["rules_version"], "v2")
        self.run_cycle()
        self.assertEqual(self.jobs(), [], "paired once")

    def test_a_pairing_recorded_before_versions_were_recorded_is_paired_once(self):
        (self.legal / "last-pairing.json").write_text("{}")
        self.run_cycle()
        self.assertEqual(self.jobs(), ["legal_dong_code_change_pairs.py"])
        self.run_cycle()
        self.assertEqual(self.jobs(), [])


if __name__ == "__main__":
    sys.exit(unittest.main())
