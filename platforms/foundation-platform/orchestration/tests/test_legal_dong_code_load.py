"""The 법정동 load half owes a pairing until one succeeds (scripts/ops/legal-dong-code-load.sh, root ADR-0143).

On 2026-10-06 a 30527 handoff was loaded and moved to loaded/, then the pairing failed; the next run saw no
pending handoff and no steward decision, so it never paired again. These tests run the real script, sourced
the way lineage-stewardship-cycle.sh sources it, with `spark` replaced by a stand-in that writes the outputs
the real jobs write (or fails the pairing on demand), and read what it leaves in the state roots.
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
        out("--summary-output").write_text('{"steward_decisions": {}}')
    elif job == "vworld_parcel_number_change_history.py":
        out("--summary-output").write_text("{}")
    else:
        sys.exit("unexpected job " + job)
""")

DRIVER = textwrap.dedent("""
    set -euo pipefail
    work="${WORK_ROOT}/${RUN_ID}"
    container_work="/workspace/target/lakehouse/runs/${RUN_ID}"
    mkdir -p "${work}"
    spark() { printf '%s\\n' "$1" >> "${CALLS}"; python3 "${SPARK_STUB}" "${work}" "${container_work}" "$@"; }
    source "${LOAD}"
    load_legal_dong_code_handoffs
""")


class PairingOwed(unittest.TestCase):
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
        self.journal = self.root / "journal.log"
        self.calls = self.root / "calls.log"
        self.runs = 0

    def run_cycle(self, fail_pairing=False):
        self.runs += 1
        self.calls.write_text("")
        env = {"PATH": os.environ["PATH"], "LOAD": str(LOAD), "SPARK_STUB": str(self.root / "spark.py"),
               "WORK_ROOT": str(self.root / "runs"), "RUN_ID": f"run{self.runs}", "CALLS": str(self.calls),
               "journal": str(self.journal), "run_id": f"run{self.runs}",
               "FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT": str(self.legal),
               "FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT": str(self.parcel)}
        if fail_pairing:
            env["FAIL_PAIRING"] = "1"
        return subprocess.run(["bash", str(self.root / "driver.sh")], env=env, capture_output=True, text=True,
                              timeout=120)

    def jobs(self):
        return self.calls.read_text().split()

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

    def test_a_failed_pairing_after_official_history_is_owed_and_the_next_run_pays_it(self):
        self.latest_snapshot()
        (self.legal / "last-pairing.json").write_text("{}")
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

    def test_nothing_owed_runs_nothing(self):
        self.latest_snapshot()
        (self.legal / "last-pairing.json").write_text("{}")
        (self.legal / "loaded/table-20990701").mkdir(parents=True)
        result = self.run_cycle()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.jobs(), [])
        self.assertIsNone(self.marker())
        self.assertEqual(len(self.journal_lines()), 1)
        self.assertIn("legal-dong-code no pending handoff run=run1", self.journal_lines()[0])
        # The unit's journal carries the same line (root ADR-0174).
        self.assertIn("legal-dong-code no pending handoff run=run1", result.stdout.splitlines())

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


if __name__ == "__main__":
    sys.exit(unittest.main())
