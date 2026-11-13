"""The industrial-complex edge export is its own job and says what it changed (root ADR-0563 §3).

2026-11-12 20:55Z the fact job folded a complex's document facts into gold.complex_document_facts;
the export that puts them in the complex's edge profile was a step of lineage_stewardship, which
holds every spark slot, and on 2026-11-13 it was still refused its pool after 04:00Z, so the panel
showed nothing. The export uses no Spark. This holds that it is a job no Spark pool can hold, that
the runs of both tables' writers start it, that the lineage cycle no longer carries it, and that its
last line is `changed` only when a new index generation was published. The publisher is a stub.
"""

import json
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/industrial-complex-edge-export.sh"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
DEPENDENCIES = PLATFORM / "orchestration/job-dependencies.v1.json"

RUNTIME_STUB = """# shellcheck shell=bash
PUBLISHER_BIN="${STUB_PUBLISHER}"
"""

PUBLISHER_STUB = """#!/usr/bin/env bash
[ "$1" = export-industrial-complex-gold-profiles ] || { echo "unexpected command $1" >&2; exit 64; }
[ "${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_CONFIRM_EXPORT}" = true ] || exit 65
[ "${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_STORAGE_DRIVER}" = r2 ] || exit 66
[ -n "${STUB_SUMMARY}" ] || { echo "the catalog is away" >&2; exit 1; }
printf '%s' "${STUB_SUMMARY}" > "${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_SUMMARY_PATH}"
"""


class TheJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.listing = json.loads(JOBS.read_text(encoding="utf-8"))
        cls.job = next(job for job in cls.listing["jobs"] if job["id"] == "industrial_complex_edge_export")

    def test_no_spark_pool_can_hold_it(self):
        self.assertNotIn(self.job["pool"], self.listing["pools"])

    def test_the_writers_of_both_tables_start_it(self):
        self.assertEqual(self.job["started_by"], "inputs")
        entry = next(job for job in json.loads(DEPENDENCIES.read_text(encoding="utf-8"))["jobs"]
                     if job["id"] == "industrial_complex_edge_export")
        self.assertEqual({read["node_id"] for read in entry["reads"]},
                         {"gold-complex-catalog", "gold-complex-document-facts"})
        self.assertLessEqual({"intelligence_document_fact", "lineage_stewardship"}, set(entry["producers"]))

    def test_the_lineage_cycle_no_longer_exports(self):
        cycle = (PLATFORM / "scripts/ops/lineage-stewardship-cycle.sh").read_text(encoding="utf-8")
        self.assertNotIn("export-industrial-complex-gold-profiles", cycle)


class TheRun(unittest.TestCase):
    def run_script(self, summary):
        with tempfile.TemporaryDirectory(prefix="complex-edge-") as directory:
            root = pathlib.Path(directory)
            ops = root / "ops"
            ops.mkdir()
            shutil.copy(SCRIPT, ops / SCRIPT.name)
            shutil.copy(SCRIPT.with_name("job-journal.sh"), ops / "job-journal.sh")
            (ops / "admitted-writer-runtime.sh").write_text(RUNTIME_STUB, encoding="utf-8", newline="\n")
            publisher = root / "publisher"
            publisher.write_text(PUBLISHER_STUB, encoding="utf-8", newline="\n")
            publisher.chmod(0o755)
            state = root / "state"
            result = subprocess.run(
                ["bash", str(ops / SCRIPT.name)], capture_output=True, text=True, encoding="utf-8", timeout=120,
                check=False, env={"PATH": subprocess.os.environ["PATH"], "STUB_PUBLISHER": str(publisher),
                                  "STUB_SUMMARY": "" if summary is None else json.dumps(summary),
                                  "FOUNDATION_INDUSTRIAL_COMPLEX_EDGE_STATE_ROOT": str(state)})
            journal = (state / "journal.log").read_text(encoding="utf-8") if (state / "journal.log").exists() else ""
        return result, journal

    @classmethod
    def setUpClass(cls):
        if shutil.which("bash") is None or shutil.which("python3") is None:
            raise unittest.SkipTest("needs bash and python3")

    def test_a_new_index_generation_is_a_change(self):
        result, journal = self.run_script({"index_generation": 7, "artifact_count": 1, "unchanged_profile_count": 1299,
                                           "document_facts_complex_count": 1})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("generation=7 written=1 unchanged=1299 document_facts_complexes=1", journal)

    def test_nothing_changed_writes_nothing_and_says_so(self):
        result, _journal = self.run_script({"index_generation": None, "artifact_count": 0,
                                            "unchanged_profile_count": 1300, "document_facts_complex_count": 1})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome unchanged")

    def test_a_failed_export_fails_the_run_and_names_no_outcome(self):
        result, journal = self.run_script(None)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        self.assertIn("industrial-complex edge FAILED", journal)


if __name__ == "__main__":
    unittest.main()
