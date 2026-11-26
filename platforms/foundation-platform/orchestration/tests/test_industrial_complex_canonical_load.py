"""The industrial-complex canonical load is its own job and says what it changed (W103).

Until the catalog's takeover (root ADR-0564 §6) the load was a step of lineage_stewardship, after the
Spark merge that carried the complexes' region codes to the current pairs. The versions build now
carries them on every build, so the load follows that build's commits. This holds that it is a job
no Spark pool can hold, that the catalog's build starts it, that the lineage cycle no longer carries
it, and that its last line is `changed` only when a row was inserted or updated. The publisher is a
stub.
"""

import json
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/industrial-complex-canonical-load.sh"
JOBS = PLATFORM / "orchestration/jobs.v1.json"
DEPENDENCIES = PLATFORM / "orchestration/job-dependencies.v1.json"

RUNTIME_STUB = """# shellcheck shell=bash
PUBLISHER_BIN="${STUB_PUBLISHER}"
"""

DATABASE_URL_STUB = """# shellcheck shell=bash
foundation_database_url() { printf 'postgres://stub\\n'; }
"""

PUBLISHER_STUB = """#!/usr/bin/env bash
[ "$1" = load-industrial-complex-canonical ] || { echo "unexpected command $1" >&2; exit 64; }
[ "${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_CONFIRM}" = true ] || exit 65
[ -n "${DATABASE_URL}" ] || exit 66
[ -n "${STUB_SUMMARY}" ] || { echo "the database is away" >&2; exit 1; }
printf '%s' "${STUB_SUMMARY}" > "${FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_SUMMARY_PATH}"
"""


class TheJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.listing = json.loads(JOBS.read_text(encoding="utf-8"))
        cls.job = next(job for job in cls.listing["jobs"] if job["id"] == "industrial_complex_canonical_load")

    def test_no_spark_pool_can_hold_it(self):
        self.assertNotIn(self.job["pool"], self.listing["pools"])

    def test_the_catalog_s_build_starts_it(self):
        self.assertEqual(self.job["started_by"], "inputs")
        entry = next(job for job in json.loads(DEPENDENCIES.read_text(encoding="utf-8"))["jobs"]
                     if job["id"] == "industrial_complex_canonical_load")
        self.assertEqual({read["node_id"] for read in entry["reads"]}, {"gold-complex-catalog-versions"})
        self.assertIn("gold_complex_catalog_versions_build", entry["producers"])

    def test_the_lineage_cycle_no_longer_loads_or_merges(self):
        cycle = (PLATFORM / "scripts/ops/lineage-stewardship-cycle.sh").read_text(encoding="utf-8")
        self.assertNotIn("load-industrial-complex-canonical", cycle)
        self.assertNotIn("--follow-region-codes", cycle)


class TheRun(unittest.TestCase):
    def run_script(self, summary):
        with tempfile.TemporaryDirectory(prefix="complex-canonical-") as directory:
            root = pathlib.Path(directory)
            ops = root / "ops"
            ops.mkdir()
            shutil.copy(SCRIPT, ops / SCRIPT.name)
            shutil.copy(SCRIPT.with_name("job-journal.sh"), ops / "job-journal.sh")
            (ops / "admitted-writer-runtime.sh").write_text(RUNTIME_STUB, encoding="utf-8", newline="\n")
            (ops / "database-url.sh").write_text(DATABASE_URL_STUB, encoding="utf-8", newline="\n")
            publisher = root / "publisher"
            publisher.write_text(PUBLISHER_STUB, encoding="utf-8", newline="\n")
            publisher.chmod(0o755)
            state = root / "state"
            result = subprocess.run(
                ["bash", str(ops / SCRIPT.name)], capture_output=True, text=True, encoding="utf-8", timeout=120,
                check=False, env={"PATH": subprocess.os.environ["PATH"], "STUB_PUBLISHER": str(publisher),
                                  "STUB_SUMMARY": "" if summary is None else json.dumps(summary),
                                  "FOUNDATION_INDUSTRIAL_COMPLEX_CANONICAL_STATE_ROOT": str(state)})
            journal = (state / "journal.log").read_text(encoding="utf-8") if (state / "journal.log").exists() else ""
        return result, journal

    @classmethod
    def setUpClass(cls):
        if shutil.which("bash") is None or shutil.which("python3") is None:
            raise unittest.SkipTest("needs bash and python3")

    def test_an_updated_row_is_a_change(self):
        result, journal = self.run_script({"inserted_row_count": 0, "updated_row_count": 2,
                                           "unchanged_row_count": 1440, "skipped_row_count": 0})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome changed")
        self.assertIn("inserted=0 updated=2 unchanged=1440 skipped=0", journal)

    def test_nothing_written_says_so(self):
        result, _journal = self.run_script({"inserted_row_count": 0, "updated_row_count": 0,
                                            "unchanged_row_count": 1442, "skipped_row_count": 0})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "foundation-job-outcome unchanged")

    def test_a_failed_load_fails_the_run_and_names_no_outcome(self):
        result, journal = self.run_script(None)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("foundation-job-outcome", result.stdout)
        self.assertIn("industrial-complex canonical load FAILED", journal)


if __name__ == "__main__":
    unittest.main()
