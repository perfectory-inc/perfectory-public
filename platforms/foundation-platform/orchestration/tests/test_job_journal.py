"""A job's journal lines and the end of a failed run's log reach the unit's journal (root ADR-0174).

The registered jobs keep a journal file and a run log under their state directory, which belongs to
the service account: an operator cannot read it, and `journalctl -u <unit>` showed only "exit 1"
when the daily sweep refused its backlog on 2026-10-09. scripts/ops/job-journal.sh is the one way a
job writes its journal file; these tests run it, and check that every job script uses it.
"""

import os
import pathlib
import re
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
OPS = PLATFORM / "scripts/ops"
HELPER = OPS / "job-journal.sh"


def run(script, **env):
    return subprocess.run(["bash", "-c", f'set -euo pipefail; source "$HELPER"; {script}'],
                          env={"PATH": os.environ["PATH"], "HELPER": str(HELPER), **env},
                          capture_output=True, text=True, timeout=60, check=False)


class TheHelper(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-journal-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        self.journal = self.root / "journal.log"
        self.run_log = self.root / "run.log"

    def test_a_journal_line_goes_to_the_file_and_to_stdout(self):
        result = run('job_journal "$J" "sweep hub planned=3 new=0"', J=str(self.journal))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "sweep hub planned=3 new=0\n")
        line = self.journal.read_text(encoding="utf-8")
        self.assertRegex(line, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ sweep hub planned=3 new=0\n$")

    def test_an_unwritable_journal_file_still_reaches_stdout(self):
        result = run('job_journal "$J" "fold unit=complex skipped"', J=str(self.root / "missing/journal.log"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "fold unit=complex skipped\n")

    def test_a_failed_runs_tail_reaches_stderr_masked_and_prefixed(self):
        lines = [f"downloaded file {n}" for n in range(100)] + [
            "connect postgres://foundation_admin:planted-password@127.0.0.1:15434/foundation failed",
            "upstream refused bearer planted-token",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=planted-secret",
            "foundation-job-outcome changed",
            "x" * 1000,
            "progress 10%\rprogress 100%",
            "VWorld 새 파일 40건이 하루 예산을 넘었다",
        ]
        self.run_log.write_text("\n".join(lines) + "\n", encoding="utf-8")
        result = run('job_run_log_tail "$L"', L=str(self.run_log))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        relayed = result.stderr.splitlines()
        self.assertIn(str(self.run_log), relayed[0])
        self.assertEqual(len(relayed), 41, "the last 40 lines, not the whole log")
        self.assertNotIn("downloaded file 59", result.stderr)
        self.assertIn("downloaded file 99", result.stderr)
        for secret in ("planted-password", "planted-token", "planted-secret"):
            self.assertNotIn(secret, result.stderr)
        self.assertTrue(all(line.startswith("  | ") for line in relayed[1:]))
        # A relayed line never reads as the job's own outcome (job_specs.OUTCOME_LINE is anchored).
        self.assertNotRegex(result.stderr, r"(?m)^foundation-job-outcome")
        self.assertTrue(all(len(line) <= 4 + 400 + 3 for line in relayed[1:]))
        self.assertIn("  | progress 100%", relayed)
        self.assertIn("  | VWorld 새 파일 40건이 하루 예산을 넘었다", relayed)

    def test_failed_files_reach_stderr_masked_then_cut(self):
        lines = ("src:1\tupstream https://user:planted-password@example.invalid/x refused\n"
                 "src:2\t" + "API_KEY=planted-key " + "y" * 500 + "\n"
                 "\tno file id is no line\n"
                 "src:3\t\n")
        result = run('printf "%s" "$LINES" | job_failed_files; echo after', LINES=lines)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "after\n")
        relayed = result.stderr.splitlines()
        self.assertEqual(relayed[0], "failed src:1 upstream https://user:***@example.invalid/x refused")
        self.assertTrue(relayed[1].startswith("failed src:2 API_KEY=*** yyy"), relayed[1])
        self.assertEqual(len(relayed[1].split(" ", 2)[2]), 200 + 3)
        self.assertEqual(relayed[2], "failed src:3 ")
        self.assertEqual(len(relayed), 3)
        self.assertNotIn("planted-", result.stderr)

    def test_a_missing_run_log_is_said_and_does_not_fail(self):
        result = run('job_run_log_tail "$L"; echo after', L=str(self.root / "none.log"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "after\n")
        self.assertIn("empty or missing", result.stderr)

    def test_inside_an_err_trap_the_first_failure_keeps_its_status(self):
        self.run_log.write_text("publisher: refused\n", encoding="utf-8")
        result = run('trap \'job_journal "$J" "x FAILED" >&2; job_run_log_tail "$L"\' ERR; false',
                     J=str(self.journal), L=str(self.run_log))
        self.assertEqual(result.returncode, 1)
        self.assertIn("x FAILED", result.stderr)
        self.assertIn("  | publisher: refused", result.stderr)


# A job's journal file written any other way than job_journal is a line no operator sees.
DIRECT_WRITE = re.compile(r'>>\s*"\$\{journal\}"')
TAIL_COPY = re.compile(r'^\s*(?:trap .*; )?tail -5 "\$\{run_log\}" >> "\$\{journal\}"')


def job_scripts():
    return sorted(path for path in OPS.glob("*.sh") if path.name != HELPER.name
                  and "${journal}" in path.read_text(encoding="utf-8"))


class EveryJobUsesIt(unittest.TestCase):
    def test_the_check_sees_the_scripts_that_keep_a_journal(self):
        names = {path.name for path in job_scripts()}
        self.assertLessEqual({"daily-source-sweep.sh", "map-edit-fold.sh", "lineage-stewardship-cycle.sh",
                              "legal-dong-code-collect.sh", "legal-dong-code-load.sh",
                              "parcel-number-change-collect.sh", "vworld-parcel-edition-collect.sh",
                              "raon-large-files.sh"}, names)

    def test_journal_lines_go_through_the_helper(self):
        for path in job_scripts():
            with self.subTest(path.name):
                text = path.read_text(encoding="utf-8")
                self.assertIn('source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"', text)
                direct = [line for line in text.splitlines()
                          if DIRECT_WRITE.search(line) and not TAIL_COPY.search(line)
                          and "job_journal" not in line]
                self.assertEqual(direct, [], "write the journal with job_journal, so the unit's journal has it")

    def test_a_failed_run_relays_the_end_of_its_log(self):
        for path in job_scripts():
            text = path.read_text(encoding="utf-8")
            if "run_log=" not in text:
                continue
            with self.subTest(path.name):
                self.assertIn('job_run_log_tail "${run_log}"', text)

    def test_the_check_rejects_a_direct_write(self):
        planted = "printf '%s x\\n' \"$(date -u +%FT%TZ)\" >> \"${journal}\""
        self.assertTrue(DIRECT_WRITE.search(planted) and not TAIL_COPY.search(planted))
        kept = 'tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true'
        self.assertTrue(TAIL_COPY.search(kept))


if __name__ == "__main__":
    unittest.main()
