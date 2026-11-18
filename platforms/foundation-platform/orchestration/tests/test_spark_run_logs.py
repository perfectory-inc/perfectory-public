"""A Spark run's log file is made private before Spark writes to it, and the one-off that blanks
the credentials already logged does what it says.

2026-11-11 23:34Z..23:38Z Spark printed the lakehouse catalog writer token seven times into the
실거래 Gold's run log, a 0644 file under 0755 and 0777 directories: every account on the host could
read it. The settings no longer carry the value (infra/lakehouse/spark/tests/
test_no_credential_enters_the_spark_settings.py); what the engine prints is still its own choice, so
the file it prints into is the job account's.
"""

from __future__ import annotations

import os
import pathlib
import re
import stat
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
OPS = PLATFORM / "scripts/ops"
RUNTIME = OPS / "admitted-writer-runtime.sh"
REDACT = OPS / "redact-logged-credentials.py"
CONTINUES = chr(92)
REDIRECT = re.compile(r'>>?\s*("[^"]+"|\S+)\s+2>&1')


def spark_runs_kept_in_a_file(text: str) -> list[tuple[int, str]]:
    """(index of the statement's first line, the file its output goes to) of each
    lakehouse_spark_run whose output a script keeps."""
    lines, found, index = text.split("\n"), [], 0
    while index < len(lines):
        start = index
        while lines[index].rstrip().endswith(CONTINUES) and index + 1 < len(lines):
            index += 1
        statement = "\n".join(lines[start:index + 1])
        index += 1
        if "lakehouse_spark_run " not in statement or statement.lstrip().startswith("#"):
            continue
        target = REDIRECT.search(statement)
        if target and target.group(1) != "/dev/null":
            found.append((start, target.group(1)))
    return found


class EverySparkRunLogIsMadePrivateFirst(unittest.TestCase):
    def test_a_kept_spark_log_is_made_by_lakehouse_run_log(self):
        kept = 0
        for script in sorted(OPS.glob("*.sh")):
            text = script.read_text(encoding="utf-8")
            lines = text.split("\n")
            for start, target in spark_runs_kept_in_a_file(text):
                kept += 1
                made = f"lakehouse_run_log {target}"
                # Made in the lines just before the run: the statement's own function or block.
                before = "\n".join(lines[max(0, start - 6):start])
                self.assertIn(made, before, f"{script.name}:{start + 1} keeps Spark's output in {target} "
                                            "without making the file with lakehouse_run_log first")
        self.assertGreaterEqual(kept, 6, "the scan no longer finds the Spark runs that keep a log")

    @unittest.skipIf(os.name == "nt", "file modes are the host's")
    def test_the_file_is_the_accounts_and_its_groups_only(self):
        definition = next(line for line in RUNTIME.read_text(encoding="utf-8").split("\n")
                          if line.startswith("lakehouse_run_log()"))
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory, "run.log")
            script = f'umask 022\n{definition}\nlakehouse_run_log "$1"\necho line >>"$1"\nlakehouse_run_log "$1"\n'
            subprocess.run(["bash", "-c", script, "bash", str(log)], check=True)
            self.assertEqual(stat.S_IMODE(log.stat().st_mode), 0o640)
            self.assertEqual(log.read_text(), "line\n", "a file that exists is kept as it is")
            log.chmod(0o644)
            subprocess.run(["bash", "-c", f'{definition}\nlakehouse_run_log "$1"; lakehouse_run_log /dev/null',
                            "bash", str(log)], check=True)
            self.assertEqual(stat.S_IMODE(log.stat().st_mode), 0o640, "a log an earlier step made is closed too")


@unittest.skipIf(os.name == "nt", "file modes and in-place maps are the host's")
class TheOneOffBlanksWhatWasLogged(unittest.TestCase):
    SECRET = "fixture-credential-0123456789abcdef"

    def run_it(self, root: pathlib.Path, *roots: str) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-I", "-B", str(REDACT), "--environment-dir", str(root / "etc"),
                               "--no-journal", *roots], capture_output=True, text=True)

    def host(self, directory: str) -> pathlib.Path:
        root = pathlib.Path(directory)
        (root / "etc/release").mkdir(parents=True)
        (root / "etc/lakehouse.env").write_text(
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=https://catalog.example.invalid/a/long/address\n"
            f"FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN={self.SECRET}\n"
            "SHORT_TOKEN=abc\n", encoding="utf-8")
        (root / "etc/release/reader.env").write_text(f'READER_SECRET_ACCESS_KEY="{self.SECRET}-reader"\n',
                                                     encoding="utf-8")
        (root / "runs/a").mkdir(parents=True)
        return root

    def test_each_occurrence_is_blanked_in_place_and_the_file_closed_to_others(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.host(directory)
            leaked = root / "runs/a/job.py.log"
            before = (f"INFO start\nINFO props {{spark.sql.catalog.r2.token={self.SECRET}, a=b}}\n"
                      f"INFO again token={self.SECRET} key={self.SECRET}-reader\nINFO end abc\n")
            leaked.write_text(before, encoding="utf-8")
            leaked.chmod(0o644)
            clean = root / "runs/a/other.log"
            clean.write_text("INFO https://catalog.example.invalid/a/long/address abc\n", encoding="utf-8")
            clean.chmod(0o644)
            data = root / "runs/a/summary.json"
            data.write_text(self.SECRET, encoding="utf-8")
            inode = leaked.stat().st_ino
            done = self.run_it(root, str(root / "runs"), str(root / "absent"))
            self.assertEqual(done.returncode, 0, done.stderr)
            after = leaked.read_text(encoding="utf-8")
            self.assertNotIn(self.SECRET, after)
            self.assertEqual(len(after), len(before), "the file keeps its length: an appender keeps its place")
            self.assertEqual(leaked.stat().st_ino, inode, "the file is edited through its own inode")
            self.assertIn("INFO start\n", after)
            self.assertIn("INFO end abc\n", after, "a short value named like a credential is not looked for")
            self.assertEqual(stat.S_IMODE(leaked.stat().st_mode), 0o640)
            self.assertEqual(stat.S_IMODE(clean.stat().st_mode), 0o644, "a file that held nothing is not touched")
            self.assertIn("catalog.example.invalid", clean.read_text(encoding="utf-8"), "a setting is not a credential")
            self.assertEqual(data.read_text(encoding="utf-8"), self.SECRET, "only log files are edited")
            self.assertNotIn(self.SECRET, done.stdout + done.stderr)
            self.assertIn("job.py.log: blanked 3", done.stdout)
            self.assertIn("credentials_known=2 files_scanned=2 files_redacted=1 occurrences=3 journal_lines=not-read",
                          done.stdout)
            again = self.run_it(root, str(root / "runs"))
            self.assertIn("files_redacted=0 occurrences=0", again.stdout)

    def test_an_environment_it_cannot_read_is_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.host(directory)
            (root / "etc/lakehouse.env").unlink()
            (root / "etc/lakehouse.env").mkdir()
            done = self.run_it(root, str(root / "runs"))
            self.assertEqual(done.returncode, 1, done.stdout)


if __name__ == "__main__":
    unittest.main()
