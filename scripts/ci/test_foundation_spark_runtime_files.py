"""The Spark runtime lane's file runner: every gated file runs, side by side, and any failure fails it.

scripts/verify/foundation-spark-runtime-files.sh runs inside the pinned Spark container, where a
run costs minutes. What it decides needs no Spark: which files run, how many at once, and whether
the run failed. These tests give it stand-in test files and a real interpreter.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "verify" / "foundation-spark-runtime-files.sh"
BASH = shutil.which("bash")

GATED = '''import time, unittest

@unittest.skipUnless(True, "requires pyspark")
class T(unittest.TestCase):
    def test_it(self):
        time.sleep({sleep})
        self.assertTrue({passes})
'''
GATED_SCRATCH = '''import os, shutil, tempfile, time, unittest
from pathlib import Path

@unittest.skipUnless(True, "requires pyspark")
class T(unittest.TestCase):
    def test_it(self):{body}
'''
UNGATED = '''import unittest

class T(unittest.TestCase):
    def test_it(self):
        raise AssertionError("a file without the runtime gate ran")
'''


@unittest.skipUnless(BASH, "requires bash")
class SparkRuntimeFilesTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.directory, ignore_errors=True)

    def write(self, name: str, body: str) -> None:
        (self.directory / name).write_text(body, encoding="utf-8")

    def gated(self, name: str, *, sleep: float = 0, passes: bool = True) -> None:
        self.write(name, GATED.format(sleep=sleep, passes=passes))

    def run_files(self, jobs: int, extra: dict[str, str] | None = None) -> tuple[subprocess.CompletedProcess[str], float]:
        environment = {**os.environ, "SPARK_RUNTIME_JOBS": str(jobs), "PYTHONDONTWRITEBYTECODE": "1", **(extra or {})}
        started = time.monotonic()
        result = subprocess.run(
            [BASH, str(RUNNER), str(self.directory)],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        return result, time.monotonic() - started

    def test_every_gated_file_runs_and_only_those(self):
        self.gated("test_a_spark.py")
        self.gated("test_b_iceberg.py")
        self.write("test_pure_logic.py", UNGATED)

        result, _ = self.run_files(jobs=2)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("== test_a_spark.py (", result.stdout)
        self.assertIn("== test_b_iceberg.py (", result.stdout)
        self.assertNotIn("test_pure_logic", result.stdout)
        self.assertIn("2 files, 2 at a time", result.stdout)

    def test_one_failing_file_fails_the_run_and_the_others_still_report(self):
        self.gated("test_a_fails.py", passes=False)
        self.gated("test_b_passes.py", sleep=0.5)

        result, _ = self.run_files(jobs=2)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("== test_a_fails.py (", result.stdout)
        self.assertIn("exit 1)", result.stdout)
        self.assertIn("FAILED (exit 1)", result.stdout)
        self.assertIn("== test_b_passes.py (", result.stdout)
        self.assertIn("test_it (test_b_passes.T", result.stdout)

    def test_a_failure_in_the_last_file_still_fails_the_run(self):
        self.gated("test_a_passes.py")
        self.gated("test_z_fails.py", passes=False)

        result, _ = self.run_files(jobs=1)

        self.assertNotEqual(result.returncode, 0)

    def test_files_run_side_by_side(self):
        for name in ("test_a.py", "test_b.py", "test_c.py"):
            self.gated(name, sleep=2)

        _, serial = self.run_files(jobs=1)
        result, parallel = self.run_files(jobs=3)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertGreaterEqual(serial, 6)
        self.assertLess(parallel, serial - 3, f"three 2s files at once took {parallel:.1f}s")

    def test_durations_are_listed_slowest_first(self):
        self.gated("test_a_quick.py")
        self.gated("test_b_slow.py", sleep=4)

        result, _ = self.run_files(jobs=2)

        durations = result.stdout.split("== durations, slowest first", 1)[1]
        self.assertLess(durations.index("test_b_slow.py"), durations.index("test_a_quick.py"))

    def test_a_file_that_clears_its_scratch_siblings_cannot_reach_another_files(self):
        # gold_incremental deletes every sibling stage directory under spark.local.dir, as production
        # serializes its runs; two files sharing /tmp lost each other's stages mid-run.
        self.write("test_a_keeps_a_stage.py", GATED_SCRATCH.format(body='''
        stage = Path(tempfile.gettempdir()) / "stage-a"
        stage.mkdir()
        time.sleep(3)
        self.assertTrue(stage.exists(), "another file deleted this file's stage")'''))
        self.write("test_b_clears_stages.py", GATED_SCRATCH.format(body='''
        time.sleep(1)
        for dead in Path(tempfile.gettempdir()).glob("stage-*"):
            shutil.rmtree(dead, ignore_errors=True)'''))

        result, _ = self.run_files(jobs=2)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_spark_scratch_is_the_files_own_temporary_directory(self):
        self.write("test_a_scratch.py", GATED_SCRATCH.format(body='''
        arguments = os.environ["PYSPARK_SUBMIT_ARGS"]
        self.assertIn("--conf spark.local.dir=" + tempfile.gettempdir() + " ", arguments)
        self.assertTrue(arguments.endswith("--jars x.jar pyspark-shell"), arguments)'''))

        result, _ = self.run_files(jobs=1, extra={"PYSPARK_SUBMIT_ARGS": "--jars x.jar pyspark-shell"})

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_list_prints_the_gated_files_only(self):
        self.gated("test_b_iceberg.py")
        self.gated("test_a_spark.py")
        self.write("test_pure_logic.py", UNGATED)

        result = subprocess.run([BASH, str(RUNNER), "--list", str(self.directory)], capture_output=True, text=True, check=False)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), ["test_a_spark.py", "test_b_iceberg.py"])

    def test_given_targets_run_and_nothing_else(self):
        # A shard (root ADR-0365) passes its part: whole files and unittest names inside one.
        self.gated("test_a_spark.py")
        self.write("test_b_two.py", GATED.format(sleep=0, passes=True) + '''

class U(unittest.TestCase):
    def test_other(self):
        raise AssertionError("a class outside the target ran")
''')
        self.gated("test_c_not_mine.py", passes=False)
        environment = {**os.environ, "SPARK_RUNTIME_JOBS": "2", "PYTHONDONTWRITEBYTECODE": "1"}

        result = subprocess.run(
            [BASH, str(RUNNER), str(self.directory), "test_a_spark.py", "test_b_two.T.test_it"],
            capture_output=True, text=True, env=environment, check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("2 files, 2 at a time", result.stdout)
        self.assertIn("== test_b_two.T.test_it (", result.stdout)
        self.assertNotIn("test_c_not_mine", result.stdout)
        self.assertNotIn("test_other", result.stdout)

    def test_no_gated_file_is_a_failure(self):
        self.write("test_pure_logic.py", UNGATED)

        result, _ = self.run_files(jobs=2)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no runtime-gated test file found", result.stderr)


if __name__ == "__main__":
    unittest.main()
