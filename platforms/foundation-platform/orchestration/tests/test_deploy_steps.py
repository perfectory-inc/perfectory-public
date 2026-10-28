"""The deploy steps that ran on every deploy say why, or skip while their inputs are unchanged (root ADR-0308).

2026-10-27: the deploy of 77198c1f spent 1 min 51 s bringing 48 lakehouse tables to contracts that
release_changes.py had just reported unchanged; the same bytes had been migrated by the deploy before.
"""

import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config/deploy-steps.contract.json"
STEPS = PLATFORM / "scripts/deploy/deploy_steps.py"
# Where each step is run, and so where its contract entry is read.
CALLERS = {
    "lakehouse-migrate": PLATFORM / "scripts/deploy/foundation-release.sh",
    "postgres-chain": PLATFORM / "scripts/deploy/foundation-release.sh",
    "start-once": PLATFORM / "scripts/deploy/deploy-start-once.sh",
}


def steps(*args):
    return subprocess.run([sys.executable, "-I", "-B", str(STEPS), *map(str, args)],
                          capture_output=True, text=True, check=False, timeout=60)


class TheContract(unittest.TestCase):
    def test_every_step_has_inputs_or_a_reason_and_a_budget(self):
        entries = json.loads(CONTRACT.read_text(encoding="utf-8"))["steps"]
        self.assertEqual(set(entries), set(CALLERS))
        for step, entry in entries.items():
            with self.subTest(step):
                if "inputs" in entry:
                    self.assertNotIn("always", entry)
                    self.assertTrue(entry["inputs"])
                    for item in entry["inputs"]:
                        self.assertTrue((PLATFORM / item).exists(), f"{item} names nothing")
                else:
                    self.assertTrue(entry["always"].strip(), "a step that always runs says why")
                    self.assertGreater(entry["budget_seconds"], 0)

    def test_the_deploy_asks_about_each_step_by_its_name(self):
        for step, caller in CALLERS.items():
            with self.subTest(step):
                self.assertIn(f" {step} ", caller.read_text(encoding="utf-8").replace('"', " "))


class TheGate(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="deploy-steps-")
        self.addCleanup(temp.cleanup)
        self.release = pathlib.Path(temp.name) / "release"
        self.state = pathlib.Path(temp.name) / "state"
        (self.release / "config").mkdir(parents=True)
        (self.release / "config/deploy-steps.contract.json").write_text(json.dumps({"steps": {
            "migrate": {"inputs": ["contracts", "job.py"]},
            "chain": {"always": "its image is the release's own", "budget_seconds": 60},
        }}))
        (self.release / "contracts").mkdir()
        (self.release / "contracts/a.json").write_text("{}")
        (self.release / "job.py").write_text("print()")

    def test_a_step_never_run_here_runs_and_once_it_ran_the_same_bytes_skip_it(self):
        self.assertEqual(steps("should-run", self.release, self.state, "migrate").returncode, 0)
        self.assertEqual(steps("ran", self.release, self.state, "migrate").returncode, 0)
        skipped = steps("should-run", self.release, self.state, "migrate")
        self.assertEqual(skipped.returncode, 3, skipped.stdout + skipped.stderr)
        self.assertIn("skipped", skipped.stdout)

    def test_a_changed_input_runs_it_again(self):
        steps("ran", self.release, self.state, "migrate")
        (self.release / "contracts/a.json").write_text('{"added": 1}')
        self.assertEqual(steps("should-run", self.release, self.state, "migrate").returncode, 0)

    def test_a_readme_is_not_an_input(self):
        steps("ran", self.release, self.state, "migrate")
        (self.release / "contracts/README.md").write_text("prose")
        self.assertEqual(steps("should-run", self.release, self.state, "migrate").returncode, 3)

    def test_a_missing_input_or_contract_never_skips(self):
        steps("ran", self.release, self.state, "migrate")
        (self.release / "job.py").unlink()
        self.assertNotIn(steps("should-run", self.release, self.state, "migrate").returncode, (0, 3))
        shutil.rmtree(self.release / "config")
        self.assertNotIn(steps("should-run", self.release, self.state, "migrate").returncode, (0, 3))

    def test_a_step_that_always_runs_runs_and_says_when_it_is_over_its_budget(self):
        self.assertEqual(steps("should-run", self.release, self.state, "chain").returncode, 0)
        within = steps("budget", self.release, "chain", 60)
        self.assertEqual((within.returncode, within.stderr), (0, ""))
        over = steps("budget", self.release, "chain", 61)
        self.assertEqual(over.returncode, 0)
        self.assertIn("chain took 61 s, over its budget of 60 s", over.stderr)


if __name__ == "__main__":
    unittest.main()
