"""The host's one scheduled entry point (scripts/ops/start-scheduled-job.sh) takes turns (ADR-0138).

A `takes_turns` job starts at most once between starts of the jobs in its pool that cannot run
beside it. sudo, systemctl and journalctl are stand-ins that record what the script asked for.
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/start-scheduled-job.sh"

JOBS = {
    "pools": {"spark": {"slots": 3, "description": "fixture"}},
    "jobs": [
        {"id": "floor", "pool": "spark", "pool_slots": 3, "systemd_service": "foundation-floor.service", "enabled": True},
        {"id": "fold", "pool": "spark", "pool_slots": 2, "systemd_service": "foundation-fold.service", "enabled": True},
        {"id": "bake", "pool": "spark", "pool_slots": 1, "takes_turns": True,
         "systemd_service": "foundation-bake.service", "enabled": True},
        {"id": "paused", "pool": "spark", "pool_slots": 3, "systemd_service": "foundation-paused.service", "enabled": False},
    ],
}

# systemctl show answers a fresh invocation once `start` was asked for, and an ended successful run.
SYSTEMCTL = """#!/bin/sh
root="$FIXTURE_ROOT"
case "$*" in
  *InvocationID*) if [ -f "$root/started" ]; then echo new; else echo old; fi ;;
  *ActiveState*) echo inactive ;;
  *Result*) echo success ;;
esac
"""
SUDO = """#!/bin/sh
echo "$*" >> "$FIXTURE_ROOT/sudo.log"
touch "$FIXTURE_ROOT/started"
"""
JOURNALCTL = "#!/bin/sh\nexit 0\n"


class TakingTurns(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="start-scheduled-job-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.starts = self.root / "starts"
        (self.root / "jobs.json").write_text(json.dumps(JOBS))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        for name, body in (("systemctl", SYSTEMCTL), ("sudo", SUDO), ("journalctl", JOURNALCTL)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FIXTURE_ROOT": str(self.root), "FOUNDATION_SCHEDULED_JOBS_FILE": str(self.root / "jobs.json"),
                    "FOUNDATION_SCHEDULED_STARTS_DIR": str(self.starts)}

    def start(self, job):
        for path in (self.root / "started", self.root / "sudo.log"):
            path.unlink(missing_ok=True)
        result = subprocess.run(["bash", str(SCRIPT)], env={**self.env, "SSH_ORIGINAL_COMMAND": job},
                                capture_output=True, text=True, timeout=60)
        log = self.root / "sudo.log"
        return result, log.read_text() if log.exists() else ""

    def record(self, job, seconds):
        self.starts.mkdir(exist_ok=True)
        (self.starts / job).write_text(f"{seconds}\n")

    def test_the_first_start_of_a_turn_taking_job_runs_and_is_recorded(self):
        result, asked = self.start("bake")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-bake.service", asked)
        self.assertTrue((self.starts / "bake").is_file())
        # A run that starts says nothing of its own outcome: the unit's journal does.
        self.assertNotIn("foundation-job-outcome", result.stdout)

    def test_a_second_bake_waits_until_the_jobs_it_blocks_have_started(self):
        self.record("bake", 2000)
        self.record("floor", 1000)  # FLOOR last started before the bake did: it is still waiting
        result, asked = self.start("bake")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("bake deferred: floor cannot run beside bake", result.stdout)
        self.assertEqual(asked, "", "a deferred job must not be started")
        # Nothing ran, so the DAG records no output events and starts nothing downstream (ADR-0171).
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome unchanged")
        sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
        import job_specs  # noqa: E402
        self.assertEqual(job_specs.job_outcome(result.stdout), "unchanged")
        self.assertEqual((self.starts / "bake").read_text(), "2000\n")
        # A job that never started here counts as waiting too.
        (self.starts / "floor").unlink()
        result, asked = self.start("bake")
        self.assertIn("deferred", result.stdout)
        self.assertEqual(asked, "")

    def test_a_bake_its_inputs_or_an_operator_started_is_never_deferred(self):
        # Root ADR-0179: on 2026-10-10 a Gold rebuild's new snapshot started the bake and takes_turns
        # deferred it until the next morning. Only a clock start takes turns.
        self.record("bake", 2000)
        self.record("floor", 1000)
        result, asked = self.start("bake triggered")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("deferred", result.stdout)
        self.assertIn("systemctl start --no-block foundation-bake.service", asked)
        # Anything else after the id is still refused.
        result, asked = self.start("bake now")
        self.assertEqual(result.returncode, 64)
        self.assertEqual(asked, "")

    def test_the_bake_runs_again_once_the_jobs_it_blocks_have_had_their_turn(self):
        self.record("bake", 2000)
        self.record("floor", 3000)
        result, asked = self.start("bake")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("foundation-bake.service", asked)

    def test_jobs_that_can_run_beside_it_or_are_switched_off_do_not_hold_it(self):
        # The fold (2 slots) fits beside the bake (1 of 3); the paused job is not enabled.
        self.record("bake", 2000)
        self.record("floor", 3000)
        result, asked = self.start("bake")
        self.assertIn("foundation-bake.service", asked)

    def test_a_job_that_does_not_take_turns_is_never_deferred(self):
        self.record("fold", 2000)
        self.record("floor", 1000)
        result, asked = self.start("fold")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("foundation-fold.service", asked)


if __name__ == "__main__":
    unittest.main()
