"""A run that finds its lock held by another run waits for it; it does not fail (scripts/ops/held-lock.sh).

2026-10-19: an operator's attended parcel bake held the by-PNU lane lock; the scheduled bake beside it
exited 75, its unit failed, and a failure notice went out for a run that only had to wait its turn.
"""
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import job_specs  # noqa: E402

HELPER = job_specs.PLATFORM_ROOT / "scripts/ops/held-lock.sh"
ADMISSION = job_specs.PLATFORM_ROOT / "infra/systemd/foundation-release-admission.conf"


def held(env, held_locks=""):
    script = f'source "{HELPER.as_posix()}"; lock_held_by_another_run "the parcel lane"; echo not reached'
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30,
                          env={"PATH": "/usr/bin:/bin", "FOUNDATION_HELD_LOCKS": held_locks, **env})


class HeldLock(unittest.TestCase):
    def test_a_jobs_own_run_waits_and_any_other_run_stops_at_75(self):
        result = held({"FOUNDATION_JOB_SERVICE": "1"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "foundation-job-outcome waiting the parcel lane is held by another run\n")
        self.assertEqual(job_specs.job_outcome(result.stdout), "waiting")
        # A terminal, an operator's transient unit, a cut-over chain: it stops, so nothing after it runs.
        for env in ({}, {"FOUNDATION_JOB_SERVICE": ""}, {"FOUNDATION_JOB_SERVICE": "true"}):
            with self.subTest(env=env):
                result = held(env)
                self.assertEqual((result.returncode, result.stdout), (75, ""))

    def test_a_caller_that_decides_the_outcome_learns_what_was_held(self):
        with tempfile.TemporaryDirectory() as directory:
            record = Path(directory) / "held"
            self.assertEqual(held({}, str(record)).returncode, 75)
            self.assertEqual(record.read_text(encoding="utf-8"), "the parcel lane\n")

    def test_every_registered_jobs_unit_runs_as_the_jobs_own_service(self):
        # foundation-release.sh installs this drop-in on every registered data job's unit (root ADR-0134 §3).
        self.assertRegex(ADMISSION.read_text(encoding="utf-8"), r"(?m)^Environment=FOUNDATION_JOB_SERVICE=1$")

    def test_no_job_ends_a_held_lock_as_a_failure(self):
        # A job's program that finds its lock held ends through the helper, never a bare 75.
        bare = re.compile(r"\b(exit|return)\s+75\b|flock -n.*\b75\b")
        scripts = set()
        for spec in job_specs.load_specs():
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            program = re.search(r"^ExecStart=(\S+)", unit, flags=re.MULTILINE).group(1)
            if program.startswith(job_specs.RELEASE_PREFIX) and program.endswith(".sh"):
                scripts.add(job_specs.PLATFORM_ROOT / program[len(job_specs.RELEASE_PREFIX):])
        self.assertTrue(scripts)
        for script in sorted(scripts):
            lines = [line for line in script.read_text(encoding="utf-8").splitlines()
                     if not line.lstrip().startswith("#")]
            with self.subTest(script.name):
                self.assertEqual([line for line in lines if bare.search(line)], [])


if __name__ == "__main__":
    unittest.main()
