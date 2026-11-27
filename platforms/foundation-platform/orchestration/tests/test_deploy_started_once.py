"""The jobs a deploy starts once under the new release, and the unit limit that holds the whole deploy
(scripts/deploy/foundation-deploy.sh steps 6 and 7, root ADR-0173; ADR-0270 §1). Split from
test_foundation_autodeploy.py, which tests the deploy's steps end to end.
"""

import json
import pathlib
import subprocess
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
JOBS = PLATFORM / "orchestration/jobs.v1.json"
sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
import job_specs  # noqa: E402


class TheJobsADeployStartsOnce(unittest.TestCase):
    def test_they_are_enabled_and_carry_the_limit_the_deploy_waits_for(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = [job for job in jobs if job.get("started_once_after_deploy")]
        self.assertTrue(once, "a deploy that starts nothing proves nothing about the new release")
        for job in once:
            with self.subTest(job["id"]):
                self.assertTrue(job["enabled"], "a disabled job is never started by the deploy")
                # foundation-deploy.sh waits each one's own limit for one start.
                self.assertGreater(job_specs.run_limit_minutes(job), job["time_limit_minutes"])

    def test_the_deploy_waits_the_limit_job_specs_gives_each_start(self):
        # foundation-deploy.sh adds time_limit_minutes and stop_grace_minutes itself (the release it
        # reads from may not carry job_specs); it must add them as job_specs does (root ADR-0270 §1).
        # The deploy starts the unit itself, so the start never waits: a run's limit, not the scheduler's.
        script = (PLATFORM / "scripts/deploy/foundation-deploy.sh").read_text(encoding="utf-8")
        program = script.split("jobs_of() {\n", 1)[1].split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        waited = subprocess.run([sys.executable, "-c", program, "started-once", str(JOBS)],
                                capture_output=True, text=True, check=True).stdout.split()
        listing = json.loads(JOBS.read_text(encoding="utf-8"))
        # Only a job this host runs (job_specs.runs_on_host, root ADR-0625): the deploy starts no other.
        here = job_specs.runs_on_host(listing)
        expected = []
        for job in listing["jobs"]:
            if job["id"] in here and job.get("started_once_after_deploy"):
                expected += [job["systemd_service"], str(job_specs.run_limit_minutes(job)), job["id"]]
        self.assertTrue(expected)
        self.assertEqual(waited, expected)

    def test_systemd_never_stops_a_deploy_halfway(self):
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        once = sum(job_specs.run_limit_minutes(job) + 1 for job in jobs if job.get("started_once_after_deploy"))
        unit = (PLATFORM / "infra/systemd/foundation-autodeploy.service").read_text()
        limit = next(line for line in unit.splitlines() if line.startswith("TimeoutStartSec="))
        hours = int(limit.removeprefix("TimeoutStartSec=").removesuffix("h"))
        # The staging smoke's own bound (root ADR-0177), read where foundation-release.sh reads it.
        smoke = json.loads((PLATFORM / "config/staging-gate.contract.json").read_text(encoding="utf-8"))
        smoke_minutes = -(-smoke["unit_limit_seconds"] // 60)
        # 5h waiting for running jobs, the staging smoke, an hour to build and migrate, then the
        # started jobs.
        self.assertGreaterEqual(hours * 60, 5 * 60 + smoke_minutes + 60 + once,
                                "a deploy can outlive its unit's limit")


if __name__ == "__main__":
    unittest.main()
