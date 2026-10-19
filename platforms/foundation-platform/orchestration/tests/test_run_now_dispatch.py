"""Each operator run-now request starts once, before it expires (scripts/ops/run-now-dispatch.sh, root ADR-0282 §4).

The script's fixed paths are pointed at a fixture host; `sudo` records what it would run as the deploy
account and runs nothing.
"""

import datetime
import json
import os
import pathlib
import shlex
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/run-now-dispatch.sh"
ENABLED_ID = "11111111-1111-4111-8111-111111111111"
OFF_ID = "22222222-2222-4222-8222-222222222222"
EXPIRED_ID = "33333333-3333-4333-8333-333333333333"


def at(minutes):
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)).isoformat()


class RunNowDispatch(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="run-now-")
        self.addCleanup(temp.cleanup)
        root = self.root = pathlib.Path(temp.name)
        self.requests = root / "switches/run-now.d"
        self.requests.mkdir(parents=True)
        self.marks = root / "marks"
        jobs = root / "opt/current/orchestration"
        jobs.mkdir(parents=True)
        (jobs / "jobs.v1.json").write_text(json.dumps({"jobs": [
            {"id": "shape_ledger_check", "enabled": True}, {"id": "parcel_registry", "enabled": False}]}))
        (root / "release-deploy.conf").write_text("FOUNDATION_DEPLOYER=deployer\n")
        text = SCRIPT.read_text(encoding="utf-8")
        for name, value in (("switches", root / "switches"), ("marks", self.marks),
                            ("release_root", root / "opt"), ("host_conf", root / "release-deploy.conf")):
            line = next(line for line in text.splitlines() if line.startswith(f"{name}="))
            text = text.replace(line, f"{name}={shlex.quote(value.as_posix())}", 1)
        self.script = root / "run-now-dispatch.sh"
        self.script.write_text(text)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "sudo").write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$FIXTURE_ROOT/sudo.log"\n'
                                      '[[ -z "${FIXTURE_AIRFLOW_FAILS:-}" ]]\n')
        (bin_dir / "sudo").chmod(0o755)
        (bin_dir / "systemd-run").write_text(
            '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$FIXTURE_ROOT/systemd-run.log"\n')
        (bin_dir / "systemd-run").chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FIXTURE_ROOT": str(root)}

    def request(self, request_id, job, expires_at):
        (self.requests / request_id).write_text(
            f"request={request_id}\nactor=staff\nrequested_at={at(-1)}\nreason=a run\njob={job}\nexpires_at={expires_at}\n")

    def dispatch(self, **env):
        result = subprocess.run(["bash", str(self.script)], env={**self.env, **env},
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        log = self.root / "sudo.log"
        return result.stdout, log.read_text().splitlines() if log.exists() else []

    def test_an_enabled_jobs_request_triggers_its_dag_once_with_the_request_id(self):
        self.request(ENABLED_ID, "shape_ledger_check", at(9))
        out, asked = self.dispatch()
        self.assertEqual(len(asked), 1, out)
        self.assertIn("-u deployer -H bash", asked[0])
        self.assertIn("airflow dags trigger foundation_shape_ledger_check --conf", asked[0])
        self.assertIn(f'"operator_request": "{ENABLED_ID}"', asked[0])
        (self.root / "sudo.log").unlink()
        out, asked = self.dispatch()
        self.assertEqual(asked, [], "a request was triggered twice")

    def test_an_expired_request_is_never_started(self):
        self.request(EXPIRED_ID, "shape_ledger_check", at(-1))
        out, asked = self.dispatch()
        self.assertEqual(asked, [])
        self.assertIn("expired", out)
        self.assertTrue((self.marks / "expired" / EXPIRED_ID).exists())

    def test_a_trigger_that_fails_is_tried_again_and_a_disabled_job_starts_through_the_gate(self):
        self.request(ENABLED_ID, "shape_ledger_check", at(9))
        self.request(OFF_ID, "parcel_registry", at(9))
        out, asked = self.dispatch(FIXTURE_AIRFLOW_FAILS="1")
        self.assertEqual(len(asked), 1)
        self.assertFalse((self.marks / "triggered" / ENABLED_ID).exists())
        self.assertIn("parcel_registry is off; started through the gate", out)
        started = (self.root / "systemd-run.log").read_text().splitlines()
        self.assertEqual(len(started), 1)
        self.assertIn(f"--unit operator-run-now-{OFF_ID} -p User=foundation-scheduler", started[0])
        self.assertIn(f"-E SSH_ORIGINAL_COMMAND=parcel_registry requested {OFF_ID}", started[0])
        self.assertTrue(started[0].endswith("/opt/current/scripts/ops/start-scheduled-job.sh"))
        out, asked = self.dispatch()
        self.assertEqual(len(asked), 2, "the failed trigger was not tried again")
        self.assertFalse(any("parcel_registry" in line for line in asked), "a job that is off went to Airflow")
        self.assertEqual(len((self.root / "systemd-run.log").read_text().splitlines()), 1, "started twice")

    def test_a_partial_write_or_a_request_without_a_job_starts_nothing(self):
        (self.requests / f"{ENABLED_ID}.partial").write_text("job=shape_ledger_check\n")
        (self.requests / OFF_ID).write_text(f"expires_at={at(9)}\n")
        self.request(EXPIRED_ID, "../../etc", at(9))
        out, asked = self.dispatch()
        self.assertEqual(asked, [], out)


class TheReleaseWatchesTheRequests(unittest.TestCase):
    def test_the_path_unit_is_installed_and_enabled_and_watches_a_directory_the_host_makes(self):
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertIn('"${release_root}"/current/infra/systemd/*.path; do', release)
        self.assertIn("systemctl enable --now foundation-run-now.path", release)
        unit = (PLATFORM / "infra/systemd/foundation-run-now.path").read_text(encoding="utf-8")
        watched = next(line.split("=", 1)[1] for line in unit.splitlines() if line.startswith("PathChanged="))
        made = {entry["path"] for entry in json.loads(
            (PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["state_directories"]}
        self.assertIn(watched, made)


if __name__ == "__main__":
    unittest.main()
