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
            {"id": "shape_ledger_check", "enabled": True, "systemd_service": "foundation-shape-ledger-check.service"},
            {"id": "parcel_registry", "enabled": False, "systemd_service": "foundation-parcel-registry.service"}]}))
        deploy = root / "opt/current/scripts/deploy"
        deploy.mkdir(parents=True)
        (deploy / "job_units.py").write_bytes((PLATFORM / "scripts/deploy/job_units.py").read_bytes())
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
        # systemctl: FIXTURE_RUNNING names the running unit (list-units), its invocation is `inv-1`,
        # and a stop is recorded.
        (bin_dir / "systemctl").write_text(
            '#!/usr/bin/env bash\n'
            'case "$1" in\n'
            '  list-units) for last; do :; done\n'
            '    [[ "${last}" != "${FIXTURE_RUNNING:-}" ]] || echo "${last} loaded activating x fixture" ;;\n'
            '  show) echo inv-1 ;;\n'
            '  stop) printf "%s\\n" "$*" >> "$FIXTURE_ROOT/stopped.log" ;;\n'
            'esac\n')
        (bin_dir / "systemctl").chmod(0o755)
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


class AnOperatorsStop(RunNowDispatch):
    """Root ADR-0282 §5: <job>.stop ends the job's run in progress, once, within ten minutes."""

    STOP_ID = "77777777-7777-4777-8777-777777777777"

    def stop(self, asked_minutes_ago=1):
        (self.root / "switches/shape_ledger_check.stop").write_text(
            f"request={self.STOP_ID}\nactor=staff-a\nrequested_at={at(-asked_minutes_ago)}\nreason=wrong round\n")

    def stopped(self):
        log = self.root / "stopped.log"
        return log.read_text().splitlines() if log.exists() else []

    def test_a_stop_ends_the_run_in_progress_once_and_names_its_invocation(self):
        self.stop()
        out, _ = self.dispatch(FIXTURE_RUNNING="foundation-shape-ledger-check.service")
        self.assertEqual(self.stopped(), ["stop --no-block foundation-shape-ledger-check.service"], out)
        mark = (self.marks / "stopped" / self.STOP_ID).read_text().splitlines()
        self.assertEqual(mark, ["invocation=inv-1", "actor=staff-a"])
        self.dispatch(FIXTURE_RUNNING="foundation-shape-ledger-check.service")
        self.assertEqual(len(self.stopped()), 1, "a stop was acted on twice")

    def test_a_late_stop_or_one_with_nothing_running_stops_nothing(self):
        self.stop(asked_minutes_ago=11)
        out, _ = self.dispatch(FIXTURE_RUNNING="foundation-shape-ledger-check.service")
        self.assertEqual(self.stopped(), [])
        self.assertIn("more than 10 minutes ago", out)
        (self.marks / "stopped" / self.STOP_ID).unlink()
        self.stop()
        out, _ = self.dispatch()
        self.assertEqual(self.stopped(), [])
        self.assertIn("is not running", out)


class AnApprovedCutOver(RunNowDispatch):
    """Root ADR-0433 §3: an approved by-PNU cut-over starts once, now when the operator window has room
    for its first lane's steps, else when the window next opens; it is never refused for the hour.

    The clock is FIXTURE_NOW (a fake `date +%s`). The jobs place the scheduled window at 04:30-21:50 UTC
    as on 2026-11-06, and the runner's --start-budget answers FIXTURE_BUDGET minutes.
    """

    APPROVAL = "44444444-4444-4444-8444-444444444444"
    STOP_ID = "55555555-5555-4555-8555-555555555555"

    def setUp(self):
        super().setUp()
        root = self.root
        (root / "opt/current/orchestration/jobs.v1.json").write_text(json.dumps({"jobs": [
            {"id": "shape_ledger_check", "enabled": True, "systemd_service": "foundation-shape-ledger-check.service"},
            {"id": "parcel_registry", "enabled": False, "systemd_service": "foundation-parcel-registry.service"},
            {"id": "gold_panel_rebuild", "enabled": True, "pool": "spark", "schedule": "30 4 * * *",
             "systemd_service": "foundation-gold-panel-rebuild.service"},
            {"id": "public_auction_gold", "enabled": True, "pool": "spark", "schedule": "35 21 * * *",
             "systemd_service": "foundation-public-auction-gold.service"}]}))
        ops = root / "opt/current/scripts/ops"
        ops.mkdir(parents=True)
        (ops / "release-progress.py").write_bytes((PLATFORM / "scripts/ops/release-progress.py").read_bytes())
        (ops / "by-pnu-lane-cutover.sh").write_text(
            '#!/usr/bin/env bash\n[[ "$3" == --start-budget ]] || exit 64\n'
            '[[ -n "${FIXTURE_BUDGET:-}" ]] || { echo "refused: nope is not a by_pnu_lanes lane" >&2; exit 65; }\n'
            'echo "${FIXTURE_BUDGET}"\n')
        (ops / "by-pnu-lane-cutover.sh").chmod(0o755)
        bin_dir = root / "bin"
        real_date = subprocess.run(["bash", "-c", "command -v date"], capture_output=True, text=True,
                                   check=True).stdout.strip()
        (bin_dir / "date").write_text(
            '#!/usr/bin/env bash\n'
            f'if [[ "$*" == "+%s" && -n "${{FIXTURE_NOW:-}}" ]]; then echo "$FIXTURE_NOW"; else exec {real_date} "$@"; fi\n')
        (bin_dir / "date").chmod(0o755)
        # systemctl: list-timers lists FIXTURE_TIMER; list-units shows FIXTURE_RUNNING; stops are recorded.
        (bin_dir / "systemctl").write_text(
            '#!/usr/bin/env bash\n'
            'case "$1" in\n'
            '  list-timers) for last; do :; done\n'
            '    [[ "${last}" != "${FIXTURE_TIMER:-}" ]] || echo "Fri 2026-11-06 21:50:00 UTC 18h left - - ${last} x" ;;\n'
            '  list-units) for last; do :; done\n'
            '    [[ "${last}" != "${FIXTURE_RUNNING:-}" ]] || echo "foundation-by-pnu-lane-cutover-g1.service loaded activating x fixture" ;;\n'
            '  show) echo inv-1 ;;\n'
            '  stop) printf "%s\\n" "$*" >> "$FIXTURE_ROOT/stopped.log" ;;\n'
            'esac\n')
        self.progress_dir = root / "releases"
        self.env["FOUNDATION_RELEASE_PROGRESS_DIR"] = str(self.progress_dir)

    @staticmethod
    def epoch(hour, minute, second=0, day=6):
        return int(datetime.datetime(2026, 11, day, hour, minute, second, tzinfo=datetime.timezone.utc).timestamp())

    def approve(self, asked, expires_hours=48, lanes="parcel-ownership", generation="1"):
        requests = self.root / "switches/cutover.d"
        requests.mkdir(parents=True, exist_ok=True)
        iso = lambda epoch: datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat()  # noqa: E731
        (requests / self.APPROVAL).write_text(
            f"request={self.APPROVAL}\nactor=staff-owner\nrequested_at={iso(asked)}\nreason=g1 first publish\n"
            f"job=by_pnu_lane_cutover\nexpires_at={iso(asked + expires_hours * 3600)}\nlanes={lanes}\n"
            f"generation={generation}\n")

    def started(self):
        log = self.root / "systemd-run.log"
        return log.read_text().splitlines() if log.exists() else []

    def progress(self):
        path = self.progress_dir / f"{self.APPROVAL}.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def at(self, now, budget="202", **env):
        out, _ = self.dispatch(FIXTURE_NOW=str(now), FIXTURE_BUDGET=budget, **env)
        return out

    def forget(self):
        for path in (self.marks / "cutover").glob("*") if (self.marks / "cutover").exists() else ():
            path.unlink()
        (self.progress_dir / f"{self.APPROVAL}.jsonl").unlink(missing_ok=True)

    def test_the_eleven_o_six_first_start_runs_at_once_inside_the_window(self):
        # 00:37:35Z: 233 minutes of the window are left and the first lane's steps take 202.
        now = self.epoch(0, 37, 35)
        self.approve(now)
        out = self.at(now)
        (start,) = self.started()
        self.assertNotIn("--on-calendar", start, out)
        self.assertTrue(start.startswith(f"--quiet --collect --unit operator-cutover-{self.APPROVAL} "), start)
        self.assertTrue(start.endswith(
            f"/opt/current/scripts/ops/by-pnu-pack-operator.sh parcel-ownership cutover 1 --approved {self.APPROVAL}"),
            start)
        self.at(now + 60)
        self.assertEqual(len(self.started()), 1, "an approval was started twice")

    def test_the_eleven_o_six_late_start_waits_for_the_window_instead_of_refusing(self):
        # 03:46:38Z: 44 minutes left, the gold step alone needs 60. The approval waits for 21:50.
        now = self.epoch(3, 46, 38)
        self.approve(now)
        out = self.at(now)
        (start,) = self.started()
        self.assertIn(f"--unit operator-cutover-{self.APPROVAL} --on-calendar 2026-11-06 21:50:00 UTC", start, out)
        self.assertIn("--timer-property=AccuracySec=1s", start)
        self.assertTrue(start.endswith(f"cutover 1 --approved {self.APPROVAL}"), start)
        (scheduled,) = self.progress()
        self.assertEqual((scheduled["state"], scheduled["start_at"], scheduled["budget_minutes"]),
                         ("scheduled", "2026-11-06T21:50:00Z", 202))
        # Its timer waits: nothing more. A reboot took the timer and it was not used: set again.
        self.at(now + 60, FIXTURE_TIMER=f"operator-cutover-{self.APPROVAL}.timer")
        self.assertEqual(len(self.started()), 1)
        out = self.at(now + 120)
        self.assertEqual(len(self.started()), 2, out)
        self.assertIn("scheduling it again", out)
        # Once the operator script used it, it is done.
        (self.marks / "cutover-used" / self.APPROVAL).touch()
        self.at(now + 180)
        self.at(now + 240)
        self.assertEqual(len(self.started()), 2)

    def test_late_at_night_the_window_runs_past_midnight(self):
        # 23:00Z: the window runs to 04:30, 330 minutes; 202 fit, so it starts now.
        now = self.epoch(23, 0)
        self.approve(now)
        self.at(now)
        (start,) = self.started()
        self.assertNotIn("--on-calendar", start)

    def test_an_approval_that_cannot_fit_or_outlives_its_expiry_is_refused_with_why(self):
        noon = self.epoch(12, 0)
        for case, (asked, budget, expires_hours) in {
            "steps longer than the window": (noon, "500", 48),
            "expires before the window opens": (noon, "202", 1),
            "expired": (noon - 7200, "202", 1),
            "lanes the runner cannot plan": (noon, "", 48),
        }.items():
            with self.subTest(case=case):
                self.forget()
                self.approve(asked, expires_hours=expires_hours)
                self.at(noon, budget=budget)
                self.assertEqual(self.started(), [])
                (refused,) = self.progress()
                self.assertEqual(refused["state"], "refused", refused)
        for lanes, generation in (("parcel ownership", "1"), ("parcel-ownership", "0"), ("parcel-ownership", "1;id")):
            with self.subTest(lanes=lanes, generation=generation):
                self.forget()
                self.approve(self.epoch(0, 37), lanes=lanes, generation=generation)
                self.at(self.epoch(0, 37))
                self.assertEqual(self.started(), [])

    def test_a_stop_of_the_runner_cancels_waiting_approvals_and_ends_a_run(self):
        now = self.epoch(3, 46, 38)
        self.approve(now)
        self.at(now)
        asked = datetime.datetime.fromtimestamp(now + 60, datetime.timezone.utc).isoformat()
        (self.root / "switches/by_pnu_lane_cutover.stop").write_text(
            f"request={self.STOP_ID}\nactor=staff-owner\nrequested_at={asked}\nreason=baked from the wrong Gold\n")
        running = "foundation-by-pnu-lane-cutover-g*.service"
        out = self.at(now + 120, FIXTURE_RUNNING=running)
        stopped = (self.root / "stopped.log").read_text().splitlines()
        self.assertEqual(stopped, [f"stop operator-cutover-{self.APPROVAL}.timer",
                                   "stop --no-block foundation-by-pnu-lane-cutover-g1.service"], out)
        cancelled = self.progress()[-1]
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertIn("baked from the wrong Gold", cancelled["reason"])
        self.at(now + 180, FIXTURE_RUNNING=running)
        self.assertEqual(len((self.root / "stopped.log").read_text().splitlines()), 2, "a stop was acted on twice")
        self.assertEqual(len(self.started()), 1, "a cancelled approval was scheduled again")


class TheReleaseWatchesTheRequests(unittest.TestCase):
    def test_the_path_unit_is_installed_and_enabled_and_watches_a_directory_the_host_makes(self):
        release = (PLATFORM / "scripts/deploy/foundation-release.sh").read_text(encoding="utf-8")
        self.assertIn('"${release_root}"/current/infra/systemd/*.path; do', release)
        self.assertIn("systemctl enable --now foundation-run-now.path", release)
        unit = (PLATFORM / "infra/systemd/foundation-run-now.path").read_text(encoding="utf-8")
        watched = [line.split("=", 1)[1] for line in unit.splitlines() if line.startswith("PathChanged=")]
        made = {entry["path"] for entry in json.loads(
            (PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["state_directories"]}
        self.assertLessEqual(set(watched), made)
        # An approved cut-over is a request too (root ADR-0433 §3).
        self.assertIn("/var/lib/foundation-jobs/switches/cutover.d", watched)


if __name__ == "__main__":
    unittest.main()
