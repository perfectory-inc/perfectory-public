"""The host's one scheduled entry point (scripts/ops/start-scheduled-job.sh) takes turns (ADR-0138).

A `takes_turns` job starts at most once between starts of the jobs in its pool that cannot run
beside it. sudo, systemctl and journalctl are stand-ins that record what the script asked for.
"""

import datetime
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/start-scheduled-job.sh"

JOBS = {
    "pools": {"spark": {"slots": 3, "description": "fixture"}},
    "jobs": [
        {"id": "floor", "pool": "spark", "pool_slots": 3, "systemd_service": "foundation-floor.service", "enabled": True,
         "wait_limit_minutes": 2},
        {"id": "fold", "pool": "spark", "pool_slots": 2, "systemd_service": "foundation-fold.service", "enabled": True,
         "wait_limit_minutes": 0},
        {"id": "bake", "pool": "spark", "pool_slots": 1, "takes_turns": True,
         "systemd_service": "foundation-bake.service", "enabled": True, "wait_limit_minutes": 0},
        {"id": "paused", "pool": "spark", "pool_slots": 3, "systemd_service": "foundation-paused.service", "enabled": False,
         "wait_limit_minutes": 0},
    ],
}

# systemctl show answers a fresh invocation once `start` was asked for, and an ended successful run.
# list-units (job_units.py's question, root ADR-0184) answers the next state of FIXTURE_RUN_STATES,
# one per question, and then nothing: the run has ended. A question about several units is the
# pool's (job_units.py pool-full): it answers the units FIXTURE_HELD names as running, and only them.
SYSTEMCTL = """#!/bin/sh
root="$FIXTURE_ROOT"
case "$*" in
  list-units*)
    if [ "$#" -gt 7 ]; then
      for unit; do case ",${FIXTURE_HELD:-}," in *",$unit,"*) echo "$unit loaded activating x fixture" ;; esac; done
      exit 0
    fi
    asked=$(cat "$root/listed" 2>/dev/null || echo 0); asked=$((asked + 1)); echo "$asked" > "$root/listed"
    state=$(echo "${FIXTURE_RUN_STATES:-}" | cut -d, -f"$asked")
    for last; do :; done
    [ -z "$state" ] || echo "$last loaded $state x fixture" ;;
  *InvocationID*) if [ -f "$root/started" ]; then echo new; else echo old; fi ;;
  *Result*) echo "${FIXTURE_RESULT:-success}" ;;
esac
"""
SLEEP = """#!/bin/sh
echo "$*" >> "$FIXTURE_ROOT/sleep.log"
[ -z "${FIXTURE_REAL_SLEEP:-}" ] || exec /bin/sleep 0.2
"""
SUDO = """#!/bin/sh
echo "$*" >> "$FIXTURE_ROOT/sudo.log"
touch "$FIXTURE_ROOT/started"
"""
# Release admission's `release-build-running`: 75 while FIXTURE_BUILD says a build holds the lock.
ADMISSION = """#!/bin/sh
[ "$1" = release-build-running ] || exit 64
[ -z "${FIXTURE_BUILD:-}" ] || exit 75
# FIXTURE_BUILD_ANSWERS: a build holds the lock for that many questions, then has finished.
if [ -n "${FIXTURE_BUILD_ANSWERS:-}" ]; then
  asked=$(cat "$FIXTURE_ROOT/built" 2>/dev/null || echo 0); asked=$((asked + 1)); echo "$asked" > "$FIXTURE_ROOT/built"
  [ "$asked" -gt "$FIXTURE_BUILD_ANSWERS" ] || exit 75
fi
"""
# The run's journal: FIXTURE_JOURNAL, or a run that ended by saying it changed its outputs.
JOURNALCTL = """#!/bin/sh
printf '%b' "${FIXTURE_JOURNAL-loaded\\nfoundation-job-outcome changed\\n}"
"""


class HostFixture(unittest.TestCase):
    """The script under stand-ins for sudo, systemctl, journalctl and sleep."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="start-scheduled-job-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.starts = self.root / "starts"
        (self.root / "jobs.json").write_text(json.dumps(JOBS))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        for name, body in (("systemctl", SYSTEMCTL), ("sudo", SUDO), ("journalctl", JOURNALCTL), ("sleep", SLEEP),
                           ("admission", ADMISSION)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        self.deferred = self.root / "deferred"
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                    "FIXTURE_ROOT": str(self.root), "FOUNDATION_SCHEDULED_JOBS_FILE": str(self.root / "jobs.json"),
                    "FOUNDATION_SCHEDULED_STARTS_DIR": str(self.starts),
                    "FOUNDATION_RELEASE_ADMISSION": str(bin_dir / "admission"),
                    "FOUNDATION_SCHEDULED_DEFERRED_DIR": str(self.deferred),
                    "FOUNDATION_JOB_SWITCHES_DIR": str(self.root / "switches"),
                    "FOUNDATION_SCHEDULED_CONSUMED_DIR": str(self.root / "consumed"),
                    "FOUNDATION_OPERATOR_STOPS_DIR": str(self.root / "stopped")}
        (self.root / "switches/run-now.d").mkdir(parents=True)

    def start(self, job, **env):
        for path in (self.root / "started", self.root / "sudo.log", self.root / "listed", self.root / "sleep.log",
                     self.root / "built"):
            path.unlink(missing_ok=True)
        result = subprocess.run(["bash", str(SCRIPT)], env={**self.env, "SSH_ORIGINAL_COMMAND": job, **env},
                                capture_output=True, text=True, timeout=60)
        log = self.root / "sudo.log"
        return result, log.read_text() if log.exists() else ""

    def record(self, job, seconds):
        self.starts.mkdir(exist_ok=True)
        (self.starts / job).write_text(f"{seconds}\n")


class TheRunOutlivesItsConnection(HostFixture):
    """Root ADR-0270 §3 (deploy D2): a dropped SSH session never stops the service it started."""

    def test_a_dropped_connection_leaves_the_run_running(self):
        for path in (self.root / "started", self.root / "sudo.log", self.root / "listed", self.root / "sleep.log"):
            path.unlink(missing_ok=True)
        env = {**self.env, "SSH_ORIGINAL_COMMAND": "fold", "FIXTURE_REAL_SLEEP": "1",
               "FIXTURE_RUN_STATES": ",".join(["activating"] * 200)}
        gate = subprocess.Popen(["bash", str(SCRIPT)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        sleeping = self.root / "sleep.log"
        for _ in range(100):
            if sleeping.exists():
                break
            subprocess.run([sys.executable, "-c", "import time; time.sleep(0.1)"], check=True)
        self.assertTrue(sleeping.exists(), "the gate never reached its follow loop")
        gate.send_signal(signal.SIGHUP)
        gate.communicate(timeout=30)
        self.assertEqual(gate.returncode, 143)
        asked = (self.root / "sudo.log").read_text()
        self.assertIn("systemctl start --no-block foundation-fold.service", asked)
        self.assertNotIn("stop", asked, "the connection's end stopped the run")

    def test_the_next_try_joins_the_run_in_progress_and_reports_it_once(self):
        # The run the dropped connection left behind is still going: InvocationID already answers it.
        (self.root / "started").touch()
        result = subprocess.run(["bash", str(SCRIPT)], env={**self.env, "SSH_ORIGINAL_COMMAND": "fold",
                                                            "FIXTURE_RUN_STATES": "activating,activating"},
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("foundation-fold.service invocation=new", result.stdout)
        self.assertEqual(result.stdout.count("foundation-job-outcome changed"), 2,
                         "the journal's line and the one repeated as the run's own, for one run")


class EveryStartTakesThePoolsSlots(HostFixture):
    """Root ADR-0270 §2b: a run Airflow did not start still holds its pool's slots (2026-10-18)."""

    def test_a_job_does_not_start_while_an_operator_started_run_holds_the_pool(self):
        # The disabled job is what the operator starts; Airflow never sees it.
        result, asked = self.start("floor", FIXTURE_HELD="foundation-paused.service")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(asked, "", "a job started beside a full pool")
        self.assertIn("foundation-job-outcome waiting the spark pool has no room for foundation-floor.service",
                      result.stdout)
        self.assertIn("foundation-paused.service holds 3", result.stdout)
        self.assertFalse((self.starts / "floor").exists(), "a start that did not happen was recorded")

    def test_a_job_that_fits_beside_what_runs_starts(self):
        result, asked = self.start("bake", FIXTURE_HELD="foundation-fold.service")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-bake.service", asked)


class TheOperatorsSwitches(HostFixture):
    """Root ADR-0282: a paused job starts no run; an operator's run-now runs once, unexpired."""

    REQUEST = "44444444-4444-4444-8444-444444444444"

    def pause(self, job):
        (self.root / "switches" / job).write_text("paused\nactor=staff\nreason=a stop for the night\n")

    def ask(self, job, minutes=9, request=None):
        when = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
        (self.root / "switches/run-now.d" / (request or self.REQUEST)).write_text(
            f"request={request or self.REQUEST}\nactor=staff\nreason=a supervised run\njob={job}\n"
            f"expires_at={when.isoformat()}\n")

    def test_a_paused_job_is_skipped_and_starts_nothing(self):
        self.pause("floor")
        for request in ("floor", "floor triggered"):
            with self.subTest(request=request):
                result, asked = self.start(request)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(asked, "")
                self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome skipped paused")
                self.assertIn("a stop for the night", result.stdout)
        listing = json.loads((self.root / "jobs.json").read_text())
        listing["jobs"][0]["check_service"] = "foundation-floor-check@lane.service"
        (self.root / "jobs.json").write_text(json.dumps(listing))
        result, asked = self.start("floor check")
        self.assertEqual((result.returncode, asked), (0, ""), result.stderr)
        self.assertNotIn("foundation-job-outcome", result.stdout, "a check says no outcome")
        sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
        import job_specs  # noqa: E402
        self.assertEqual(job_specs.job_outcome("foundation-job-outcome skipped paused\n"), "skipped")
        self.assertIn("skipped", job_specs.WITHHOLDS_EVENTS)

    def test_a_run_now_runs_once_even_while_paused(self):
        self.pause("floor")
        self.ask("floor")
        result, asked = self.start(f"floor requested {self.REQUEST}")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-floor.service", asked)
        self.assertIn(f"run-now {self.REQUEST} of floor by staff: a supervised run", result.stdout)
        self.assertTrue((self.root / "consumed" / self.REQUEST).exists())
        again, asked = self.start(f"floor requested {self.REQUEST}")
        self.assertEqual((again.returncode, asked), (0, ""), "a run-now was used twice")
        self.assertEqual(again.stdout.splitlines()[-1], "foundation-job-outcome skipped the request was used already")

    def test_a_run_now_that_expired_or_is_another_jobs_or_unknown_starts_nothing(self):
        self.ask("floor", minutes=-1)
        result, asked = self.start(f"floor requested {self.REQUEST}")
        self.assertEqual((result.returncode, asked), (0, ""))
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome skipped the request expired")
        other = "55555555-5555-4555-8555-555555555555"
        self.ask("fold", request=other)
        result, asked = self.start(f"floor requested {other}")
        self.assertEqual(asked, "")
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome skipped the request is another job's")
        result, asked = self.start("floor requested 66666666-6666-4666-8666-666666666666")
        self.assertEqual(result.stdout.splitlines()[-1], "foundation-job-outcome skipped no such request")
        self.assertFalse((self.root / "consumed").exists() and list((self.root / "consumed").iterdir()),
                         "a refused request was marked used")
        for malformed in ("floor requested ../../x", "floor requested 4444", "floor check requested " + self.REQUEST):
            with self.subTest(request=malformed):
                result, asked = self.start(malformed)
                self.assertEqual((result.returncode, asked), (64, ""), result.stdout)

    def test_a_run_an_operator_stopped_says_who_stopped_it(self):
        # foundation-run-now marks the invocation it stopped (the fixture's runs are `new`).
        (self.root / "stopped").mkdir()
        (self.root / "stopped" / self.REQUEST).write_text("invocation=new\nactor=staff-a\n")
        (self.root / "stopped" / "other").write_text("invocation=\nactor=staff-b\n")
        result, _ = self.start("floor", FIXTURE_RESULT="signal")
        self.assertEqual(result.returncode, 1)
        self.assertIn("foundation-job-outcome failed stopped by staff-a", result.stdout)
        (self.root / "stopped" / self.REQUEST).unlink()
        result, _ = self.start("floor", FIXTURE_RESULT="signal")
        self.assertIn("foundation-job-outcome failed systemd result=signal", result.stdout)

    def test_a_run_now_starts_a_job_that_is_off(self):
        # `paused` is listed with enabled false: Airflow never starts it, an operator's run-now may.
        result, asked = self.start("paused")
        self.assertEqual((result.returncode, asked), (64, ""))
        self.ask("paused")
        result, asked = self.start(f"paused requested {self.REQUEST}")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-paused.service", asked)


class TakingTurns(HostFixture):
    """A `takes_turns` job starts at most once between starts of the jobs it blocks (ADR-0138)."""

    def test_the_first_start_of_a_turn_taking_job_runs_and_is_recorded(self):
        result, asked = self.start("bake")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-bake.service", asked)
        self.assertTrue((self.starts / "bake").is_file())
        # The run's outcome is the unit journal's line, repeated before the result line (ADR-0260).
        self.assertEqual(result.stdout.splitlines()[-2:], [
            "foundation-job-outcome changed", "start-scheduled-job: foundation-bake.service result=success"])

    def test_the_run_is_followed_while_job_units_says_it_runs(self):
        # A oneshot is `activating` for its whole run; stopping or reloading it still runs. The wait is
        # job_units.py's definition (root ADR-0184), not a list of states of its own.
        result, asked = self.start("fold", FIXTURE_RUN_STATES="activating,reloading,deactivating,maintenance")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("foundation-fold.service result=success", result.stdout)
        self.assertEqual((self.root / "sleep.log").read_text().split().count("5"), 4)

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


class TheRunSaysHowItEnded(HostFixture):
    """Root ADR-0260 §2: success is what the run said at its end, not that its unit exited 0."""

    def outcome(self, result):
        sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
        import job_specs  # noqa: E402
        return job_specs.job_outcome(result.stdout)

    def test_a_job_that_exits_0_without_an_outcome_is_failed(self):
        # The unit ended `success`, but the run never said it reached its end: a gate that crashed
        # and let the script fall through, an early `exit 0`, an ERR trap that did not fire.
        result, _ = self.start("fold", FIXTURE_JOURNAL="baking 6 lanes\\nall six cut over\\n")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertEqual(result.stdout.splitlines()[-2:], [
            "foundation-job-outcome failed no outcome reported",
            "start-scheduled-job: foundation-fold.service result=success"])
        self.assertEqual(self.outcome(result), "failed")

    def test_a_run_that_says_failed_is_failed_though_it_exited_0(self):
        result, _ = self.start("fold", FIXTURE_JOURNAL="foundation-job-outcome failed gate crashed\\n")
        self.assertEqual(result.returncode, 1)
        self.assertIn("foundation-job-outcome failed gate crashed", result.stdout.splitlines()[-2])
        self.assertEqual(self.outcome(result), "failed")

    def test_a_run_that_waits_is_not_failed_and_starts_nothing(self):
        result, _ = self.start("fold", FIXTURE_JOURNAL="foundation-job-outcome waiting edition awaits its contract\\n")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.outcome(result), "waiting")

    def test_the_last_outcome_line_decides_and_a_bad_line_is_no_line(self):
        result, _ = self.start("fold", FIXTURE_JOURNAL="foundation-job-outcome changed\\nfoundation-job-outcome unchanged\\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.outcome(result), "unchanged")
        result, _ = self.start("fold", FIXTURE_JOURNAL="foundation-job-outcome finished\\n")
        self.assertEqual(result.returncode, 1)

    def test_a_unit_systemd_failed_stays_failed_whatever_it_said(self):
        # 2026-10-16: the by-PNU bake printed `changed` for the lane that published, then exited 1
        # for the one that did not. The failed run's last outcome line must say failed.
        result, _ = self.start("fold", FIXTURE_RESULT="exit-code")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout.splitlines()[-2:], [
            "foundation-job-outcome failed systemd result=exit-code after the run said changed",
            "start-scheduled-job: foundation-fold.service result=exit-code"])
        self.assertEqual(self.outcome(result), "failed")

    def test_a_failed_unit_that_said_nothing_or_failed_is_failed(self):
        for journal in ("loaded\\n", "foundation-job-outcome failed shard 26\\n"):
            result, _ = self.start("fold", FIXTURE_RESULT="exit-code", FIXTURE_JOURNAL=journal)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout.splitlines()[-2], "foundation-job-outcome failed systemd result=exit-code")
            self.assertEqual(self.outcome(result), "failed")


class TheCheckBeforeThePool(HostFixture):
    """Root ADR-0277: `<job> check` starts the job's check service, and only that."""

    def setUp(self):
        super().setUp()
        listing = json.loads((self.root / "jobs.json").read_text())
        listing["jobs"][0]["check_service"] = "foundation-floor-check@lane.service"
        (self.root / "jobs.json").write_text(json.dumps(listing))

    def test_it_starts_the_check_service_and_repeats_its_answer(self):
        result, asked = self.start("floor check", FIXTURE_JOURNAL="plan\\nfoundation-job-check unchanged\\n")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-floor-check@lane.service", asked)
        self.assertNotIn("foundation-floor.service", asked)
        self.assertEqual(result.stdout.splitlines()[-2:], [
            "foundation-job-check unchanged", "start-scheduled-job: foundation-floor-check@lane.service result=success"])
        self.assertNotIn("foundation-job-outcome", result.stdout, "a check is not a run")

    def test_a_check_is_not_the_jobs_start_and_is_never_deferred(self):
        self.record("bake", 2000)
        result, _ = self.start("floor check")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.starts / "floor").exists(), "takes_turns counts runs, not checks")

    def test_a_failed_check_gives_no_answer(self):
        result, _ = self.start("floor check", FIXTURE_RESULT="exit-code",
                               FIXTURE_JOURNAL="foundation-job-check unchanged\\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("foundation-job-check", result.stdout)

    def test_no_check_starts_during_a_release_build_and_nothing_is_owed(self):
        result, asked = self.start("floor check", FIXTURE_BUILD="1")
        self.assertEqual((result.returncode, asked), (0, ""), result.stderr)
        self.assertNotIn("foundation-job-check", result.stdout)
        self.assertFalse(self.deferred.exists() and any(self.deferred.iterdir()))

    def test_a_job_without_a_check_and_a_triggered_check_are_refused(self):
        for request in ("fold check", "floor triggered check", "floor check triggered"):
            with self.subTest(request):
                result, asked = self.start(request)
                self.assertEqual((result.returncode, asked), (64, ""), result.stdout)


class AJobDueDuringAReleaseBuild(HostFixture):
    """Root ADR-0260: a release build's lock is a designed wait, not a failed unit and its alert."""

    def test_it_is_not_started_says_it_waits_and_is_recorded(self):
        result, asked = self.start("fold", FIXTURE_BUILD="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(asked, "", "no unit starts, so no ExecStartPre refuses and no OnFailure alerts")
        self.assertEqual(result.stdout.splitlines()[-1],
                         "foundation-job-outcome waiting a release build is running; this job starts after it finishes")
        sys.path.insert(0, str(PLATFORM / "orchestration/dags"))
        import job_specs  # noqa: E402
        self.assertEqual(job_specs.job_outcome(result.stdout), "waiting")
        self.assertEqual(sorted(path.name for path in self.deferred.iterdir()), ["fold"])

    def test_a_job_that_may_wait_starts_when_the_build_finishes_and_owes_nothing(self):
        # Deploy D4 (root ADR-0270 §2b): floor waits up to 2 minutes; the build ends after 3 questions.
        result, asked = self.start("floor", FIXTURE_BUILD_ANSWERS="3")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("floor waits up to 2 minutes: a release build is running", result.stdout)
        self.assertIn("systemctl start --no-block foundation-floor.service", asked)
        self.assertIn("floor waited 90 seconds for the host", result.stdout)
        self.assertEqual((self.root / "sleep.log").read_text().split()[:3], ["30", "30", "30"])
        self.assertFalse(self.deferred.exists() and list(self.deferred.iterdir()), "a start that ran was deferred")

    def test_a_job_still_blocked_at_its_limit_is_deferred_as_before(self):
        result, asked = self.start("floor", FIXTURE_BUILD="1")
        self.assertEqual(asked, "")
        self.assertEqual(result.stdout.splitlines()[-1],
                         "foundation-job-outcome waiting a release build is running; this job starts after it finishes")
        self.assertEqual((self.root / "sleep.log").read_text().split(), ["30"] * 4, "2 minutes, asked every 30 s")
        self.assertEqual(sorted(path.name for path in self.deferred.iterdir()), ["floor"])

    def test_a_job_that_may_wait_waits_for_its_pool_and_reports_at_its_limit(self):
        result, asked = self.start("floor", FIXTURE_HELD="foundation-paused.service")
        self.assertEqual(asked, "")
        self.assertIn("floor waits up to 2 minutes: the spark pool has no room", result.stdout)
        self.assertIn("foundation-job-outcome waiting the spark pool has no room for foundation-floor.service",
                      result.stdout)
        self.assertEqual((self.root / "sleep.log").read_text().split(), ["30"] * 4)

    def test_the_record_goes_when_the_job_starts(self):
        self.start("fold", FIXTURE_BUILD="1")
        result, asked = self.start("fold triggered")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-fold.service", asked)
        self.assertEqual(list(self.deferred.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
