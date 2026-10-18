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
# list-units (job_units.py's question, root ADR-0184) answers the next state of FIXTURE_RUN_STATES,
# one per question, and then nothing: the run has ended.
SYSTEMCTL = """#!/bin/sh
root="$FIXTURE_ROOT"
case "$*" in
  list-units*)
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
"""
SUDO = """#!/bin/sh
echo "$*" >> "$FIXTURE_ROOT/sudo.log"
touch "$FIXTURE_ROOT/started"
"""
# Release admission's `release-build-running`: 75 while FIXTURE_BUILD says a build holds the lock.
ADMISSION = """#!/bin/sh
[ "$1" = release-build-running ] || exit 64
[ -z "${FIXTURE_BUILD:-}" ] || exit 75
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
                    "FOUNDATION_SCHEDULED_DEFERRED_DIR": str(self.deferred)}

    def start(self, job, **env):
        for path in (self.root / "started", self.root / "sudo.log", self.root / "listed", self.root / "sleep.log"):
            path.unlink(missing_ok=True)
        result = subprocess.run(["bash", str(SCRIPT)], env={**self.env, "SSH_ORIGINAL_COMMAND": job, **env},
                                capture_output=True, text=True, timeout=60)
        log = self.root / "sudo.log"
        return result, log.read_text() if log.exists() else ""

    def record(self, job, seconds):
        self.starts.mkdir(exist_ok=True)
        (self.starts / job).write_text(f"{seconds}\n")


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
    """Root ADR-0276: `<job> check` starts the job's check service, and only that."""

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

    def test_the_record_goes_when_the_job_starts(self):
        self.start("fold", FIXTURE_BUILD="1")
        result, asked = self.start("fold triggered")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("systemctl start --no-block foundation-fold.service", asked)
        self.assertEqual(list(self.deferred.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
