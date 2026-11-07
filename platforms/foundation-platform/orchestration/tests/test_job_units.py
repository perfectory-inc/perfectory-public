"""The host's one answer to "which jobs are registered" and "is this unit running" (root ADR-0184).

scripts/deploy/job_units.py is asked by release admission, the deploy (through admission), the
autodeploy, the operator script and the scheduler's dispatcher. A stand-in systemctl answers its
one question; every other script is checked not to ask systemd the question itself.
"""

import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
REPOSITORY = PLATFORM.parents[1]
JOB_UNITS = PLATFORM / "scripts/deploy/job_units.py"
JOBS = PLATFORM / "orchestration/jobs.v1.json"

# Answers `list-units` from FAKE_UNITS ("unit=state" pairs, each listed when a pattern matches it),
# prints a status glyph before a failed unit as some systemd versions do, and fails under FAKE_DOWN.
FAKE_SYSTEMCTL = r'''#!/usr/bin/env python3
import fnmatch, json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if os.environ.get("FAKE_DOWN"):
    sys.exit("Failed to connect to bus")
units = dict(pair.split("=") for pair in os.environ.get("FAKE_UNITS", "").split(",") if pair)
patterns = [arg for arg in args[1:] if not arg.startswith("-")]
for unit, state in units.items():
    if any(fnmatch.fnmatch(unit, pattern) for pattern in patterns):
        print(("● " if state == "failed" else "") + f"{unit} loaded {state} x fixture unit")
'''


class TheOperatorWindowIsTheCutOvers(unittest.TestCase):
    """Root ADR-0425: the scheduled window comes from the job list, and the clock-started batch it
    places inside it is kept out of the rest of the day (2026-11-05 23:46Z)."""

    def setUp(self):
        sys.path.insert(0, str(JOB_UNITS.parent))
        self.addCleanup(sys.path.remove, str(JOB_UNITS.parent))
        import job_units
        self.units = job_units
        self.jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]

    def at(self, hour, minute):
        return 1793836800 + hour * 3600 + minute * 60  # 2026-11-05T00:00:00Z

    def test_the_unit_price_migration_is_held_out_of_the_night_and_its_schedule_fits(self):
        window = self.units.scheduled_window(self.jobs)
        (migration,) = [job for job in self.jobs if job["id"] == "unit_price_versions_migration"]
        self.assertTrue(self.units.kept_out_of_operator_window(migration, window),
                        "its 18:45 schedule is meant to end before the operator window")
        self.assertEqual(self.units.operator_window(JOBS, migration["id"], self.at(18, 45)), "")
        # The deploy's unpause at 23:46Z ran the missed 18:45 tick; the cut-over at 00:37Z lost to it.
        for hour, minute in ((23, 46), (0, 37), (20, 0)):
            with self.subTest(at=f"{hour:02d}:{minute:02d}"):
                self.assertIn("operator window",
                              self.units.operator_window(JOBS, migration["id"], self.at(hour, minute)))

    def test_jobs_an_input_starts_or_whose_schedule_runs_past_the_window_are_not_held(self):
        window = self.units.scheduled_window(self.jobs)
        for job in self.jobs:
            if job["started_by"] != "schedule" or job["pool"] != "spark":
                with self.subTest(job=job["id"]):
                    self.assertFalse(self.units.kept_out_of_operator_window(job, window))
                    self.assertEqual(self.units.operator_window(JOBS, job["id"], self.at(23, 46)), "")


class AnApprovedCutOverStartsInTheWindow(unittest.TestCase):
    """Root ADR-0433 §3: an approved cut-over starts when the operator window has room for its first
    lane's steps, now or at the window's next opening; it is never refused for the hour. Window
    04:30-21:50 UTC (synthetic jobs, the 2026-11-06 edges)."""

    JOBS = [{"pool": "spark", "schedule": "30 4 * * *"}, {"pool": "spark", "schedule": "35 21 * * *"},
            {"pool": "spark", "schedule": "5 * * * *"}, {"pool": "light", "schedule": "0 12 * * *"}]

    def setUp(self):
        sys.path.insert(0, str(JOB_UNITS.parent))
        self.addCleanup(sys.path.remove, str(JOB_UNITS.parent))
        import job_units
        self.units = job_units

    def at(self, hour, minute, second=0, day=0):
        return 1793923200 + day * 86400 + hour * 3600 + minute * 60 + second  # 2026-11-06T00:00:00Z

    def test_the_eleven_o_six_starts(self):
        # 00:37:35Z: 233 minutes left for parcel-ownership's 202: now. 03:46:38Z: 44 left: 21:50.
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(0, 37, 35), 202), self.at(0, 37, 35))
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(3, 46, 38), 202), self.at(21, 50))

    def test_the_window_runs_past_midnight_and_a_late_evening_waits_a_day(self):
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(12, 0), 60), self.at(21, 50))
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(21, 50), 400), self.at(21, 50))
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(23, 0), 330), self.at(23, 0))
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(23, 1), 330), self.at(21, 50, day=1))
        self.assertEqual(self.units.operator_start(self.JOBS, self.at(4, 29), 1), self.at(4, 29))

    def test_steps_longer_than_the_whole_window_never_fit(self):
        with self.assertRaises(ValueError):
            self.units.operator_start(self.JOBS, self.at(22, 0), 401)
        result = subprocess.run([sys.executable, str(JOB_UNITS), "operator-start", str(JOBS), "0", "100000"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 64, result.stderr)
        self.assertIn("never fit", result.stderr)

    def test_the_command_answers_from_the_real_job_list(self):
        result = subprocess.run([sys.executable, str(JOB_UNITS), "operator-start", str(JOBS), str(self.at(12, 0)), "202"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        start, end = self.units.scheduled_window(json.loads(JOBS.read_text(encoding="utf-8"))["jobs"])
        self.assertEqual(int(result.stdout), self.at(12, 0) if not start <= 720 < end else self.at(0, 0) + end * 60)


class AnApprovalsProgressIsAppendedLineByLine(unittest.TestCase):
    """Root ADR-0433 §6: scripts/ops/release-progress.py appends one JSON line per event."""

    SCRIPT = PLATFORM / "scripts/ops/release-progress.py"
    APPROVAL = "44444444-4444-4444-8444-444444444444"

    def run_progress(self, directory, *args):
        return subprocess.run([sys.executable, str(self.SCRIPT), *args], capture_output=True, text=True,
                              env={**os.environ, "FOUNDATION_RELEASE_PROGRESS_DIR": str(directory)})

    def test_each_call_appends_a_line_and_nothing_else_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            for args in ((self.APPROVAL, "scheduled", "start_at=2026-11-06T21:50:00Z", "budget_minutes=202"),
                         (self.APPROVAL, "step", "lane=parcel-ownership", "step=gold", "outcome=passed", "seconds=1602",
                          "reason=a=b")):
                self.assertEqual(self.run_progress(directory, *args).returncode, 0)
            lines = [json.loads(line) for line in
                     (pathlib.Path(directory) / f"{self.APPROVAL}.jsonl").read_text().splitlines()]
            self.assertEqual([(line["state"], line.get("budget_minutes"), line.get("seconds")) for line in lines],
                             [("scheduled", 202, None), ("step", None, 1602)])
            self.assertEqual(lines[1]["reason"], "a=b")
            for args in (("../etc/passwd", "scheduled"), (self.APPROVAL, "done"), (self.APPROVAL, "step", "nokey"),
                         (self.APPROVAL, "step", "state=x"), (self.APPROVAL,)):
                with self.subTest(args=args):
                    self.assertEqual(self.run_progress(directory, *args).returncode, 64)
            self.assertEqual(sorted(path.name for path in pathlib.Path(directory).iterdir()), [f"{self.APPROVAL}.jsonl"])


class EveryStartTakesThePoolsSlots(unittest.TestCase):
    """Root ADR-0270 §2b: what runs holds its pool's slots, whoever started it (2026-10-18)."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-units-pool-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "systemctl").write_text(FAKE_SYSTEMCTL)
        (self.root / "systemctl").chmod(0o755)

    def pool_full(self, job, units=None, **env):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), "pool-full", str(JOBS), job],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
                 "FAKE_LOG": str(self.root / "systemctl.log"),
                 "FAKE_UNITS": ",".join(f"{unit}={state}" for unit, state in (units or {}).items()), **env},
            capture_output=True, text=True, timeout=30)

    def test_a_job_an_operator_started_holds_the_slots_a_scheduled_start_needs(self):
        # The incident: a disabled one-off holding every spark slot, then FLOOR came due.
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        whole = next(job for job in jobs if job["pool"] == "spark" and job.get("pool_slots", 1) == 3
                     and job["id"] != "building_register_floor")
        result = self.pool_full("building_register_floor", {whole["systemd_service"]: "activating"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no room for foundation-building-register-floor.service", result.stdout)
        self.assertIn(f"{whole['systemd_service']} holds 3", result.stdout)

    def test_room_is_an_empty_answer_and_a_stopped_unit_holds_nothing(self):
        for units in ({}, {"foundation-gold-panel-rebuild.service": "failed"},
                      {"foundation-gold-panel-rebuild.service": "inactive"}):
            with self.subTest(units=units):
                result = self.pool_full("building_register_floor", units)
                self.assertEqual((result.returncode, result.stdout.strip()), (0, ""), result.stderr)

    def test_a_run_joins_itself_and_a_pool_without_slots_is_not_bounded_here(self):
        result = self.pool_full("building_register_floor", {"foundation-building-register-floor.service": "activating"})
        self.assertEqual(result.stdout.strip(), "", "a run in progress is joined, not counted against itself")
        result = self.pool_full("shape_ledger_check", {"foundation-gold-panel-rebuild.service": "activating"})
        self.assertEqual(result.stdout.strip(), "")

    def test_an_instance_of_a_templated_job_holds_slots_and_is_asked_about(self):
        # A parcel tile rehearsal is an instance of the bake template, not a job of its own.
        rehearsal = "foundation-parcel-tile-bake@rehearsal-bake.service"
        result = self.pool_full("building_register_floor", {rehearsal: "activating"})
        self.assertIn(f"{rehearsal} holds", result.stdout)
        result = self.pool_full(rehearsal, {"foundation-building-register-floor.service": "activating"})
        self.assertIn(f"no room for {rehearsal}", result.stdout)

    def test_no_answer_from_systemd_and_an_unknown_job_are_refusals(self):
        self.assertEqual(self.pool_full("building_register_floor", FAKE_DOWN="1").returncode, 2)
        self.assertEqual(self.pool_full("no_such_job").returncode, 64)


class CapacityGoesByPriorityNotByClock(unittest.TestCase):
    """Root ADR-0435: the spark pool is the resource; a waiting publish claim holds back every lower
    start and a later one of its class, and a running backfill yields at its next step boundary.

    The 2026-11-06 replay: unit_price_versions_migration (a backfill) started at 23:46Z from a missed
    tick; at 00:37Z the parcel-ownership cut-over waited 45 minutes for it and failed."""

    CUTOVER = "foundation-by-pnu-lane-cutover-g1.service"
    MIGRATION = "foundation-unit-price-versions-migrate.service"

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-units-capacity-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "systemctl").write_text(FAKE_SYSTEMCTL)
        (self.root / "systemctl").chmod(0o755)
        self.claims = self.root / "capacity"

    def job_units(self, *args, units=None):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), *args],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
                 "FAKE_LOG": str(self.root / "systemctl.log"), "FOUNDATION_CAPACITY_CLAIMS_DIR": str(self.claims),
                 "FAKE_UNITS": ",".join(f"{unit}=activating" for unit in units or ())},
            capture_output=True, text=True, timeout=30)

    def test_every_class_is_declared_and_the_backfill_and_the_publishers_name_theirs(self):
        listing = json.loads(JOBS.read_text(encoding="utf-8"))
        classes = listing["priority_classes"]
        self.assertGreater(classes["publish"]["value"], classes["default"]["value"])
        self.assertGreater(classes["default"]["value"], classes["backfill"]["value"])
        by_id = {job["id"]: job for job in listing["jobs"]}
        self.assertEqual(by_id["unit_price_versions_migration"]["priority_class"], "backfill")
        for job_id, job in by_id.items():
            if re.fullmatch(r"by_pnu_.*_bake", job_id):
                self.assertEqual(job.get("priority_class"), "publish", job_id)
        self.assertEqual(listing["capacity_claimants"]["by_pnu_lane_cutover"]["priority_class"], "publish")

    def test_the_eleven_o_six_backfill_yields_to_the_waiting_cut_over_and_nothing_lower_starts(self):
        claimed = self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER)
        self.assertEqual(claimed.returncode, 0, claimed.stderr)
        # 00:37Z: the migration runs and the cut-over's unit waits with its claim.
        running = [self.MIGRATION, self.CUTOVER]
        yielded = self.job_units("preempted", str(JOBS), "unit_price_versions_migration", units=running)
        self.assertEqual(yielded.returncode, 0, yielded.stderr)
        self.assertIn("yielded the spark pool to by_pnu_lane_cutover (publish)", yielded.stdout)
        # After the migration ended at its step boundary, no default or backfill start, and no later
        # publish start, takes the pool the cut-over waits for.
        for job in ("unit_price_versions_migration", "gold_panel_rebuild", "by_pnu_unit_price_bake"):
            with self.subTest(job=job):
                held = self.job_units("pool-full", str(JOBS), job, units=[self.CUTOVER])
                self.assertEqual(held.returncode, 0, held.stderr)
                self.assertIn("kept for claims queued before", held.stdout)
                self.assertIn("by_pnu_lane_cutover", held.stdout)

    def test_a_claim_whose_unit_stopped_holds_nothing_and_a_release_removes_it(self):
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        # The cut-over's unit is gone (it ended, or crashed before it took the claim off).
        for job in ("unit_price_versions_migration", "gold_panel_rebuild"):
            self.assertEqual(self.job_units("pool-full", str(JOBS), job).stdout.strip(), "")
        self.assertEqual(self.job_units("preempted", str(JOBS), "unit_price_versions_migration",
                                        units=[self.MIGRATION]).stdout.strip(), "")
        self.assertEqual(self.job_units("release", "by_pnu_lane_cutover").returncode, 0)
        self.assertEqual(sorted(self.claims.iterdir()), [])

    def test_only_a_yielding_class_is_preempted_and_only_a_declared_claimant_claims(self):
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        floor = self.job_units("preempted", str(JOBS), "building_register_floor",
                               units=["foundation-building-register-floor.service", self.CUTOVER])
        self.assertEqual(floor.stdout.strip(), "", "a default job runs to its end; the claim waits for it")
        for args in (("someone_else", self.CUTOVER), ("by_pnu_lane_cutover", "foundation-gold-panel-rebuild.service")):
            with self.subTest(args=args):
                self.assertEqual(self.job_units("claim", str(JOBS), *args).returncode, 64)


class TheOneDefinition(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-units-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "systemctl").write_text(FAKE_SYSTEMCTL)
        (self.root / "systemctl").chmod(0o755)
        self.log = self.root / "systemctl.log"

    def running(self, *patterns, units=None, **env):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), "running", *patterns],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}", "FAKE_LOG": str(self.log),
                 "FAKE_UNITS": ",".join(f"{unit}={state}" for unit, state in (units or {}).items()), **env},
            capture_output=True, text=True, timeout=30)

    def test_every_state_but_inactive_and_failed_runs(self):
        # A oneshot is `activating` for its whole run and never `active` (2026-10-10).
        units = {f"foundation-{state}.service": state
                 for state in ("activating", "active", "deactivating", "reloading", "refreshing", "maintenance",
                               "inactive", "failed")}
        result = self.running("foundation-*", units=units)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), sorted(
            f"foundation-{state}.service {state}"
            for state in ("activating", "active", "deactivating", "reloading", "refreshing", "maintenance")))

    def test_it_asks_one_listing_of_services_by_name_or_pattern(self):
        result = self.running("foundation-a.service", "foundation-silver-refresh@*.service",
                              units={"foundation-a.service": "activating",
                                     "foundation-silver-refresh@x.service": "deactivating",
                                     "foundation-b.service": "active"})
        self.assertEqual(result.stdout.splitlines(),
                         ["foundation-a.service activating", "foundation-silver-refresh@x.service deactivating"])
        asked = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(len(asked), 1)
        self.assertEqual(asked[0][0], "list-units")
        # --all: a unit is listed whatever its state, so the rule above decides, not systemd's filter.
        self.assertLessEqual({"--all", "--type=service", "--plain", "--no-legend", "--full"}, set(asked[0]))

    def test_nothing_running_is_an_empty_answer_and_no_answer_is_not(self):
        result = self.running("foundation-a.service", units={"foundation-a.service": "inactive"})
        self.assertEqual((result.returncode, result.stdout), (0, ""))
        result = self.running("foundation-a.service", FAKE_DOWN="1")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("systemd cannot report", result.stderr)

    def test_the_registered_services_are_every_jobs_disabled_ones_too(self):
        # 2026-10-11: the deploy waited only for the enabled jobs and met a disabled lane running.
        result = subprocess.run([sys.executable, "-I", "-B", str(JOB_UNITS), "services", str(JOBS)],
                                capture_output=True, text=True, check=True)
        jobs = json.loads(JOBS.read_text(encoding="utf-8"))["jobs"]
        checks = {job["check_service"] for job in jobs if job.get("check_service")}
        # Root ADR-0277: a check service is admitted and waited for like the job's own.
        self.assertTrue(checks, "the registry has a check service to prove this on")
        self.assertEqual(result.stdout.split(), sorted({job["systemd_service"] for job in jobs} | checks))
        self.assertTrue(any(not job["enabled"] for job in jobs), "the registry has a disabled job to prove this on")


# Forms that ask systemd "does it run" without job_units.py. `systemctl is-active <unit>` without
# --quiet only prints the state for a person (the operator's `status`), so it is not one of them.
ASKS_SYSTEMD_ITSELF = re.compile(
    r"list-units|is-active\s+(--quiet|-q)\b|(-p|--property=?)\s*ActiveState|\bstate ActiveState\b"
    r"|[\"']is-active[\"']|[\"']ActiveState[\"']|[\"']--property=ActiveState[\"']")


def code_lines(path):
    """Lines that run: shell and Python comments are skipped, so prose may name what it forbids."""
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.lstrip().startswith("#"):
            yield number, line


class NothingElseAsks(unittest.TestCase):
    """Every host script that decides whether a unit runs goes through job_units.py."""

    SCANNED = (PLATFORM / "scripts", PLATFORM / "orchestration/dags", PLATFORM / "infra", REPOSITORY / "scripts/deploy")

    def offenders(self, roots):
        found = []
        for root in roots:
            for path in sorted(root.rglob("*")):
                if path.suffix not in (".sh", ".py", ".service", ".conf") or path == JOB_UNITS or not path.is_file():
                    continue
                found += [f"{path}:{number}: {line.strip()}" for number, line in code_lines(path)
                          if ASKS_SYSTEMD_ITSELF.search(line)]
        return found

    def test_no_script_asks_systemd_whether_a_unit_runs_on_its_own(self):
        self.assertEqual(self.offenders(self.SCANNED), [])

    def test_the_scan_finds_each_form_it_forbids(self):
        planted = tempfile.TemporaryDirectory(prefix="job-units-scan-")
        self.addCleanup(planted.cleanup)
        root = pathlib.Path(planted.name)
        forms = ['systemctl is-active --quiet "${unit}" && refuse',
                 'systemctl list-units --state=active,activating "${unit}"',
                 'state="$(systemctl show -p ActiveState --value "${unit}")"',
                 'while [[ "$(state ActiveState)" == activating ]]; do',
                 'subprocess.run(["/usr/bin/systemctl", "show", "--property=ActiveState", "--value", unit])']
        for index, form in enumerate(forms):
            (root / f"planted-{index}.sh").write_text(f"# prose may say list-units\n{form}\n")
        self.assertEqual(len(self.offenders([root])), len(forms))


class TheDeferredJobs(unittest.TestCase):
    """Root ADR-0260: where the runner records a job a release build kept from starting, and which."""

    def ask(self, *argv, directory):
        return subprocess.run([sys.executable, "-I", str(JOB_UNITS), *argv], capture_output=True, text=True,
                              env={**os.environ, "FOUNDATION_SCHEDULED_DEFERRED_DIR": str(directory)}, check=True)

    def test_the_runner_and_the_deploy_read_one_place(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.ask("deferred-dir", directory=directory).stdout.strip(), directory)
            self.assertEqual(self.ask("deferred", directory=pathlib.Path(directory) / "never-made").stdout.strip(), "")
            for name in ("map_edit_fold_complex", "outbox_publish", "not a job", ".hidden"):
                (pathlib.Path(directory) / name).write_text("0\n")
            self.assertEqual(self.ask("deferred", directory=directory).stdout.split(),
                             ["map_edit_fold_complex", "outbox_publish"])


if __name__ == "__main__":
    unittest.main()
