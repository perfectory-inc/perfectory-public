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
import time
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


class NoStartIsRefusedForTheTimeOfDay(unittest.TestCase):
    """Root ADR-0435: the clock windows of ADR-0425 and ADR-0433 §3 are gone. 2026-11-06 03:46Z the
    scheduled window refused the parcel-ownership cut-over; capacity and priority decide instead."""

    def test_the_window_answers_are_gone_from_the_one_place_that_gave_them(self):
        for command in (["operator-window", str(JOBS), "unit_price_versions_migration", "1793922360"],
                        ["operator-start", str(JOBS), "1793936798", "202"]):
            with self.subTest(command=command[0]):
                result = subprocess.run([sys.executable, "-I", "-B", str(JOB_UNITS), *command],
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 64, result.stdout)
        listing = json.loads(JOBS.read_text(encoding="utf-8"))
        self.assertNotIn("operator_window_means", listing)


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

    def job_units(self, *args, units=None, queue=None):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), *args],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
                 "FAKE_LOG": str(self.root / "systemctl.log"), "FOUNDATION_CAPACITY_CLAIMS_DIR": str(self.claims),
                 "FOUNDATION_START_QUEUE_DIR": str(queue or self.root / "queue"),
                 "FAKE_UNITS": ",".join(f"{unit}=activating" for unit in units or ())},
            capture_output=True, text=True, timeout=30)

    def test_every_class_is_declared_and_the_backfill_and_the_publishers_name_theirs(self):
        listing = json.loads(JOBS.read_text(encoding="utf-8"))
        classes = listing["priority_classes"]
        self.assertGreater(classes["publish"]["value"], classes["first_publish"]["value"])
        self.assertGreater(classes["first_publish"]["value"], classes["default"]["value"])
        self.assertGreater(classes["default"]["value"], classes["backfill"]["value"])
        by_id = {job["id"]: job for job in listing["jobs"]}
        self.assertEqual(by_id["unit_price_versions_migration"]["priority_class"], "backfill")
        for job_id, job in by_id.items():
            if re.fullmatch(r"by_pnu_.*_bake", job_id):
                self.assertEqual(job.get("priority_class"), "publish", job_id)
        # Root ADR-0439: a lane's first publish ranks below refreshing what users read now, and the
        # panel Gold rebuild takes the class of the bake that waits on it.
        self.assertEqual(listing["capacity_claimants"]["by_pnu_lane_cutover"]["priority_class"], "first_publish")
        # So does the kind Gold rebuild: 2026-11-07 and 11-08 its start ended `waiting` behind a cut-over's claim.
        for job_id in ("gold_panel_rebuild", "gold_kind_rebuild"):
            self.assertEqual((by_id[job_id]["priority_class"], by_id[job_id]["priority_class_from"]),
                             ("publish", "by_pnu_serving_bake"), job_id)

    def test_the_eleven_o_six_backfill_yields_to_the_waiting_cut_over_and_nothing_lower_starts(self):
        claimed = self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER)
        self.assertEqual(claimed.returncode, 0, claimed.stderr)
        # 00:37Z: the migration runs and the cut-over's unit waits with its claim.
        running = [self.MIGRATION, self.CUTOVER]
        yielded = self.job_units("preempted", str(JOBS), "unit_price_versions_migration", units=running)
        self.assertEqual(yielded.returncode, 0, yielded.stderr)
        self.assertIn("yielded the spark pool to by_pnu_lane_cutover (first_publish)", yielded.stdout)
        # After the migration ended at its step boundary, no default or backfill start takes the pool
        # the cut-over waits for.
        for job in ("unit_price_versions_migration", "building_register_floor"):
            with self.subTest(job=job):
                held = self.job_units("pool-full", str(JOBS), job, units=[self.CUTOVER])
                self.assertEqual(held.returncode, 0, held.stderr)
                self.assertIn("kept for claims queued before", held.stdout)
                self.assertIn("by_pnu_lane_cutover", held.stdout)

    def test_a_served_lanes_refresh_is_not_held_for_a_first_publish(self):
        # Root ADR-0439: 2026-11-07 07:42Z..09:00Z and from 09:12Z two lane cut-overs held the spark pool
        # while the parcel lane served a 193-hour-old manifest; the panel Gold rebuild and the bakes that
        # refresh served lanes now outrank a first publish.
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        for job in ("gold_panel_rebuild", "by_pnu_unit_price_bake", "by_pnu_serving_bake"):
            with self.subTest(job=job):
                free = self.job_units("pool-full", str(JOBS), job, units=[self.CUTOVER])
                self.assertEqual(free.returncode, 0, free.stderr)
                self.assertNotIn("kept for claims queued before", free.stdout)

    def test_a_claim_whose_unit_stopped_holds_nothing_and_a_release_removes_it(self):
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        # The cut-over's unit is gone (it ended, or crashed before it took the claim off).
        for job in ("unit_price_versions_migration", "gold_panel_rebuild"):
            self.assertEqual(self.job_units("pool-full", str(JOBS), job).stdout.strip(), "")
        self.assertEqual(self.job_units("preempted", str(JOBS), "unit_price_versions_migration",
                                        units=[self.MIGRATION]).stdout.strip(), "")
        self.assertEqual(self.job_units("release", "by_pnu_lane_cutover").returncode, 0)
        self.assertEqual(sorted(self.claims.iterdir()), [])

    def test_a_start_refused_long_enough_outranks_a_claim_queued_anew(self):
        # Root ADR-0462, the 2026-11-09 replay: the cut-over's claim (first_publish) was queued anew every
        # run, and a Silver load (default) refused at 07:00Z had no way past it. Its wait now ages it: four
        # hours in the queue tie a fresh claim; past them the start goes first.
        job = "building_register_floor"
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        queue = self.root / "queue"
        queue.mkdir()
        for hours, held in ((0, True), (3, True), (5, False), (9, False)):
            with self.subTest(hours=hours):
                record = {"since": int(time.time()) - hours * 3600, "attempts": 1, "last_at": int(time.time()), "reason": "-"}
                (queue / f"{job}.json").write_text(json.dumps(record))
                result = self.job_units("pool-full", str(JOBS), job, units=[self.CUTOVER], queue=queue)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual("kept for claims queued before" in result.stdout, held, result.stdout)
        # What runs still holds its slots: aging orders the waiters, it takes no running job's place.
        result = self.job_units("pool-full", str(JOBS), job, units=[self.CUTOVER, "foundation-gold-panel-rebuild.service"],
                                queue=queue)
        self.assertIn("has no room", result.stdout)

    def test_the_release_makes_the_queue_the_runner_writes(self):
        # Root ADR-0462: the scheduler account owns it, and tmpfiles cannot (it runs before that account exists).
        release = (JOB_UNITS.parent / "foundation-release.sh").read_text(encoding="utf-8")
        source = JOB_UNITS.read_text(encoding="utf-8")
        (made,) = re.findall(r"^scheduler_queue=(\S+)$", release, re.MULTILINE)
        self.assertIn(f'QUEUE_DIR = "{made}"', source)
        self.assertIn('-m 0755 "${scheduler_queue}"', release)

    def test_the_start_records_lie_beside_the_queue_where_the_reconciler_reads_them(self):
        # Root ADR-0467: under the scheduler's 0750 home the reconciler could not see that a slot had no start.
        release = (JOB_UNITS.parent / "foundation-release.sh").read_text(encoding="utf-8")
        runner = (JOB_UNITS.parents[1] / "ops/start-scheduled-job.sh").read_text(encoding="utf-8")
        (made,) = re.findall(r"^scheduler_starts=(\S+)$", release, re.MULTILINE)
        self.assertIn(f'STARTS_DIR = "{made}"', JOB_UNITS.read_text(encoding="utf-8"))
        self.assertIn(f'starts="${{FOUNDATION_SCHEDULED_STARTS_DIR:-{made}}}"', runner)
        self.assertIn('-m 0755 "${scheduler_starts}"', release)
        self.assertIn('cp -n --preserve=timestamps "${scheduler_home}/starts/"*', release)

    def test_no_start_record_is_read_before_the_deploy_seeded_them(self):
        starts = self.root / "starts"
        starts.mkdir()
        (starts / "floor").write_text("1000\n")
        (starts / "floor.next").write_text("2000\n")
        read = [sys.executable, "-I", "-B", "-c", "import json, sys; sys.path.insert(0, sys.argv[1]); import job_units;"
                " print(json.dumps(job_units.last_starts()))", str(JOB_UNITS.parent)]
        env = {**os.environ, "FOUNDATION_SCHEDULED_STARTS_DIR": str(starts)}
        self.assertEqual(subprocess.run(read, env=env, capture_output=True, text=True, check=True).stdout.strip(), "null")
        (starts / ".seeded").write_text("")
        self.assertEqual(json.loads(subprocess.run(read, env=env, capture_output=True, text=True, check=True).stdout),
                         {"floor": 1000})

    def test_the_queue_keeps_its_first_refusal_and_a_start_takes_it_off(self):
        queue = self.root / "queue"
        for attempt in (1, 2):
            result = self.job_units("queue", "building_register_floor", f"refusal {attempt}", queue=queue)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"refused {attempt} times", result.stdout)
        record = json.loads((queue / "building_register_floor.json").read_text())
        self.assertEqual((record["attempts"], record["reason"]), (2, "refusal 2"))
        waited = self.job_units("dequeue", "building_register_floor", queue=queue)
        self.assertEqual(waited.returncode, 0, waited.stderr)
        self.assertGreaterEqual(int(waited.stdout), 0)
        self.assertFalse((queue / "building_register_floor.json").exists())
        self.assertEqual(self.job_units("dequeue", "building_register_floor", queue=queue).stdout.strip(), "",
                         "a job that was not queued waited for nothing")
        self.assertEqual(len((queue / "history.jsonl").read_text().splitlines()), 1)
        self.assertEqual(self.job_units("queue", "../etc", "x", queue=queue).returncode, 64)

    def test_only_a_yielding_class_is_preempted_and_only_a_declared_claimant_claims(self):
        self.assertEqual(self.job_units("claim", str(JOBS), "by_pnu_lane_cutover", self.CUTOVER).returncode, 0)
        floor = self.job_units("preempted", str(JOBS), "building_register_floor",
                               units=["foundation-building-register-floor.service", self.CUTOVER])
        self.assertEqual(floor.stdout.strip(), "", "a default job runs to its end; the claim waits for it")
        for args in (("someone_else", self.CUTOVER), ("by_pnu_lane_cutover", "foundation-gold-panel-rebuild.service")):
            with self.subTest(args=args):
                self.assertEqual(self.job_units("claim", str(JOBS), *args).returncode, 64)


# As FAKE_SYSTEMCTL, and answers `show` from FAKE_MEMORY ("unit=MemoryMax:MemoryCurrent" pairs) the way
# systemd prints the two properties: bytes, or `infinity` and `[not set]`.
FAKE_SYSTEMCTL_WITH_MEMORY = FAKE_SYSTEMCTL.replace("units = dict(", r'''if args and args[0] == "show":
    wanted = args[args.index("--") + 1:]
    memory = dict(pair.split("=") for pair in os.environ.get("FAKE_MEMORY", "").split(",") if pair)
    print("\n\n".join(f"Id={unit}\nMemoryMax={memory[unit].split(':')[0]}\nMemoryCurrent={memory[unit].split(':')[1]}"
                      for unit in wanted if unit in memory))
    sys.exit(0)
units = dict(''', 1)


class AMemoryClaimWaitsForMemoryNotForAnEmptyPool(unittest.TestCase):
    """Root ADR-0563: the fact job's model is admitted against the host's available memory.

    2026-11-13: intelligence_document_fact took all three spark slots so its 24G model never sat beside
    Spark; the pool was never empty and its start waited 24,600 s, while node-exporter's MemAvailable
    was 30 GiB or more for 95% of the week. It now holds no spark slot and states the model runtime
    unit as its memory_claim."""

    MODEL = "intelligence-model-runtime-ollama.service"
    GIB = 1 << 30

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-units-memory-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        (self.root / "systemctl").write_text(FAKE_SYSTEMCTL_WITH_MEMORY)
        (self.root / "systemctl").chmod(0o755)

    def pool_full(self, available_gib, units=None, held_gib=None, cap=None):
        meminfo = self.root / "meminfo"
        meminfo.write_text(f"MemTotal:       65651712 kB\nMemAvailable:   {int(available_gib * (1 << 20))} kB\n")
        current = "[not set]" if held_gib is None else str(int(held_gib * self.GIB))
        return subprocess.run(
            [sys.executable, "-I", "-B", str(JOB_UNITS), "pool-full", str(JOBS), "intelligence_document_fact"],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
                 "FAKE_LOG": str(self.root / "systemctl.log"), "FOUNDATION_HOST_MEMINFO": str(meminfo),
                 "FOUNDATION_CAPACITY_CLAIMS_DIR": str(self.root / "capacity"),
                 "FOUNDATION_START_QUEUE_DIR": str(self.root / "queue"),
                 "FAKE_MEMORY": f"{self.MODEL}={cap or 24 * self.GIB}:{current}",
                 "FAKE_UNITS": ",".join(f"{unit}=activating" for unit in units or ())},
            capture_output=True, text=True, timeout=30)

    def test_the_fact_job_states_the_model_as_its_claim_and_holds_no_spark_slot(self):
        listing = json.loads(JOBS.read_text(encoding="utf-8"))
        job = next(job for job in listing["jobs"] if job["id"] == "intelligence_document_fact")
        self.assertEqual(job["memory_claim"], {"units": [self.MODEL]})
        self.assertEqual((job["pool"], listing["pools"][job["pool"]]["slots"]), ("documents", 1))
        self.assertRegex(listing["memory_claims"]["keep_available"], r"^[0-9]+g$")
        unit = (PLATFORM / "infra/systemd/foundation-intelligence-document-fact.service").read_text(encoding="utf-8")
        self.assertIn(f"Requires={self.MODEL}", unit)

    def test_it_starts_beside_a_running_spark_job_when_the_host_has_the_memory(self):
        # The 11-13 replay: a spark-pool job and a lane cut-over's claim on the whole pool, 40 GiB available.
        claims = self.root / "capacity"
        claims.mkdir()
        cutover = "foundation-by-pnu-lane-cutover-g1.service"
        (claims / "by_pnu_lane_cutover.json").write_text(json.dumps({"unit": cutover, "since": int(time.time())}))
        result = self.pool_full(40, units=["foundation-gold-panel-rebuild.service", cutover])
        self.assertEqual((result.returncode, result.stdout.strip()), (0, ""), result.stderr)

    def test_it_is_refused_while_the_model_and_what_stays_free_do_not_fit_in_what_is_available(self):
        result = self.pool_full(28)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("the host has no memory for foundation-intelligence-document-fact.service's claim", result.stdout)
        self.assertIn(f"{self.MODEL} may take 24.0 GiB more, 28.0 GiB is available and 6.0 GiB stays free", result.stdout)
        self.assertEqual(self.pool_full(30).stdout.strip(), "")

    def test_what_the_model_already_holds_is_not_claimed_twice(self):
        # The runtime is up with 20 GiB resident, so MemAvailable already lacks it: 4 GiB more is the claim.
        self.assertEqual(self.pool_full(10, held_gib=20).stdout.strip(), "")
        self.assertIn("may take 4.0 GiB more", self.pool_full(9, held_gib=20).stdout)

    def test_a_unit_with_no_cap_cannot_be_claimed(self):
        result = self.pool_full(60, cap="infinity")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cannot be sized", result.stdout)
        self.assertIn("states no MemoryMax", result.stdout)

    def test_the_other_document_job_still_holds_the_one_slot(self):
        result = self.pool_full(60, units=["foundation-intelligence-document-text.service"])
        self.assertIn("the documents pool has no room", result.stdout)


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
