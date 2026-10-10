"""The job list builds the DAGs the scheduler runs (root ADR-0122); these pin what it may say."""

import copy
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402


def bake(jobs):
    return next(job for job in jobs["jobs"] if job["id"] == "by_pnu_serving_bake")


def real_inputs():
    return (
        json.loads(job_specs.JOBS.read_text(encoding="utf-8")),
        json.loads(job_specs.GRAPH.read_text(encoding="utf-8")),
    )


class TheRealJobList(unittest.TestCase):
    def setUp(self):
        self.specs = job_specs.load_specs()

    def test_every_job_builds_a_dag_with_lineage(self):
        self.assertTrue(self.specs, "the job list builds no DAG")
        for spec in self.specs:
            with self.subTest(spec.dag_id):
                self.assertTrue(spec.inputs, "a job must report what it reads")

    def test_floor_cycle_owns_one_complete_spark_job_and_only_its_real_edge(self):
        jobs, _ = real_inputs()
        floor = next(job for job in jobs["jobs"] if job["id"] == "building_register_floor")
        self.assertTrue(floor["enabled"])
        self.assertEqual(floor["pool"], "spark")
        self.assertEqual(floor["pipeline_graph_edges"], [
            "source-building-hub-bulk-to-silver-building-register-floors"
        ])
        release = (job_specs.PLATFORM_ROOT / "scripts/deploy/foundation-release.sh").read_text()
        self.assertIn('"${release_root}"/current/infra/systemd/*.service', release)
        self.assertTrue(job_specs.service_unit_file(floor["systemd_service"]).is_file())
        unit = job_specs.service_unit_file(floor["systemd_service"]).read_text()
        self.assertIn("User=foundation-platform\n", unit)
        for environment_file in [
            "/etc/foundation-platform/recovery.env",
            "/etc/foundation-platform/source-sweep.env",
            "/etc/foundation-platform/map-edit-fold.env",
        ]:
            self.assertIn(f"EnvironmentFile={environment_file}\n", unit)
        self.assertIn("ProtectSystem=strict\n", unit)
        self.assertNotRegex(unit, r"(?m)^ReadWritePaths=", "the installer owns the dedicated namespace policy")
        self.assertIn("UMask=0007\n", unit)
        self.assertIn("RuntimeDirectory=foundation-building-register-floor\n", unit)
        self.assertIn("RuntimeDirectoryMode=0700\n", unit)
        self.assertNotIn("LoadCredential=", unit)
        self.assertNotIn("EnvironmentFile=/opt/foundation-platform/current/", unit)
        self.assertIn('building-register-floor-cycle.sh" cleanup\'', unit)
        self.assertIn("TimeoutStopSec=120\n", unit)

    def test_a_job_runs_in_exactly_one_place(self):
        # ADR-0122 §4: an enabled job's systemd timer no longer ships; a job still on systemd keeps
        # its timer. Otherwise it would run twice, or nowhere.
        timers = {path.name for path in job_specs.SYSTEMD.glob("*.timer")}
        for spec in self.specs:
            if spec.systemd_timer is None:
                # No timer: Airflow runs it, or (disabled_reason) nothing does until a deploy turns it on.
                if not spec.enabled:
                    job = next(job for job in real_inputs()[0]["jobs"] if job["id"] == spec.job_id)
                    self.assertTrue(job.get("disabled_reason"), f"{spec.dag_id} runs nowhere and says nothing")
                continue
            with self.subTest(spec.dag_id):
                self.assertEqual(
                    spec.systemd_timer in timers,
                    not spec.enabled,
                    "enabled in Airflow and still a systemd timer, or neither",
                )

    def test_a_shipped_timer_starts_the_service_its_job_names(self):
        for spec in self.specs:
            if spec.systemd_timer is None:
                continue
            timer = job_specs.SYSTEMD / spec.systemd_timer
            if not timer.is_file():
                continue
            with self.subTest(spec.dag_id):
                units = re.findall(r"^Unit=(\S+)$", timer.read_text(encoding="utf-8"), flags=re.MULTILINE)
                self.assertEqual(units, [spec.systemd_service])

    def test_the_timeout_outlasts_the_service_own_limit(self):
        # Airflow stopping a job systemd would still let finish turns a slow success into a failure.
        for spec in self.specs:
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            limit = re.search(r"^TimeoutStartSec=(\d+)(m|h)$", unit, flags=re.MULTILINE)
            with self.subTest(spec.dag_id):
                self.assertIsNotNone(limit, "the service states its own time limit")
                minutes = int(limit.group(1)) * (60 if limit.group(2) == "h" else 1)
                self.assertGreater(spec.timeout_minutes, minutes)

    def test_what_each_service_runs_is_executable_in_the_repository(self):
        # systemd answers 203/EXEC when ExecStart is not executable. The stewardship cycle shipped
        # without its executable bit on 2026-09-30 and never ran once; the timer hid it until
        # Airflow showed the failure (2026-10-01). Read from git, so a Windows checkout cannot pass
        # a file that a Linux host would refuse to execute.
        release = job_specs.RELEASE_PREFIX
        for spec in self.specs:
            unit = job_specs.service_unit_file(spec.systemd_service).read_text(encoding="utf-8")
            exec_start = re.search(r"^ExecStart=(.+)$", unit, flags=re.MULTILINE).group(1)
            if "RuntimeDirectory=foundation-building-register-floor" in unit:
                # The runtime snapshot binds run AND cleanup to the physical release.
                launcher = re.search(r'exec "\$\$\{release\}/([^"]+)"', exec_start)
                self.assertIsNotNone(launcher)
                exec_start = release + launcher.group(1)
            else:
                exec_start = exec_start.split()[0]
            with self.subTest(spec.dag_id):
                self.assertTrue(exec_start.startswith(release), f"{exec_start} is not in the release")
                relative = exec_start[len(release):]
                listed = subprocess.run(
                    ["git", "ls-files", "-s", "--", relative],
                    cwd=job_specs.PLATFORM_ROOT, capture_output=True, text=True, check=True,
                ).stdout.split()
                self.assertTrue(listed, f"{relative} is not tracked")
                self.assertEqual(listed[0], "100755", f"{relative} is not executable in git")

    def test_dag_ids_are_unique(self):
        ids = [spec.dag_id for spec in self.specs]
        self.assertEqual(len(ids), len(set(ids)))


class WhatTheJobListMayNotSay(unittest.TestCase):
    def refused(self, mutate):
        jobs, graph = real_inputs()
        jobs = copy.deepcopy(jobs)
        mutate(jobs)
        with self.assertRaises(job_specs.JobListError):
            job_specs.load_specs(jobs, graph)

    def test_an_edge_the_pipeline_graph_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0]["pipeline_graph_edges"].append("no-such-edge"))

    def test_a_job_that_names_no_edge(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pipeline_graph_edges=[]))

    def test_a_dag_id_prefix_that_is_missing_or_not_a_name(self):
        self.refused(lambda jobs: jobs.pop("dag_id_prefix"))
        self.refused(lambda jobs: jobs.update(dag_id_prefix="Foundation-"))

    def test_a_pool_the_scheduler_does_not_have(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(pool="big"))

    def test_a_pool_without_slots_or_a_reason(self):
        self.refused(lambda jobs: jobs["pools"]["spark"].update(slots=0))
        self.refused(lambda jobs: jobs["pools"]["spark"].update(description=" "))
        self.refused(lambda jobs: jobs["pools"].update(default_pool={"slots": 1, "description": "x"}))

    def test_a_long_bake_cannot_hold_every_slot_the_hourly_folds_need(self):
        # The bake runs up to 21 hours. Taking two of the three slots, the hourly folds (two each)
        # would wait behind it as well; with one, a fold always fits beside it (root ADR-0138).
        # Lighter and as wide as a fold, it never starts ahead of one, but one run of it can already
        # hold the pool when a fold's turn comes: FLOOR, lineage and the other fold, then the bake.
        def both_slots(jobs):
            bake(jobs)["pool_slots"] = 2
        jobs = copy.deepcopy(real_inputs()[0])
        both_slots(jobs)
        problems = job_specs.pool_starvation(jobs)
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 2295 minutes for pool 'spark'")
                            and "by_pnu_serving_bake" in problem for problem in problems), problems)
        self.refused(both_slots)

    def test_the_starvation_bound_counts_every_job_that_can_hold_it_back(self):
        # FLOOR (3 slots, weight 10) can be held back once by lineage (same weight), once by each
        # lighter job narrower than it, which may start in room FLOOR does not fit (both folds, one
        # bake run), and by what already holds the pool when its turn comes: one panel Gold rebuild
        # (not retried, root ADR-0139), whose 165 minutes outlast any one Silver lane's 160.
        jobs = copy.deepcopy(real_inputs()[0])
        spark = [job for job in jobs["jobs"] if job["pool"] == "spark"]
        floor = next(job for job in spark if job["id"] == "building_register_floor")
        wait, blockers = job_specs.longest_wait_minutes(floor, [job for job in spark if job is not floor], 3)
        self.assertEqual(wait, 2 * 130 + 5 + 2 * (2 * 130 + 5) + 165 + 1260)
        self.assertEqual(set(blockers), {"lineage_stewardship", "map_edit_fold_admin", "map_edit_fold_complex",
                                         "gold_panel_rebuild", "by_pnu_serving_bake"})
        # A fold (2 slots) never waits for the bake (1): they fit together.
        fold = next(job for job in spark if job["id"] == "map_edit_fold_admin")
        wait, blockers = job_specs.longest_wait_minutes(fold, [job for job in spark if job is not fold], 3)
        self.assertNotIn("by_pnu_serving_bake", blockers)
        # The panel Gold rebuild takes all three slots: a fold waits for one run of it, which fills
        # the fold's bound to its limit (20 x 60 minutes).
        self.assertEqual(wait, (2 * 250 + 5) + (2 * 130 + 5) + (2 * 130 + 5) + 165)
        self.assertEqual(wait, job_specs.STARVATION_CYCLES * 60)

    def test_a_retry_holds_the_slots_again(self):
        # Airflow retries a failed run: one run can hold its slots for (retries + 1) x its timeout.
        floor = {"id": "f", "timeout_minutes": 250, "retries": 1}
        self.assertEqual(job_specs.hold_minutes(floor), 505)
        self.assertEqual(job_specs.hold_minutes({**floor, "retries": 0}), 250)
        # Planted: FLOOR retried three times would keep the hourly folds waiting past the limit,
        # although its timeout alone would not.
        def retried(jobs):
            next(job for job in jobs["jobs"] if job["id"] == "building_register_floor")["retries"] = 3
        jobs = copy.deepcopy(real_inputs()[0])
        retried(jobs)
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 1710 minutes")
                            for problem in job_specs.pool_starvation(jobs)))
        self.refused(retried)

    def test_the_bake_is_not_retried_and_takes_turns(self):
        # A retried bake would hold its slot for another 21 hours; a turn-taking one defers to
        # FLOOR and lineage between runs (start-scheduled-job.sh).
        spec = next(spec for spec in job_specs.load_specs() if spec.job_id == "by_pnu_serving_bake")
        self.assertEqual(spec.retries, 0)
        self.assertTrue(bake(real_inputs()[0])["takes_turns"])
        weights = {spec.job_id: spec.priority_weight for spec in job_specs.load_specs() if spec.pool == "spark"}
        self.assertGreater(min(weights["building_register_floor"], weights["lineage_stewardship"]),
                           max(weights["map_edit_fold_admin"], weights["map_edit_fold_complex"]))
        self.assertGreater(weights["map_edit_fold_admin"], weights["by_pnu_serving_bake"])

    def test_invalid_retries_weight_or_turn_taking(self):
        self.refused(lambda jobs: bake(jobs).update(retries=4))
        self.refused(lambda jobs: bake(jobs).update(priority_weight=0))
        self.refused(lambda jobs: bake(jobs).update(takes_turns="yes"))

    def test_a_job_taking_more_slots_than_its_pool_has(self):
        self.refused(lambda jobs: bake(jobs).update(pool_slots=4))
        self.refused(lambda jobs: bake(jobs).update(pool_slots=0))

    def test_the_real_pools_starve_nothing(self):
        self.assertEqual(job_specs.pool_starvation(real_inputs()[0]), [])

    def test_a_schedule_the_starvation_check_cannot_read(self):
        with self.assertRaises(job_specs.JobListError):
            job_specs.shortest_interval_minutes("*/5 * * * *")
        self.assertEqual(job_specs.shortest_interval_minutes("0,30 * * * *"), 30)
        self.assertEqual(job_specs.shortest_interval_minutes("15 10 * * *"), 1440)
        self.assertEqual(job_specs.shortest_interval_minutes("0 1,23 * * *"), 120)


    def test_a_service_the_release_does_not_ship(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="foundation-no-such-job.service"))

    def test_a_service_name_outside_the_platform(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(systemd_service="ssh.service"))

    def test_two_jobs_on_one_service(self):
        self.refused(lambda jobs: jobs["jobs"][1].update(systemd_service=jobs["jobs"][0]["systemd_service"]))

    def test_an_enabled_flag_that_is_not_a_boolean(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(enabled="yes"))

    def test_a_job_that_both_moves_data_and_reads_the_contracts(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(reads_data_contracts=True))

    def test_a_duplicate_job_id(self):
        self.refused(lambda jobs: jobs["jobs"].append(copy.deepcopy(jobs["jobs"][0])))

    def test_a_disabled_job_without_a_timer_that_does_not_say_why(self):
        self.refused(lambda jobs: bake(jobs).update(enabled=False, disabled_reason="  "))
        self.refused(lambda jobs: bake(jobs).update(enabled=False))

    def test_an_enabled_job_that_still_carries_a_disabled_reason(self):
        self.refused(lambda jobs: jobs["jobs"][0].update(disabled_reason="stale"))


def job(jobs, job_id):
    return next(entry for entry in jobs["jobs"] if entry["id"] == job_id)


LANES = [
    "silver_refresh_building_register_titles",
    "silver_refresh_building_register_units",
    "silver_refresh_building_register_unit_areas",
    "silver_refresh_building_register_apartment_price",
    "silver_refresh_building_register_exclusive_unit",
]
# The VWorld land lanes of root ADR-0169 step 3: the same shape, one source each.
LAND_LANES = [
    "silver_refresh_land_characteristic",
    "silver_refresh_land_forest_ledger",
    "silver_refresh_land_individual_price",
    "silver_refresh_land_right_registration",
    "silver_refresh_land_transfer_history",
    "silver_refresh_land_use_plan",
    "silver_refresh_land_use_zone_code",
]
ALL_LANES = LANES + LAND_LANES


class JobsStartedByTheirInputs(unittest.TestCase):
    """Jobs chained by what their runs changed, read from the edges they carry out (root ADR-0171)."""

    def refused(self, mutate, expected):
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        mutate(jobs)
        with self.assertRaises(job_specs.JobListError) as raised:
            job_specs.load_specs(jobs, graph)
        self.assertIn(expected, str(raised.exception))

    def test_the_chain_from_source_to_serving(self):
        specs = {spec.job_id: spec for spec in job_specs.load_specs()}
        for lane in ALL_LANES:
            self.assertEqual(specs[lane].started_by, "inputs")
            self.assertEqual(specs[lane].producers, ("source_sweep",))
        self.assertIn("building_register_floor", specs["gold_panel_rebuild"].producers)
        self.assertLessEqual({"silver_refresh_building_register_titles", "silver_refresh_building_register_units",
                              "silver_refresh_building_register_unit_areas"},
                             set(specs["gold_panel_rebuild"].producers))
        self.assertEqual(specs["by_pnu_serving_bake"].producers, ("gold_panel_rebuild",))
        # What the outside world or staff drive stays on the clock.
        for root in ["source_sweep", "outbox_publish", "map_edit_fold_admin", "map_edit_fold_complex",
                     "lineage_stewardship", "building_register_floor", "data_quality"]:
            self.assertEqual(specs[root].started_by, "schedule", root)

    def test_the_new_lanes_wait_for_their_supervised_first_run(self):
        jobs, _ = real_inputs()
        for lane in ALL_LANES:
            entry = job(jobs, lane)
            self.assertFalse(entry["enabled"], lane)
            self.assertIn("supervised", entry["disabled_reason"])
            instance = lane.removeprefix("silver_refresh_").replace("_", "-")
            self.assertEqual(entry["systemd_service"], f"foundation-silver-refresh@{instance}.service")
        for lane in LAND_LANES:
            # A land lane refuses a run while an object it could load is unmeasured (ADR-0169 §1).
            self.assertIn("bronze-object-members.sh", job(jobs, lane)["disabled_reason"])

    def test_started_by_is_stated_and_known(self):
        self.refused(lambda jobs: jobs["jobs"][0].pop("started_by"), "started_by must be one of")
        self.refused(lambda jobs: jobs["jobs"][0].update(started_by="upstream"), "started_by must be one of")

    def test_a_job_no_listed_job_feeds_is_refused(self):
        # Started by its inputs with nothing writing them, only its fallback would ever start it.
        self.refused(lambda jobs: job(jobs, "outbox_publish").update(started_by="inputs"),
                     "outbox_publish: started by its inputs, but no job in the list writes any of them")

    def test_an_enabled_job_whose_feeders_are_all_off_is_refused(self):
        def bake_without_gold(jobs):
            job(jobs, "gold_panel_rebuild").update(enabled=False, disabled_reason="planted")
        self.refused(bake_without_gold, "by_pnu_serving_bake: enabled and started by its inputs, but every job "
                                        "that writes them (gold_panel_rebuild) is switched off")

    def test_a_job_that_writes_its_own_input_is_refused(self):
        # The fold reads the edit ledger it writes: started by its inputs, it would start itself.
        self.refused(lambda jobs: job(jobs, "map_edit_fold_admin").update(started_by="inputs"),
                     "map_edit_fold_admin: started by its inputs, it writes one of them")

    def test_jobs_that_start_each_other_in_a_circle_are_refused(self):
        def circle(jobs):
            # FLOOR writes a Gold input; planted, Gold writes FLOOR's hub source. Both started by
            # their inputs, each changed run would start the other, forever.
            job(jobs, "building_register_floor")["started_by"] = "inputs"
            job(jobs, "gold_panel_rebuild")["pipeline_graph_edges"].append(
                "daily-source-sweep-to-source-building-hub-bulk")
        self.refused(circle, "jobs start each other in a circle: building_register_floor -> gold_panel_rebuild "
                             "-> building_register_floor")

    def test_a_job_started_by_its_inputs_cycles_as_often_as_what_feeds_it(self):
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        cycles = job_specs.cycle_minutes(jobs, graph)
        self.assertEqual(cycles["gold_panel_rebuild"], 1440)
        # A sweep every hour would start the lanes every hour, and through them Gold and the bake.
        job(jobs, "source_sweep")["schedule"] = "40 * * * *"
        cycles = job_specs.cycle_minutes(jobs, graph)
        for job_id in [*ALL_LANES, "gold_panel_rebuild", "by_pnu_serving_bake"]:
            self.assertEqual(cycles[job_id], 60, job_id)
        # ... and their waits, counted in those cycles, starve (the bound did not move): every lane
        # and the Gold rebuild may wait past 20 hourly runs.
        problems = job_specs.pool_starvation(jobs, graph)
        for job_id in [LANES[0], LAND_LANES[0], "gold_panel_rebuild"]:
            waits = [int(problem.split()[3]) for problem in problems if problem.startswith(f"{job_id} may wait ")]
            self.assertEqual(len(waits), 1, problems)
            self.assertGreater(waits[0], job_specs.STARVATION_CYCLES * 60)
        # Its own fallback schedule counts as well.
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        job(jobs, "by_pnu_serving_bake")["schedule"] = "15 * * * *"
        self.assertEqual(job_specs.cycle_minutes(jobs, graph)["by_pnu_serving_bake"], 60)


class TheSilverLanesInTheSparkPool(unittest.TestCase):
    """Five lanes of all three slots fit only because a lighter, as-wide job never jumps ahead."""

    def spark(self, jobs):
        return [entry for entry in jobs["jobs"] if entry["pool"] == "spark"]

    def wait(self, jobs, job_id):
        spark = self.spark(jobs)
        starved = job(jobs, job_id)
        return job_specs.longest_wait_minutes(starved, [entry for entry in spark if entry is not starved], 3)

    def test_the_fold_waits_for_one_lane_at_most_and_still_fits_its_bound(self):
        jobs = copy.deepcopy(real_inputs()[0])
        wait, blockers = self.wait(jobs, "map_edit_fold_admin")
        # FLOOR and lineage (heavier) and the other fold (as heavy) once each, then whatever holds
        # the pool when its turn comes: the Gold rebuild's 165 minutes, longer than a lane's 160.
        self.assertEqual(wait, (2 * 250 + 5) + (2 * 130 + 5) + (2 * 130 + 5) + 165)
        self.assertEqual(wait, job_specs.STARVATION_CYCLES * 60)
        self.assertFalse(set(ALL_LANES) & set(blockers))
        # Counted the way the bound counted before root ADR-0171, the five lanes would add up.
        self.assertEqual(wait + 5 * 160, 2000)

    def test_a_lane_that_holds_its_slots_longer_than_the_gold_rebuild_starves_the_folds(self):
        jobs = copy.deepcopy(real_inputs()[0])
        job(jobs, LANES[2])["timeout_minutes"] = 170
        problems = job_specs.pool_starvation(jobs)
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 1205 minutes")
                            and LANES[2] in problem for problem in problems), problems)
        jobs = copy.deepcopy(real_inputs()[0])
        job(jobs, LANES[0])["retries"] = 1  # a retry holds the slots again
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 1360 minutes")
                            for problem in job_specs.pool_starvation(jobs)))

    def test_lanes_weighted_above_the_folds_starve_them(self):
        # Heavier, every lane may start ahead of a waiting fold: five of them, then the Gold rebuild.
        jobs = copy.deepcopy(real_inputs()[0])
        for lane in LANES:
            job(jobs, lane)["priority_weight"] = 6
        wait, blockers = self.wait(jobs, "map_edit_fold_admin")
        self.assertEqual(wait, 1035 + 5 * 160 + 165)
        self.assertLessEqual(set(LANES), set(blockers))
        self.assertTrue(any(problem.startswith("map_edit_fold_admin may wait 2000 minutes")
                            for problem in job_specs.pool_starvation(jobs)))

    def test_a_lighter_narrower_job_still_counts_every_time(self):
        # The bake (1 slot) is lighter than FLOOR but fits where FLOOR (3) does not, so Airflow may
        # start it ahead: it stays among the jobs counted ahead.
        jobs = copy.deepcopy(real_inputs()[0])
        wait, blockers = self.wait(jobs, "building_register_floor")
        self.assertIn("by_pnu_serving_bake", blockers)
        self.assertEqual(wait, 265 + 2 * 265 + 1260 + 165)

    def test_the_real_list_fits(self):
        self.assertEqual(job_specs.pool_starvation(real_inputs()[0]), [])


class TheRunOutcome(unittest.TestCase):
    """A run's last `foundation-job-outcome` line decides whether its outputs start anything."""

    def test_changed_and_unchanged(self):
        self.assertEqual(job_specs.job_outcome(b"loaded\nfoundation-job-outcome changed\nresult=success\n"), "changed")
        self.assertEqual(job_specs.job_outcome(b"nothing to do\nfoundation-job-outcome unchanged\n"), "unchanged")
        self.assertEqual(job_specs.job_outcome("foundation-job-outcome unchanged\r\n"), "unchanged")

    def test_no_line_counts_as_changed(self):
        for output in [b"", None, b"silver-refresh-outcome lane=x outcome=unchanged\n",
                       b"foundation-job-outcome maybe\n", b"note: foundation-job-outcome unchanged\n",
                       b"foundation-job-outcome unchanged but more\n"]:
            with self.subTest(output=output):
                self.assertEqual(job_specs.job_outcome(output), "changed")

    def test_the_last_line_decides_and_only_the_tail_is_read(self):
        self.assertEqual(job_specs.job_outcome(
            b"foundation-job-outcome unchanged\nfoundation-job-outcome changed\n"), "changed")
        early = b"foundation-job-outcome unchanged\n" + b"x" * job_specs.OUTCOME_TAIL_BYTES
        self.assertEqual(job_specs.job_outcome(early), "changed")

    def test_an_asset_is_the_dataset_the_run_reports(self):
        for spec in job_specs.load_specs():
            for dataset in spec.inputs + spec.outputs:
                namespace, name = dataset
                self.assertEqual(job_specs.asset_uri(dataset), f"{namespace}://{name}")
        uris = {job_specs.asset_uri(dataset) for spec in job_specs.load_specs() for dataset in spec.inputs + spec.outputs}
        datasets = {dataset for spec in job_specs.load_specs() for dataset in spec.inputs + spec.outputs}
        self.assertEqual(len(uris), len(datasets), "two datasets share an asset")


class ThePoolsTheSchedulerHas(unittest.TestCase):
    """jobs.v1.json `pools` is the one list: the runtime creates them, compose leaves room for them."""

    def test_parallelism_leaves_a_slot_for_default_pool_jobs(self):
        compose = (job_specs.PLATFORM_ROOT / "compose.orchestration.yml").read_text(encoding="utf-8")
        [parallelism] = re.findall(r'AIRFLOW__CORE__PARALLELISM: "(\d+)"', compose)
        slots = sum(job_specs.declared_pools(real_inputs()[0]).values())
        # Every declared pool full at once, and outbox_publish (default_pool) still starts.
        self.assertGreaterEqual(int(parallelism), slots + 1)

    def test_the_small_spark_differs_from_spark_only_in_its_cap_and_name(self):
        # compose.lakehouse.yml cannot share a service through extends or anchors
        # (check-container-runtime-policy.sh), so the fold's Spark is written out in full. It must
        # stay the same Spark: only the memory cap the host budget counts and the name may differ.
        compose = (job_specs.PLATFORM_ROOT / "compose.lakehouse.yml").read_text(encoding="utf-8")
        def block(name):
            body = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^\S|^  \S)", compose).group(1)
            return [line for line in body.splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                    and not line.startswith(("    mem_limit:", "    container_name:"))]
        self.assertEqual(block("spark-small"), block("spark"))
        self.assertRegex(compose, r"(?m)^  spark-small:\n(?:    .*\n)*?    mem_limit: \d+[mg]$")

    def test_the_runtime_creates_the_declared_pools_and_no_others(self):
        runtime = (job_specs.PLATFORM_ROOT / "scripts/deploy/airflow-runtime.sh").read_text(encoding="utf-8")
        self.assertNotRegex(runtime, r"pools set [a-z]", "a pool named in the script is a second list")
        self.assertIn('json.load(open(sys.argv[1]))["pools"]', runtime)


class EveryBakedSurfaceHasAJob(unittest.TestCase):
    """A serving surface made by a bake is produced by a registered job, or exempt with a reason."""

    def test_the_real_lists_pass(self):
        self.assertEqual(job_specs.baked_surfaces_without_a_job(), [])

    def test_the_by_pnu_bakes_are_registered_jobs(self):
        jobs, graph = real_inputs()
        edges = {edge["id"]: edge["to"] for edge in graph["edges"]}
        produced = {edges[edge]: job["id"] for job in jobs["jobs"] for edge in job["pipeline_graph_edges"]}
        self.assertEqual(produced["parcel-by-pnu-serving"], "by_pnu_serving_bake")
        self.assertEqual(produced["building-by-pnu-serving"], "by_pnu_serving_bake")

    def problems(self, mutate_jobs=lambda jobs: None, mutate_graph=lambda graph: None):
        jobs, graph = (copy.deepcopy(value) for value in real_inputs())
        mutate_jobs(jobs)
        mutate_graph(graph)
        return job_specs.baked_surfaces_without_a_job(jobs, graph)

    def test_a_planted_baked_surface_without_a_job_is_refused(self):
        def plant(graph):
            graph["nodes"].append({"id": "planted-by-pnu-serving", "type": "serving_surface",
                                   "surface_kind": "r2_baked_documents"})
            graph["edges"].append({"id": "gold-parcel-panel-to-planted-by-pnu-serving",
                                   "from": "gold-parcel-panel", "to": "planted-by-pnu-serving"})
        self.assertEqual(self.problems(mutate_graph=plant), [
            "baked serving surface 'planted-by-pnu-serving' has no producing job in jobs.v1.json and no exemption"])

    def test_a_planted_tile_unit_without_a_job_is_refused(self):
        def plant(graph):
            graph["nodes"].append({"id": "planted-tiles", "type": "serving_surface", "surface_kind": "tiles"})
        self.assertEqual(len(self.problems(mutate_graph=plant)), 1)

    def test_removing_a_bake_job_exposes_its_surface(self):
        def drop(jobs):
            bake(jobs)["pipeline_graph_edges"].remove("gold-building-panel-to-building-by-pnu-serving")
        self.assertEqual(self.problems(mutate_jobs=drop), [
            "baked serving surface 'building-by-pnu-serving' has no producing job in jobs.v1.json and no exemption"])

    def test_a_stale_unknown_or_unreasoned_exemption_is_refused(self):
        for surface, reason, expected in [
            ("parcel-by-pnu-serving", "kept by hand", "is stale"),
            ("no-such-surface", "kept by hand", "is not a baked serving surface"),
            ("gongzzang-panel", "kept by hand", "is not a baked serving surface"),
        ]:
            with self.subTest(surface=surface):
                problems = self.problems(mutate_jobs=lambda jobs: jobs["serving_surfaces_without_a_job"].append(
                    {"surface": surface, "reason": reason}))
                self.assertEqual(len(problems), 1, problems)
                self.assertIn(expected, problems[0])
        problems = self.problems(mutate_jobs=lambda jobs: jobs["serving_surfaces_without_a_job"][0].update(reason=""))
        self.assertEqual(problems, ["exemption 'parcel-tiles' gives no reason"])


class FloorCycleAdapter(unittest.TestCase):
    # The host layout (root ADR-0134): the read-only source under releases/<sha>, its trusted
    # build output under artifacts/<sha>, and FLOOR's configuration under config/<sha>.
    RELEASE_ID = "e" * 40

    def release_wrapper(self, root, publisher_source, release_id=RELEASE_ID):
        release = root / "releases" / release_id
        ops = release / "scripts/ops"
        ops.mkdir(parents=True)
        for name in ["building-register-floor-cycle.sh", "runtime-database-url.py", "admitted-writer-runtime.sh"]:
            (ops / name).write_bytes((job_specs.PLATFORM_ROOT / "scripts/ops" / name).read_bytes())
        artifacts = root / "artifacts" / release_id
        artifacts.mkdir(parents=True)
        binary = artifacts / "foundation-outbox-publisher"
        binary.write_text(publisher_source)
        binary.chmod(0o555)
        (artifacts / "build.json").write_text(json.dumps({"source": release_id, "publisher_image": "sha256:" + "a" * 64, "tippecanoe_image": "sha256:" + "d" * 64, "files": {
            "foundation-outbox-publisher": hashlib.sha256(publisher_source.encode()).hexdigest(),
            "jars/fixture.jar": "0" * 64}}))
        return ops / "building-register-floor-cycle.sh", release

    @staticmethod
    def floor_config(release):
        config = release.parent.parent / "config" / release.name / "building-register-floor.env"
        config.parent.mkdir(parents=True, exist_ok=True)
        return config

    def test_run_and_cleanup_use_the_same_captured_configuration_after_current_switches(self):
        unit = (job_specs.SYSTEMD / "foundation-building-register-floor.service").read_text()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            releases = [root / "releases" / (name * 40) for name in ["a", "b"]]
            for release in releases:
                wrapper = release / "scripts/ops/building-register-floor-cycle.sh"
                wrapper.parent.mkdir(parents=True)
                wrapper.write_text('#!/bin/sh\nprintf "%s:%s" "' + release.name + '" "${1:-run}"\n')
                wrapper.chmod(0o755)
            current = root / "current"
            current.symlink_to(releases[0], target_is_directory=True)
            runtime = root / "runtime"
            runtime.mkdir()
            for release in releases:
                self.floor_config(release).write_text(
                    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=" + str(release) + "\n")
                # Nothing inside the release is read: admission refuses any file it does not hold.
                (release / ".foundation-floor.env").write_text(
                    "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=/not-the-config\n")
            invocation = "a" * 32
            environment = {"PATH": os.environ["PATH"], "RUNTIME_DIRECTORY": str(runtime),
                           "INVOCATION_ID": invocation}
            for phase, expected in [("ExecStart", releases[0].name + ":run"),
                                    ("ExecStopPost", releases[0].name + ":cleanup")]:
                command = re.search(r"^" + phase + r"=/bin/bash -ec '(.+)'$", unit, flags=re.MULTILINE)
                self.assertIsNotNone(command)
                script = command.group(1).replace("$$", "$").replace(
                    "/opt/foundation-platform/current", str(current))
                result = subprocess.run(["bash", "-ec", script], env=environment,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected)
                current.unlink()
                current.symlink_to(releases[1], target_is_directory=True)
            snapshot = runtime / (invocation + ".env")
            captured = snapshot.read_bytes()
            start = re.search(r"^ExecStart=/bin/bash -ec '(.+)'$", unit, re.MULTILINE).group(1)
            start = start.replace("$$", "$").replace("/opt/foundation-platform/current", str(current))
            collision = subprocess.run(["bash", "-ec", start], env=environment, capture_output=True)
            self.assertNotEqual(collision.returncode, 0)
            self.assertEqual(collision.stdout, b"")
            self.assertEqual(snapshot.read_bytes(), captured)
            snapshot.unlink()
            stop = re.search(r"^ExecStopPost=/bin/bash -ec '(.+)'$", unit, re.MULTILINE).group(1).replace("$$", "$")
            missing = subprocess.run(["bash", "-ec", stop], env=environment, capture_output=True)
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(missing.stdout, b"")
            snapshot.symlink_to(self.floor_config(releases[1]))
            linked = subprocess.run(["bash", "-ec", stop], env=environment, capture_output=True)
            self.assertNotEqual(linked.returncode, 0)
            self.assertEqual(linked.stdout, b"")
            snapshot.unlink()
            # A copied config must not redirect this invocation away from the captured current.
            self.floor_config(releases[1]).write_text(
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT=" + str(releases[0]) + "\n")
            redirected = subprocess.run(["bash", "-ec", start], env=environment, capture_output=True)
            self.assertNotEqual(redirected.returncode, 0)
            self.assertEqual(redirected.stdout, b"")
            self.assertFalse(snapshot.exists(), "failed start must not redirect ExecStopPost either")
            # A link in place of the configuration is not read either.
            config = self.floor_config(releases[1])
            config.unlink()
            config.symlink_to(self.floor_config(releases[0]))
            linked = subprocess.run(["bash", "-ec", start], env=environment, capture_output=True)
            self.assertNotEqual(linked.returncode, 0)
            self.assertEqual(linked.stdout, b"")
            self.assertFalse(snapshot.exists())

    def test_compose_connection_is_projected_without_copying_credentials(self):
        source_url = "postgres://reader:encoded%40password@postgres:5433/catalogue?sslmode=disable"
        config = {"services": {
            "foundation-api": {"environment": {"DATABASE_URL": source_url}},
            "postgres": {"ports": [{"host_ip": "127.0.0.1", "published": "15439", "target": 5433, "protocol": "tcp"}]},
        }}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            docker = root / "docker"
            docker.write_text("#!/bin/sh\ncat <<'JSON'\n" + json.dumps(config) + "\nJSON\n")
            docker.chmod(0o755)
            script, release = self.release_wrapper(root, '#!/bin/sh\nprintf "%s" "$DATABASE_URL"\n')
            env = {
                "PATH": str(root) + os.pathsep + os.environ["PATH"],
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": str(release),
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
                "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": "/fixture/state",
                "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": "/fixture/ivy",
            }
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, source_url.replace("postgres:5433", "127.0.0.1:15439"))
            self.assertEqual(result.stderr, "")
            # A failed Compose command must not be masked by a successful JSON consumer.
            docker.write_text(docker.read_text() + "exit 19\n")
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertNotIn("password", result.stderr)

    def test_publisher_is_the_release_artifact_for_run_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            _, release = self.release_wrapper(root, '#!/bin/sh\nprintf "%s" "$1"\n')
            (root / "current").symlink_to(release, target_is_directory=True)
            # The shared binary production ran before root ADR-0134, and the old override.
            stale = root / "bin/foundation-outbox-publisher"
            stale.parent.mkdir()
            stale.write_text('#!/bin/sh\nprintf stale\n')
            stale.chmod(0o755)
            env = {
                "PATH": os.environ["PATH"], "INVOCATION_ID": "a" * 32,
                "DATABASE_URL": "postgres://fixture/fixture",
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": str(release),
                "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
                "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
                "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": str(root / "state"),
                "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": str(root / "ivy"),
                "FOUNDATION_BUILDING_REGISTER_FLOOR_PUBLISHER_BIN": str(stale),
                "PUBLISHER_BIN": str(stale),
            }
            script = root / "current/scripts/ops/building-register-floor-cycle.sh"
            for args, command in [([], "run-building-register-floor-cycle"), (["cleanup"], "stop-building-register-floor-cycle")]:
                result = subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, command)
            # A substituted artifact is refused before it runs, for cleanup as well.
            binary = root / "artifacts" / release.name / "foundation-outbox-publisher"
            binary.chmod(0o755)
            binary.write_text('#!/bin/sh\nprintf substituted\n')
            binary.chmod(0o555)
            for args in [[], ["cleanup"]]:
                result = subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 65)
                self.assertEqual(result.stdout, "")
                self.assertIn("publisher sha256 differs", result.stderr)

    def test_cleanup_needs_only_invocation_and_rejects_unknown_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            script, _ = self.release_wrapper(pathlib.Path(directory), '#!/usr/bin/env bash\n[[ "$#" == 1 && "$1" == stop-building-register-floor-cycle ]] || exit 99\nprintf cleaned\n')
            env = {"PATH": os.environ["PATH"], "INVOCATION_ID": "a" * 32}
            result = subprocess.run(["bash", str(script), "cleanup"], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "cleaned")
            for args in [["unknown"], ["cleanup", "extra"], ["run", "extra"]]:
                result = subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 64)
                self.assertEqual(result.stdout, "")
            del env["INVOCATION_ID"]
            result = subprocess.run(["bash", str(script), "cleanup"], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")

    def test_required_environment_and_child_exit_are_preserved(self):
        required = {
            "DATABASE_URL": "postgres://fixture/fixture",
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_PATH": "/fixture/history.json",
            "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE": "sha256:" + "a" * 64,
            "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT": "/fixture/state",
            "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE": "/fixture/ivy",
        }
        with tempfile.TemporaryDirectory() as directory:
            script, release = self.release_wrapper(pathlib.Path(directory), '#!/usr/bin/env bash\n[[ "$#" == 1 && "$1" == run-building-register-floor-cycle ]] || exit 99\nprintf "%s" "$DATABASE_URL"\nexit 23\n')
            required["FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT"] = str(release)
            env = {"PATH": os.environ["PATH"], **required}
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 23)
            self.assertEqual(result.stdout, required["DATABASE_URL"])
            for name in required:
                with self.subTest(name=name):
                    missing = {key: value for key, value in env.items() if key != name}
                    result = subprocess.run(["bash", str(script)], env=missing, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
            redirected = subprocess.run(["bash", str(script)], env={**env,
                "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT": "/not-the-running-release"}, capture_output=True, text=True)
            self.assertNotEqual(redirected.returncode, 23)
            self.assertNotEqual(redirected.returncode, 0)
            self.assertEqual(redirected.stdout, "")


if __name__ == "__main__":
    unittest.main()
