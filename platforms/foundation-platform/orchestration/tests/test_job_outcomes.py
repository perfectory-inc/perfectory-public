"""Each job is held to the outcomes its definition expects, not to its unit's exit (root ADR-0480).

Five times in a fortnight something looked running and produced nothing: the Dagster code location
failed every load for 2.5 days while the deploy said "observing"; the court auction collector said
`changed` every night and not one item detail reached Bronze; the shape ledger check waited for a host
chain never provisioned; ten first Silver loads were refused their pool and never tried again; the
data_go_kr file data's first start was skipped and nothing loaded. No unit failed in any of them. These
tests hold every job's declaration (orchestration/jobs.v1.json expected_outcome), the outcome ledger
the runner appends to (scripts/deploy/job_units.py) and the verdicts (scripts/ops/job_outcomes.py),
and replay each incident against the real definitions to show the check would have fired.
"""

import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
JOBS = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))
ENDPOINTS = json.loads((PLATFORM / "docs/catalog/public-source-endpoint-catalog.v1.json").read_text(encoding="utf-8"))
RULES = (PLATFORM / "infra/observability/prometheus/foundation-api.rules.yml").read_text(encoding="utf-8")


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


outcomes = load(PLATFORM / "scripts/ops/job_outcomes.py", "job_outcomes")
job_units = load(PLATFORM / "scripts/deploy/job_units.py", "job_units")
BY_ID = {job["id"]: job for job in JOBS["jobs"]}


def at(stamp):
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def record(job, stamp, outcome, reason=""):
    return {"job": job, "at": int(at(stamp)), "outcome": outcome, "via": "run", "reason": reason}


def enabled(job_id, since):
    """The real definition of `job_id`, switched on at `since` (a replay of a day it was on)."""
    return {**BY_ID[job_id], "enabled": True, "enabled_since": since}


def missed(jobs, records, now, ledger_from):
    summary = outcomes.fold({"first_at": int(at(ledger_from))}, records)
    return {(found["job"], found["expectation"]) for found in outcomes.overdue(jobs, summary, at(now))}


class EveryJobDeclaresItsOutcome(unittest.TestCase):
    def test_every_job_says_how_soon_it_must_change_or_why_it_need_not(self):
        for job in JOBS["jobs"]:
            with self.subTest(job=job["id"]):
                declared = job.get("expected_outcome")
                self.assertIsInstance(declared, dict, "jobs.v1.json expected_outcome is missing (root ADR-0480)")
                self.assertLessEqual(set(declared), {"changes_within_hours", "basis", "no_change_bound_reason", "done_within_hours"})
                hours = declared.get("changes_within_hours", "absent")
                if hours is None:
                    self.assertTrue(declared.get("no_change_bound_reason", "").strip())
                    self.assertNotIn("basis", declared)
                else:
                    self.assertIsInstance(hours, int)
                    self.assertGreater(hours, 0)
                    self.assertTrue(declared.get("basis", "").strip())
                    self.assertNotIn("no_change_bound_reason", declared)

    def test_a_change_bound_is_longer_than_the_jobs_own_turn(self):
        # A bound shorter than one schedule period and the run's limits would report every healthy run.
        for job in JOBS["jobs"]:
            hours = job["expected_outcome"]["changes_within_hours"]
            if hours:
                with self.subTest(job=job["id"]):
                    period = outcomes.cron_period_seconds(job["schedule"])
                    self.assertGreaterEqual(hours * 3600, period + 60 * job["time_limit_minutes"])

    def test_every_job_has_a_done_bound(self):
        for job in JOBS["jobs"]:
            with self.subTest(job=job["id"]):
                self.assertGreater(outcomes.done_budget_seconds(job), 3600)

    def test_a_silver_lane_owes_no_change_sooner_than_its_source_publishes(self):
        # The provider's cadence is declared once, in the endpoint catalog (root ADR-0443); a lane of one
        # endpoint may not owe a change sooner, and one of an irregular endpoint owes none.
        cadence = {}
        for endpoint in ENDPOINTS["endpoints"]:
            slug = (endpoint.get("bronze") or {}).get("source_slug", "")
            if endpoint.get("expected_cadence") and "__" in slug:
                cadence.setdefault(slug.split("__", 1)[1], []).append(endpoint["expected_cadence"])
        checked = 0
        for job in JOBS["jobs"]:
            name = job["id"].removeprefix("silver_refresh_")
            if name == job["id"] or len(cadence.get(name, [])) != 1:
                continue
            checked += 1
            bound, declared = cadence[name][0]["max_age_hours"], job["expected_outcome"]["changes_within_hours"]
            with self.subTest(job=job["id"]):
                if bound is None:
                    self.assertIsNone(declared)
                elif declared is not None:
                    self.assertGreaterEqual(declared, bound)
        self.assertGreater(checked, 3)

    def test_the_ledger_and_the_alerts_name_one_metric_file(self):
        self.assertIn(outcomes.METRICS_NAME, RULES)
        for name in ("foundation_job_outcome_met_seconds", "foundation_job_outcome_expected_seconds",
                     "foundation_job_outcome_info", "foundation_job_outcomes_last_recorded_seconds",
                     "foundation_job_outcomes_recorded"):
            self.assertIn(name, RULES)
            self.assertIn(name, outcomes.HELP)


class TheLedger(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="job-outcomes-")
        self.addCleanup(self.temp.cleanup)
        os.environ["FOUNDATION_START_QUEUE_DIR"] = self.temp.name
        self.addCleanup(os.environ.pop, "FOUNDATION_START_QUEUE_DIR")

    def test_each_outcome_is_appended_and_read_once(self):
        job_units.record_outcome("fold", "foundation-job-outcome changed", 100, "run", "abc")
        job_units.record_outcome("fold", "foundation-job-outcome waiting the spark pool is full", 200)
        job_units.record_outcome("fold", "foundation-job-check unchanged", 300, "check")
        job_units.record_outcome("fold", "foundation-job-check would_change", 400, "check")
        records, offset = job_units.outcomes_after(0)
        self.assertEqual([(r["at"], r["outcome"]) for r in records],
                         [(100, "changed"), (200, "waiting"), (300, "unchanged"), (400, "checked")])
        self.assertEqual(records[0]["invocation"], "abc")
        self.assertEqual(records[1]["reason"], "the spark pool is full")
        self.assertEqual(job_units.outcomes_after(offset), ([], offset))
        job_units.record_outcome("fold", "foundation-job-outcome unchanged", 500)
        self.assertEqual([r["at"] for r in job_units.outcomes_after(offset)[0]], [500])

    def test_a_line_still_being_written_waits_for_the_next_read(self):
        job_units.record_outcome("fold", "foundation-job-outcome changed", 100)
        with open(job_units.outcomes_path(), "a", encoding="utf-8") as handle:
            handle.write('{"job": "fold", "at": 2')
        records, offset = job_units.outcomes_after(0)
        self.assertEqual(len(records), 1)
        with open(job_units.outcomes_path(), "a", encoding="utf-8") as handle:
            handle.write('00, "outcome": "unchanged"}\n')
        self.assertEqual([r["at"] for r in job_units.outcomes_after(offset)[0]], [200])

    def test_a_failed_run_carries_its_code_version_and_fold_keeps_the_newest_ended_run(self):
        # Root ADR-0499: the reconciler compares a failed run's code version with the running release's.
        version = lambda job_id: f"v-{job_id}"  # noqa: E731
        job_units.record_outcome("fold", "foundation-job-outcome changed", 100, code_version=version)
        job_units.record_outcome("fold", "foundation-job-outcome failed systemd result=exit-code", 200, code_version=version)
        job_units.record_outcome("fold", "foundation-job-outcome waiting the spark pool is full", 300, code_version=version)
        job_units.record_outcome("fold", "foundation-job-check would_change", 400, "check", code_version=version)
        records, _ = job_units.outcomes_after(0)
        self.assertEqual([r.get("code_version") for r in records], [None, "v-fold", None, None])
        summary = outcomes.fold({}, records)
        self.assertEqual(summary["ran"], {"fold": {"outcome": "failed", "at": 200, "code_version": "v-fold"}})
        self.assertEqual(summary["jobs"]["fold"], {"changed": 100, "failed": 200, "waiting": 300, "checked": 400})
        summary = outcomes.fold(summary, [{"job": "fold", "at": 500, "outcome": "unchanged", "via": "run"}])
        self.assertEqual(summary["ran"]["fold"], {"outcome": "unchanged", "at": 500})

    def test_a_failed_line_is_kept_though_no_version_can_be_made(self):
        def broken(job_id):
            return None

        job_units.record_outcome("fold", "foundation-job-outcome failed", 100, code_version=broken)
        self.assertNotIn("code_version", job_units.outcomes_after(0)[0][0])

    def test_what_is_not_an_outcome_is_refused(self):
        for line in ("loaded 3 rows", "foundation-job-outcome finished", ""):
            with self.subTest(line=line), self.assertRaises(ValueError):
                job_units.record_outcome("fold", line, 1)
        with self.assertRaises(KeyError):
            job_units.record_outcome("Fold;rm", "foundation-job-outcome changed", 1)


class TheVerdicts(unittest.TestCase):
    def test_a_healthy_hourly_collector_misses_nothing(self):
        job = enabled("onbid_public_auction_collect", "2026-10-01T00:00:00Z")
        records = [record(job["id"], f"2026-11-10T{hour:02d}:07:30Z", "changed") for hour in range(24)]
        self.assertEqual(missed([job], records, "2026-11-11T00:30:00Z", "2026-11-01T00:00:00Z"), set())

    def test_an_hourly_collector_that_writes_nothing_misses_its_change(self):
        job = enabled("onbid_public_auction_collect", "2026-10-01T00:00:00Z")
        records = [record(job["id"], "2026-11-10T00:07:30Z", "changed")] + [
            record(job["id"], f"2026-11-10T{hour:02d}:07:30Z", "unchanged") for hour in range(1, 8)]
        self.assertEqual(missed([job], records, "2026-11-10T07:30:00Z", "2026-11-01T00:00:00Z"),
                         {("onbid_public_auction_collect", "change")})

    def test_a_check_that_found_nothing_to_do_is_done(self):
        job = enabled("silver_refresh_land_use_plan", "2026-10-01T00:00:00Z")
        records = [record(job["id"], "2026-11-10T07:00:30Z", "unchanged")]
        self.assertEqual(missed([job], records, "2026-11-11T07:30:00Z", "2026-11-01T00:00:00Z"), set())

    def test_a_reconciled_job_whose_outputs_stay_current_is_done_though_nothing_ran(self):
        # 2026-11-09..10 (the W56 replay): the by-PNU bake's clock starts were deferred by turn-taking and
        # the kind Gold rebuild waited for the panel rebuild, while the asset reconciler found every output
        # of both current (gold.parcel_panel's two commits only recorded new pins, no row changed).
        bake = enabled("by_pnu_serving_bake", "2026-10-01T00:00:00Z")
        kinds = enabled("gold_kind_rebuild", "2026-10-01T00:00:00Z")
        records = [record(bake["id"], "2026-11-08T20:26:18Z", "changed"),
                   record(bake["id"], "2026-11-09T10:46:13Z", "skipped", "deferred: gold_kind_rebuild, ..."),
                   record(bake["id"], "2026-11-10T10:15:08Z", "skipped", "deferred: gold_unit_price_rebuild"),
                   record(kinds["id"], "2026-11-09T10:52:06Z", "unchanged"),
                   record(kinds["id"], "2026-11-10T09:15:04Z", "waiting", "the Gold panel rebuild is held by another run")]
        now = at("2026-11-11T12:00:00Z")
        summary = outcomes.fold({"first_at": int(at("2026-11-01T00:00:00Z"))}, records)
        self.assertEqual({(found["job"], found["expectation"]) for found in outcomes.overdue([bake, kinds], summary, now)},
                         {("by_pnu_serving_bake", "done"), ("gold_kind_rebuild", "done")})
        summary = outcomes.fold_current(summary, {"by_pnu_serving_bake", "gold_kind_rebuild"}, now - 300)
        self.assertEqual(outcomes.overdue([bake, kinds], summary, now), [])
        self.assertEqual(summary["lines"], len(records), "the reconciler's verdict is not a ledger line")
        # The newest run's outcome stays what the readiness report shows.
        self.assertEqual(outcomes.expectations(bake, summary)[0]["last"], "skipped")

    def test_a_job_switched_off_is_not_judged(self):
        job = {**BY_ID["data_go_kr_file_data"], "enabled": False}
        self.assertEqual(missed([job], [], "2026-12-31T00:00:00Z", "2026-11-01T00:00:00Z"), set())

    def test_no_job_is_judged_for_the_time_before_the_ledger(self):
        # The ledger begins on its first deploy: a monthly lane is not reported for the month before.
        job = enabled("silver_refresh_land_use_plan", "2026-01-01T00:00:00Z")
        self.assertEqual(missed([job], [], "2026-11-11T12:00:00Z", "2026-11-11T07:00:00Z"), set())

    def test_the_textfile_names_the_owner_and_runbook_of_the_unit(self):
        job = enabled("shape_ledger_check", "2026-10-16T00:00:00Z")
        unit = "[Unit]\nDocumentation=file:///opt/foundation-platform/current/docs/runbooks/x.md\nX-Foundation-Owner=platform\n"
        text = outcomes.textfile([job], outcomes.fold({}, [record(job["id"], "2026-11-06T12:30:00Z", "waiting")]), lambda service: unit)
        self.assertIn('foundation_job_outcome_info{job_id="shape_ledger_check",unit="foundation-shape-ledger-check.service",'
                      'owner="platform",runbook="docs/runbooks/x.md"} 1', text)
        self.assertIn('foundation_job_outcome_expected_seconds{job_id="shape_ledger_check",expectation="done"} '
                      f"{outcomes.done_budget_seconds(job)}", text)
        self.assertIn(f"foundation_job_outcomes_last_recorded_seconds {int(at('2026-11-06T12:30:00Z'))}", text)


class TheFiveIncidentsWouldHaveFired(unittest.TestCase):
    """Each incident replayed against today's definitions (root ADR-0001: a guard names its incident)."""

    def test_1_the_dagster_code_location_dead_for_three_days(self):
        # 2026-11-06 21:16Z..11-09: every load failed, no dual-run check compared, the deploy said
        # "observing". Dagster decides beside Airflow and runs no job yet, so its outcome is the
        # comparison it appends; FoundationDualRunSilent holds it (foundation-dagster.rules.test.yml), and
        # once it runs jobs their outcomes reach this ledger like Airflow's. The scheduler that runs the
        # jobs is held by FoundationJobOutcomesSilent: no outcome of any job for an hour.
        for alert in ("FoundationDualRunSilent", "FoundationJobOutcomesSilent", "FoundationDagsterCodeLocationDown"):
            self.assertIn(f"alert: {alert}", RULES)
        tests = (PLATFORM / "infra/observability/prometheus/foundation-job-outcomes.rules.test.yml").read_text(encoding="utf-8")
        self.assertIn("alertname: FoundationDualRunSilent", tests)
        self.assertIn("alertname: FoundationJobOutcomesSilent", tests)

    def test_2_the_court_collector_that_fetched_no_item_detail(self):
        # Every night court_auction_collect said `changed` (its lists) and no item detail reached Bronze:
        # catalog.source_catalog had no row for the detail source (measured 2026-11-11). Its endpoint
        # declares a daily cadence, so foundation-api renders a bound for it, and a bound with no finished
        # run is FoundationPlatformIngestionSourceNeverCollected (foundation-ingestion.rules.test.yml).
        detail = [e for e in ENDPOINTS["endpoints"] if (e.get("bronze") or {}).get("source_slug") == "courtauctiongokr__auction_item_detail"]
        self.assertEqual(len(detail), 1)
        self.assertIsNotNone(detail[0]["expected_cadence"]["max_age_hours"])
        self.assertIn("alert: FoundationPlatformIngestionSourceNeverCollected", RULES)
        tests = (PLATFORM / "infra/observability/prometheus/foundation-ingestion.rules.test.yml").read_text(encoding="utf-8")
        self.assertIn("courtauctiongokr__auction_item_detail", tests)

    def test_3_the_shape_ledger_check_waiting_for_a_chain_never_provisioned(self):
        # From 2026-11-06 the check said `waiting` each day (the host chain that writes the ledger did not
        # exist) and failed only once its 48 hours were up; no unit failed while it waited.
        job = enabled("shape_ledger_check", "2026-10-16T00:00:00Z")
        records = [record(job["id"], "2026-11-05T12:31:00Z", "unchanged"),
                   record(job["id"], "2026-11-06T12:31:00Z", "waiting", "shape ledgers stale for 38 tables"),
                   record(job["id"], "2026-11-07T12:31:00Z", "waiting", "shape ledgers stale for 38 tables")]
        self.assertEqual(missed([job], records, "2026-11-06T14:00:00Z", "2026-11-01T00:00:00Z"), set())
        self.assertEqual(missed([job], records, "2026-11-06T15:00:00Z", "2026-11-01T00:00:00Z"),
                         {("shape_ledger_check", "done")})

    def test_4_ten_first_silver_loads_refused_their_pool_and_never_tried_again(self):
        # 2026-11-09 07:00Z: ten new Silver loads were refused behind a cut-over's claim and said
        # `waiting`; none was started again (root ADR-0462). A lane whose check found nothing to do is done.
        refused = ["silver_refresh_building_maintenance_regular_inspection_history", "silver_refresh_building_permit_annex_parking",
                   "silver_refresh_building_permit_building_outline", "silver_refresh_building_permit_floor_overview",
                   "silver_refresh_building_permit_major_repair", "silver_refresh_building_permit_site_location",
                   "silver_refresh_building_permit_structure_ledger", "silver_refresh_building_permit_temporary_building",
                   "silver_refresh_building_permit_demolition_loss", "silver_refresh_building_permit_parking"]
        jobs = [enabled(job_id, "2026-11-09T06:00:00Z") for job_id in refused] + [enabled("silver_refresh_land_use_plan", "2026-10-10T00:00:00Z")]
        records = [record(job_id, "2026-11-09T07:00:27Z", "waiting", "the spark pool is kept for claims queued before: by_pnu_lane_cutover")
                   for job_id in refused] + [record("silver_refresh_land_use_plan", f"2026-11-{day}T07:00:30Z", "unchanged")
                                             for day in ("09", "10")]
        self.assertEqual(missed(jobs, records, "2026-11-10T08:00:00Z", "2026-11-01T00:00:00Z"), set())
        self.assertEqual(missed(jobs, records, "2026-11-10T11:00:00Z", "2026-11-01T00:00:00Z"),
                         {(job_id, "done") for job_id in refused})

    def test_5_the_data_go_kr_file_data_that_never_loaded(self):
        # Switched on 2026-11-09 03:00Z; its first start (11:40Z) was skipped by the pool, and no run of
        # it ever ended `changed` or `unchanged`.
        job = enabled("data_go_kr_file_data", "2026-11-09T03:00:00Z")
        records = [record(job["id"], "2026-11-09T11:40:01Z", "waiting", "the spark pool is kept for claims")]
        self.assertEqual(missed([job], records, "2026-11-10T05:00:00Z", "2026-11-01T00:00:00Z"), set())
        self.assertEqual(missed([job], records, "2026-11-10T06:00:00Z", "2026-11-01T00:00:00Z"),
                         {("data_go_kr_file_data", "done")})
        # Had it run every day and written nothing, its change bound (a month and a day) reports it.
        daily = [record(job["id"], f"2026-{month}-{day:02d}T11:41:00Z", "unchanged")
                 for month, days in (("11", range(10, 31)), ("12", range(1, 26))) for day in days]
        self.assertEqual(missed([job], daily, "2026-12-15T12:00:00Z", "2026-11-01T00:00:00Z"), set())
        self.assertEqual(missed([job], daily, "2026-12-25T12:00:00Z", "2026-11-01T00:00:00Z"),
                         {("data_go_kr_file_data", "change")})


if __name__ == "__main__":
    unittest.main()
