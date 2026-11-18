"""Every job's run fits its unit, from measured run times (root ADR-0386, jobs.v1.json per_run_cap_means).

The incidents: the onbid collector ran into its 40-minute limit for four days and, committing at the
end, kept nothing (2026-10-22..26); the rt-molit Silver load was killed at 40 minutes every hour
(2026-10-30, #988). Both had a time limit and no per-run bound that was measured to fit it.
"""

import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
ROADMAP = PLATFORM.parents[1] / "docs" / "roadmap" / "production-readiness.md"
UNITS = ("run", "request", "download", "object")
HEADROOM = 0.9  # the share of the limit #988 left rt-molit's Silver load (1,800 + 206 + 120 of 2,400 s)
STOP_SECONDS = 120  # the unit's stop and the outcome line, after the item in flight


def resolve(pointer, root):
    """The value a `<file>#<JSON pointer>` names, the file under `root`."""
    file, _, path = pointer.partition("#")
    value = json.loads((root / file).read_text(encoding="utf-8"))
    for part in path.split("/")[1:]:
        value = value[part]
    return value


def section(pointer, root):
    """The object that holds the value a pointer names."""
    file, _, path = pointer.rpartition("/")[0].partition("#")
    return resolve(f"{file}#{path}", root)


def fits(job, root):
    """The reasons one job's measured runs do not fit its limit; empty when they do."""
    cap, limit = job["per_run_cap"], job["time_limit_minutes"] * 60
    measured, reasons = cap["measured_run_seconds"], []
    if measured["timeouts"]:
        reasons.append(f"{measured['timeouts']} measured runs timed out")
    if measured["max"] > HEADROOM * limit:
        reasons.append(f"its longest run, {measured['max']} s, is above {HEADROOM:.0%} of its {limit} s limit")
    if cap["deadline"]:
        deadline = resolve(cap["deadline"], root)
        if deadline > HEADROOM * limit:
            reasons.append(f"its deadline, {deadline} s, is above {HEADROOM:.0%} of its {limit} s limit")
        items = section(cap["deadline"], root)
        if "measured_object_seconds" in items:
            seconds = items["measured_object_seconds"]
            if items["run_start_seconds"] + items["max_objects"] * seconds["p90"] > deadline:
                reasons.append("its items at their measured p90 outlast its deadline")
            if deadline + seconds["max"] + STOP_SECONDS >= limit:
                reasons.append("the item in flight at its deadline outlasts its limit")
    return reasons


def first_run_problems(job):
    """Why an enabled job with no measured runs may not take its first run unattended (root ADR-0457)."""
    first, limit = job["per_run_cap"].get("first_run") or {}, job["time_limit_minutes"] * 60
    estimate = first.get("estimate_seconds")
    if not (isinstance(estimate, int) and not isinstance(estimate, bool) and estimate > 0 and first.get("estimated_from")):
        return ["no measured runs; an enabled one states first_run (estimate_seconds, estimated_from)"]
    reasons = []
    if estimate > HEADROOM * limit:
        reasons.append(f"its first run's estimate, {estimate} s, is above {HEADROOM:.0%} of its {limit} s limit")
    if job.get("commits") != "as_it_goes":
        reasons.append("a first run stopped at its limit would lose its work: only an as_it_goes job runs unmeasured")
    if job.get("retries"):
        reasons.append("an unmeasured run that times out must not be started again at once: retries 0")
    return reasons


def problems(jobs, root=PLATFORM):
    """Everything the job list's per_run_cap entries get wrong, one line each."""
    found = []
    for job in jobs["jobs"]:
        name, cap = job["id"], job.get("per_run_cap")
        if cap is None:
            found.append(f"{name}: no per_run_cap")
            continue
        if cap.get("unit") not in UNITS:
            found.append(f"{name}: unit {cap.get('unit')!r} is not one of {UNITS}")
        if (cap.get("unit") == "run") != (not cap.get("limits")):
            found.append(f"{name}: unit run states no limits, and any other unit states where its counts live")
        for pointer in cap.get("limits", []) + [cap["deadline"]] * bool(cap.get("deadline")):
            try:
                value = resolve(pointer, root)
            except (OSError, KeyError, ValueError):
                found.append(f"{name}: {pointer} names nothing")
                continue
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                found.append(f"{name}: {pointer} is {value!r}, not a positive count")
        if found and found[-1].startswith(f"{name}:"):
            continue
        first = cap.get("first_run")
        if cap.get("measured_run_seconds") is None:
            if job["enabled"]:
                found.extend(f"{name}: {why}" for why in first_run_problems(job))
            elif not cap.get("unmeasured_reason"):
                found.append(f"{name}: no measured runs; a job not enabled says why (unmeasured_reason)")
            continue
        if first:
            found.append(f"{name}: its runs are measured; its first_run is stale")
        reasons, exception = fits(job, root), cap.get("open_exception")
        if reasons and not (exception and exception.get("why") and exception.get("tracked_by")):
            found.append(f"{name}: does not fit its unit and declares no open_exception: " + "; ".join(reasons))
        if exception and not reasons:
            found.append(f"{name}: fits its unit; its open_exception is stale")
    return found


def real_jobs():
    return json.loads((PLATFORM / "orchestration" / "jobs.v1.json").read_text(encoding="utf-8"))


def real_graph():
    return json.loads((PLATFORM / "docs" / "catalog" / "pipeline-graph.v1.json").read_text(encoding="utf-8"))


# The readers of an enabled job held off on purpose, each with why; the list only shrinks
# (root ADR-0468, revised 2026-11-10).
# A held-off reader that is switched on is a stale entry: the change that turns it on deletes its line.
HELD_OFF = {
    "gold_parcel_regulation_zones_rebuild": "its first national build ran 9,610 s, past the 150-minute limit "
                                            "(2026-11-07, root ADR-0435 개정 ②)",
    "gold_court_auction_rebuild": "the court-auction lane's own work (W18h, root ADR-0353)",
    "gold_parcel_nearby_rebuild": "its first full build on Spark timed out at 160 minutes four times "
                                  "(2026-11-12..13) holding the one rebuild lock; it is born on DuckDB (W81b, W83b)",
}


def idle_readers(jobs, graph, held_off=HELD_OFF):
    """The jobs switched off that read what an enabled job makes (root ADR-0468 and its 2026-11-10 revision).

    A job is turned on so its data is seen: a reader left off keeps it from the screen however well the
    producer runs. 2026-11-09: the eight building-permit Silver lanes and data_go_kr_file_data were
    switched on to load unattended, and gold_parcel_building_permits_rebuild, gold_parcel_nearby_rebuild
    and their bakes stayed off (ADR-0468). 2026-11-10: six lanes the auto-promotion had published
    (ownership, sub-parcels, energy, demolitions, factories, register masters) kept their first publish
    while their Silver loaders ran, because their Gold and bake jobs were off (the 2026-11-10 revision).
    A reader turned on takes its own first run through the same gate (first_run_problems), so the rule
    reaches down the chain to the bake. A reader in `held_off` is the declared exception."""
    edges = {edge["id"]: edge for edge in graph["edges"]}
    by_id = {job["id"]: job for job in jobs["jobs"]}

    def reads(job):
        return {edges[edge]["from"] for edge in job["pipeline_graph_edges"] if edge in edges}

    def makes(job):
        return {edges[edge]["to"] for edge in job["pipeline_graph_edges"] if edge in edges}

    found = [f"{name}: held off, but it is on; delete its HELD_OFF line" for name in held_off
             if by_id.get(name, {}).get("enabled")]
    for job in jobs["jobs"]:
        if not job["enabled"]:
            continue
        first = (job.get("per_run_cap") or {}).get("first_run")
        for reader in jobs["jobs"]:
            if (reader is not job and not reader["enabled"] and reader["id"] not in held_off
                    and makes(job) & (reads(reader) - makes(reader))):
                found.append(f"{reader['id']}: off, but it reads what {job['id']} "
                             + ("takes its first run to make" if first else "makes"))
    return found


class EveryJobFitsItsUnit(unittest.TestCase):
    def test_the_job_list_has_no_problems(self):
        self.assertEqual(problems(real_jobs()), [])

    def test_no_reader_of_a_first_run_is_left_off(self):
        self.assertEqual(idle_readers(real_jobs(), real_graph()), [])

    def test_every_open_exception_is_on_the_roadmap(self):
        roadmap = ROADMAP.read_text(encoding="utf-8")
        for job in real_jobs()["jobs"]:
            if job["per_run_cap"].get("open_exception"):
                # An exception nobody reads is a timeout waiting to happen.
                self.assertTrue(f"`{job['id']}`" in roadmap, f"{job['id']}: open_exception not on the roadmap")


class TheGuardRefuses(unittest.TestCase):
    """Synthetic cases: each one a way the two incidents could come back."""

    def one(self, **cap):
        jobs = copy.deepcopy(real_jobs())
        job = next(job for job in jobs["jobs"] if job["id"] == "outbox_publish")
        job["per_run_cap"].update(cap)
        return problems({"jobs": [job]})

    def test_a_job_without_a_cap(self):
        jobs = copy.deepcopy(real_jobs())
        del jobs["jobs"][0]["per_run_cap"]
        self.assertIn(f"{jobs['jobs'][0]['id']}: no per_run_cap", problems(jobs))

    def test_runs_that_timed_out(self):
        measured = dict(real_jobs()["jobs"][2]["per_run_cap"]["measured_run_seconds"], timeouts=3)
        self.assertRegex(" ".join(self.one(measured_run_seconds=measured)), "3 measured runs timed out")

    def test_a_longest_run_too_close_to_the_limit(self):
        measured = dict(real_jobs()["jobs"][2]["per_run_cap"]["measured_run_seconds"], max=850)  # of 900 s
        self.assertRegex(" ".join(self.one(measured_run_seconds=measured)), "above 90% of its 900 s limit")

    def test_a_stale_exception(self):
        self.assertRegex(" ".join(self.one(open_exception={"why": "x", "tracked_by": "y"})), "stale")

    def test_an_enabled_job_never_measured(self):
        self.assertRegex(" ".join(self.one(measured_run_seconds=None, unmeasured_reason="new")), "no measured runs")

    def first_run(self, estimate, **job):
        jobs = copy.deepcopy(real_jobs())
        one = next(j for j in jobs["jobs"] if j["id"] == "outbox_publish")  # 15-minute limit, as_it_goes
        one.update({"commits": "as_it_goes", "retries": 0, **job})
        one["per_run_cap"].update(measured_run_seconds=None,
                                  first_run={"estimate_seconds": estimate, "estimated_from": "a neighbour's run"})
        return problems({"jobs": [one]})

    def test_an_enabled_first_run_bounded_and_estimated_inside_its_limit_passes(self):
        self.assertEqual(self.first_run(600), [])

    def test_a_first_run_estimated_past_its_limit(self):
        self.assertRegex(" ".join(self.first_run(850)), "estimate, 850 s, is above 90% of its 900 s limit")

    def test_a_first_run_that_commits_at_the_end(self):
        # The onbid collector (2026-10-22..26): a run stopped at its limit kept nothing.
        self.assertRegex(" ".join(self.first_run(600, commits="at_end")), "lose its work")

    def test_a_first_run_that_retries(self):
        self.assertRegex(" ".join(self.first_run(600, retries=2)), "retries 0")

    def test_a_first_run_left_after_its_runs_are_measured(self):
        self.assertRegex(" ".join(self.one(first_run={"estimate_seconds": 1, "estimated_from": "x"})), "stale")

    def test_a_reader_left_off_behind_a_first_run(self):
        # 2026-11-09: data_go_kr_file_data on to load unattended, gold_parcel_nearby_rebuild off.
        jobs = copy.deepcopy(real_jobs())
        gold = next(job for job in jobs["jobs"] if job["id"] == "gold_parcel_nearby_rebuild")
        gold.update(enabled=False)
        not_held = {name: why for name, why in HELD_OFF.items() if name != gold["id"]}
        self.assertIn("gold_parcel_nearby_rebuild: off, but it reads what data_go_kr_file_data takes its first run to make",
                      idle_readers(jobs, real_graph(), not_held))

    def test_a_reader_left_off_behind_a_measured_loader(self):
        # 2026-11-10: silver_refresh_land_ownership loads on schedule, gold_parcel_ownership_rebuild off, so
        # the published ownership lane kept its first publish (root ADR-0468, revised 2026-11-10).
        jobs = copy.deepcopy(real_jobs())
        gold = next(job for job in jobs["jobs"] if job["id"] == "gold_parcel_ownership_rebuild")
        gold.update(enabled=False)
        self.assertIn("gold_parcel_ownership_rebuild: off, but it reads what silver_refresh_land_ownership makes",
                      idle_readers(jobs, real_graph()))
        self.assertEqual(idle_readers(jobs, real_graph(), {**HELD_OFF, gold["id"]: "synthetic"}), [])

    def test_a_held_off_reader_switched_on_is_stale(self):
        jobs = copy.deepcopy(real_jobs())
        next(job for job in jobs["jobs"] if job["id"] == "gold_court_auction_rebuild").update(enabled=True)
        self.assertIn("gold_court_auction_rebuild: held off, but it is on; delete its HELD_OFF line",
                      idle_readers(jobs, real_graph()))

    def test_a_count_that_names_nothing(self):
        self.assertRegex(" ".join(self.one(unit="object", limits=["config/none.json#/max_objects"])), "names nothing")

    def test_an_item_cap_that_outlasts_its_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            contract = {"silver_load": {"max_objects": 60, "run_time_budget_seconds": 1800, "run_start_seconds": 180,
                                        "measured_object_seconds": {"p90": 165, "max": 206}}}
            (root / "c.json").write_text(json.dumps(contract), encoding="utf-8")
            job = {"id": "synthetic", "enabled": True, "time_limit_minutes": 40,
                   "per_run_cap": {"unit": "object", "limits": ["c.json#/silver_load/max_objects"],
                                   "deadline": "c.json#/silver_load/run_time_budget_seconds",
                                   "measured_run_seconds": {"runs": 1, "max": 300, "timeouts": 0}}}
            self.assertRegex(" ".join(problems({"jobs": [job]}, root)), "outlast its deadline")
            contract["silver_load"]["max_objects"] = 9  # what #988 measured to fit
            (root / "c.json").write_text(json.dumps(contract), encoding="utf-8")
            self.assertEqual(problems({"jobs": [job]}, root), [])


class TheMeasurement(unittest.TestCase):
    """scripts/ops/job-run-times.py reads systemd's journal into the measured block."""

    def setUp(self):
        spec = importlib.util.spec_from_file_location("job_run_times", PLATFORM / "scripts/ops/job-run-times.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)

    def event(self, unit, at, message_id="", message=""):
        return json.dumps({"UNIT": unit, "__REALTIME_TIMESTAMP": str(at * 1_000_000),
                           "MESSAGE_ID": message_id, "MESSAGE": message})

    def test_a_run_is_start_to_its_first_end_and_a_timeout_is_counted(self):
        unit, m = "foundation-synthetic.service", self.module
        lines = [self.event(unit, 100, m.STARTING), self.event(unit, 160, message="Deactivated successfully."),
                 self.event(unit, 165, m.FINISHED),
                 self.event(unit, 200, m.STARTING),
                 self.event(unit, 2600, message="foundation-synthetic.service: Failed with result 'timeout'."),
                 self.event("foundation-other.service", 300, m.STARTING), "not json"]
        runs = m.runs(lines, {unit})
        self.assertEqual(runs[unit], [(100, 60, False), (200, 2400, True)])
        block = m.measured(runs[unit], 0, "now")
        self.assertEqual((block["runs"], block["p50"], block["max"], block["timeouts"]), (2, 60, 2400, 1))
        self.assertEqual(m.measured(runs[unit], 150, "now")["runs"], 1)
        self.assertIsNone(m.measured(runs[unit], 300, "now"))


if __name__ == "__main__":
    unittest.main()
