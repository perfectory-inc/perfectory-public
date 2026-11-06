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
        if cap.get("measured_run_seconds") is None:
            if job["enabled"] or not cap.get("unmeasured_reason"):
                found.append(f"{name}: no measured runs; only a job not enabled may, saying why (unmeasured_reason)")
            continue
        reasons, exception = fits(job, root), cap.get("open_exception")
        if reasons and not (exception and exception.get("why") and exception.get("tracked_by")):
            found.append(f"{name}: does not fit its unit and declares no open_exception: " + "; ".join(reasons))
        if exception and not reasons:
            found.append(f"{name}: fits its unit; its open_exception is stale")
    return found


def real_jobs():
    return json.loads((PLATFORM / "orchestration" / "jobs.v1.json").read_text(encoding="utf-8"))


class EveryJobFitsItsUnit(unittest.TestCase):
    def test_the_job_list_has_no_problems(self):
        self.assertEqual(problems(real_jobs()), [])

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
