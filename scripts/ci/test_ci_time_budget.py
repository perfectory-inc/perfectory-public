"""Unit tests for scripts/ci/ci_time_budget.py. The budget is read from the contract, never restated."""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_time_budget as tb  # noqa: E402

MONDAY = datetime(2030, 1, 7, tzinfo=timezone.utc)  # synthetic week, a Monday


def run(sha, name, start_min, end_min, conclusion="success", pr=1, base=MONDAY):
    """A synthetic run; `pr` only names its branch."""
    stamp = lambda m: (base + timedelta(minutes=m)).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {
        "name": name,
        "head_sha": sha,
        "run_started_at": stamp(start_min),
        "updated_at": stamp(end_min),
        "conclusion": conclusion,
        "head_branch": f"topic-{pr}",
    }


def week_of(minutes: list[float]) -> dict:
    runs = [run(f"sha{i:04d}", "W", 0, m, pr=i) for i, m in enumerate(minutes)]
    return tb.weekly_trend(tb.group_attempts(runs), [MONDAY])[0]


class ContractTest(unittest.TestCase):
    def test_contract_has_one_positive_budget(self):
        self.assertGreater(tb.load_budget(), 0)

    def test_malformed_budget_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.json"
            for bad in ({"schema_version": 1}, {"schema_version": 1, "pull_request_wall_minutes": 0},
                        {"schema_version": 1, "pull_request_wall_minutes": True},
                        {"schema_version": 2, "pull_request_wall_minutes": 5}):
                path.write_text(json.dumps(bad), encoding="utf-8")
                with self.assertRaises(ValueError):
                    tb.load_budget(path)


class GroupingTest(unittest.TestCase):
    def test_wall_is_first_start_to_last_finish_per_sha(self):
        attempts = tb.group_attempts([run("a", "X", 0, 5), run("a", "Y", 2, 12), run("b", "X", 1, 4)])
        by_sha = {a["head_sha"]: a for a in attempts}
        self.assertEqual(by_sha["a"]["minutes"], 12.0)
        self.assertEqual(by_sha["a"]["workflows"], {"X": 5.0, "Y": 10.0})
        self.assertEqual(by_sha["b"]["minutes"], 3.0)

    def test_superseded_sha_with_a_cancelled_run_is_dropped(self):
        attempts = tb.group_attempts([run("a", "X", 0, 5), run("a", "Y", 0, 1, conclusion="cancelled")])
        self.assertEqual(attempts, [])

    def test_rerun_keeps_longest_workflow_run(self):
        attempts = tb.group_attempts([run("a", "X", 0, 3), run("a", "X", 10, 18)])
        self.assertEqual(attempts[0]["workflows"]["X"], 8.0)
        self.assertEqual(attempts[0]["minutes"], 18.0)


class StatsTest(unittest.TestCase):
    def test_nearest_rank_percentile(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertEqual(tb.percentile(values, 0.5), 5)
        self.assertEqual(tb.percentile(values, 0.9), 9)
        self.assertEqual(tb.percentile([7], 0.9), 7)

    def test_weekly_rows_split_by_utc_monday_and_name_the_worst_workflow(self):
        runs = [
            run("a", "Fast", 0, 2), run("a", "Slow", 0, 20),
            run("b", "Fast", 0, 4, pr=2),
            run("c", "Fast", 0, 1, pr=3, base=MONDAY + timedelta(weeks=1)),
        ]
        rows = tb.weekly_trend(tb.group_attempts(runs), [MONDAY, MONDAY + timedelta(weeks=1)])
        self.assertEqual([r["count"] for r in rows], [2, 1])
        self.assertEqual(rows[0]["worst_workflow"], "Slow")
        self.assertEqual(rows[0]["max"], 20.0)
        self.assertEqual(rows[0]["attempts"][0]["head_sha"], "a")

    def test_attempt_outside_the_window_is_not_counted(self):
        rows = tb.weekly_trend(tb.group_attempts([run("a", "X", -60, 5)]), [MONDAY])
        self.assertEqual(rows[0]["count"], 0)


class VerdictTest(unittest.TestCase):
    """If the comparison breaks in either direction, one of these fails."""

    def setUp(self):
        self.budget = tb.load_budget()

    def test_p50_over_budget_fails_and_warns_each_slow_pr(self):
        over = self.budget + 1
        code, lines = tb.verdict(week_of([over, over, 1.0]), self.budget)
        self.assertEqual(code, 1)
        self.assertEqual(sum(line.startswith("::warning::") for line in lines), 2)

    def test_p50_within_budget_passes_but_still_warns(self):
        code, lines = tb.verdict(week_of([1.0, 1.0, self.budget + 1]), self.budget)
        self.assertEqual(code, 0)
        self.assertEqual(sum(line.startswith("::warning::") for line in lines), 1)

    def test_exactly_at_budget_passes(self):
        self.assertEqual(tb.verdict(week_of([self.budget] * 3), self.budget)[0], 0)

    def test_empty_week_cannot_pass(self):
        self.assertEqual(tb.verdict(week_of([]), self.budget)[0], 2)


class CommandLineTest(unittest.TestCase):
    """measure → judge end to end on fixture runs and a fixture contract."""

    def _run(self, budget: float, minutes: list[float]) -> int:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            contract = tmp / "contract.json"
            contract.write_text(json.dumps({"schema_version": 1, "pull_request_wall_minutes": budget}), encoding="utf-8")
            runs = tmp / "runs.json"
            runs.write_text(json.dumps([run(f"s{i}", "W", 0, m, pr=i) for i, m in enumerate(minutes)]), encoding="utf-8")
            trend, summary = tmp / "trend.json", tmp / "summary.md"
            now = (MONDAY + timedelta(weeks=1, hours=1)).isoformat()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(tb.main(["--contract", str(contract), "measure", "--weeks", "2", "--now", now,
                                          "--runs-json", str(runs), "--out", str(trend), "--markdown", str(summary)]), 0)
                self.assertIn("| 2030-01-07 | 3 |", summary.read_text(encoding="utf-8"))
                return tb.main(["--contract", str(contract), "judge", str(trend)])

    def test_raising_and_lowering_the_fixture_budget_flips_the_exit_code(self):
        self.assertEqual(self._run(30, [20, 20, 20]), 0)
        self.assertEqual(self._run(10, [20, 20, 20]), 1)


def job(wait_s, conclusion="success", steps=True, base=MONDAY):
    """A synthetic job that waited `wait_s` seconds for a runner."""
    stamp = lambda s: (base + timedelta(seconds=s)).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {"created_at": stamp(0), "started_at": stamp(wait_s), "conclusion": conclusion,
            "steps": [{"name": "x"}] if steps else []}


class RunnerWaitTest(unittest.TestCase):
    """Root ADR-0582: the weekly report carries the runner wait percentiles."""

    def test_only_jobs_that_took_a_runner_count(self):
        waits = tb.job_waits([job(30), job(5, conclusion="skipped"), job(9, conclusion="cancelled", steps=False),
                              job(12, conclusion="cancelled"), {"created_at": None, "started_at": None}])
        self.assertEqual(waits, [30.0, 12.0])

    def test_the_sample_spreads_over_the_whole_week(self):
        runs = [dict(run(f"s{i}", "W", i * 60, i * 60 + 1), id=i) for i in range(100)]
        picked = tb.sample_evenly(runs, 5)
        self.assertEqual([r["id"] for r in picked], [0, 25, 50, 74, 99])
        self.assertEqual(len(tb.sample_evenly(runs[:3], 5)), 3)

    def test_percentiles_per_week_and_event(self):
        samples = [
            {"event": "pull_request", "run_started_at": "2030-01-07T01:00:00Z", "waits": [1.0, 2.0, 3.0, 100.0]},
            {"event": "merge_group", "run_started_at": "2030-01-08T01:00:00Z", "waits": [7.0]},
            {"event": "push", "run_started_at": "2030-01-14T01:00:00Z", "waits": [9.0]},
        ]
        rows = {(r["week_start"], r["event"]): r for r in tb.queue_trend(samples, [MONDAY])}
        self.assertEqual(len(rows), len(tb.QUEUE_EVENTS))
        pr = rows[("2030-01-07", "pull_request")]
        self.assertEqual((pr["runs"], pr["jobs"], pr["p50"], pr["p90"], pr["max"]), (1, 4, 2.0, 100.0, 100.0))
        self.assertEqual(rows[("2030-01-07", "merge_group")]["p50"], 7.0)
        self.assertIsNone(rows[("2030-01-07", "push")]["p50"], "next week's run is not this week's")

    def test_queue_runs_are_not_pull_request_attempts(self):
        runs = [run("a", "X", 0, 5), dict(run("b", "X", 0, 50), event="merge_group")]
        self.assertEqual([a["head_sha"] for a in tb.group_attempts(runs)], ["a"])

    def test_measure_writes_the_runner_wait_table_and_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            runs = tmp / "runs.json"
            runs.write_text(json.dumps([
                dict(run("a", "W", 0, 5), id=1, jobs=[job(40), job(60)]),
                dict(run("b", "W", 0, 9), id=2, event="merge_group", jobs=[job(300)]),
            ]), encoding="utf-8")
            trend, summary = tmp / "trend.json", tmp / "summary.md"
            now = (MONDAY + timedelta(weeks=1, hours=1)).isoformat()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(tb.main(["measure", "--weeks", "1", "--now", now, "--runs-json", str(runs),
                                          "--out", str(trend), "--markdown", str(summary)]), 0)
            text = summary.read_text(encoding="utf-8")
            self.assertIn("| 2030-01-07 | pull_request | 1 | 2 | 40 | 60 | 60 | 60 |", text)
            self.assertIn("| 2030-01-07 | merge_group | 1 | 1 | 300 |", text)
            data = json.loads(trend.read_text(encoding="utf-8"))
            self.assertEqual(data["weeks"][0]["count"], 1)
            self.assertEqual({r["event"] for r in data["runner_wait"]}, set(tb.QUEUE_EVENTS))


if __name__ == "__main__":
    unittest.main()
