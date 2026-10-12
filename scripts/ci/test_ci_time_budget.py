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


if __name__ == "__main__":
    unittest.main()
