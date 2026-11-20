"""The switch check's rules (orchestration/dagster/verify_group.py, root ADR-0583 §6): which expected run a
run answered, the deadline (two ticks, at most 48 hours), the stop rule's count, and a serving that fell
behind the same hour a week before. Run by the CI `dagster-observe` lane in the code location's image.
"""

import datetime
import importlib.util
import pathlib
import sys
import unittest
from dataclasses import replace

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
HOUR = 3600.0


def at(text):
    return datetime.datetime.fromisoformat(text).replace(tzinfo=datetime.UTC).timestamp()


@unittest.skipUnless(HAS_DAGSTER, "requires dagster (the CI dagster-observe lane runs it)")
class TheSwitchCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import verify_group

        cls.verify = verify_group
        cls.spec = next(spec for spec in verify_group.job_specs.load_specs() if spec.job_id == "map_edit_fold_admin")

    def test_the_deadline_is_two_ticks_and_at_most_two_days(self):
        hourly = replace(self.spec, schedule="35 * * * *")
        self.assertEqual(self.verify.deadline(hourly, at("2026-11-20T10:35:00")), 2 * HOUR)
        weekly = replace(self.spec, schedule="0 3 * * 1")
        self.assertEqual(self.verify.deadline(weekly, at("2026-11-16T03:00:00")), 48 * HOUR)
        self.assertEqual(self.verify.deadline(replace(self.spec, schedule=None), at("2026-11-16T03:00:00")), 48 * HOUR)

    def test_a_run_open_at_the_moment_or_created_after_it_answers_it(self):
        tick = at("2026-11-20T10:35:00")
        self.assertTrue(self.verify.answered(tick, [(tick - HOUR, None)]), "still open")
        self.assertTrue(self.verify.answered(tick, [(tick - HOUR, tick + 60)]), "open then")
        self.assertTrue(self.verify.answered(tick, [(tick + HOUR, tick + 2 * HOUR)]), "created after")
        # 2026-11-20: a deploy held asset_reconcile 07:25-07:35; its next run folded those ticks (decide.py).
        self.assertTrue(self.verify.answered(tick, [(tick + 3 * HOUR, None)]), "late, but it folds the tick")
        self.assertFalse(self.verify.answered(tick, [(tick - 2 * HOUR, tick - HOUR)]), "ended before")

    def test_only_a_passed_deadline_counts_as_missed(self):
        ticks = [at("2026-11-20T08:35:00"), at("2026-11-20T09:35:00"), at("2026-11-20T10:35:00")]
        now = at("2026-11-20T11:40:00")
        found = self.verify.missed(ticks, [], now, lambda _: 2 * HOUR)
        self.assertEqual(found, ticks[:2], "two hours after 10:35 has not come")
        self.assertEqual(self.verify.missed(ticks, [(ticks[0] + 60, None)], now, lambda _: 2 * HOUR), [])

    def test_a_serving_behind_its_week_before_by_more_than_its_interval(self):
        rows = [{"name": "kept", "now": 3 * HOUR, "week_before": 2 * HOUR, "interval": 12 * HOUR},
                {"name": "fell", "now": 30 * HOUR, "week_before": 2 * HOUR, "interval": 12 * HOUR},
                {"name": "new", "now": 13 * HOUR, "week_before": None, "interval": 12 * HOUR},
                {"name": "unmeasured", "now": None, "week_before": 2 * HOUR, "interval": 12 * HOUR}]
        # 2026-11-20 14:2xZ: parcel-court-auction had no series a week before; it is not behind its week.
        self.assertEqual([row["name"] for row in self.verify.behind(rows)], ["fell"])


if __name__ == "__main__":
    unittest.main()
