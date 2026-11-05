"""The serving monitor's report becomes the series FoundationByPnuServingStale reads (root ADR-0306).

2026-10-28..11-05 the 공매 lane served a manifest 191 hours old against its 48-hour budget, and the
only signal was a failed monitor unit among some thirty open notices. The age and the budget are
now series, so the alert names the lane and the age.
"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("serving_freshness_metrics",
                                              PLATFORM / "scripts/ops/serving_freshness_metrics.py")
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)


def report(**overrides) -> dict:
    return {"served_published_at_utc": "2099-10-28T21:04:47Z", "served_age_hours": 191.0,
            "freshness_budget_hours": 48.0, "stale": True, **overrides}


class ServingFreshnessMetricsTest(unittest.TestCase):
    def run_writer(self, body: dict | None) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "report.json"
        path.write_text("" if body is None else json.dumps(body), encoding="utf-8")
        self.assertEqual(metrics.main([str(path), "parcel-public-auction", str(directory)]), 0)
        return directory / "by-pnu-serving-parcel-public-auction.prom"

    def test_a_stale_lane_writes_its_age_and_budget_in_seconds(self):
        written = self.run_writer(report()).read_text(encoding="utf-8").splitlines()
        self.assertIn('foundation_by_pnu_served_manifest_age_seconds{lane="parcel-public-auction"} 687600', written)
        self.assertIn('foundation_by_pnu_serving_freshness_budget_seconds{lane="parcel-public-auction"} 172800', written)
        self.assertTrue(any(line.startswith("foundation_by_pnu_serving_monitor_timestamp_seconds{") for line in written))

    def test_a_run_not_held_to_the_budget_or_without_a_report_writes_nothing(self):
        self.assertFalse(self.run_writer(report(freshness_budget_hours=None)).exists())
        self.assertFalse(self.run_writer(None).exists())


if __name__ == "__main__":
    unittest.main()
