"""scripts/ops/gold_input_readiness.py: a lane's cut-over waits, claiming nothing, until every input its
Gold's planner requires exists (root ADR-0471). Incident it prevents: 2026-11-09 parcel-building-permits
(16:58Z) and parcel-nearby (17:42Z) claimed the spark pool for a gold step the planner refused in under
75 s, 8 and 7 of their Silver tables not loaded yet, and parcel-nearby's 18:42Z claim held the pool for
more than 18 minutes ahead of the load that makes its tables."""

import os
import subprocess
import sys
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts/ops/gold_input_readiness.py"
sys.path.insert(0, str(PLATFORM / "scripts/ops"))
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
import gold_input_readiness as readiness  # noqa: E402
import gold_rebuild  # noqa: E402


class GoldInputReadiness(unittest.TestCase):
    def test_the_inputs_are_the_planners_required_and_unaccepted_optional_ones(self):
        nearby = readiness.required_inputs("gold.parcel_nearby")
        self.assertIn("silver.parcel_boundaries", nearby)
        self.assertIn("silver.airport", nearby)
        self.assertIn("reference.legal_dong_code_change", nearby)
        # An optional input the contract accepts absent is not waited for; one it does not accept is,
        # because the planner refuses without it (2026-11-09 parcel-court-auction, its photo table).
        entry = gold_rebuild.load_contract()["tables"]["gold.parcel_court_auction"]
        court = readiness.required_inputs("gold.parcel_court_auction")
        for name in entry.get("absent_inputs_accepted", {}):
            self.assertNotIn(name, court)
        self.assertIn("silver.court_auction_item", court)
        with self.assertRaises(ValueError):
            readiness.required_inputs("gold.no_such_table")

    def test_a_lane_waits_while_an_input_is_not_loaded(self):
        loaded = {"silver.parcel_boundaries"}
        ready, line = readiness.verdict("gold.x", ("silver.parcel_boundaries", "silver.airport"), loaded.__contains__)
        self.assertFalse(ready)
        self.assertEqual(line, "waiting gold.x: 1 of the 2 inputs its planner requires not loaded yet: silver.airport")
        loaded.add("silver.airport")
        ready, line = readiness.verdict("gold.x", ("silver.parcel_boundaries", "silver.airport"), loaded.__contains__)
        self.assertTrue(ready, line)
        # A producer that names its inputs only under Spark leaves the check to its planner.
        self.assertTrue(readiness.verdict("gold.x", None, None)[0])

    def test_no_catalog_is_cannot_tell(self):
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("FOUNDATION_PLATFORM_LAKEHOUSE_")}
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "gold.parcel_nearby"],
                                capture_output=True, text=True, env=environment, timeout=60)
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn("cannot tell", result.stderr)
        result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT)], capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 64)


if __name__ == "__main__":
    unittest.main()
