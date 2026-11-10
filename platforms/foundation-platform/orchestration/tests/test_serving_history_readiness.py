"""scripts/ops/serving_history_readiness.py: a lane's first publish waits for the recent 실거래 history of
the datasets its document computes windows from (root ADR-0479, superseding ADR-0371 §5). Incident it
prevents: on 2026-11-03 only 3 of 250 months of 토지 were loaded; cut over then, every 법정동's 12-month
토지 count and m² median would have been read from a quarter of its months, with nothing on the screen
to say so. A dataset not whole beyond that window says from when it is held instead."""

import json
import sys
import unittest
from datetime import date
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/ops"))
import serving_history_readiness as readiness  # noqa: E402

sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/jobs"))
import real_transaction_silver_to_gold as gold  # noqa: E402

DATASETS = {"real_transaction_industrial_trade": {"first_month": "2098-01"},
            "real_transaction_land_trade": {"first_month": "2098-02"}}
LANES = {"parcel-x": {"first_publish_waits_for_history": ["real_transaction_industrial_trade",
                                                          "real_transaction_land_trade"]},
         "parcel-y": {}}


def key(dataset, month):
    return f"bronze/source=rtmolitgokr__{dataset}/period={month}/scope=nationwide/export--sha256-{'a' * 64}.csv"


class HistoryReadiness(unittest.TestCase):
    def test_months_run_across_a_year_and_stop_at_the_last_whole_month(self):
        self.assertEqual(readiness.months("2098-11", "2099-02"), ["2098-11", "2098-12", "2099-01", "2099-02"])
        self.assertEqual(readiness.last_whole_month(date(2099, 1, 5)), "2098-12")
        self.assertEqual(readiness.last_whole_month(date(2099, 3, 1)), "2099-02")

    def test_loaded_months_are_what_the_commits_and_the_durable_properties_record(self):
        history = {"snapshots": [{"summary": {readiness.RECORD_KEY: key("real_transaction_industrial_trade", "2098-01")
                                              + "," + key("real_transaction_land_trade", "2098-02")}},
                                 {"summary": {}}],
                   # An expired snapshot's record survives as a table property (root ADR-0332).
                   "properties": {readiness.RECORD_KEY + ".7": key("real_transaction_industrial_trade", "2098-02"),
                                  "write.format.default": "parquet"}}
        report = readiness.completeness(readiness.recorded_objects(history), DATASETS, "2098-03", 36)
        self.assertEqual(report["real_transaction_industrial_trade"]["loaded"], 2)
        self.assertEqual(report["real_transaction_industrial_trade"]["expected"], 3)
        self.assertEqual(report["real_transaction_industrial_trade"]["missing_first"], ["2098-03"])
        self.assertEqual((report["real_transaction_land_trade"]["loaded"], report["real_transaction_land_trade"]["expected"]),
                         (1, 2))

    def test_a_lane_waits_until_the_recent_history_of_every_named_dataset_is_loaded(self):
        objects = {key("real_transaction_industrial_trade", m) for m in ("2098-01", "2098-02")}
        ready, line = readiness.verdict("parcel-x", readiness.completeness(objects, DATASETS, "2098-02", 36), LANES)
        self.assertFalse(ready)
        self.assertIn("real_transaction_land_trade 0/1 (missing 2098-02)", line)
        objects.add(key("real_transaction_land_trade", "2098-02"))
        ready, line = readiness.verdict("parcel-x", readiness.completeness(objects, DATASETS, "2098-02", 36), LANES)
        self.assertTrue(ready, line)

    def test_the_older_history_is_not_waited_for(self):
        # Root ADR-0479: 2098-03 to 2098-05 are the recent 3 months; 2098-01 and 02 are older and missing.
        objects = {key(d, m) for d in DATASETS for m in ("2098-03", "2098-04", "2098-05")}
        report = readiness.completeness(objects, DATASETS, "2098-05", 3)
        ready, line = readiness.verdict("parcel-x", report, LANES)
        self.assertTrue(ready, line)
        self.assertEqual((report["real_transaction_industrial_trade"]["loaded"],
                          report["real_transaction_industrial_trade"]["expected"]), (3, 5))
        objects.discard(key("real_transaction_land_trade", "2098-04"))
        ready, line = readiness.verdict("parcel-x", readiness.completeness(objects, DATASETS, "2098-05", 3), LANES)
        self.assertFalse(ready)
        self.assertIn("real_transaction_land_trade 2/3 (missing 2098-04)", line)
        # A lane that names no history is ready; a dataset the contract does not know is refused.
        self.assertTrue(readiness.verdict("parcel-y", {}, LANES)[0])
        with self.assertRaises(ValueError):
            readiness.verdict("parcel-x", {}, LANES)

    def test_the_real_lane_waits_for_the_datasets_and_months_its_windows_read(self):
        lanes = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))["by_pnu_lanes"]
        contract = json.loads((PLATFORM / "infra/lakehouse/contracts/rt-molit-real-transaction.contract.json")
                              .read_text(encoding="utf-8"))
        self.assertEqual(lanes["parcel-real-transaction"]["first_publish_waits_for_history"],
                         list(gold.REGION_DATASETS))
        self.assertIn(gold.INDUSTRIAL, gold.REGION_DATASETS)  # the comparables' dataset
        self.assertGreaterEqual(contract["recent_history_months"], max(gold.COMPARABLE_MONTHS, gold.REGION_MONTHS))


if __name__ == "__main__":
    unittest.main()
