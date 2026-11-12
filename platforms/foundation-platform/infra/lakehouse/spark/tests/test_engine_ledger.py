"""The ledger of what still runs on Spark is the directory, and no file joins it (root ADR-0557 §2, §5).

The incident this holds off: 2026-11-11, when the 실거래 Gold failed 26 times at a 20 GB JVM, the
rt-molit Silver load ran out of heap and one Spark pool kept 18 jobs waiting. The owner's rule after
it is that no transform is added to Spark and each one leaves only with an engine-equality verdict;
a Spark job nobody listed would be a job nobody plans to move.
"""
import json
import unittest
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-engine.contract.json"
# How a job file starts a Spark session; a file without it runs inside another file's session.
SESSION_START = "getOrCreate()"


def session_starters() -> set[str]:
    return {path.stem for path in JOBS.glob("*.py") if SESSION_START in path.read_text(encoding="utf-8")}


class SparkLedger(unittest.TestCase):
    def setUp(self):
        self.engines = json.loads(CONTRACT.read_text(encoding="utf-8"))["engines"]
        self.ledger = self.engines["spark_sessions"]

    def test_every_file_that_starts_a_spark_session_is_in_the_ledger_and_nothing_else_is(self):
        found, listed = session_starters(), set(self.ledger)
        self.assertEqual(
            found - listed, set(),
            "these files start a Spark session and are not in lakehouse-engine.contract.json engines.spark_sessions. "
            "Root ADR-0557 §2: a new transform runs in DuckDB inside the publisher, not on Spark.")
        self.assertEqual(
            listed - found, set(),
            "these ledger entries no longer start a Spark session: remove them with the pull request that moved them "
            "(root ADR-0557 §5), so the count of what is left is the ledger's length.")

    def test_spark_keeps_one_standing_role_and_it_is_table_maintenance(self):
        staying = {name: entry["stays"] for name, entry in self.ledger.items() if "stays" in entry}
        self.assertEqual(staying, {"lakehouse_maintenance": "maintenance"})

    def test_every_other_entry_names_the_wave_it_moves_in(self):
        waves = set(self.engines["waves"])
        for name, entry in self.ledger.items():
            if "stays" in entry:
                continue
            with self.subTest(job=name):
                self.assertEqual(set(entry), {"wave"})
                self.assertIn(str(entry["wave"]), waves)


if __name__ == "__main__":
    unittest.main()
