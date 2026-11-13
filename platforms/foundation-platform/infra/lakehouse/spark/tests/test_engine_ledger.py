"""The ledger of what still runs on Spark is the directory, and no file joins it (root ADR-0559 §2, §5).

The incident this holds off: 2026-11-11, when the 실거래 Gold failed 26 times at a 20 GB JVM, the
rt-molit Silver load ran out of heap and one Spark pool kept 18 jobs waiting. The owner's rule after
it is that no transform is added to Spark and each one leaves only with an engine-equality verdict;
a Spark job nobody listed would be a job nobody plans to move.
"""
import json
import re
import unittest
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "lakehouse-engine.contract.json"
# How a job file starts a Spark session: itself, or through one of the session builders the jobs
# define (`build_spark_session`, `build_session`). A file with none of them runs inside another
# file's session.
SESSION_START = re.compile(r"getOrCreate\(\)|build_spark_session\(|build_session\(")
# What a line that left Spark records of the verdict it moved on (`engine_equality.Verdict.report`).
MOVED_FIELDS = {"pull_request", "rows", "fingerprint"}


def session_starters() -> set[str]:
    return {path.stem for path in JOBS.glob("*.py") if SESSION_START.search(path.read_text(encoding="utf-8"))}


def problems(ledger: dict, starters: set[str]) -> list[str]:
    """Every way `ledger` disagrees with the files that start a Spark session."""
    found = []
    for name in sorted(starters - set(ledger)):
        found.append(f"{name} starts a Spark session and is not in engines.spark_sessions: a new transform runs in "
                     "DuckDB inside the publisher, not on Spark (root ADR-0559 §2)")
    for name, entry in sorted(ledger.items()):
        if "moved" in entry:
            if name in starters:
                found.append(f"{name} is recorded as moved and still starts a Spark session")
            if set(entry) != {"moved"} or set(entry["moved"]) != MOVED_FIELDS:
                found.append(f"{name}: a moved line records exactly {sorted(MOVED_FIELDS)} of its equal verdict")
        elif name not in starters:
            found.append(f"{name} no longer starts a Spark session: record the verdict it moved on as `moved` "
                         "(root ADR-0559 §5), do not leave or drop the line")
    return found


class SparkLedger(unittest.TestCase):
    def setUp(self):
        self.engines = json.loads(CONTRACT.read_text(encoding="utf-8"))["engines"]
        self.ledger = self.engines["spark_sessions"]

    def test_the_ledger_is_the_files_that_start_a_spark_session(self):
        self.assertEqual(problems(self.ledger, session_starters()), [])

    def test_a_job_that_starts_spark_unlisted_or_leaves_without_its_verdict_is_named(self):
        ledger = {"kept": {"wave": 1}, "left": {"wave": 2}, "proven": {"moved": {
            "pull_request": 1, "rows": 2, "fingerprint": "rows:2;digest_sum:5"}}, "claimed": {"moved": {"pull_request": 1}}}
        found = problems(ledger, {"kept", "added", "proven"})
        self.assertEqual(len(found), 4)
        for name in ("added", "left", "proven", "claimed"):
            self.assertTrue(any(line.startswith(name) for line in found), name)

    def test_a_session_builder_call_counts_as_starting_spark(self):
        for call in ("spark = builder.getOrCreate()", "spark = shared.build_spark_session(args)", "spark = build_session(args)"):
            self.assertTrue(SESSION_START.search(call), call)
        self.assertIsNone(SESSION_START.search("frame.sparkSession.createDataFrame(rows)"))

    def test_spark_keeps_one_standing_role_and_it_is_table_maintenance(self):
        staying = {name: entry["stays"] for name, entry in self.ledger.items() if "stays" in entry}
        self.assertEqual(staying, {"lakehouse_maintenance": "maintenance"})

    def test_every_line_still_on_spark_names_the_wave_it_moves_in(self):
        waves = set(self.engines["waves"])
        for name, entry in self.ledger.items():
            if "stays" in entry or "moved" in entry:
                continue
            with self.subTest(job=name):
                self.assertEqual(set(entry), {"wave"})
                self.assertIn(str(entry["wave"]), waves)


if __name__ == "__main__":
    unittest.main()
