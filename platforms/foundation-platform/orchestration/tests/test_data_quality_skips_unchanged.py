"""The data-quality job does not scan a table it already counted (scripts/ops/data-quality-check.py).

Each allowed-values result records the table's snapshot and a fingerprint of its rules; the catalog
keeps every result. An unchanged table is not counted again, a recorded failure still fails the run,
and a new snapshot or a changed rule is counted again. Table and values are synthetic.
"""

from __future__ import annotations

import pathlib
import sys
import types
import unittest
from types import SimpleNamespace

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "scripts/ops"))

from test_internal_http import load_quality_job  # noqa: E402

CONTRACT = {
    "id": "gold.example",
    "schema": [{
        "physicalName": "gold.example",
        "properties": [
            {"name": "pnu"},
            {"name": "status", "quality": [{"metric": "invalidValues", "arguments": {"validValues": ["a", "b"]}}]},
        ],
    }],
}
FIELD_URN = "urn:li:assertion:field-status"


class Run:
    """One check_contracts run against fakes of the lakehouse (`lakehouse-query`) and the catalog."""

    def __init__(self, job, snapshot, last=None, absent=False):
        self.queries, self.reports = [], []
        self.totals = {"passed": [], "failed": [], "absent": [], "unregistered": [], "unchanged": []}
        job.yaml = SimpleNamespace(safe_load=lambda _text: CONTRACT)
        job.report = lambda urn, passed, properties: self.reports.append((urn, passed, properties))
        events = [] if last is None else [{"result": {"type": last[0], "nativeResults": [
            {"key": k, "value": v} for k, v in last[1].items()]}}]
        job.assertions_of = lambda _id: [{
            "urn": FIELD_URN,
            "info": {"type": "FIELD", "fieldAssertion": {"fieldValuesAssertion": {"field": {"path": "status"}}}},
            "runEvents": {"runEvents": events},
        }]
        self.snapshot, self.absent = snapshot, absent
        job.check_contracts([SimpleNamespace(read_text=lambda encoding: "")], self.totals, read=self.read)

    def read(self, queries):
        self.queries.extend(queries)
        answers = []
        for table, sql, snapshot in queries:
            if self.absent:
                answers.append({"table": table, "snapshot_id": None, "absent": True, "rows": []})
            elif sql == "DESCRIBE {table}":
                self.assertIsNone(snapshot)
                answers.append({"table": table, "snapshot_id": self.snapshot,
                                "rows": [{"column_name": "pnu"}, {"column_name": "status"}]})
            elif sql.startswith("SELECT count(*)"):
                # The count is of the snapshot the description read, never of a newer one.
                self.assertEqual(snapshot, self.snapshot)
                answers.append({"table": table, "snapshot_id": snapshot, "rows": [{"total": 10, "f0": 2}]})
            else:
                raise AssertionError(sql)
        return answers

    @staticmethod
    def assertIsNone(value):
        assert value is None, value

    @staticmethod
    def assertEqual(left, right):
        assert left == right, (left, right)

    def counted(self):
        return any(sql.startswith("SELECT count(*)") for _, sql, _ in self.queries)


class SkipsUnchangedTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("yaml", types.ModuleType("yaml"))
        cls.job = load_quality_job()
        cls.rules = cls.job.rules_fingerprint([(FIELD_URN, "status")], {"status": ["a", "b"]})

    def test_a_first_check_counts_and_records_the_snapshot_and_the_rules(self):
        run = Run(self.job, 41)
        self.assertTrue(run.counted())
        ((_, passed, properties),) = run.reports
        self.assertFalse(passed)
        self.assertEqual((properties["snapshot_id"], properties["rules"]), ("41", self.rules))

    def test_an_unchanged_table_is_not_counted_and_its_recorded_result_still_counts(self):
        run = Run(self.job, 41, ("SUCCESS", {"snapshot_id": "41", "rules": self.rules, "invalid_rows": "0"}))
        self.assertFalse(run.counted())
        self.assertEqual(run.reports, [])
        self.assertEqual(len(run.totals["passed"]), 1)
        self.assertEqual(run.totals["unchanged"], ["gold.example snapshot=41"])

    def test_a_recorded_failure_on_an_unchanged_table_still_fails_the_run(self):
        run = Run(self.job, 41, ("FAILURE", {"snapshot_id": "41", "rules": self.rules, "invalid_rows": "2"}))
        self.assertFalse(run.counted())
        self.assertEqual(len(run.totals["failed"]), 1)
        self.assertIn("invalid=2", run.totals["failed"][0])

    def test_a_new_snapshot_is_counted_again(self):
        run = Run(self.job, 42, ("SUCCESS", {"snapshot_id": "41", "rules": self.rules, "invalid_rows": "0"}))
        self.assertTrue(run.counted())
        self.assertEqual(run.reports[0][2]["snapshot_id"], "42")

    def test_a_changed_rule_is_counted_again_on_the_same_snapshot(self):
        run = Run(self.job, 41, ("SUCCESS", {"snapshot_id": "41", "rules": "other-rules", "invalid_rows": "0"}))
        self.assertTrue(run.counted())

    def test_a_table_the_catalog_does_not_hold_is_named_absent_and_not_counted(self):
        run = Run(self.job, None, absent=True)
        self.assertFalse(run.counted())
        self.assertEqual((run.totals["absent"], run.reports), (["gold.example"], []))

    def test_the_count_reads_the_fields_allowed_values_at_the_table_s_snapshot(self):
        run = Run(self.job, 41)
        ((table, sql, snapshot),) = [query for query in run.queries if query[1].startswith("SELECT count(*)")]
        self.assertEqual((table, snapshot), ("gold.example", 41))
        self.assertEqual(sql, "SELECT count(*) AS total, count_if(status IS NOT NULL AND status NOT IN ('a', 'b')) AS f0 FROM {source}")

    def test_a_result_recorded_before_snapshots_were_recorded_is_counted_again(self):
        run = Run(self.job, 41, ("SUCCESS", {"invalid_rows": "0"}))
        self.assertTrue(run.counted())


if __name__ == "__main__":
    unittest.main()
