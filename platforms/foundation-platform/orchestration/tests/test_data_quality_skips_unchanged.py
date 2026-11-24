"""The data-quality job reports each rule as a Dagster asset check and does not scan a table it already
counted (scripts/ops/data-quality-check.py, root ADR-0609 §2).

Each allowed-values result records the table's snapshot and a fingerprint of its rules; Dagster keeps
every evaluation. An unchanged table is not counted again, a recorded failure still fails the run, and a
new snapshot or a changed rule is counted again. Table and values are synthetic.
"""

from __future__ import annotations

import io
import json
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stdout
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
STATUS = ("gold.example", "contract_allowed_values_status")
COLUMNS = ("gold.example", "contract_columns_present")


class Run:
    """One check_contracts run against fakes of the lakehouse (`lakehouse-query`), Dagster and DataHub."""

    def __init__(self, job, snapshot, last=None, absent=False, judged=False, first_checked=None, now=1_800_000_000,
                 dagster_down=False, datahub_down=False):
        self.queries, self.reports, self.asked = [], [], []
        self.dagster_down, self.datahub_down = dagster_down, datahub_down
        self.totals = job.new_totals()
        self.first_checked = {} if first_checked is None else first_checked
        job.yaml = SimpleNamespace(safe_load=lambda _text: CONTRACT)
        self.snapshot, self.absent, self.last = snapshot, absent, last
        job.check_contracts([SimpleNamespace(read_text=lambda encoding: "")], self.totals, read=self.read,
                            first_checked=self.first_checked, now=now, latest=self.latest,
                            judged_before=lambda contract: self.judged(contract, judged),
                            publish=lambda *report: self.reports.append(report))

    def judged(self, contract, judged):
        self.asked.append(contract)
        if self.datahub_down:
            raise OSError("GMS did not answer")
        return judged

    def latest(self, wanted):
        assert wanted == [STATUS], wanted
        if self.dagster_down:
            raise RuntimeError("Dagster answered errors")
        return {} if self.last is None else {STATUS: self.last}

    def read(self, queries):
        self.queries.extend(queries)
        answers = []
        for table, sql, snapshot in queries:
            if self.absent:
                answers.append({"table": table, "snapshot_id": None, "absent": True, "rows": []})
            elif sql == "DESCRIBE {table}":
                assert snapshot is None, snapshot
                answers.append({"table": table, "snapshot_id": self.snapshot,
                                "rows": [{"column_name": "pnu"}, {"column_name": "status"}]})
            elif sql.startswith("SELECT count(*)"):
                # The count is of the snapshot the description read, never of a newer one.
                assert snapshot == self.snapshot, (snapshot, self.snapshot)
                answers.append({"table": table, "snapshot_id": snapshot, "rows": [{"total": 10, "f0": 2}]})
            else:
                raise AssertionError(sql)
        return answers

    def counted(self):
        return any(sql.startswith("SELECT count(*)") for _, sql, _ in self.queries)

    def reported(self, check):
        """(passed, severity, metadata) of the one report of `check`."""
        (found,) = [(passed, severity, metadata) for _, name, passed, severity, metadata in self.reports if name == check]
        return found


class SkipsUnchangedTables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("yaml", types.ModuleType("yaml"))
        cls.job = load_quality_job()
        cls.rules = cls.job.rules_fingerprint(["status"], {"status": ["a", "b"]})

    def test_a_first_check_counts_and_records_the_snapshot_and_the_rules(self):
        run = Run(self.job, 41)
        self.assertTrue(run.counted())
        passed, severity, metadata = run.reported(STATUS[1])
        self.assertFalse(passed)
        self.assertEqual(severity, "WARN", "a contract checked for the first time only reports")
        self.assertEqual((metadata["snapshot_id"], metadata["rules"], metadata["invalid_rows"]), ("41", self.rules, 2))
        self.assertEqual(run.reported(COLUMNS[1])[:2], (True, "WARN"))

    def test_an_unchanged_table_is_not_counted_and_its_recorded_result_still_counts(self):
        run = Run(self.job, 41, (True, {"snapshot_id": "41", "rules": self.rules, "invalid_rows": 0,
                                        "restated_from_run": "a-check-run"}))
        self.assertFalse(run.counted())
        # The recorded result stands for this run too, with the metadata it was counted with.
        self.assertEqual(run.reported(STATUS[1]), (True, "WARN", {"snapshot_id": "41", "rules": self.rules,
                                                                   "invalid_rows": 0}))
        # The recorded allowed-values result, and the schema rule every contract with columns has.
        self.assertEqual(len(run.totals["passed"]), 2)
        self.assertEqual(run.totals["unchanged"], ["gold.example snapshot=41"])

    def test_a_recorded_failure_on_an_unchanged_table_still_fails_the_run(self):
        # Reported at WARN on its first day, enforced a week later on the same snapshot: it says ERROR now.
        run = Run(self.job, 41, (False, {"snapshot_id": "41", "rules": self.rules, "invalid_rows": 2}),
                  first_checked={"gold.example": 0})
        self.assertFalse(run.counted())
        self.assertEqual(len(run.totals["failed"]), 1)
        self.assertIn("invalid=2", run.totals["failed"][0])
        self.assertEqual(run.reported(STATUS[1])[:2], (False, "ERROR"))

    def test_dagster_not_answering_costs_only_the_skip(self):
        run = Run(self.job, 41, (True, {"snapshot_id": "41", "rules": self.rules, "invalid_rows": 0}),
                  dagster_down=True)
        self.assertTrue(run.counted())
        self.assertEqual(run.reported(STATUS[1])[2]["snapshot_id"], "41")

    def test_a_new_snapshot_is_counted_again(self):
        run = Run(self.job, 42, (True, {"snapshot_id": "41", "rules": self.rules, "invalid_rows": 0}))
        self.assertTrue(run.counted())
        self.assertEqual(run.reported(STATUS[1])[2]["snapshot_id"], "42")

    def test_a_changed_rule_is_counted_again_on_the_same_snapshot(self):
        run = Run(self.job, 41, (True, {"snapshot_id": "41", "rules": "other-rules", "invalid_rows": 0}))
        self.assertTrue(run.counted())

    def test_a_table_the_catalog_does_not_hold_is_named_absent_and_not_counted(self):
        run = Run(self.job, None, absent=True)
        self.assertFalse(run.counted())
        self.assertEqual((run.totals["absent"], run.reports, run.asked), (["gold.example"], [], []))

    def test_the_count_reads_the_fields_allowed_values_at_the_table_s_snapshot(self):
        run = Run(self.job, 41)
        ((table, sql, snapshot),) = [query for query in run.queries if query[1].startswith("SELECT count(*)")]
        self.assertEqual((table, snapshot), ("gold.example", 41))
        self.assertEqual(sql, "SELECT count(*) AS total, count_if(status IS NOT NULL AND status NOT IN ('a', 'b')) AS f0 FROM {source}")

    def test_an_evaluation_without_a_snapshot_is_counted_again(self):
        run = Run(self.job, 41, (True, {"invalid_rows": 0}))
        self.assertTrue(run.counted())


class Enforcement(unittest.TestCase):
    """A contract checked for the first time warns for a week; one DataHub had judged stays enforced."""

    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("yaml", types.ModuleType("yaml"))
        cls.job = load_quality_job()

    def test_a_contract_never_checked_warns_for_a_week_then_fails(self):
        first_checked = {}
        run = Run(self.job, 41, first_checked=first_checked, now=1_800_000_000)
        self.assertEqual((run.totals["failed"], run.totals["warned"]), ([], ["gold.example.status invalid=2"]))
        self.assertEqual(first_checked, {"gold.example": 1_800_000_000})
        six_days = Run(self.job, 41, first_checked=first_checked, now=1_800_000_000 + 6 * 86400)
        self.assertEqual(six_days.totals["failed"], [])
        self.assertEqual(first_checked, {"gold.example": 1_800_000_000}, "the first check is kept, not moved")
        self.assertEqual(six_days.asked, [], "DataHub is asked once, for a contract the file does not hold")
        a_week = Run(self.job, 41, first_checked=first_checked, now=1_800_000_000 + 7 * 86400)
        self.assertEqual(a_week.totals["failed"], ["gold.example.status invalid=2"])
        self.assertEqual(a_week.reported(STATUS[1])[:2], (False, "ERROR"))

    def test_an_enforced_contract_s_passing_rule_says_error_too(self):
        run = Run(self.job, 41, first_checked={"gold.example": 0})
        self.assertEqual(run.reported(COLUMNS[1])[:2], (True, "ERROR"))

    def test_datahub_not_answering_reports_only_this_run_and_records_nothing(self):
        first_checked = {}
        run = Run(self.job, 41, datahub_down=True, first_checked=first_checked)
        self.assertEqual((run.totals["failed"], run.totals["warned"]), ([], ["gold.example.status invalid=2"]))
        self.assertEqual(first_checked, {}, "asked again on the next run")
        self.assertEqual(run.reported(STATUS[1])[:2], (False, "WARN"))

    def test_a_contract_datahub_had_judged_is_enforced_at_once_and_recorded(self):
        first_checked = {}
        run = Run(self.job, 42, judged=True, first_checked=first_checked)
        self.assertEqual((run.totals["failed"], run.totals["warned"]), (["gold.example.status invalid=2"], []))
        self.assertEqual(run.asked, ["gold.example"])
        self.assertEqual(first_checked, {"gold.example": 0})
        self.assertEqual(run.reported(STATUS[1])[:2], (False, "ERROR"))


class TheReports(unittest.TestCase):
    """What reaches the unit's journal, and what Dagster is asked."""

    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("yaml", types.ModuleType("yaml"))
        cls.job = load_quality_job()

    def test_a_report_is_one_pipes_line_on_the_tables_asset_key(self):
        out = io.StringIO()
        with redirect_stdout(out):
            self.job.report("silver.example_table", "contract_columns_present", True, "WARN", {"missing_columns": "none"})
        message = json.loads(out.getvalue())
        self.assertEqual(message["__dagster_pipes_version"], "0.1")
        self.assertEqual(message["method"], "report_asset_check")
        self.assertEqual(message["params"], {
            "asset_key": "iceberg/silver/example_table", "check_name": "contract_columns_present", "passed": True,
            "severity": "WARN", "metadata": {"missing_columns": {"raw_value": "none", "type": "__infer__"}}})

    def test_a_table_outside_the_lakehouse_is_on_the_declared_platform(self):
        self.assertEqual(self.job.asset_key("intelligence.document_fact"), ["perfectory", "intelligence", "document_fact"])

    def test_the_newest_evaluations_are_one_query_and_a_check_never_evaluated_is_missing(self):
        asked = []

        def ask(query):
            asked.append(query)
            return {"c0": [{"evaluation": {"success": False, "metadataEntries": [
                        {"label": "snapshot_id", "text": "41"}, {"label": "invalid_rows", "intValue": 2}]}}],
                    "c1": []}

        found = self.job.last_evaluations([STATUS, ("silver.other", "contract_allowed_values_kind")], ask=ask)
        self.assertEqual(found, {STATUS: (False, {"snapshot_id": "41", "invalid_rows": 2})})
        (query,) = asked
        self.assertIn('c0: assetCheckExecutions(assetKey: {path: ["iceberg", "gold", "example"]}, '
                      'checkName: "contract_allowed_values_status", limit: 1)', query)
        self.assertIn('c1: assetCheckExecutions(assetKey: {path: ["iceberg", "silver", "other"]}', query)

    def test_nothing_to_look_up_asks_nothing(self):
        self.assertEqual(self.job.last_evaluations([], ask=lambda query: self.fail(query)), {})


if __name__ == "__main__":
    unittest.main()
