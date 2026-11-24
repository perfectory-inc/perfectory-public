"""The data-quality job counts the Silver rows under an abolished 법정동 code that has no pair
(scripts/ops/data-quality-check.py `contract_region_codes_paired`, root ADR-0609 §2, ADR-0145).

A Gold reader translates a code only through a pair of `reference.legal_dong_code_change`, so a row
under an abolished code with none reaches no served parcel. The incident replayed here: the change
table held one year's changes, a source still shipped an older abolished code, and its rows were in
Silver and in no Gold. Tables, codes and counts are synthetic (99999).
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

TABLE = "silver.example"
CHECK = "contract_region_codes_paired"
CONTRACTS = {
    TABLE: {"id": TABLE, "schema": [{"physicalName": TABLE, "properties": [{"name": "pnu"}]}]},
    "gold.example": {"id": "gold.example",
                     "schema": [{"physicalName": "gold.example", "properties": [{"name": "pnu"}]}]},
    "silver.unkeyed": {"id": "silver.unkeyed",
                       "schema": [{"physicalName": "silver.unkeyed", "properties": [{"name": "name"}]}]},
}
# The full table: a current code, a renumbered one with its pair, and two abolished with none.
CURRENT, RENUMBERED, OLD, OLDER = "9999910100", "9999910200", "9999810100", "9999710100"
SNAPSHOT_ROWS = [{"region_cd": RENUMBERED, "abolished_date": "20990701"},
                 {"region_cd": OLD, "abolished_date": "20970118"},
                 {"region_cd": OLDER, "abolished_date": None}]


class Run:
    """One check_region_codes run against fakes of the lakehouse (`lakehouse-query`) and Dagster."""

    def __init__(self, job, under=None, paired=(RENUMBERED,), last=None, snapshot=41, first_checked=None,
                 now=1_800_000_000, code_tables=True, dagster_down=False):
        self.queries, self.reports = [], []
        self.totals = job.new_totals()
        self.first_checked = {} if first_checked is None else first_checked
        self.under = {} if under is None else under
        self.paired, self.last, self.snapshot = paired, last, snapshot
        self.code_tables, self.dagster_down = code_tables, dagster_down
        job.yaml = SimpleNamespace(safe_load=lambda name: CONTRACTS[name])
        paths = [SimpleNamespace(read_text=lambda encoding, name=name: name) for name in CONTRACTS]
        job.check_region_codes(paths, self.totals, read=self.read, first_checked=self.first_checked, now=now,
                               latest=self.latest, publish=lambda *report: self.reports.append(report))

    def latest(self, wanted):
        assert wanted == [(TABLE, CHECK)], wanted
        if self.dagster_down:
            raise RuntimeError("Dagster answered errors")
        return {} if self.last is None else {(TABLE, CHECK): self.last}

    def read(self, queries):
        self.queries.extend(queries)
        answers = []
        for table, sql, snapshot in queries:
            if table.startswith("reference.") and not self.code_tables:
                answers.append({"table": table, "snapshot_id": None, "absent": True, "rows": []})
            elif table == "reference.legal_dong_code_snapshot":
                # Only the newest full table, and only codes it holds as abolished alone.
                assert "max(snapshot_date)" in sql and "count_if(status = '존재') = 0" in sql, sql
                answers.append({"table": table, "snapshot_id": 7, "rows": SNAPSHOT_ROWS})
            elif table == "reference.legal_dong_code_change":
                assert "kind = 'pair'" in sql, sql
                answers.append({"table": table, "snapshot_id": 8, "rows": [{"old_code": code} for code in self.paired]})
            elif sql == "DESCRIBE {table}":
                assert table == TABLE, "only the Silver table keyed by pnu is read"
                answers.append({"table": table, "snapshot_id": self.snapshot, "rows": [{"column_name": "pnu"}]})
            else:
                # The count is of the snapshot the description read, and only of the unpaired codes.
                assert table == TABLE and snapshot == self.snapshot, (table, snapshot)
                assert f"'{RENUMBERED}'" not in sql and f"'{CURRENT}'" not in sql, sql
                answers.append({"table": table, "snapshot_id": snapshot,
                                "rows": [{"code": code, "n": n} for code, n in self.under.items() if f"'{code}'" in sql]})
        return answers

    def counted(self):
        return any(sql.startswith("SELECT substr(pnu, 1, 10)") for _, sql, _ in self.queries)

    def reported(self):
        """(passed, severity, metadata) of the one report."""
        ((table, check, passed, severity, metadata),) = self.reports
        assert (table, check) == (TABLE, CHECK), (table, check)
        return passed, severity, metadata


class RegionCodesArePaired(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("yaml", types.ModuleType("yaml"))
        cls.job = load_quality_job()

    def test_rows_under_an_abolished_code_with_no_pair_fail_and_are_named(self):
        # The incident: the source still ships the code abolished before the change table's first pair.
        run = Run(self.job, under={OLD: 11, OLDER: 3, RENUMBERED: 500, CURRENT: 900})
        passed, severity, metadata = run.reported()
        self.assertFalse(passed)
        self.assertEqual(severity, "WARN", "a rule run for the first time only reports")
        self.assertEqual((metadata["unpaired_rows"], metadata["unpaired_codes"]), (14, 2))
        self.assertEqual(metadata["codes"], f"{OLD}(20970118):11,{OLDER}(-):3")
        self.assertEqual(metadata["snapshot_id"], "41")
        self.assertEqual(len(run.totals["warned"]), 1)

    def test_a_renumbered_code_with_its_pair_passes(self):
        run = Run(self.job, under={RENUMBERED: 500, CURRENT: 900})
        passed, _, metadata = run.reported()
        self.assertTrue(passed)
        self.assertEqual((metadata["unpaired_rows"], metadata["codes"]), (0, "none"))

    def test_once_the_pair_is_recorded_the_same_rows_pass(self):
        # The fix is the change table's content: no row of the Silver table changed.
        before = Run(self.job, under={OLD: 11})
        after = Run(self.job, under={OLD: 11}, paired=(RENUMBERED, OLD, OLDER))
        self.assertFalse(before.reported()[0])
        self.assertTrue(after.reported()[0])
        self.assertFalse(after.counted(), "no unpaired abolished code is left to count")
        self.assertNotEqual(before.reported()[2]["rules"], after.reported()[2]["rules"])

    def test_an_unchanged_table_under_the_same_codes_is_not_counted_again(self):
        first = Run(self.job, under={OLD: 11})
        recorded = (False, {**first.reported()[2], "restated_from_run": "a-check-run"})
        run = Run(self.job, under={OLD: 11}, last=recorded)
        self.assertFalse(run.counted())
        self.assertEqual(run.reported(), (False, "WARN", first.reported()[2]))
        # A new snapshot of the table, or a pair appended, counts again.
        self.assertTrue(Run(self.job, under={OLD: 11}, last=recorded, snapshot=42).counted())
        self.assertTrue(Run(self.job, under={OLD: 11}, last=recorded, paired=(RENUMBERED, OLDER)).counted())

    def test_the_rule_is_enforced_a_week_after_its_own_first_run_whatever_the_contract_s_age(self):
        key = f"{TABLE}#{CHECK}"
        # The contract was enforced long ago; the rule is new, so it only reports.
        fresh = Run(self.job, under={OLD: 11}, first_checked={TABLE: 0})
        self.assertEqual(fresh.reported()[1], "WARN")
        self.assertEqual(fresh.first_checked[key], 1_800_000_000)
        old = Run(self.job, under={OLD: 11}, first_checked={key: 1_800_000_000 - 8 * 86400})
        self.assertEqual(old.reported()[1], "ERROR")
        self.assertEqual(len(old.totals["failed"]), 1)

    def test_dagster_not_answering_costs_only_the_skip(self):
        run = Run(self.job, under={OLD: 11}, dagster_down=True)
        self.assertTrue(run.counted())
        self.assertFalse(run.reported()[0])

    def test_without_the_code_tables_the_rule_is_not_run(self):
        run = Run(self.job, under={OLD: 11}, code_tables=False)
        self.assertEqual((run.reports, run.totals["checks"]), ([], []))


if __name__ == "__main__":
    unittest.main()
