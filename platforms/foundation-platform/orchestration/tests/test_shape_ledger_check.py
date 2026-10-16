"""The shape ledger check fails on every way production can leave a committed ledger (root ADR-0259)."""

import importlib.util
import json
import pathlib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("shape_ledger_check", PLATFORM / "scripts/ops/shape-ledger-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class Differences(unittest.TestCase):
    SHAPE = {"has_rows": True, "fields": [[1, "a", "string", True]], "columns": [1], "earlier": []}

    def lakehouse(self, **tables):
        return {"captured_at": "then", "tables": tables}

    def test_the_same_shape_captured_later_is_no_difference(self):
        self.assertEqual(check.differences(self.lakehouse(t=self.SHAPE),
                                           {**self.lakehouse(t=self.SHAPE), "captured_at": "now"}, "tables"), [])

    def test_a_new_table_a_gone_table_and_a_changed_shape_are_each_named(self):
        rewritten = {**self.SHAPE, "earlier": [[1, 2]]}
        found = check.differences(self.lakehouse(kept=self.SHAPE, gone=self.SHAPE),
                                  self.lakehouse(kept=rewritten, new=self.SHAPE), "tables")
        self.assertEqual(found, ["new: new in production", "gone: gone from production", "kept: shape changed"])

    def test_postgres_tables_that_gained_or_lost_rows_are_named(self):
        found = check.differences({"tables_with_rows": ["a", "b"]}, {"tables_with_rows": ["b", "c"]},
                                  "tables_with_rows")
        self.assertEqual(found, ["c: now holds rows", "a: no longer holds rows"])

    def test_the_committed_ledgers_are_where_the_check_reads_them(self):
        for relative, key in ((check.LAKEHOUSE, "tables"), (check.POSTGRES, "tables_with_rows")):
            self.assertIn(key, json.loads((PLATFORM / relative).read_text(encoding="utf-8")))

    def test_it_runs_after_every_deploy_and_daily(self):
        jobs = json.loads((PLATFORM / "orchestration/jobs.v1.json").read_text(encoding="utf-8"))["jobs"]
        job = next(job for job in jobs if job["id"] == "shape_ledger_check")
        self.assertTrue(job["enabled"] and job["started_once_after_deploy"])
        self.assertEqual(job["systemd_service"], "foundation-shape-ledger-check.service")


if __name__ == "__main__":
    unittest.main()
