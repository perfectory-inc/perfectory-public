"""The SQL every reader takes a versioned table's current rows through is the contract artifact's.

Root ADR-0564 §4: the current-row rule is defined once (the contract) and its SQL is rendered once
(the Rust contract, into `current_rows_sql` of the artifact). The incident this prevents is
2026-10-16: `silver.industrial_complexes` was read with no current-row rule, every complex came back
three times (4,326 rows) and the boundary release stopped. A reader that renders its own rank query
is that defect waiting for its table.

The cases are the ones the Rust rule over scanned rows passes
(`crates/lakehouse/lakehouse-domain/tests/lakehouse_contract_artifact.rs`), run here in SQLite: the text is
standard SQL, so the engine it is checked in is neither of the engines that run it in production.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
JOBS = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS))

import platform_contracts  # noqa: E402

CASES = Path(__file__).resolve().parent / "fixtures" / "current_rows_cases.json"


def contracts() -> list[dict]:
    return list(platform_contracts.load_lakehouse_artifact()["contracts"].values())


def run(contract: dict, rows: list[dict]) -> list[dict]:
    columns = sorted({name for row in rows for name in row})
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(f"CREATE TABLE versions ({', '.join(columns)})")
        connection.executemany(f"INSERT INTO versions VALUES ({', '.join('?' for _ in columns)})",
                               [[row.get(name) for name in columns] for row in rows])
        sql = platform_contracts.current_rows_sql(contract, "versions", columns)
        return [dict(zip(columns, found)) for found in connection.execute(sql).fetchall()]
    finally:
        connection.close()


def ordered(rows: list[dict]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True) for row in rows)


class CurrentRowsSqlTest(unittest.TestCase):
    def test_the_shared_cases_pick_the_rows_the_rust_rule_picks(self):
        cases = json.loads(CASES.read_text(encoding="utf-8"))["cases"]
        self.assertTrue(cases)
        for case in cases:
            with self.subTest(case["case"]):
                contract = platform_contracts.load_lakehouse_contract(case["table"])
                self.assertEqual(ordered(run(contract, case["rows"])), ordered(case["current"]))

    def test_every_versioned_table_carries_the_query_and_no_other_table_does(self):
        versioned = 0
        for contract in contracts():
            latest = platform_contracts.current_row_latest(contract)
            with self.subTest(contract["table_name"]):
                if latest is None:
                    self.assertIsNone(contract["current_rows_sql"])
                    with self.assertRaisesRegex(ValueError, "keeps no versions"):
                        platform_contracts.current_rows_sql(contract, "t", ["a"])
                    continue
                versioned += 1
                sql = platform_contracts.current_rows_sql(contract, "t", list(latest.key), "a = 1")
                self.assertIn(f"PARTITION BY {', '.join(latest.key)} ORDER BY "
                              + ", ".join(f"{column} DESC NULLS LAST" for column in latest.newest_first), sql)
                self.assertIn("FROM t WHERE a = 1", sql)
                predicate = platform_contracts.current_row_predicate(contract)
                self.assertEqual(f"AND ({predicate})" in sql, predicate is not None)
                # A removal marker is dropped after the newest row is chosen: dropped before, the
                # row it removed would look current again.
                inner, _, outer = sql.partition("WHERE foundation_current_rank = 1")
                self.assertNotIn(f"'{platform_contracts.REMOVED}'", inner)
                self.assertEqual(f"<> '{platform_contracts.REMOVED}'" in outer, latest.removal_column is not None)
        self.assertGreater(versioned, 0)

    def test_a_tie_is_kept_by_the_query_so_a_reader_that_needs_one_row_counts(self):
        contract = platform_contracts.load_lakehouse_contract("gold.complex_document_facts")
        tied = [{"official_complex_code": "99999", "run_id": "run-2"}] * 2
        self.assertEqual(len(run(contract, tied)), 2)

    def test_a_projection_that_is_not_column_names_is_refused(self):
        contract = platform_contracts.load_lakehouse_contract("gold.complex_document_facts")
        for columns in ([], ["run_id; DROP TABLE t"], ["*"]):
            with self.assertRaisesRegex(ValueError, "column names"):
                platform_contracts.current_rows_sql(contract, "t", columns)

    def test_a_contract_whose_text_lost_a_slot_is_refused(self):
        contract = dict(platform_contracts.load_lakehouse_contract("gold.complex_document_facts"))
        contract["current_rows_sql"] = contract["current_rows_sql"].replace("{filter}", "TRUE")
        with self.assertRaisesRegex(ValueError, "three slots"):
            platform_contracts.current_rows_sql(contract, "t", ["run_id"])


if __name__ == "__main__":
    unittest.main()
