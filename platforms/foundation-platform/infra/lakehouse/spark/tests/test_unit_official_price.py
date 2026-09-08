"""Execute the production relational queries against synthetic 99999 fixtures."""

import re
import contextlib
import io
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from unit_official_price import DICTIONARY_SQL, CONFLICTING_SQL, PRICES_SQL, JOIN_SQL, refuse_conflicting, parse_args


class UnitOfficialPriceTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.create_function("regexp", 2, lambda pattern, value: bool(value and re.fullmatch(pattern, value)))
        self.db.executescript("""
            CREATE TABLE exclusive_source (mgmt_key TEXT, pnu TEXT, dong_name TEXT, ho_name TEXT);
            CREATE TABLE price_source (mgmt_key TEXT, base_date TEXT, price_won TEXT);
        """)
        self.db.executemany("INSERT INTO exclusive_source VALUES (?,?,?,?)", [
            ("fixture-a", "9999900000100000001", "101동", "101호"),
            ("fixture-a", "9999900000100000001", "101동", "101호"),
            ("fixture-b", "9999900000100000001", "102동", "101호"),
        ])

    def views(self):
        self.db.execute("CREATE TEMP VIEW unit_dictionary AS " + DICTIONARY_SQL)
        self.db.execute("CREATE TEMP VIEW annual_prices AS " + PRICES_SQL)

    def test_dictionary_folds_identical_observations_and_refuses_ambiguity(self):
        self.views()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM unit_dictionary").fetchone()[0], 2)
        self.assertEqual(self.db.execute(CONFLICTING_SQL).fetchone()[0], 0)
        self.db.execute("INSERT INTO exclusive_source VALUES ('fixture-a','9999900000100000002','101동','101호')")
        conflicting = self.db.execute(CONFLICTING_SQL).fetchone()[0]
        self.assertEqual(conflicting, 1)
        with self.assertRaisesRegex(ValueError, "conflicting=1"):
            refuse_conflicting(conflicting)

    def test_year_history_duplicate_notices_and_unmatched_prices(self):
        self.db.executemany("INSERT INTO price_source VALUES (?,?,?)", [
            ("fixture-a", "20260101", "232000000"),
            ("fixture-a", "20260101", "232000000"),
            ("fixture-a", "20250101", "221000000"),
            ("fixture-b", "20260101", "190000000"),
            ("fixture-missing", "20260101", "100000000"),
            ("fixture-a", "bad-date", "100000000"),
        ])
        self.views()
        rows = self.db.execute(JOIN_SQL + " ORDER BY dong_name, base_year DESC").fetchall()
        self.assertEqual(rows, [
            ("9999900000100000001", "101동", "101호", 2026, 232000000),
            ("9999900000100000001", "101동", "101호", 2025, 221000000),
            ("9999900000100000001", "102동", "101호", 2026, 190000000),
        ])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM price_source").fetchone()[0], 6)

    def test_distinct_prices_are_preserved_for_the_catalog_resolution(self):
        self.db.executemany("INSERT INTO price_source VALUES (?,?,?)", [
            ("fixture-a", "20260101", "100"), ("fixture-a", "20260101", "200"),
        ])
        self.views()
        self.assertEqual(len(self.db.execute(JOIN_SQL).fetchall()), 2)

    def test_submission_requires_one_province_and_positive_snapshot_ids(self):
        self.assertEqual(parse_args(["--sido", "99", "--vintage", "202606"]).sido, "99")
        for args in ([], ["--sido", "99999"], ["--sido", "99", "--price-snapshot-id", "0"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(args)


if __name__ == "__main__":
    unittest.main()
