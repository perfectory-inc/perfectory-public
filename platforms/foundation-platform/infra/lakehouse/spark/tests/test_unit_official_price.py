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
            CREATE TABLE price_source (mgmt_key TEXT, base_date TEXT, notice_date TEXT, price_won TEXT);
        """)
        self.db.executemany("INSERT INTO exclusive_source VALUES (?,?,?,?)", [
            ("fixture-a", "9999900000100000001", "101동", "101호"),
            ("fixture-a", "9999900000100000001", "101동", "101호"),
            ("fixture-b", "9999900000100000001", "102동", "101호"),
        ])

    def views(self):
        self.db.execute("CREATE TEMP VIEW unit_dictionary AS " + DICTIONARY_SQL)
        self.db.execute("CREATE TEMP VIEW reference_prices AS " + PRICES_SQL)

    def test_dictionary_folds_identical_observations_and_refuses_ambiguity(self):
        self.views()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM unit_dictionary").fetchone()[0], 2)
        self.assertEqual(self.db.execute(CONFLICTING_SQL).fetchone()[0], 0)
        self.db.execute("INSERT INTO exclusive_source VALUES ('fixture-a','9999900000100000002','101동','101호')")
        conflicting = self.db.execute(CONFLICTING_SQL).fetchone()[0]
        self.assertEqual(conflicting, 1)
        with self.assertRaisesRegex(ValueError, "conflicting=1"):
            refuse_conflicting(conflicting)

    def test_reference_dates_corrections_and_unmatched_prices(self):
        self.db.executemany("INSERT INTO price_source VALUES (?,?,?,?)", [
            ("fixture-a", " 20100101 ", "20100430", "37000000"),
            ("fixture-a", "20100101", "20100531", "36000000"),
            ("fixture-a", "20100601", "20100930", "35000000"),
            ("fixture-b", "20100101", "20100430", "190000000"),
            ("fixture-missing", "20100101", "20100430", "100000000"),
            ("fixture-a", "bad-date", "20100430", "100000000"),
        ])
        self.views()
        rows = self.db.execute(JOIN_SQL + " ORDER BY dong_name, base_date DESC").fetchall()
        # The join now leads with the register key (mgm_bldrgst_pk = the dictionary
        # mgmt_key), so each row is 6-wide: (mgm_bldrgst_pk, pnu, dong, ho, base_date, price).
        self.assertEqual(rows, [
            ("fixture-a", "9999900000100000001", "101동", "101호", "20100601", 35000000),
            ("fixture-a", "9999900000100000001", "101동", "101호", "20100101", 36000000),
            ("fixture-b", "9999900000100000001", "102동", "101호", "20100101", 190000000),
        ])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM price_source").fetchone()[0], 6)

    def test_equal_notice_dates_use_numeric_price_tiebreak_only_within_reference_date(self):
        self.db.executemany("INSERT INTO price_source VALUES (?,?,?,?)", [
            ("fixture-a", "20260101", "20260430", "100"),
            ("fixture-a", "20260101", "20260430", "20"),
            ("fixture-a", "20260101", "20260430", "100"),
            ("fixture-a", "20260601", None, "90"),
        ])
        self.views()
        rows = self.db.execute(JOIN_SQL + " ORDER BY base_date").fetchall()
        # 6-wide rows: index 4 is base_date, index 5 is price_won.
        self.assertEqual([(r[4], r[5]) for r in rows], [("20260101", 100), ("20260601", 90)])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM price_source").fetchone()[0], 4)

    def test_submission_requires_one_province_and_positive_snapshot_ids(self):
        self.assertEqual(parse_args(["--sido", "99", "--vintage", "202606"]).sido, "99")
        for args in ([], ["--sido", "99999"], ["--sido", "99", "--price-snapshot-id", "0"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(args)


if __name__ == "__main__":
    unittest.main()
