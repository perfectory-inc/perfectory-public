"""Execute the production relational queries against synthetic 99999 fixtures."""

import re
import contextlib
import io
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from unit_official_price import (DICTIONARY_SQL, CONFLICTING_SQL, PRICES_SQL, JOIN_SQL, refuse_conflicting, parse_args,
                                 newest_shared_vintage, batch_source_snapshot_id, batch_vintage, newest_batch,
                                 order_batches)


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

    def test_without_a_vintage_the_newest_month_both_tables_hold_is_joined(self):
        self.assertIsNone(parse_args([]).vintage)
        self.assertEqual(newest_shared_vintage(["209906", "209907"], ["209907", "209905"]), "209907")
        # One lane has refreshed to a month the other has not: the month both hold, never a mix
        # (root ADR-0182); the next run joins the newer month once both hold it.
        self.assertEqual(newest_shared_vintage(["209907"], ["209908", "209907"]), "209907")
        for exclusive, prices in ((["209906"], ["209907"]), ([], ["209907"])):
            with self.subTest(exclusive=exclusive, prices=prices), self.assertRaises(ValueError):
                newest_shared_vintage(exclusive, prices)

    def test_submission_takes_the_nation_and_positive_snapshot_ids(self):
        self.assertEqual(parse_args(["--vintage", "202606"]).vintage, "202606")
        for args in (["--sido", "99"], ["--price-snapshot-id", "0"], ["--vintage", "2099"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(args)

    def test_the_panel_reads_the_newest_vintages_batch(self):
        older, newer = batch_source_snapshot_id(1, 2, "209907"), batch_source_snapshot_id(3, 4, "209908")
        self.assertEqual(batch_vintage(newer), "209908")
        self.assertEqual(newest_batch([older, newer, older]), newer)
        self.assertEqual(newest_batch([older]), older)
        # An id this job did not write: which batch is newest is unknown, so nothing is chosen.
        self.assertIsNone(newest_batch([older, "older-source"]))
        self.assertIsNone(newest_batch([]))
        self.assertIsNone(batch_vintage("price:1|exclusive:2|vintage:2099"))
        # A vintage joined again from newer hub snapshots: the later batch, by commit order.
        rejoined = batch_source_snapshot_id(5, 6, "209908")
        self.assertEqual(newest_batch([newer, rejoined], [older, newer, rejoined]), rejoined)
        self.assertEqual(newest_batch([newer, rejoined], [rejoined, newer]), newer)
        # Without the order it is refused, not guessed.
        with self.assertRaisesRegex(ValueError, "2 batches of vintage 209908 and their order is unknown"):
            newest_batch([newer, rejoined])

    def test_an_expired_or_tagged_append_still_orders_the_vintage(self):
        # 2026-11-17 (shape of silver.unit_official_price, ids synthetic): four batches of one vintage.
        # Two were appended in snapshots expiry has removed, one in a snapshot a tag keeps off main's
        # chain, one on main's chain. Only main's chain was read, so the vintage was refused.
        expired_a, expired_b = batch_source_snapshot_id(1, 9, "209909"), batch_source_snapshot_id(2, 9, "209909")
        tagged, on_main = batch_source_snapshot_id(3, 9, "209909"), batch_source_snapshot_id(3, 8, "209909")
        load = "unit-official-price/base-date-v2/{}/{}".format
        ingested = {load("11", expired_a): 71, load("11", expired_b): 72, load("11", tagged): 81,
                    load("26", tagged): 81, load("11", on_main): 91, load("26", on_main): 91}
        # 90 (expired parent) <- 91 <- 92 = main; 81 is held by a tag and older than 91.
        snapshots = {81: (80, 1_000), 91: (90, 2_000), 92: (91, 3_000)}
        order = order_batches(ingested, 92, snapshots)
        self.assertEqual(list(order), [tagged, on_main])
        self.assertEqual(order.before_retained, {expired_a, expired_b})
        self.assertEqual(newest_batch([expired_a, expired_b, tagged, on_main], order), on_main)
        # Only expired batches: nothing says which is newest, so it is refused, not guessed.
        with self.assertRaisesRegex(ValueError, "2 batches of vintage 209909 and their order is unknown"):
            newest_batch([expired_a, expired_b], order)
        # 2026-11-22 (shape of vintage 202610, ids synthetic): a hub compaction made the join write the
        # vintage twice and expiry removed both appends. Batches that hold the same rows are one batch,
        # and the largest id stands for it; batches that differ are still refused.
        asked = []
        self.assertEqual(
            newest_batch([expired_a, expired_b], order, lambda batches: asked.append(set(batches)) or True),
            max(expired_a, expired_b))
        self.assertEqual(asked, [{expired_a, expired_b}])
        with self.assertRaisesRegex(ValueError, "2 batches of vintage 209909 and their order is unknown"):
            newest_batch([expired_a, expired_b], order, lambda batches: False)

        # The rows are compared only when the order is lost: an ordered vintage is never scanned,
        # and neither is one with a batch the registry does not record.
        def never(batches):
            self.fail(f"compared the rows of a vintage whose order is not lost: {sorted(batches)}")

        self.assertEqual(newest_batch([expired_a, expired_b, tagged, on_main], order, never), on_main)
        with self.assertRaisesRegex(ValueError, "order is unknown"):
            newest_batch([expired_a, batch_source_snapshot_id(4, 9, "209909")], order, never)
        # A batch the registry does not record at all is still refused.
        unrecorded = batch_source_snapshot_id(4, 9, "209909")
        with self.assertRaisesRegex(ValueError, "order is unknown"):
            newest_batch([on_main, unrecorded], order)
        # Equal instants: the place in main's chain decides.
        self.assertEqual(list(order_batches({load("11", tagged): 91, load("11", on_main): 92}, 92,
                                            {91: (90, 5), 92: (91, 5)})), [tagged, on_main])


if __name__ == "__main__":
    unittest.main()
