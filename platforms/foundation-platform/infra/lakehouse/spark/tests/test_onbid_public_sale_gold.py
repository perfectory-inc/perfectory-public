"""공매 (Onbid) by-PNU kind: which PNU each sale is served under (root ADR-0287).

Synthetic PNUs (prefix 99999) and made-up management numbers. Pins: a round takes the PNU of its
newest row that has one (a removal marker keeps it, a later blank does not erase it); a bid result
takes its round's PNU; a sale on a retired PNU follows the lineage to every current successor, in
addition to the successor's own sales; a retired PNU no path leaves keeps its sale, flagged; a sale
is never dropped.
"""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import onbid_public_sale_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


CURRENT, RETIRED, SPLIT_A, SPLIT_B, STRANDED = pnu(1), pnu(2), pnu(3), pnu(4), pnu(5)
T1, T2, T3 = datetime(2099, 1, 1, 0), datetime(2099, 1, 1, 3), datetime(2099, 1, 1, 6)
ITEM_SCHEMA = "cltr_mng_no string, pbct_cdtn_no long, pnu string, row_state string, ingested_at_utc timestamp, source_line_number int"
RESULT_SCHEMA = "cltr_mng_no string, pbct_cdtn_no long, winning_bid_amount_krw long, ingested_at_utc timestamp, source_line_number int"


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_carries_lineage_optionally(self):
        required, optional, anchor, whole = job.lane_inputs("parcel-public-sale")
        self.assertEqual(anchor, job.PARCEL_SOURCE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.ITEM_SOURCE, job.RESULT_SOURCE})
        self.assertEqual(optional, {job.LINEAGE_SOURCE: "--no-carry-lineage"})
        self.assertEqual(set(whole), {job.ITEM_SOURCE, job.RESULT_SOURCE})


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class PlacementSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("onbid-public-sale").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def items(self):
        return self.spark.createDataFrame([
            ("X-1", 1, CURRENT, None, T1, 1),            # open, on a current parcel
            ("X-2", 1, RETIRED, None, T1, 2),            # listed on a parcel since re-lotted
            ("X-2", 1, None, None, T2, 3),               # a later blank PNU does not erase it
            ("X-3", 1, STRANDED, None, T1, 4),
            ("X-3", 1, STRANDED, "removed", T3, 5),      # closed: the marker keeps its PNU
            ("X-4", 1, None, None, T1, 6),               # never had a PNU
        ], ITEM_SCHEMA)

    def test_a_round_keeps_the_pnu_of_its_newest_row_that_has_one(self):
        rows = {r.cltr_mng_no: r for r in job.rounds(self.items()).collect()}
        self.assertEqual(rows["X-2"].source_pnu, RETIRED)
        self.assertEqual((rows["X-3"].source_pnu, rows["X-3"].is_open), (STRANDED, False))
        self.assertTrue(rows["X-1"].is_open)
        self.assertIsNone(rows["X-4"].source_pnu)

    def test_a_bid_result_takes_its_rounds_pnu(self):
        results = self.spark.createDataFrame([
            ("X-3", 1, 100, T3, 1), ("X-3", 1, 120, datetime(2099, 1, 2), 2), ("X-9", 1, 5, T3, 3)], RESULT_SCHEMA)
        rows = {r.cltr_mng_no: r for r in job.result_rounds(results, job.rounds(self.items())).collect()}
        self.assertEqual((rows["X-3"].source_pnu, rows["X-3"].winning_bid_amount_krw), (STRANDED, 120))
        self.assertIsNone(rows["X-9"].source_pnu, "a result whose round was never listed stays visible, unplaced")

    def test_a_retired_pnu_follows_lineage_to_every_successor_and_a_stranded_one_is_flagged(self):
        current = self.spark.createDataFrame([(CURRENT,), (SPLIT_A,), (SPLIT_B,)], "pnu string")
        candidates = self.spark.createDataFrame([
            (SPLIT_A, RETIRED, 1, "origin:split:confirmed"), (SPLIT_B, RETIRED, 1, "origin:split:confirmed"),
            (CURRENT, RETIRED, 2, "lineage:confirmed")], "successor_pnu string, source_pnu string, hop int, path string")
        placed = job.placed(job.rounds(self.items()), current, candidates).collect()
        where = sorted((r.cltr_mng_no, r.pnu, r.attached_via) for r in placed)
        self.assertEqual(where, sorted([
            ("X-1", CURRENT, None),
            ("X-2", CURRENT, "lineage:confirmed"),          # in addition to CURRENT's own X-1
            ("X-2", SPLIT_A, "origin:split:confirmed"),
            ("X-2", SPLIT_B, "origin:split:confirmed"),
            ("X-3", STRANDED, job.LINEAGE_MISSING),         # no path: kept, flagged, never dropped
        ]))

    def test_without_lineage_every_retired_sale_is_kept_and_flagged(self):
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        placed = {(r.cltr_mng_no, r.pnu, r.attached_via) for r in job.placed(job.rounds(self.items()), current, None).collect()}
        self.assertEqual(placed, {("X-1", CURRENT, None), ("X-2", RETIRED, job.LINEAGE_MISSING),
                                  ("X-3", STRANDED, job.LINEAGE_MISSING)})


if __name__ == "__main__":
    unittest.main()
