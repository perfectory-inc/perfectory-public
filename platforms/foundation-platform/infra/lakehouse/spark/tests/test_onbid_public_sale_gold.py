"""공매 (Onbid) by-PNU kind: which PNU each sale is served under (root ADR-0287).

Synthetic PNUs (prefix 99999) and made-up management numbers. Pins: a round takes the PNU of its
current item row (the contract's `current_rows`: newest per key by source_snapshot_id), and a round
whose current row is a removal marker is dropped; a sale on a retired PNU follows the lineage to
every current successor, in addition to the successor's own sales; a retired PNU no path leaves
keeps its sale, flagged; a sale is never dropped. The served document (ADR-0287 §7) holds only the
rounds open as of the build, soonest-closing first, cut at the document ceiling, with codes as codes
and names only from Onbid's own code reference.
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
ITEM_SCHEMA = "cltr_mng_no string, pbct_cdtn_no long, pnu string, row_state string, source_snapshot_id string"


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_carries_lineage_optionally(self):
        required, optional, anchor, whole = job.lane_inputs("parcel-public-sale")
        self.assertEqual(anchor, job.PARCEL_SOURCE)
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.ITEM_SOURCE}, "open rounds only: no bid results read")
        self.assertEqual(optional, {job.LINEAGE_SOURCE: "--no-carry-lineage"})
        self.assertEqual(set(whole), {job.ITEM_SOURCE})

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind("parcel-public-sale")
        self.assertEqual((kind.producer, kind.section, kind.columns), (job.JOB_NAME, "public_sale", ("public_sale_json",)))

    def test_parse_args_accepts_the_section_coverage_flags_the_harness_passes(self):
        # The gold-rebuild harness passes --section-coverage-* to every kind producer whose contract
        # entry sets section_coverage (root ADR-0272). This own-producer must accept them like the
        # panel/by_pnu_kind producers, or the scheduled rebuild dies on "unrecognized arguments"
        # (2026-10-21: it did, once the 공매 kind's entry gained section_coverage). It accepts but does
        # not enforce them: a 시도's open-auction count swings daily and legitimately reaches zero, so
        # main() holds this kind to the row-loss floor, not a per-region gate (root ADR-0292).
        args = job.parse_args([
            "--lane", "parcel-public-sale", "--iceberg-snapshot-id", "1",
            "--source-snapshots-path", "/x", "--validate-only",
            "--section-coverage-snapshot", "123", "--max-section-coverage-drop", "0.05",
            "--section-coverage-min-pnus", "1000"])
        self.assertEqual(args.section_coverage_snapshot, "123")
        self.assertEqual((args.max_section_coverage_drop, args.section_coverage_min_pnus), (0.05, 1000))

    def test_the_parcel_universe_is_the_served_edition_not_all_editions(self):
        # silver.parcel_boundaries holds several cadastral editions side by side; the panel, map and
        # catalog are built from the ONE served edition (root ADR-0148). This own-producer kind is
        # served on the parcel lane, so it must read the SAME served edition — never every edition's
        # current rows. Reading all editions (the generic read_source) attached sales to parcels a
        # newer, unpromoted edition holds but the served map does not: 2026-10-21 it placed 632 PNUs
        # of editions 202609/202610 under the served 202606 set, which the by-PNU serving gate refused.
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertRegex(
            src, r'read_served_parcels\([^)]*\)\.select\("pnu"\)',
            "공매 parcel universe must come from panel.read_served_parcels (the served edition)")
        self.assertNotRegex(
            src, r'read_source\([^)]*PARCEL_SOURCE[^)]*\)\.select\("pnu"\)',
            "공매 must not build its parcel universe from the all-editions read_source")


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
        # source_snapshot_id orders the versions; the highest is current (root ADR-0267).
        return self.spark.createDataFrame([
            ("X-1", 1, None, None, "snap-1"),            # an earlier version with no PNU...
            ("X-1", 1, CURRENT, None, "snap-2"),         # ...the current (newest) row carries the PNU
            ("X-2", 1, RETIRED, None, "snap-1"),         # current row on a parcel since re-lotted
            ("X-3", 1, STRANDED, None, "snap-1"),        # current row on a retired parcel no path leaves
            ("X-4", 1, None, None, "snap-1"),            # never had a PNU
            ("X-5", 1, STRANDED, None, "snap-1"),        # a closed round: its current row...
            ("X-5", 1, STRANDED, "removed", "snap-2"),   # ...is a removal marker, so it has no current row
        ], ITEM_SCHEMA)

    def test_a_round_takes_the_pnu_of_its_current_row(self):
        # Current-row semantics (root ADR-0267 current_rows): a round's PNU is the PNU of its current
        # item row — the newest per key by source_snapshot_id — with no scan back for an earlier row
        # that had one. Measured 2026-10-20 on production Silver (r2.silver.onbid_real_estate_item,
        # 79,246 items): 0 had a blank PNU on the current row while an earlier version had one, so no
        # attachable sale loses its PNU; pnu_null_share_guard catches any future source change.
        rows = {r.cltr_mng_no: r for r in job.rounds(self.items()).collect()}
        self.assertEqual(rows["X-1"].source_pnu, CURRENT)   # the newest version's PNU, not the older blank
        self.assertEqual(rows["X-2"].source_pnu, RETIRED)
        self.assertTrue(rows["X-1"].is_open)
        self.assertIsNone(rows["X-4"].source_pnu)           # never had a PNU: kept, unplaced
        self.assertNotIn("X-5", rows, "a round whose current row is a removal marker has no current row")

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

    def test_a_sale_on_a_parcel_merged_into_a_current_one_is_added_to_it(self):
        # ADR-0287 §3: the carry never crosses a merge (one attribute, several originals); a sale is
        # added, so the merged land shows it, labelled; a link not yet in effect is not followed.
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        merges = self.spark.createDataFrame([
            (RETIRED, CURRENT, "official"), (STRANDED, CURRENT, "needs_review")],
            "predecessor_pnu string, successor_pnu string, grade string")
        where = {(r.cltr_mng_no, r.pnu, r.attached_via)
                 for r in job.placed(job.rounds(self.items()), current, None, merges).collect()}
        self.assertEqual(where, {("X-1", CURRENT, None), ("X-2", CURRENT, "merged:official"),
                                 ("X-3", STRANDED, job.LINEAGE_MISSING)})

    def test_without_lineage_every_retired_sale_is_kept_and_flagged(self):
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        placed = {(r.cltr_mng_no, r.pnu, r.attached_via) for r in job.placed(job.rounds(self.items()), current, None).collect()}
        self.assertEqual(placed, {("X-1", CURRENT, None), ("X-2", RETIRED, job.LINEAGE_MISSING),
                                  ("X-3", STRANDED, job.LINEAGE_MISSING)})

    def test_the_servable_gold_drops_a_stranded_sale_with_no_parcel_document(self):
        # Root ADR-0292: this kind is a section of the parcel panel's document, so a lineage_missing
        # sale — on a retired PNU the panel serves no document for — has nothing to attach to and is
        # never written to the servable Gold, though placed() still flags it (the counter and silver
        # keep it). Measured 2026-10-22 on production Gold: 593 of 5,617 rows were lineage_missing, so
        # the by-PNU serving gate 404'd 10.6% and refused the publish; servable() leaves 5,024 in panel.
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        placed = job.placed(job.rounds(self.items()), current, None)
        servable = {(r.cltr_mng_no, r.pnu, r.attached_via) for r in job.servable(placed).collect()}
        self.assertEqual(servable, {("X-1", CURRENT, None)})  # X-2, X-3 (lineage_missing) are gone



BUILD = datetime(2099, 1, 2, 0)
ROUND_SCHEMA = ", ".join(
    ["cltr_mng_no string", "pbct_cdtn_no long", "is_open boolean", "source_pnu string"]
    + [f"{name} {kind}" for name, kind in (
        ("onbid_cltr_no", "long"), ("onbid_pbanc_no", "long"), ("pbct_no", "long"), ("pbct_nsq", "string"),
        ("item_name", "string"), ("prpt_div_cd", "string"), ("pbct_stat_cd", "string"), ("dsps_mthod_cd", "string"),
        ("bid_mthod_cd", "string"), ("usg_lcls_ctgr_id", "string"), ("usg_mcls_ctgr_id", "string"),
        ("usg_scls_ctgr_id", "string"), ("appraisal_amount_krw", "long"), ("lowest_bid_price_krw", "long"),
        ("lowest_bid_price_raw", "string"), ("first_bid_price_krw", "long"), ("usbd_nft", "long"),
        ("bid_begins_at", "timestamp"), ("bid_ends_at", "timestamp"), ("land_area_m2", "double"),
        ("building_area_m2", "double"), ("share_item_yn", "string"))])


def round_row(key, is_open, ends, pnu_value=CURRENT, usage="10000"):
    return (key, 1, is_open, pnu_value, 1, 2, 3, "1", "synthetic item", "0007", "0001", "0001", "0001",
            usage, usage + "1", usage + "11", 100, 70, "70", 100, 0, datetime(2099, 1, 1), ends, 10.0, None, "N")


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("onbid-public-sale-document").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def rounds(self, rows):
        return self.spark.createDataFrame(rows, ROUND_SCHEMA)

    def test_only_rounds_open_with_a_close_time_ahead_are_served(self):
        rows = self.rounds([
            round_row("OPEN", True, datetime(2099, 1, 5)),
            round_row("CLOSED", False, datetime(2099, 1, 5)),        # removed from the list
            round_row("ENDED", True, datetime(2099, 1, 1)),          # still listed, but its close time passed
            round_row("NO-END", True, None),                         # the screen could never tell its end
        ])
        served = {r.cltr_mng_no for r in job.served_rounds(rows, BUILD).collect()}
        self.assertEqual(served, {"OPEN"})

    def test_usage_names_come_only_from_onbids_code_reference(self):
        rows = self.rounds([round_row("A", True, datetime(2099, 1, 5))])
        unnamed = job.with_usage_names(rows, None).first()
        self.assertIsNone(unnamed.usg_lcls_ctgr_name, "no reference loaded: a code without a name, never ours")
        codes = self.spark.createDataFrame([("10000", "synthetic-major"), ("100001", "synthetic-middle")], "code string, name string")
        named = job.with_usage_names(rows, codes).first()
        self.assertEqual((named.usg_lcls_ctgr_name, named.usg_mcls_ctgr_name, named.usg_scls_ctgr_name),
                         ("synthetic-major", "synthetic-middle", None))

    def test_a_document_lists_rounds_soonest_closing_first_with_codes_and_sources(self):
        import json
        from pyspark.sql import functions as F

        rows = job.with_usage_names(self.rounds([
            round_row("LATE", True, datetime(2099, 1, 9)), round_row("SOON", True, datetime(2099, 1, 3))]), None)
        placed = rows.withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(None).cast("string"))
        doc = job.document_rows(placed).first()
        body = json.loads(doc.public_sale_json)
        self.assertEqual([r["cltr_mng_no"] for r in body["rounds"]], ["SOON", "LATE"])
        self.assertEqual(body["rounds"][0]["prpt_div_cd"], "0007", "codes stay codes")
        self.assertIn("bid_ends_at", body["rounds"][0])
        self.assertEqual(body["source_datasets"], list(job.SOURCE_DATASETS))
        self.assertEqual((doc.round_count, doc.attached_via), (2, None))

    def test_a_document_carries_exactly_its_records_fields(self):
        # The bake serves the JSON as written; its paths are the record's (contracts/by-pnu-documents).
        import json
        import by_pnu_kinds
        from pyspark.sql import functions as F

        rows = job.with_usage_names(self.rounds([round_row("A", True, datetime(2099, 1, 5))]), None)
        placed = rows.withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(None).cast("string"))
        written = by_pnu_kinds.json_paths(json.loads(job.document_rows(placed).first().public_sale_json), "public_sale")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_public_sale_by_pnu") if p.startswith("public_sale.")}
        self.assertEqual(written, recorded)

    def test_a_document_over_the_ceiling_keeps_the_soonest_closing_rounds(self):
        import json
        from pyspark.sql import functions as F

        rows = job.with_usage_names(self.rounds([
            round_row(f"R{day}", True, datetime(2099, 1, day)) for day in range(3, 9)]), None)
        placed = rows.withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(None).cast("string"))
        one = len(json.loads(job.document_rows(placed).first().public_sale_json)["rounds"][0].__repr__())
        small = job.DOCUMENT_ENVELOPE_BYTES + 3 * (one + 200)
        body = json.loads(job.document_rows(placed, limit_bytes=small).first().public_sale_json)
        kept = [r["cltr_mng_no"] for r in body["rounds"]]
        self.assertTrue(0 < len(kept) < 6)
        self.assertEqual(kept, [f"R{day}" for day in range(3, 3 + len(kept))], "the soonest-closing are kept")

    def test_a_pnu_holding_its_own_round_is_not_attached_and_one_holding_only_carried_rounds_is(self):
        from pyspark.sql import functions as F

        rows = job.with_usage_names(self.rounds([
            round_row("OWN", True, datetime(2099, 1, 5)), round_row("CARRIED", True, datetime(2099, 1, 6))]), None)
        mixed = rows.withColumn("pnu", F.lit(CURRENT)).withColumn(
            "attached_via", F.when(F.col("cltr_mng_no") == "CARRIED", F.lit("lineage:confirmed")))
        self.assertIsNone(job.document_rows(mixed).first().attached_via)
        carried = rows.where(F.col("cltr_mng_no") == "CARRIED").withColumn("pnu", F.lit(SPLIT_A)).withColumn(
            "attached_via", F.lit("origin:split:confirmed"))
        self.assertEqual(job.document_rows(carried).first().attached_via, "origin:split:confirmed")


if __name__ == "__main__":
    unittest.main()
