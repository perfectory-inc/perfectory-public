"""법원경매 (court auction) by-PNU kind: which PNU each item is served under (root ADR-0287).

Synthetic PNUs (prefix 99999) and made-up case numbers. Pins: an item takes the PNU of its current
row (the contract's `current_rows`: newest per key by source_snapshot_id), and an item whose current
row is a removal marker is dropped; an item on a retired PNU follows the lineage to every current
successor; a stranded one is kept, flagged; an item is never dropped. The served document holds only
the items in progress with a sale date still ahead, soonest first, cut at the document ceiling, with
codes as codes and the source attribution the 공공저작물 policy requires (저작권법 제24조의2, root
ADR-0281 §6). Mirrors test_onbid_public_auction_gold.py; court has no media index and no usage-name
reference, so those onbid tests are absent here.
"""

import os
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import court_auction_silver_to_gold as job  # noqa: E402

DONG = "9999910100"


def pnu(main):
    return f"{DONG}1{main:04d}0000"


CURRENT, RETIRED, SPLIT_A, SPLIT_B, STRANDED = pnu(1), pnu(2), pnu(3), pnu(4), pnu(5)
ITEM_SCHEMA = ("court_office_code string, case_no string, property_seq int, item_seq int, "
               "pnu string, row_state string, source_snapshot_id string")


def item(case, pnu_value, row_state, snap):
    # One 물건 (court office B, case, property 1, item 1); `case` is its discriminator in the tests.
    return ("B", case, 1, 1, pnu_value, row_state, snap)


class InputsTest(unittest.TestCase):
    def test_the_rebuild_reads_its_own_silver_and_carries_lineage_optionally(self):
        required, optional, anchor, whole = job.lane_inputs("parcel-court-auction")
        self.assertEqual(anchor, job.PARCEL_SOURCE)
        # The code change table is read so every PNU follows a code change (root ADR-0382 §2).
        self.assertEqual(set(required), {job.PARCEL_SOURCE, job.ITEM_SOURCE, job.CODE_CHANGE_SOURCE})
        # The case photos are optional, like 공매's media (root ADR-0384).
        self.assertEqual(optional, {job.LINEAGE_SOURCE: "--no-carry-lineage", job.PHOTO_SOURCE: "--no-photos"})
        # The item table maps its changed keys to PNUs; a parcel edition or lineage change moves
        # items their rows do not name, so those two rebuild the kind whole (root ADR-0312 §2).
        self.assertEqual(set(whole), {job.PARCEL_SOURCE, job.LINEAGE_SOURCE})
        self.assertEqual(set(job.declared_sources("parcel-court-auction", True, True)), set(required) | set(optional),
                         "the harness pins every required input and every present optional")
        self.assertEqual(job.declared_sources("parcel-court-auction", False, False),
                         (job.PARCEL_SOURCE, job.ITEM_SOURCE, job.CODE_CHANGE_SOURCE))

    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind("parcel-court-auction")
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, "court_auction", ("court_auction_json",)))

    def test_parse_args_accepts_the_section_coverage_flags_the_harness_passes(self):
        # Like 공매, the gold-rebuild harness passes --section-coverage-* to this own-producer (root
        # ADR-0272); it accepts but does not enforce them, or the scheduled rebuild dies on them.
        args = job.parse_args([
            "--lane", "parcel-court-auction", "--iceberg-snapshot-id", "1",
            "--source-snapshots-path", "/x", "--validate-only",
            "--section-coverage-snapshot", "123", "--max-section-coverage-drop", "0.05",
            "--section-coverage-min-pnus", "1000"])
        self.assertEqual(args.section_coverage_snapshot, "123")
        self.assertEqual((args.max_section_coverage_drop, args.section_coverage_min_pnus), (0.05, 1000))

    def test_the_parcel_universe_is_the_served_edition_not_all_editions(self):
        # Like 공매 (root ADR-0148): the kind reads the ONE served cadastral edition, via
        # panel.read_served_parcels, never every edition's current rows from the generic read_source.
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertRegex(
            src, r'read_served_parcels\([^)]*\)\.select\("pnu"\)',
            "경매 parcel universe must come from panel.read_served_parcels (the served edition)")
        self.assertNotRegex(
            src, r'read_source\([^)]*PARCEL_SOURCE[^)]*\)\.select\("pnu"\)',
            "경매 must not build its parcel universe from the all-editions read_source")


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class PlacementSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("court-auction").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def items(self):
        # source_snapshot_id orders the versions; the highest is current (root ADR-0267).
        return self.spark.createDataFrame([
            item("C-1", None, None, "snap-1"),            # an earlier version with no PNU...
            item("C-1", CURRENT, None, "snap-2"),         # ...the current (newest) row carries the PNU
            item("C-2", RETIRED, None, "snap-1"),         # current row on a parcel since re-lotted
            item("C-3", STRANDED, None, "snap-1"),        # current row on a retired parcel no path leaves
            item("C-4", None, None, "snap-1"),            # never had a PNU
            item("C-5", STRANDED, None, "snap-1"),        # an item: its current row...
            item("C-5", STRANDED, "removed", "snap-2"),   # ...is a removal marker, so it has no current row
        ], ITEM_SCHEMA)

    def test_an_item_takes_the_pnu_of_its_current_row(self):
        rows = {r.case_no: r for r in job.rounds(self.items()).collect()}
        self.assertEqual(rows["C-1"].source_pnu, CURRENT)   # the newest version's PNU, not the older blank
        self.assertEqual(rows["C-2"].source_pnu, RETIRED)
        self.assertTrue(rows["C-1"].is_open)
        self.assertIsNone(rows["C-4"].source_pnu)           # never had a PNU: kept, unplaced
        self.assertNotIn("C-5", rows, "an item whose current row is a removal marker has no current row")

    def test_a_retired_pnu_follows_lineage_to_every_successor_and_a_stranded_one_is_flagged(self):
        current = self.spark.createDataFrame([(CURRENT,), (SPLIT_A,), (SPLIT_B,)], "pnu string")
        candidates = self.spark.createDataFrame([
            (SPLIT_A, RETIRED, 1, "origin:split:confirmed"), (SPLIT_B, RETIRED, 1, "origin:split:confirmed"),
            (CURRENT, RETIRED, 2, "lineage:confirmed")], "successor_pnu string, source_pnu string, hop int, path string")
        placed = job.onbid.placed(job.rounds(self.items()), current, candidates).collect()
        where = sorted((r.case_no, r.pnu, r.attached_via) for r in placed)
        self.assertEqual(where, sorted([
            ("C-1", CURRENT, None),
            ("C-2", CURRENT, "lineage:confirmed"),          # in addition to CURRENT's own C-1
            ("C-2", SPLIT_A, "origin:split:confirmed"),
            ("C-2", SPLIT_B, "origin:split:confirmed"),
            ("C-3", STRANDED, job.LINEAGE_MISSING),         # no path: kept, flagged, never dropped
        ]))

    def test_an_item_on_a_parcel_merged_into_a_current_one_is_added_to_it(self):
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        merges = self.spark.createDataFrame([
            (RETIRED, CURRENT, "official"), (STRANDED, CURRENT, "needs_review")],
            "predecessor_pnu string, successor_pnu string, grade string")
        where = {(r.case_no, r.pnu, r.attached_via)
                 for r in job.onbid.placed(job.rounds(self.items()), current, None, merges).collect()}
        self.assertEqual(where, {("C-1", CURRENT, None), ("C-2", CURRENT, "merged:official"),
                                 ("C-3", STRANDED, job.LINEAGE_MISSING)})

    def test_the_servable_gold_drops_a_stranded_item_with_no_parcel_document(self):
        # Root ADR-0292: a lineage_missing item has no parcel document to attach to, so it is never
        # written to the servable Gold, though placed() still flags it (counter and silver keep it).
        current = self.spark.createDataFrame([(CURRENT,)], "pnu string")
        placed = job.onbid.placed(job.rounds(self.items()), current, None)
        servable = {(r.case_no, r.pnu, r.attached_via) for r in job.onbid.servable(placed).collect()}
        self.assertEqual(servable, {("C-1", CURRENT, None)})  # C-2, C-3 (lineage_missing) are gone


BUILD = date(2099, 1, 2)
ROUND_SCHEMA = ", ".join(
    ["court_office_code string", "case_no string", "property_seq int", "item_seq int",
     "is_open boolean", "source_pnu string"]
    + [f"{name} {kind}" for name, kind in (
        ("user_case_no", "string"), ("court_name", "string"), ("department_name", "string"),
        ("building_name", "string"), ("case_status_code", "string"), ("item_status_code", "string"),
        ("item_in_progress_yn", "string"), ("usage_lcls_code", "string"), ("usage_mcls_code", "string"),
        ("usage_scls_code", "string"), ("appraisal_amount_krw", "long"), ("minimum_bid_price_krw", "long"),
        ("sale_amount_krw", "long"), ("sale_date", "date"), ("sale_time_raw", "string"),
        ("sale_decision_date", "date"), ("sale_place", "string"), ("failed_bid_count", "int"))])


def round_row(case, is_open, sale_date, in_progress="Y", pnu_value=CURRENT):
    return ("B", case, 1, 1, is_open, pnu_value, "2099타경1", "테스트지방법원", "경매1계", "테스트 물건",
            "2", "1", in_progress, "01", "0101", "010101", 100, 70, None, sale_date, "10:00", None, "제1호 법정", 1)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("court-auction-document").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def rounds(self, rows):
        return self.spark.createDataFrame(rows, ROUND_SCHEMA)

    def test_only_items_in_progress_with_a_sale_date_ahead_are_served(self):
        rows = self.rounds([
            round_row("OPEN", True, date(2099, 1, 5)),
            round_row("CLOSED", False, date(2099, 1, 5)),            # removal marker: not open
            round_row("DONE", True, date(2099, 1, 1)),               # sale date already passed
            round_row("NOT-INPROG", True, date(2099, 1, 5), in_progress="N"),  # site does not mark it in progress
            round_row("NO-DATE", True, None),                        # the screen could never tell its sale date
        ])
        served = {r.case_no for r in job.served_rounds(rows, BUILD).collect()}
        self.assertEqual(served, {"OPEN"})

    def test_a_document_lists_items_soonest_sale_date_first_with_source_and_attribution(self):
        import json
        from pyspark.sql import functions as F

        rows = self.rounds([round_row("LATE", True, date(2099, 1, 9)), round_row("SOON", True, date(2099, 1, 3))])
        placed = rows.withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(None).cast("string"))
        doc = job.document_rows(placed).first()
        body = json.loads(doc.court_auction_json)
        self.assertEqual([r["case_no"] for r in body["rounds"]], ["SOON", "LATE"])
        self.assertEqual(body["rounds"][0]["case_status_code"], "2", "codes stay codes")
        self.assertIn("sale_date", body["rounds"][0])
        self.assertEqual(body["source_datasets"], list(job.SOURCE_DATASETS))
        self.assertEqual(body["source_attribution"], job.SOURCE_ATTRIBUTION, "the 공공저작물 attribution is carried")
        self.assertEqual((doc.round_count, doc.attached_via), (2, None))

    def test_a_document_carries_exactly_its_records_fields(self):
        # The guard-equivalent path check: the bake serves the JSON as written, so its paths are the
        # registry's current revision (root ADR-0336; by_pnu_document_records.rs SPARK_HELD names this test).
        import json
        import by_pnu_kinds
        from pyspark.sql import functions as F

        rows = self.rounds([round_row("A", True, date(2099, 1, 5))])
        # A round with one photo, so the photo subtree's paths are written too (root ADR-0384).
        photo = F.expr(f"CAST(array(named_struct('sha256', repeat('a', 64), 'media_kind', 'photo', 'kind_code', '1', "
                       f"'sequence', 0, 'title', 'front', 'media_format', 'jpg', 'size_bytes', 100)) AS {job.PHOTO_STRUCT})")
        placed = (rows.withColumn("photos", photo).withColumn("pnu", F.col("source_pnu"))
                  .withColumn("attached_via", F.lit(None).cast("string")))
        written = by_pnu_kinds.json_paths(json.loads(job.document_rows(placed).first().court_auction_json), "court_auction")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_court_auction_by_pnu") if p.startswith("court_auction.")}
        self.assertEqual(written, recorded)

    def test_a_document_over_the_ceiling_keeps_the_soonest_sale_dates(self):
        import json
        from pyspark.sql import functions as F

        rows = self.rounds([round_row(f"R{day}", True, date(2099, 1, day)) for day in range(3, 9)])
        placed = rows.withColumn("pnu", F.col("source_pnu")).withColumn("attached_via", F.lit(None).cast("string"))
        one = len(json.loads(job.document_rows(placed).first().court_auction_json)["rounds"][0].__repr__())
        small = job.DOCUMENT_ENVELOPE_BYTES + 3 * (one + 200)
        body = json.loads(job.document_rows(placed, limit_bytes=small).first().court_auction_json)
        kept = [r["case_no"] for r in body["rounds"]]
        self.assertTrue(0 < len(kept) < 6)
        self.assertEqual(kept, [f"R{day}" for day in range(3, 3 + len(kept))], "the soonest sale dates are kept")

    def test_a_pnu_holding_its_own_item_is_not_attached_and_one_holding_only_carried_items_is(self):
        from pyspark.sql import functions as F

        rows = self.rounds([round_row("OWN", True, date(2099, 1, 5)), round_row("CARRIED", True, date(2099, 1, 6))])
        mixed = rows.withColumn("pnu", F.lit(CURRENT)).withColumn(
            "attached_via", F.when(F.col("case_no") == "CARRIED", F.lit("lineage:confirmed")))
        self.assertIsNone(job.document_rows(mixed).first().attached_via)
        carried = rows.where(F.col("case_no") == "CARRIED").withColumn("pnu", F.lit(SPLIT_A)).withColumn(
            "attached_via", F.lit("origin:split:confirmed"))
        self.assertEqual(job.document_rows(carried).first().attached_via, "origin:split:confirmed")


CHANGE_SCHEMA = ("court_office_code string, case_no string, property_seq int, item_seq int, pnu string, "
                 "row_state string, source_snapshot_id string, item_in_progress_yn string, sale_date date")


def versioned(case, pnu_value, snap, sale_date, row_state=None):
    return ("B", case, 1, 1, pnu_value, row_state, snap, "Y", sale_date)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class AffectedSparkTest(unittest.TestCase):
    """Root ADR-0312 §2: only the PNUs an item change or a passed sale date reaches are rebuilt."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("court-auction-affected").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_a_moved_item_reaches_both_pnus_a_passed_sale_date_its_own_and_nothing_else_is_touched(self):
        import json

        base = [versioned("MOVED", CURRENT, "s1", date(2099, 1, 9)),
                versioned("PASSED", SPLIT_A, "s1", date(2099, 1, 3)),
                versioned("STILL", SPLIT_B, "s1", date(2099, 1, 9))]
        old = {job.ITEM_SOURCE: self.spark.createDataFrame(base, CHANGE_SCHEMA)}
        new = {job.ITEM_SOURCE: self.spark.createDataFrame(base + [versioned("MOVED", RETIRED, "s2", date(2099, 1, 9))],
                                                           CHANGE_SCHEMA),
               job.PARCEL_SOURCE: self.spark.createDataFrame([(p,) for p in (CURRENT, RETIRED, SPLIT_A, SPLIT_B)],
                                                             "pnu string")}

        def served(pnu_value, case):
            body = {"rounds": [{"court_office_code": "B", "case_no": case, "property_seq": 1, "item_seq": 1}]}
            return (pnu_value, json.dumps(body))

        gold = self.spark.createDataFrame([served(CURRENT, "MOVED"), served(SPLIT_A, "PASSED"), served(SPLIT_B, "STILL")],
                                          "pnu string, court_auction_json string")
        how = job.Placement("2099-01-04T00:00:00Z")
        affected = job.affected_pnus(old, new, how, gold, "2099-01-02T00:00:00Z", [job.ITEM_SOURCE])
        self.assertEqual({r.pnu for r in affected.collect()}, {CURRENT, RETIRED, SPLIT_A})


    def test_every_build_carries_an_item_to_the_current_code_and_a_code_change_reaches_it(self):
        # Root ADR-0382 §2 (owner rule 2026-10-31): a PNU read once is not kept; the build carries it
        # through the pairs in force, and a new pair reaches the items under either of its codes.
        from pyspark.sql import functions as F

        old_code, new_code = "9999920100", DONG
        renamed = f"{old_code}100070000"
        ELSEWHERE = "9999930100100090000"  # under neither code of the pair
        changes_schema = "kind string, old_code string, new_code string, level string, effective_date string, source string"
        pair = ("pair", old_code, new_code, "eupmyeondong", "2099-01-03", "derived")
        items = (self.spark.createDataFrame([versioned("RENAMED", None, "s1", date(2099, 1, 9)),
                                             versioned("STILL", ELSEWHERE, "s1", date(2099, 1, 9))], CHANGE_SCHEMA)
                 .withColumn("source_code_pnu_raw", F.when(F.col("case_no") == "RENAMED", F.lit(renamed))))
        old = {job.ITEM_SOURCE: job.located(items, {}),
               job.CODE_CHANGE_SOURCE: self.spark.createDataFrame([], changes_schema)}
        new = {job.ITEM_SOURCE: job.located(items, {old_code: new_code}),
               job.CODE_CHANGE_SOURCE: self.spark.createDataFrame([pair], changes_schema)}
        placed = {r.case_no: r.pnu for r in new[job.ITEM_SOURCE].collect()}
        self.assertEqual(placed, {"RENAMED": f"{new_code}100070000", "STILL": ELSEWHERE},
                         "the source code PNU stands in for an unresolved one and is carried")
        self.assertEqual({r.case_no for r in job.code_change_keys(old, new).collect()}, {"RENAMED"})
        self.assertIsNone(job.code_change_keys(new, new), "no row of the code table differs")


    def test_a_case_s_photos_reach_every_round_of_the_case_in_the_site_s_order(self):
        # Root ADR-0384: the 물건상세 lists photos per case; every property of the case serves them,
        # kind then sequence, and a round of another case gets an empty array.
        import json

        from platform_contracts import load_lakehouse_contract, spark_sql_type

        contract = load_lakehouse_contract(job.PHOTO_SOURCE)
        names = [c["name"] for c in contract["columns"]]
        ddl = ", ".join(f"{c['name']} {spark_sql_type(c['logical_type'])}" for c in contract["columns"])

        def photo(sha, kind, seq):
            row = {"court_office_code": "B", "case_no": "WITH", "sha256": sha * 64, "photo_kind_code": kind,
                   "sequence": seq, "title": f"synthetic{seq}.jpg", "media_format": "jpg", "size_bytes": 10,
                   "source_record_id": "o", "source_line_number": 1, "source_snapshot_id": "s1"}
            return tuple(row.get(n) for n in names)

        photos = self.spark.createDataFrame([photo("b", "000245", 1), photo("a", "000241", 2), photo("c", "000241", 1)], ddl)
        items = self.spark.createDataFrame([versioned("WITH", CURRENT, "s1", date(2099, 1, 9)),
                                            ("B", "WITH", 2, 1, SPLIT_A, None, "s1", "Y", date(2099, 1, 9)),
                                            versioned("WITHOUT", SPLIT_B, "s1", date(2099, 1, 9))], CHANGE_SCHEMA)
        frames = {job.ITEM_SOURCE: items, job.PHOTO_SOURCE: photos,
                  job.PARCEL_SOURCE: self.spark.createDataFrame([(CURRENT,), (SPLIT_A,), (SPLIT_B,)], "pnu string")}
        how = job.Placement("2099-01-04T00:00:00Z")
        docs = {r.pnu: json.loads(r.court_auction_json) for r in job.document_rows(job.placement(frames, how)).collect()}
        order = [p["sha256"][0] for p in docs[CURRENT]["rounds"][0]["photos"]]
        self.assertEqual(order, ["c", "a", "b"], "kind 000241 first, then sequence")
        self.assertEqual(docs[SPLIT_A]["rounds"][0]["photos"], docs[CURRENT]["rounds"][0]["photos"])
        self.assertEqual(docs[SPLIT_B]["rounds"][0]["photos"], [])
        first = docs[CURRENT]["rounds"][0]["photos"][0]
        self.assertEqual((first["media_kind"], first["kind_code"], first["media_format"]), ("photo", "000241", "jpg"))
        # A new photo of the case reaches both of its properties' PNUs and nothing else.
        new = {**frames, job.PHOTO_SOURCE: photos.unionByName(
            self.spark.createDataFrame([photo("d", "000241", 3)], ddl))}
        gold = job.gold_rows(job.placement(frames, how), how.as_of, "p",
                             ("pnu", *job.GOLD_CONTENT, "source_snapshot_id", "published_at_utc", "row_digest"))
        affected = job.affected_pnus(frames, new, how, gold, how.as_of, [job.PHOTO_SOURCE])
        self.assertEqual({r.pnu for r in affected.collect()}, {CURRENT, SPLIT_A})


if __name__ == "__main__":
    unittest.main()
