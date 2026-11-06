"""실거래 by-PNU kind (root ADR-0350, ADR-0351, ADR-0371).

Synthetic codes and PNUs (prefix 99999). Pins: the build places every deal from its address and lot
on each run and never reads the Silver's pnu; a 법정동 code change re-places a deal already loaded
under the old code onto the parcel of the new one, and the change reaches exactly the PNUs under
both codes; a factory parcel carries its 법정동's 공장창고 deals, masked lots included; the written
JSON's paths are exactly the registry's (root ADR-0336); the digest holds content only; a deal linked
to a 본번 group shows on every parcel of the group and once among the comparables (root ADR-0405).
"""

import json
import os
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import real_transaction_silver_to_gold as job  # noqa: E402

LANE = "parcel-real-transaction"
OLD, NEW, OTHER = "9999910100", "9999920100", "9999910200"
OLD_NAME, NEW_NAME, OTHER_NAME = "합성도 합성시 가동", "합성도 합성시 신구 가동", "합성도 합성시 나동"
GOLD_COLUMNS = ("pnu", "real_transaction_json", "transaction_total", "attached_via", "row_digest",
                "source_snapshot_id", "published_at_utc")
INFERENCE = {"precision_bar": 0.99, "min_measured_rows": 200, "area_tolerance_m2": 0.05,
             "ownership_window_days": [0, 120], "ownership_causes": ["소유권이전"], "levels": []}


def pnu(code, main, sub=0):
    return f"{code}1{main:04d}{sub:04d}"


SNAPSHOT = [(OLD, OLD_NAME, "존재"), (NEW, NEW_NAME, "존재"), (OTHER, OTHER_NAME, "존재")]
REORGANIZED = [{"kind": "pair", "old_code": OLD, "new_code": NEW, "level": "eupmyeondong",
                "effective_date": "2098-01-01", "source": "synthetic"}]


class InputsTest(unittest.TestCase):
    def test_the_kind_line_names_this_producer(self):
        import by_pnu_kinds

        kind = by_pnu_kinds.kind(LANE)
        self.assertEqual((kind.producer, kind.section, kind.columns),
                         (job.JOB_NAME, job.SECTION, ("real_transaction_json", "transaction_total")))
        self.assertFalse(kind.follows_host_panel)

    def test_no_input_rebuilds_the_kind_whole(self):
        required, optional, anchor, whole = job.lane_inputs(LANE)
        self.assertIn(job.RT_SOURCE, required)
        self.assertIn(job.CODE_CHANGE_SOURCE, required)
        self.assertEqual((optional, anchor, whole), ({job.LINEAGE_SOURCE: "--no-carry-lineage"}, job.PARCEL_SOURCE, ()))

    def test_the_build_never_reads_the_silver_s_placement(self):
        # Root ADR-0351 §3: the Silver's pnu and code are the load day's evidence, never the link.
        for column in ("pnu", "legal_dong_code", "place_identity_grade", "pnu_unresolved_reason"):
            self.assertNotIn(column, job.BUILD_COLUMNS[job.RT_SOURCE])
            self.assertNotIn(column, job.DEAL_COLUMNS)

    def test_the_parcel_universe_is_the_served_edition(self):
        src = Path(job.__file__).read_text(encoding="utf-8")
        self.assertIn("panel.read_served_parcels(spark, args, at, region=False)", src)

    def test_parse_args_accepts_the_flags_the_harness_passes(self):
        args = job.parse_args([
            "--lane", LANE, "--iceberg-snapshot-id", "1", "--source-snapshots-path", "/x", "--validate-only",
            "--section-coverage-snapshot", "123", "--max-section-coverage-drop", "0.05",
            "--section-coverage-min-pnus", "1000", "--no-carry-lineage"])
        self.assertEqual((args.section_coverage_snapshot, args.carry_lineage), ("123", False))

    def test_a_new_month_reaches_the_dongs_whose_window_lost_a_month(self):
        self.assertEqual(job.aged_out_months(None, date(2098, 5, 3)), [])
        self.assertEqual(job.aged_out_months(date(2098, 5, 1), date(2098, 5, 30)), [])
        self.assertEqual(job.aged_out_months(date(2098, 4, 28), date(2098, 6, 2)),
                         [(date(2095, 4, 1), date(2095, 6, 1)), (date(2097, 4, 1), date(2097, 6, 1))])

    def test_an_address_takes_its_current_code_through_the_pairs_in_force(self):
        rows, remap, ambiguous = job.address_codes(SNAPSHOT, REORGANIZED)
        self.assertEqual(remap, {OLD: NEW})
        self.assertEqual(ambiguous, set())
        self.assertIn((OLD_NAME, OLD, NEW), rows)
        self.assertIn((OTHER_NAME, OTHER, OTHER), rows)
        # Without the pair the old name keeps its own code.
        self.assertIn((OLD_NAME, OLD, OLD), job.address_codes(SNAPSHOT, [])[0])


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class DocumentSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("real-transaction-gold")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    DEAL_SCHEMA = ("dataset_slug string, deal_ym string, row_key string, deal_date date, deal_amount_10k_krw long, "
                   "deposit_10k_krw long, monthly_rent_10k_krw long, rent_kind string, address_raw string, "
                   "jibun_raw string, bonbun string, bubun string, building_type string, building_use string, "
                   "building_name string, building_area_m2 double, building_area_kind string, land_area_m2 double, "
                   "land_area_kind string, floor string, build_year long, land_use string, jimok string, "
                   "share_dealing_type string, dealing_gbn string, cancelled boolean, cancel_date date")

    def deal(self, key, address, jibun, day, dataset=job.INDUSTRIAL, amount=50000, land=1000.0, building=500.0,
             cancelled=False):
        bonbun = jibun if jibun.isdigit() else ""
        return (dataset, day.strftime("%Y%m"), key, day, amount, None, None, None, address, jibun, bonbun, "0" if bonbun else "",
                "일반", "공장", None, building, "total_floor", land, "plottage", None, 2001, "계획관리", None, None, "중개거래",
                cancelled, date(2098, 4, 1) if cancelled else None)

    def frames(self, deals, changes, parcels, land):
        rows, _remap, _ambiguous = job.address_codes(SNAPSHOT, changes)
        deal_frame = job.with_codes(self.spark.createDataFrame(deals, self.DEAL_SCHEMA), job.code_frame(self.spark, rows))
        change_frame = self.spark.createDataFrame(
            [(c["kind"], c["old_code"], c["new_code"], c["level"], c["effective_date"], c["source"]) for c in changes],
            "kind string, old_code string, new_code string, level string, effective_date string, source string")
        return {
            job.RT_SOURCE: deal_frame.select(*job.BUILD_COLUMNS[job.RT_SOURCE]),
            job.PARCEL_SOURCE: self.spark.createDataFrame([(p,) for p in parcels], "pnu string"),
            job.CODE_CHANGE_SOURCE: change_frame,
            job.LEGAL_DONG_SOURCE: self.spark.createDataFrame(SNAPSHOT, "region_cd string, full_name string, status string"),
            job.LAND_SOURCE: self.spark.createDataFrame(land, "pnu string, land_category string, area_m2 double, zone_name_1 string"),
            job.TITLE_SOURCE: self.spark.createDataFrame([], "pnu string, approval_year int, floor_area_m2 double"),
            job.OWNERSHIP_SOURCE: self.spark.createDataFrame(
                [], "pnu string, ownership_changed_on string, ownership_change_reason string"),
        }

    def documents(self, frames, published="2098-05-03T00:00:00Z"):
        gold = job.gold_rows(frames, INFERENCE, date.fromisoformat(published[:10]), "snap-1", published, GOLD_COLUMNS)
        return {r.pnu: (json.loads(r.real_transaction_json), r.transaction_total, r.row_digest) for r in gold.collect()}

    def test_a_legal_dong_code_change_re_places_a_deal_already_loaded(self):
        # 2098-01: 가동 (OLD) became 신구 가동 (NEW); the served parcels carry the new code only. The
        # deal was loaded under the old name before the pair was recorded.
        factory = pnu(NEW, 12)
        deals = [self.deal("a-1", OLD_NAME, "12", date(2098, 3, 10))]
        land = [(factory, "공장용지", 1000.0, "계획관리지역")]
        before = self.documents(self.frames(deals, [], [factory], land))
        # Without the pair the deal names a parcel the edition does not hold and a 법정동 the factory
        # parcel is not in: no document carries it, so the factory parcel has none.
        self.assertNotIn(factory, before)
        old_frames = self.frames(deals, [], [factory], land)
        new_frames = self.frames(deals, REORGANIZED, [factory], land)
        after = self.documents(new_frames)
        (moved,) = after[factory][0]["transactions"]
        self.assertEqual((moved["place_identity_grade"], moved["jibun_raw"]), ("code_superseded", "12"))
        self.assertEqual(after[factory][1], 1)
        self.assertEqual([c["pnu"] for c in after[factory][0]["comparables"]], [factory])
        # The change reaches both codes' parcels through views.changed_codes (root ADR-0329 §3), and
        # no other 법정동's.
        dongs, prefixes = job.affected_dongs([job.CODE_CHANGE_SOURCE], [], old=lambda: old_frames, new=new_frames,
                                             appended_deals=None, aged=[])
        self.assertIsNone(dongs)
        self.assertEqual(prefixes, ["99999101", "99999201"])
        other = pnu(OTHER, 1)
        parcels = self.spark.createDataFrame([(factory,), (other,)], "pnu string")
        reached = {r.pnu for r in job.affected_pnus(dongs, prefixes, [parcels]).collect()}
        self.assertEqual(reached, {factory})

    def test_a_factory_parcel_carries_its_dongs_factory_deals_masked_ones_too(self):
        factory, house = pnu(OTHER, 5), pnu(OTHER, 7)
        deals = [self.deal("m-1", OTHER_NAME, "3**", date(2098, 4, 2)),
                 self.deal("o-1", OTHER_NAME, "5", date(2098, 2, 1), land=800.0, building=400.0),
                 self.deal("c-1", OTHER_NAME, "3**", date(2098, 4, 20), cancelled=True),
                 self.deal("old", OTHER_NAME, "3**", date(2094, 1, 5)),
                 self.deal("h-1", OTHER_NAME, "7", date(2098, 3, 1), dataset="real_transaction_detached_house_trade")]
        land = [(factory, "공장용지", 800.0, "계획관리지역"), (house, "대", 300.0, "계획관리지역")]
        docs = self.documents(self.frames(deals, [], [factory, house], land))
        document, total, _ = docs[factory]
        self.assertEqual(total, 1)
        self.assertEqual([c["jibun_raw"] for c in document["comparables"]], ["3**", "5"])
        self.assertEqual([c["pnu"] for c in document["comparables"]], [None, factory])
        (industrial,) = [d for d in document["region"]["datasets"] if d["dataset_slug"] == job.INDUSTRIAL]
        self.assertEqual((industrial["deal_count"], industrial["cancelled_count"]), (3, 1))
        self.assertEqual(industrial["land_price_per_m2_10k_krw"]["median"], 56.3)
        # A house parcel shows its own deal and no factory comparables.
        house_doc, house_total, _ = docs[house]
        self.assertEqual((house_total, house_doc["comparables"]), (1, []))

    def test_a_main_lot_group_deal_shows_on_every_parcel_of_the_group(self):
        # Root ADR-0405: the evidence leaves 300; the deal is linked to the 본번 300 group (300, 300-1),
        # not to 300 alone, and 301 (another 본번) does not carry it.
        pick, neighbour, other = pnu(OTHER, 300), pnu(OTHER, 300, 1), pnu(OTHER, 301)
        deals = [self.deal("m-1", OTHER_NAME, "3**", date(2098, 4, 2), land=800.0)]
        land = [(pick, "공장용지", 800.0, "계획관리지역"), (neighbour, "공장용지", 500.0, "계획관리지역"),
                (other, "공장용지", 900.0, "계획관리지역")]
        frames = self.frames(deals, [], [pick, neighbour, other], land)
        frames[job.OWNERSHIP_SOURCE] = self.spark.createDataFrame(
            [(pick, "2098-05-01", "소유권이전")], "pnu string, ownership_changed_on string, ownership_change_reason string")
        inference = {**INFERENCE, "levels": [{
            "grade": "inferred_same_main_lot_group",
            "evidence": ["lot_range", "zone", "jimok", "land_area", "ownership_change"],
            "measured": {job.INDUSTRIAL: {"precision": 0.9963, "precision_wilson_low": 0.9953, "rows": 19483}}}]}
        gold = job.gold_rows(frames, inference, date(2098, 5, 3), "snap-1", "2098-05-03T00:00:00Z", GOLD_COLUMNS)
        docs = {r.pnu: (json.loads(r.real_transaction_json), r.transaction_total) for r in gold.collect()}
        for parcel in (pick, neighbour):
            document, total = docs[parcel]
            (deal,) = document["transactions"]
            self.assertEqual((deal["place_identity_grade"], total), ("inferred_same_main_lot_group", 1))
            self.assertEqual(document["main_lot_group_pnus"], [pick, neighbour])
        other_doc, other_total = docs[other]
        self.assertEqual((other_doc["transactions"], other_total, other_doc["main_lot_group_pnus"]), ([], 0, []))
        # One comparable for the deal: the group's key and size, no one PNU; counted once in the region.
        (comparable,) = other_doc["comparables"]
        self.assertEqual((comparable["pnu"], comparable["main_lot_group_key"], comparable["main_lot_group_size"]),
                         (None, pick[:15], 2))
        (industrial,) = [d for d in other_doc["region"]["datasets"] if d["dataset_slug"] == job.INDUSTRIAL]
        self.assertEqual(industrial["deal_count"], 1)

    def test_a_document_carries_exactly_the_registry_s_fields(self):
        import by_pnu_kinds

        factory = pnu(OTHER, 5)
        deals = [self.deal("o-1", OTHER_NAME, "5", date(2098, 2, 1)),
                 self.deal("l-1", OTHER_NAME, "5", date(2098, 3, 1), dataset=job.LAND, building=None)]
        docs = self.documents(self.frames(deals, [], [factory], [(factory, "공장용지", 1000.0, "계획관리지역")]))
        written = set()
        for document, _total, _digest in docs.values():
            written |= by_pnu_kinds.json_paths(document, "real_transaction")
        recorded = {p for p in by_pnu_kinds.document_record("parcel_real_transaction_by_pnu")
                    if p.startswith("real_transaction.")}
        self.assertEqual(written, recorded)

    def test_the_digest_holds_content_only(self):
        factory = pnu(OTHER, 5)
        deals = [self.deal("o-1", OTHER_NAME, "5", date(2098, 2, 1))]
        frames = self.frames(deals, [], [factory], [(factory, "공장용지", 1000.0, "계획관리지역")])
        first = job.gold_rows(frames, INFERENCE, date(2098, 5, 3), "snap-1", "2098-05-03T00:00:00Z", GOLD_COLUMNS).first()
        again = job.gold_rows(frames, INFERENCE, date(2098, 5, 28), "snap-2", "2098-05-28T00:00:00Z", GOLD_COLUMNS).first()
        self.assertEqual(first.row_digest, again.row_digest)


if __name__ == "__main__":
    unittest.main()
