"""Every parcel source's Gold row carries its parcel's place id (root ADR-0483, ADR-0288 §5).

Synthetic PNUs (prefix 99999). Pins: the kind lines that name `place_identity` are the parcel sources'
Gold, and their Gold columns put the join columns after `attached_via`; a PNU takes the id of its open
bridge period, else its latest closed one, never a code-pair line's or an admin id; the digest covers the
join columns; a 본번 group row carries its group's ids; a run recomputes the PNUs a release's appended
changelog rows name, or the ids that differ when the span is not appends only; the 실거래 contract's one
below-bar switch is off, and shown it links the land deals as their own grade (the screen's label is
held to the contract's by the web's own test).
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import by_pnu_kinds  # noqa: E402
import place_id_join as join  # noqa: E402
import place_identity_resolver as resolver  # noqa: E402

DONG = "9999910100"
SOURCES = {"parcel-public-auction", "parcel-court-auction", "parcel-factories", "parcel-nearby",
           "parcel-real-transaction"}


def pnu(main, sub=0):
    return f"{DONG}1{main:04d}{sub:04d}"


class KindLineTest(unittest.TestCase):
    def test_the_parcel_sources_kinds_carry_place_id_and_no_other_kind_does(self):
        carrying = {lane for lane, kind in by_pnu_kinds.kinds().items() if kind.place_identity}
        self.assertEqual(carrying, SOURCES)
        self.assertEqual(by_pnu_kinds.kind("parcel-real-transaction").place_identity,
                         ("place_id", "main_lot_group_place_ids"))

    def test_the_join_columns_follow_attached_via_and_precede_the_lineage(self):
        kind = by_pnu_kinds.kind("parcel-factories")
        self.assertEqual(join.gold_columns(kind), ("pnu", "factories_json", "factory_total", "attached_via",
                                                   "place_id", "row_digest", "source_snapshot_id", "published_at_utc"))
        self.assertEqual(join.built(join.gold_columns(kind)),
                         ("pnu", "factories_json", "factory_total", "attached_via", "row_digest",
                          "source_snapshot_id", "published_at_utc"))

    def test_a_line_naming_an_unknown_join_or_no_place_id_is_refused(self):
        import tempfile

        for joins in (["main_lot_group_place_ids"], ["place_id", "pnu_group"]):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "contract.json"
                path.write_text(json.dumps({"by_pnu_lanes": {"parcel-x": {
                    "host": "parcel_by_pnu_gateway", "section": "x", "columns": ["x_json"], "place_identity": joins}}}),
                    encoding="utf-8")
                with self.assertRaises(ValueError):
                    by_pnu_kinds.kinds(path)


def wilson_low(right, rows, z=1.96):
    """The low end of the 95% Wilson interval of right/rows (root ADR-0370)."""
    import math

    p = right / rows
    return (p + z * z / (2 * rows) - z * math.sqrt(p * (1 - p) / rows + z * z / (4 * rows * rows))) / (1 + z * z / rows)


class BelowBarSwitchTest(unittest.TestCase):
    def contract(self):
        import rt_molit_real_transaction as rt

        return rt.load_source_contract()["place_inference"]

    def test_the_owner_turned_the_switch_on_and_land_deals_link_by_the_group_level(self):
        # The owner's decision, 2026-11-11 (option A): the masked 토지 deals show as label_ko.
        inference = self.contract()
        self.assertIs(inference["below_bar"]["shown"], True)
        [level] = resolver.enabled_levels(inference, "real_transaction_land_trade")
        self.assertEqual(level["grade"], resolver.MAIN_LOT_GROUP_BELOW_BAR)
        off = {**inference, "below_bar": {**inference["below_bar"], "shown": False}}
        self.assertEqual(resolver.enabled_levels(off, "real_transaction_land_trade"), [])

    def test_the_label_says_the_measured_precision_not_a_written_number(self):
        import re

        below = self.contract()["below_bar"]
        [percent] = re.findall(r"(\d+)%", below["label_ko"])
        for dataset, measured in below["measured"].items():
            self.assertEqual(int(percent), round(measured["precision"] * 100), dataset)
            for holdout in measured.get("holdouts", []):
                for right, low in (("right", "precision_wilson_low"), ("group_right", "group_precision_wilson_low")):
                    self.assertAlmostEqual(wilson_low(holdout[right], holdout["rows"]), holdout[low], places=4)

    def test_shown_it_links_the_land_deals_by_the_group_level_under_its_own_grade(self):
        inference = self.contract()
        shown = {**inference, "below_bar": {**inference["below_bar"], "shown": True}}
        [level] = resolver.enabled_levels(shown, "real_transaction_land_trade")
        group = next(level for level in inference["levels"] if level["grade"] == resolver.MAIN_LOT_GROUP)
        self.assertEqual(level["grade"], resolver.MAIN_LOT_GROUP_BELOW_BAR)
        self.assertEqual(level["evidence"], group["evidence"])
        # A dataset already over the bar keeps its own grade: the switch adds nothing there.
        self.assertNotIn(resolver.MAIN_LOT_GROUP_BELOW_BAR,
                         [lv["grade"] for lv in resolver.enabled_levels(shown, "real_transaction_industrial_trade")])

    def test_a_measurement_under_the_floor_is_not_shown(self):
        inference = self.contract()
        measured = inference["below_bar"]["measured"]["real_transaction_land_trade"]
        low = {**inference, "below_bar": {**inference["below_bar"], "shown": True,
                                         "measured": {"real_transaction_land_trade": {**measured, "precision_wilson_low": 0.9}}}}
        self.assertEqual(resolver.enabled_levels(low, "real_transaction_land_trade"), [])

    def test_the_recorded_bound_is_the_wilson_low_of_its_counts(self):
        measured = self.contract()["below_bar"]["measured"]["real_transaction_land_trade"]
        self.assertAlmostEqual(wilson_low(measured["right"], measured["rows"]), measured["precision_wilson_low"], places=4)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class PlaceIdSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("place-id-join").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def bridge(self, rows):
        return self.spark.createDataFrame(
            rows, "unit string, scope string, place_id string, code string, valid_from string, valid_to string")

    def test_a_code_takes_its_open_period_else_its_latest_closed_one(self):
        ids = join.place_ids(self.bridge([
            ("parcel", "99", "open", pnu(1), "2026-06-01", None),
            ("parcel", "99", "older", pnu(1), "2020-01-01", "2026-06-01"),
            ("parcel", "99", "closed-old", pnu(2), "2019-01-01", "2020-01-01"),
            ("parcel", "99", "closed-new", pnu(2), "2020-01-01", "2024-01-01"),
            # A code pair's line is another code's form of an id, and admin ids are not parcels.
            ("parcel", "99", "pair", pnu(3), join.PAIR_SOURCE, join.PAIR_SOURCE),
            ("admin", "99", "dong", pnu(4), "2026-06-01", None),
        ]))
        self.assertEqual({r.pnu: r.place_id for r in ids.collect()}, {pnu(1): "open", pnu(2): "closed-new"})

    def test_the_join_columns_land_and_the_digest_covers_them(self):
        from pyspark.sql import functions as F

        content = ("pnu", "x_json", "attached_via", "place_id", "main_lot_group_place_ids")
        columns = (*content, *join.LINEAGE)
        gold = self.spark.createDataFrame(
            [(pnu(1), json.dumps({"main_lot_group_pnus": [pnu(1), pnu(1, 1)]}), None, "d", "s", "t"),
             (pnu(5), json.dumps({"main_lot_group_pnus": []}), None, "d", "s", "t")],
            "pnu string, x_json string, attached_via string, row_digest string, source_snapshot_id string, "
            "published_at_utc string")
        ids = self.spark.createDataFrame([(pnu(1), "A"), (pnu(1, 1), "B"), (pnu(5), "E")], "pnu string, place_id string")
        group = F.from_json(F.get_json_object("x_json", "$.main_lot_group_pnus"), "array<string>")
        rows = {r.pnu: r for r in join.with_place_ids(gold, ids, content, columns, group_pnus=group).collect()}
        self.assertEqual((rows[pnu(1)].place_id, json.loads(rows[pnu(1)].main_lot_group_place_ids)), ("A", ["A", "B"]))
        self.assertEqual((rows[pnu(5)].place_id, rows[pnu(5)].main_lot_group_place_ids), ("E", None))
        moved = {r.pnu: r for r in join.with_place_ids(
            gold, ids.replace("A", "Z", "place_id"), content, columns, group_pnus=group).collect()}
        self.assertNotEqual(moved[pnu(1)].row_digest, rows[pnu(1)].row_digest)
        self.assertEqual(moved[pnu(5)].row_digest, rows[pnu(5)].row_digest)

    def test_a_span_that_is_not_appends_only_compares_the_ids(self):
        old = self.spark.createDataFrame([(pnu(1), "A"), (pnu(2), "B")], "pnu string, place_id string")
        new = self.spark.createDataFrame([(pnu(1), "A"), (pnu(2), "C"), (pnu(3), "D")], "pnu string, place_id string")
        found = join.changed_pnus(self.spark, [join.BRIDGE_SOURCE], [], "unused", {}, {}, lambda: old, new)
        self.assertEqual({r.pnu for r in found.collect()}, {pnu(2), pnu(3)})
        self.assertIsNone(join.changed_pnus(self.spark, ["silver.x"], [], "unused", {}, {}, lambda: old, new))


if __name__ == "__main__":
    unittest.main()
