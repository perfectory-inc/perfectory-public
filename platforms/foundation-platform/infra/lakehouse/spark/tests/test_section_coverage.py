"""A served section may not silently lose a region (root ADR-0272).

Synthetic tables of (section, 시도) counts: a section emptied for one region is refused and named, an
unchanged one passes, and a region a recorded code pair renames passes while emptying it does not.
The 시도 codes (9x) and PNUs are synthetic.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # noqa: F401  Import-only PySpark stub.
import section_coverage as coverage  # noqa: E402
import legal_dong_code_change_views as views  # noqa: E402
import gold_rebuild as rebuild  # noqa: E402
import by_pnu_kinds  # noqa: E402

SHARES = coverage.Bound(max_drop_fraction=0.05, min_region_pnus=100, shares=True)
COUNTS = coverage.Bound(max_drop_fraction=0.05, min_region_pnus=100, shares=False)
SERVED = {
    ("land_rights", "97"): (400, 10_000), ("land_rights", "98"): (600, 20_000), ("land_rights", "99"): (900, 30_000),
    ("price", "97"): (9_000, 10_000), ("price", "98"): (19_000, 20_000), ("price", "99"): (29_000, 30_000),
}


def pair(old, new):
    return {"kind": "pair", "old_code": old, "new_code": new, "level": "sido", "source": "fixture"}


class Breaches(unittest.TestCase):
    def test_an_unchanged_build_passes(self):
        coverage.assert_coverage("gold.x", SERVED, dict(SERVED), [], SHARES)

    def test_a_section_emptied_for_one_region_is_refused_naming_the_section_and_the_region(self):
        built = {**SERVED, ("land_rights", "98"): (0, 20_000)}
        with self.assertRaises(ValueError) as refused:
            coverage.assert_coverage("gold.x", SERVED, built, [], SHARES)
        message = str(refused.exception)
        self.assertIn("land_rights in 시도 98", message)
        self.assertIn("nothing was written", message)
        self.assertNotIn("price", message)
        self.assertNotIn("시도 97", message)

    def test_the_row_count_unchanged_does_not_hide_the_loss(self):
        # Every PNU keeps its row (the row floor sees nothing); only the section is gone.
        built = {**SERVED, ("land_rights", "99"): (0, 30_000)}
        self.assertTrue(coverage.breaches(SERVED, built, [], SHARES))

    def test_a_drop_within_the_bound_passes_and_past_it_is_refused(self):
        self.assertEqual(coverage.breaches(SERVED, {**SERVED, ("price", "99"): (27_600, 30_000)}, [], SHARES), [])
        self.assertTrue(coverage.breaches(SERVED, {**SERVED, ("price", "99"): (27_500, 30_000)}, [], SHARES))

    def test_a_region_too_small_to_judge_is_judged_only_inside_the_whole_table(self):
        served = {("forest", "97"): (50, 1_000), ("forest", "98"): (5_000, 20_000)}
        self.assertEqual(coverage.breaches(served, {**served, ("forest", "97"): (0, 1_000)}, [], SHARES), [])
        tiny = {("forest", "97"): (50, 1_000), ("forest", "98"): (60, 20_000)}
        found = coverage.breaches(tiny, {("forest", "97"): (0, 1_000), ("forest", "98"): (60, 20_000)}, [], SHARES)
        self.assertEqual(len(found), 1)
        self.assertIn("the whole table", found[0])

    def test_a_region_that_appears_or_grows_is_not_a_loss(self):
        built = {**SERVED, ("land_rights", "96"): (10, 100), ("land_rights", "97"): (800, 10_000)}
        self.assertEqual(coverage.breaches(SERVED, built, [], SHARES), [])

    def test_a_kind_table_compares_counts(self):
        served = {("land_rights", "98"): (600, 600)}
        coverage.assert_coverage("gold.k", served, {("land_rights", "98"): (590, 590)}, [], COUNTS)
        with self.assertRaises(ValueError):
            coverage.assert_coverage("gold.k", served, {("land_rights", "98"): (500, 500)}, [], COUNTS)


class Reorganization(unittest.TestCase):
    # 97 and 98 become 96: the served snapshot has the old codes, the build the new one.
    PAIRS = coverage.region_pairs([pair("9710100000", "9610100000"), pair("9810100000", "9610200000")])

    def renamed(self, land_rights=1_000):
        return {("land_rights", "96"): (land_rights, 30_000), ("land_rights", "99"): (900, 30_000),
                ("price", "96"): (28_000, 30_000), ("price", "99"): (29_000, 30_000)}

    def test_a_region_the_code_change_table_renames_passes(self):
        coverage.assert_coverage("gold.x", SERVED, self.renamed(), self.PAIRS, SHARES)

    def test_without_the_recorded_pairs_the_rename_reads_as_a_loss(self):
        self.assertTrue(coverage.breaches(SERVED, self.renamed(), [], SHARES))

    def test_a_renamed_region_whose_section_is_emptied_is_refused(self):
        with self.assertRaises(ValueError) as refused:
            coverage.assert_coverage("gold.x", SERVED, self.renamed(land_rights=0), self.PAIRS, SHARES)
        self.assertIn("land_rights in 시도 96+97+98", str(refused.exception))

    def test_a_build_still_under_the_old_codes_against_a_served_new_one_is_joined_too(self):
        # The edition a build reads can lag the one being served (2026-10-16 ran the other way).
        coverage.assert_coverage("gold.x", self.renamed(), SERVED, self.PAIRS, SHARES)

    def test_a_pair_whose_both_sides_are_served_and_built_does_not_blur_them(self):
        served = {("land_rights", "97"): (400, 10_000), ("land_rights", "96"): (400, 10_000)}
        built = {("land_rights", "97"): (0, 10_000), ("land_rights", "96"): (800, 10_000)}
        self.assertIn("land_rights in 시도 97", coverage.breaches(served, built, [("97", "96")], SHARES)[0])

    def test_the_view_lists_only_pairs_that_move_a_code_between_regions(self):
        rows = [pair("9710100000", "9610100000"), pair("9710100000", "9710200000"),
                {**pair("9810100000", "9610200000"), "kind": "evidence"}]
        self.assertEqual(views.region_pairs(rows, 2), [("97", "96")])


class MergedCounts(unittest.TestCase):
    def test_an_incremental_merge_replaces_the_affected_rows(self):
        served = {("price", "97"): (9_000, 10_000)}
        merged = coverage.merged_counts(served, {("price", "97"): (100, 100)}, {("price", "97"): (40, 100)})
        self.assertEqual(merged, {("price", "97"): (8_940, 10_000)})


class Contract(unittest.TestCase):
    KINDS = {"parcel-land-rights": by_pnu_kinds.Kind("parcel-land-rights", "parcel_by_pnu_gateway", "land_rights",
                                                      ("land_rights_json", "land_right_total"))}

    def contract(self, **overrides):
        source = json.loads(rebuild.CONTRACT_PATH.read_text(encoding="utf-8"))
        source.update(overrides)
        for key, value in overrides.items():
            if value is None:
                del source[key]
        path = Path(tempfile.mkdtemp()) / "contract.json"
        path.write_text(json.dumps(source), encoding="utf-8")
        return path

    def test_the_bound_is_declared_once_and_reaches_the_host_panel_and_every_kind(self):
        tables = rebuild.load_contract(kinds=self.KINDS)["tables"]
        declared = json.loads(rebuild.CONTRACT_PATH.read_text(encoding="utf-8"))["section_coverage"]
        self.assertIs(tables["gold.parcel_panel"]["section_coverage"], tables["gold.parcel_land_rights"]["section_coverage"])
        self.assertEqual(tables["gold.parcel_panel"]["section_coverage"], declared)
        self.assertNotIn("section_coverage", tables["gold.building_panel"])

    def test_a_contract_with_sections_and_no_bound_or_a_bound_without_its_reason_is_refused(self):
        with self.assertRaises(rebuild.PlanError):
            rebuild.load_contract(self.contract(section_coverage=None), kinds=self.KINDS)
        declared = json.loads(rebuild.CONTRACT_PATH.read_text(encoding="utf-8"))["section_coverage"]
        for broken in ({**declared, "reason": " "}, {**declared, "max_share_drop_fraction": 1},
                       {**declared, "min_region_pnus": -1}):
            with self.assertRaises(rebuild.PlanError):
                rebuild.load_contract(self.contract(section_coverage=broken), kinds=self.KINDS)

    def test_the_plan_hands_the_producer_the_served_snapshot_and_the_bound(self):
        entry = rebuild.load_contract(kinds=self.KINDS)["tables"]["gold.parcel_panel"]
        inputs = (("silver.a",), {}, "silver.a")
        silver = {"silver.a": {"head": "12", "snapshots": [
            {"snapshot_id": "11", "parent_id": None, "committed_at": "2026-01-01T00:00:00Z", "operation": "append",
             "summary": {"added-records": "1"}},
            {"snapshot_id": "12", "parent_id": "11", "committed_at": "2026-01-02T00:00:00Z", "operation": "append",
             "summary": {"added-records": "1"}}]}}
        gold = {"head": "90", "row_count": 100, "snapshots": [
            {"snapshot_id": "90", "parent_id": None, "committed_at": "2026-01-01T01:00:00Z", "operation": "overwrite",
             "summary": {"total-records": "100", rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps({"silver.a": "11"})}}]}
        entry = {**entry, "absent_inputs_accepted": {}, "unmeasured_inputs": {}}
        decision = rebuild.plan("gold.parcel_panel", entry, inputs, gold, silver)
        arguments = decision["producer_arguments"]
        self.assertEqual(decision["section_coverage_against"], "90")
        self.assertEqual(arguments[arguments.index("--section-coverage-snapshot") + 1], "90")
        self.assertIn("--max-section-coverage-drop", arguments)
        first = rebuild.plan("gold.parcel_panel", entry, inputs, None, silver)
        self.assertNotIn("--section-coverage-snapshot", first["producer_arguments"])

    def test_the_producer_flags_go_together(self):
        args = SimpleNamespace(section_coverage_snapshot="90", max_section_coverage_drop=None,
                               section_coverage_min_pnus=None, input_mode="iceberg")
        with self.assertRaises(ValueError):
            coverage.validate_arguments(args)
        coverage.validate_arguments(SimpleNamespace(section_coverage_snapshot=None, max_section_coverage_drop=None,
                                                    section_coverage_min_pnus=None, input_mode="parquet"))


if __name__ == "__main__":
    unittest.main()
