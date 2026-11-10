"""Place-identity health (orchestration/dagster/place_identity_checks.py, root ADR-0288 §6) says when a parcel
source loses its parcels.

Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere. The SQL ran against
production Trino on 2026-11-11 (read-only, one scan each, 10 s for all four): court items 64,059 current rows,
56,713 on a parcel; court sale results 22,856 / 21,126; 공매 items 63,812 / 34,042; 실거래 13,417,395 / 561,789.
The incident the check prevents: 2026-10-20, a 분구 (화성시 41590 → 동탄구 41597) left 57 live 공매 objects on
no parcel and nothing said so; a person found it when a join failed.
"""

import importlib.util
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class PlaceIdentityChecksTest(unittest.TestCase):
    def test_every_parcel_source_check_reports_and_never_blocks(self):
        import asset_checks
        import observe_assets
        import place_identity_checks
        from dagster import Definitions

        jobs, graph = observe_assets.contracts()
        keys = observe_assets.node_keys(graph)
        checks = asset_checks.build(graph)
        Definitions(assets=observe_assets.observed_assets(jobs, graph), asset_checks=checks).resolve_asset_graph()
        specs = [spec for check in checks for spec in check.check_specs if spec.name in place_identity_checks.KINDS]
        self.assertTrue(specs and not any(spec.blocking for spec in specs), "showing resolved rows wins")
        self.assertEqual({spec.asset_key for spec in specs},
                         {keys[node] for node in ("silver-court-auction-item", "silver-court-auction-sale-result",
                                                  "silver-onbid-real-estate-item", "silver-real-transaction")})

    def test_a_steady_source_passes_and_the_first_evaluation_has_nothing_to_compare(self):
        import place_identity_checks as c

        self.assertTrue(c.judge((64059, 56713), None)[0])
        # A day's new rows at the previous rate.
        self.assertTrue(c.judge((65059, 57598), (64059, 56713))[0])
        # 13.4M 실거래 rows grow by 100k a load at 4.2%: the scatter of the added rows is not a defect.
        passed, found = c.judge((13517395, 565989 - 60), (13417395, 561789))
        self.assertTrue(passed, found)

    def test_a_subdivision_that_leaves_rows_on_no_parcel_fails(self):
        import place_identity_checks as c

        # The 화성 shape: the same rows, 57 of them no longer on a parcel. The share moves 0.09 points only.
        passed, found = c.judge((63812, 34042 - 57), (63812, 34042))
        self.assertFalse(passed)
        self.assertEqual(found["newly_unresolved"], 57)

    def test_a_fall_of_a_point_of_the_share_fails(self):
        import place_identity_checks as c

        passed, found = c.judge((64059, 56713 - 641), (64059, 56713))
        self.assertFalse(passed)
        self.assertLess(found["resolved_share"], found["previous_share"])

    def test_a_failure_pages_and_records_what_the_next_evaluation_compares(self):
        import place_identity_checks as c
        from dagster import AssetCheckSeverity

        paged = []
        failed = c.result("silver.t", (100, 10, '{"parcel_absent": 90}'), (100, 90), AssetCheckSeverity.WARN,
                          pager=lambda table, found: paged.append(table) or True)
        self.assertFalse(failed.passed)
        self.assertEqual(paged, ["silver.t"])
        self.assertEqual((failed.metadata["rows"].value, failed.metadata["resolved"].value), (100, 10))
        self.assertEqual(failed.metadata["unresolved_by_reason"].value, {"parcel_absent": 90})
        steady = c.result("silver.t", (100, 90, None), (100, 90), AssetCheckSeverity.WARN,
                          pager=lambda table, found: self.fail("a passing check does not page"))
        self.assertTrue(steady.passed)

    def test_the_reasons_are_read_only_where_the_contract_has_them(self):
        import place_identity_checks as c

        self.assertIn("pnu_unresolved_reason", c.resolution_sql("silver.court_auction_item"))
        # Root ADR-0480 §2: 공매 names why a round is unplaced too.
        self.assertIn("pnu_unresolved_reason", c.resolution_sql("silver.onbid_real_estate_item"))
        # A parcel source whose contract has no reason column is counted without one.
        from unittest import mock

        with mock.patch.object(c.platform_contracts, "column_names", return_value=("cltr_mng_no", "pnu")):
            self.assertNotIn("pnu_unresolved_reason", c.resolution_sql("silver.onbid_real_estate_item"))
        # One scan of the current rows: three exceeded Trino's 8GB a node on 13.4M 실거래 rows.
        self.assertEqual(c.resolution_sql("silver.real_transaction").count("rank() OVER"), 1)


if __name__ == "__main__":
    unittest.main()
