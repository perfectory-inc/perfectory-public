"""The publish checks (orchestration/dagster/publish_checks.py, root ADR-0416) block and read what their rule says.

Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere, where a skip
fails that lane. The SQL ran against production Trino on 2026-11-05 (read-only): the zoning table had
655,574 current rows over 655,266 keys (308 duplicated rows, all layer D036, one snapshot) and the
public-auction Gold 28,802 served rounds, 28,705 whose item has media, none missing them.
"""

import importlib.util
import json
import pathlib
import sys
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"
CONTRACTS = PLATFORM / "infra" / "lakehouse" / "contracts" / "industrial_complex_lakehouse_contracts.json"


def contracts():
    return json.loads(CONTRACTS.read_text(encoding="utf-8"))["contracts"].values()


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class PublishChecksTest(unittest.TestCase):
    def test_every_publish_check_is_declared_blocking_on_its_graph_node(self):
        import asset_checks
        import observe_assets
        import publish_checks
        from dagster import Definitions

        jobs, graph = observe_assets.contracts()
        keys = observe_assets.node_keys(graph)
        checks = asset_checks.build(graph)
        Definitions(assets=observe_assets.observed_assets(jobs, graph), asset_checks=checks).resolve_asset_graph()
        specs = [spec for check in checks for spec in check.check_specs if spec.name in publish_checks.KINDS]
        self.assertTrue(specs and all(spec.blocking for spec in specs), "a publish check blocks")
        self.assertEqual({(spec.asset_key, spec.name) for spec in specs},
                         {(keys["silver-land-use-zone-area"], "current_key_unique"),
                          (keys["gold-parcel-public-auction"], "rounds_carry_their_media")})

    def test_current_rows_follow_the_contract(self):
        import publish_checks  # puts the Spark jobs directory on the path
        import platform_contracts

        for contract in contracts():
            latest = platform_contracts.current_row_latest(contract)
            if latest is None or platform_contracts.current_row_predicate(contract) is not None:
                continue
            if platform_contracts.row_versions(contract)["kind"] == "editions":
                continue
            sql = publish_checks.current_rows_sql(contract["table_name"], list(latest.key))
            self.assertIn(f"PARTITION BY {', '.join(latest.key)} ORDER BY "
                          + ", ".join(f"{c} DESC NULLS LAST" for c in latest.newest_first), sql)
            self.assertEqual(f"<> '{platform_contracts.REMOVED}'" in sql, latest.removal_column is not None,
                             contract["table_name"])

    def test_a_rule_without_a_translation_is_refused_not_guessed(self):
        import publish_checks  # puts the Spark jobs directory on the path
        import platform_contracts

        predicate = next(c for c in contracts() if platform_contracts.current_row_predicate(c) is not None)
        with self.assertRaisesRegex(ValueError, "predicate"):
            publish_checks.current_rows_sql(predicate["table_name"], ["pnu"])
        with self.assertRaisesRegex(ValueError, "column"):
            publish_checks.current_rows_sql("silver.land_use_zone_area", ["layer_code; DROP"])

    def test_key_uniqueness_fails_on_any_duplicated_current_key(self):
        import publish_checks

        failed = publish_checks.key_unique_result("t", (655574, 655266, '[{"layer_code":"D036"}]'))
        self.assertFalse(failed.passed)
        self.assertEqual(failed.metadata["duplicate_rows"].value, 308)
        self.assertEqual(failed.metadata["duplicated_keys_sample"].value, [{"layer_code": "D036"}])
        self.assertTrue(publish_checks.key_unique_result("t", (10, 10, None)).passed)
        sql = publish_checks.key_unique_sql("silver.land_use_zone_area")
        self.assertIn("GROUP BY layer_code, zone_feature_id", sql)
        self.assertIn("FROM r2.silver.land_use_zone_area", sql)

    def test_rounds_must_carry_the_media_their_item_has(self):
        import publish_checks  # puts the Spark jobs directory on the path
        import onbid_public_auction_document as document

        self.assertTrue(publish_checks.media_result("g", (28802, 28705, 0, None)).passed)
        missing = publish_checks.media_result("g", (10, 4, 4, '["99999-0001"]'))
        self.assertFalse(missing.passed)
        self.assertEqual(missing.metadata["items_missing_media_sample"].value, ["99999-0001"])
        sql = publish_checks.media_sql("gold.parcel_public_auction")
        self.assertIn(f"FROM r2.{document.PHOTO_SOURCE}", sql)
        self.assertIn("media_kind IN ('photo', 'appraisal')", sql)
        self.assertIn("'$.rounds'", sql)


if __name__ == "__main__":
    unittest.main()
