"""CI checks pure contracts without Spark, as in the parcel lane.

The real shuffle/join proof runs in the pinned Spark container as a development
artifact, outside CI. Its exact Gold row is shared with Rust DTO/identity tests.
"""
import hashlib
import json
import unittest
from pathlib import Path

import test_industrial_complex_gold_schema_evolution  # Reuse the import-only stub.
import building_panel_silver_to_gold as job
from test_deferred_pyspark_names_resolve import unresolved


class BuildingPanelContractTest(unittest.TestCase):
    def test_digest_excludes_only_lineage_and_keeps_contract_order(self):
        excluded = {"row_digest", "source_snapshot_id", "published_at_utc"}
        self.assertEqual(set(job.GOLD_COLUMNS) - set(job.CONTENT_DIGEST_COLUMNS), excluded)
        self.assertEqual(job.CONTENT_DIGEST_COLUMNS,
                         tuple(c for c in job.GOLD_COLUMNS if c not in excluded))

    def test_all_deferred_names_resolve(self):
        self.assertEqual(unresolved("building_panel_silver_to_gold"), [])

    def test_newest_floor_batch_serves_the_latest_ingest_not_the_larger_version(self):
        from datetime import datetime

        content_v1 = "building-register-floor-content-v1-" + "a" * 64
        selection_v2 = "building-register-floor-selection-v2-" + "b" * 64
        # The floor table carries both generations (root ADR-0128): content-v1 (2026-10) is the
        # current one, selection-v2 (2026-09) the legacy batch it appended beside. "v1" < "v2", so
        # ordering by the id would serve the stale batch; the ingest instant serves the current one.
        self.assertEqual(
            job.newest_floor_batch({
                selection_v2: datetime(2026, 9, 29, 17, 53),
                content_v1: datetime(2026, 10, 20, 10, 52),
            }),
            content_v1,
        )
        # Prove the naive id ordering would pick the wrong (stale) batch, which is what this avoids:
        self.assertEqual(max(selection_v2, content_v1), selection_v2)
        self.assertIsNone(job.newest_floor_batch({}))
        self.assertEqual(job.newest_floor_batch({content_v1: datetime(2026, 10, 20)}), content_v1)

    def test_shared_spark_fixture_carries_exact_contract_and_content_digest(self):
        row = json.loads((Path(__file__).parent / "fixtures" / "building_panel_gold_row.json").read_text(encoding="utf-8"))
        self.assertEqual(tuple(row), job.GOLD_COLUMNS)
        content = {c: row[c] for c in job.CONTENT_DIGEST_COLUMNS}
        digest = hashlib.sha256(json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(row["row_digest"], digest)
        self.assertEqual(row["pnu"][:5], "99999")
        buildings = json.loads(row["buildings_json"])
        orphan = json.loads(row["unlinked_units_json"])[0]
        self.assertEqual(buildings[0]["units"][0]["building_id"], buildings[0]["id"])
        self.assertIsNone(orphan["building_id"])
        self.assertNotIn("updated_at", buildings[0])
        # The register's names sit beside its codes; this fixture's register states none (so the
        # golden packs built from it keep their bytes), and a missing name is null, never the code.
        self.assertEqual((buildings[0]["purpose_code"], buildings[0]["purpose_name"]), ("03000", None))
        self.assertIsNone(buildings[0]["structure_name"])
        self.assertLessEqual({"purpose_name_raw", "structure_name_raw"}, set(job.BUILD_COLUMNS[job.TITLE_SOURCE]))
        self.assertEqual(buildings[0]["units"][0]["official_price_history"], [
            {"base_date": "20100601", "price_won": 35000000},
            {"base_date": "20100101", "price_won": 36000000},
        ])


if __name__ == "__main__":
    unittest.main()
