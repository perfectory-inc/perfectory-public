"""The panel attaches a renumbered parcel's attributes through the lineage (root ADR-0113 §6).

Runs the Spark path of `parcel_panel_silver_to_gold` against the reference semantics in
`parcel_attribute_carry.attach`, and pins the digest promise: a row whose sections are all its
own keeps the fingerprint it had before `attached_via_json` existed.
"""

import hashlib
import json
import os
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

OLD, MID, NEW = "9999910100", "9999920100", "9999930100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1", "requires the pinned Spark runtime")
class PanelCarrySparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("parcel-panel-carry").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def panel(self, links, own_price, map_pnus):
        import parcel_attribute_carry as carry
        import parcel_panel_silver_to_gold as job
        from parcel_lineage import Link

        spark = self.spark
        candidates, _ = carry.carry_candidates([Link(a, b, r, g, "") for a, b, r, g in links])
        cand = spark.createDataFrame(
            [(c.successor_pnu, c.source_pnu, c.hop, c.path) for c in candidates],
            "successor_pnu string, source_pnu string, hop int, path string",
        )
        empty = {
            "zonings": "pnu string, zonings_json string",
            "characteristics": "pnu string, characteristics_json string",
            "forest_ledger": "pnu string, forest_ledger_json string",
            "transfer_history": "pnu string, transfer_history_json string",
            "land_rights": "pnu string, land_rights_json string, land_right_total long",
        }
        sections = {name: spark.createDataFrame([], schema) for name, schema in empty.items()}
        sections["price"] = spark.createDataFrame(list(own_price.items()), "pnu string, price_json string")
        sections = {n: job.carry_section(f, job.SECTION_VIA_COLUMNS[n], cand) for n, f in sections.items()}
        parcels = spark.createDataFrame([(p,) for p in map_pnus], "pnu string")
        rows = job.build_gold_panel_frame(parcels, sections, "snap", "2099-09-01T00:00:00Z").collect()
        self.assertEqual(sorted(r.pnu for r in rows), sorted(map_pnus), "one row per map PNU")
        return {r.pnu: r for r in rows}

    def test_a_renumbered_parcel_shows_its_old_price_and_says_how(self):
        rows = self.panel(
            [(pnu(OLD, 1), pnu(NEW, 1), "code_change", "code_derived")],
            {pnu(OLD, 1): '{"price_per_m2":1}', pnu(NEW, 2): '{"price_per_m2":2}'},
            [pnu(NEW, 1), pnu(NEW, 2), pnu(NEW, 3)],
        )
        self.assertEqual(rows[pnu(NEW, 1)].price_json, '{"price_per_m2":1}')
        self.assertEqual(json.loads(rows[pnu(NEW, 1)].attached_via_json), {"price": "lineage:code_derived"})
        self.assertEqual(rows[pnu(NEW, 2)].price_json, '{"price_per_m2":2}')
        self.assertIsNone(rows[pnu(NEW, 2)].attached_via_json)
        self.assertIsNone(rows[pnu(NEW, 3)].price_json, "no lineage, no source value: stays empty")

    def test_the_own_value_wins_over_the_lineage(self):
        rows = self.panel(
            [(pnu(OLD, 1), pnu(NEW, 1), "code_change", "official")],
            {pnu(OLD, 1): '{"price_per_m2":1}', pnu(NEW, 1): '{"price_per_m2":9}'},
            [pnu(NEW, 1)],
        )
        self.assertEqual(rows[pnu(NEW, 1)].price_json, '{"price_per_m2":9}')
        self.assertIsNone(rows[pnu(NEW, 1)].attached_via_json)

    def test_a_chain_takes_the_nearest_holder(self):
        rows = self.panel(
            [
                (pnu(OLD, 1), pnu(MID, 1), "code_change", "official"),
                (pnu(MID, 1), pnu(NEW, 1), "jurisdiction_transfer", "evidence_strong"),
            ],
            {pnu(OLD, 1): '{"price_per_m2":1}', pnu(MID, 1): '{"price_per_m2":5}'},
            [pnu(NEW, 1)],
        )
        self.assertEqual(rows[pnu(NEW, 1)].price_json, '{"price_per_m2":5}')
        self.assertEqual(json.loads(rows[pnu(NEW, 1)].attached_via_json), {"price": "lineage:evidence_strong"})

    def test_a_row_without_carried_sections_keeps_its_old_digest(self):
        import parcel_panel_silver_to_gold as job

        row = self.panel([], {pnu(NEW, 2): '{"price_per_m2":2}'}, [pnu(NEW, 2)])[pnu(NEW, 2)]
        # The fingerprint as it was before the column existed — spelled here, not read from the job.
        before = [c for c in job.CONTENT_DIGEST_COLUMNS if c != "attached_via_json"]
        values = ["\x00null" if row[c] is None else str(row[c]) for c in before]
        self.assertEqual(row.row_digest, hashlib.sha256("\x1f".join(values).encode()).hexdigest())


if __name__ == "__main__":
    unittest.main()
