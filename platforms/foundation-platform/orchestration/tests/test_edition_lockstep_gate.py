"""Every served-universe lane switches to a promoted edition together (root ADR-0291 §3, ADR-0381).

Synthetic editions 209906 → 209910 and synthetic snapshot ids. A promotion's Golds rebuild one after
another; the planted case is the window in between, where before ADR-0381 each lane published its
Gold as soon as it was rebuilt and served mixed editions (the 2026-10-21 공매 class).
"""

import importlib.util
import io
import json
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

PLATFORM = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "infra/lakehouse/spark/tests"))
spec = importlib.util.spec_from_file_location("edition_lockstep_gate", PLATFORM / "scripts/ops/edition_lockstep_gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

import gold_rebuild  # noqa: E402
import iceberg_metadata  # noqa: E402
import parcel_edition_readiness as readiness  # noqa: E402
from test_parcel_edition_readiness import CANDIDATE, NOW, contract, evidence  # noqa: E402

OLD, NEW = "vworldkr__parcel-209906", "vworldkr__parcel-209910"


def gold(edition: str | None, at: str = "2099-10-21T00:00:00Z") -> dict:
    summary = {gold_rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps({"silver.parcel_boundaries": "11"})}
    if edition is not None:
        summary[gold_rebuild.SERVED_EDITION_PROPERTY] = edition
    return {"head": "1", "snapshots": [{"snapshot_id": "1", "parent_id": None, "committed_at": at,
                                        "operation": "overwrite", "summary": summary}]}


class LockstepGateTest(unittest.TestCase):
    def setUp(self):
        held = contract()
        self.promoted = readiness.promote(held, readiness.readiness(held, evidence(), NOW), CANDIDATE, "op", "why", NOW)
        self.tables = gate.lockstep_tables()

    def test_the_lanes_are_the_parcel_panel_and_its_kinds(self):
        self.assertEqual(self.tables[gate.PARCEL_LANE], gate.PARCEL_PANEL)
        self.assertIn("parcel-public-auction", self.tables, "the 공매 kind reads the served parcels")
        self.assertGreater(len(self.tables), 2)

    def test_no_lane_publishes_until_every_lane_is_rebuilt_then_all_do(self):
        tables = sorted(self.tables.values())
        # The parcel panel is rebuilt on the new edition first; every kind is still on the old one.
        histories = {table: gold(OLD) for table in tables}
        histories[gate.PARCEL_PANEL] = gold(NEW)
        for lane in self.tables:
            with self.subTest(lane=lane, stage="partway"):
                verdict = gate.verdict(lane, self.promoted, histories.__getitem__)
                self.assertTrue(verdict.startswith("waiting"), verdict)
                self.assertNotIn("parcel (", verdict, "the rebuilt lane does not hold the barrier")
        # One kind short: still nobody publishes.
        histories = {table: gold(NEW) for table in tables}
        histories[tables[-1]] = gold(OLD)
        self.assertTrue(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__).startswith("waiting"))
        # Every lane on the new edition: every lane publishes, together.
        histories = {table: gold(NEW) for table in tables}
        self.assertEqual({gate.verdict(lane, self.promoted, histories.__getitem__) for lane in self.tables}, {"ok"})

    def test_the_tiles_and_the_panels_move_together(self):
        """Root ADR-0490 §6: before it, the lockstep barrier held only the by-PNU Golds, and the map's tiles
        were promoted on their own (the PostGIS-era release served 202606 beside any Gold)."""
        everyone_new = {table: gold(NEW) for table in self.tables.values()}
        # Golds rebuilt, tiles still on the old edition: no by-PNU lane publishes, the tiles may.
        for lane in self.tables:
            verdict = gate.verdict(lane, self.promoted, everyone_new.__getitem__, tiles=OLD)
            self.assertTrue(verdict.startswith("waiting") and f"{gate.TILES_LANE} ({OLD})" in verdict, verdict)
        self.assertEqual(gate.verdict(gate.TILES_LANE, self.promoted, everyone_new.__getitem__, tiles=OLD), "ok")
        # One Gold still on the old edition: the tiles wait for it, never for themselves.
        partway = {**everyone_new, gate.PARCEL_PANEL: gold(OLD)}
        verdict = gate.verdict(gate.TILES_LANE, self.promoted, partway.__getitem__, tiles=OLD)
        self.assertTrue(verdict.startswith("waiting") and gate.TILES_LANE + " (" not in verdict, verdict)
        # Tiles promoted: every lane publishes.
        self.assertEqual({gate.verdict(lane, self.promoted, everyone_new.__getitem__, tiles=NEW) for lane in self.tables}, {"ok"})

    def test_the_tiles_serve_the_ledgers_first_edition_until_a_bake_is_promoted(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(gate.tiles_edition(self.promoted, root), OLD)
            (root / "baked-edition").write_text(NEW + "\n", encoding="utf-8")
            self.assertEqual(gate.tiles_edition(self.promoted, root), NEW)

    def test_a_day_without_a_promotion_never_waits(self):
        # Golds committed before the record: the ledger says 209906 was served then, and still is.
        histories = {table: gold(None, "2099-10-01T00:00:00Z") for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, contract(), histories.__getitem__), "ok")
        # Before the record but after the promotion's date, the ledger dates it to the new edition.
        histories = {table: gold(None, "2099-10-21T00:00:00Z") for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__), "ok")

    def test_a_lane_outside_the_served_universe_and_an_absent_gold_never_wait(self):
        self.assertEqual(gate.verdict("building", self.promoted, lambda table: gold(OLD)), "ok")
        histories = {table: {"head": None, "snapshots": []} for table in self.tables.values()}
        self.assertEqual(gate.verdict(gate.PARCEL_LANE, self.promoted, histories.__getitem__), "ok")


def not_found(kind: str) -> urllib.error.HTTPError:
    """The Iceberg REST ErrorModel a loadTable 404 answers with."""
    body = json.dumps({"error": {"message": "not found", "type": kind, "code": 404}}).encode()
    return urllib.error.HTTPError("https://catalog.invalid/t", 404, "Not Found", {}, io.BytesIO(body))


class AbsentGoldTest(unittest.TestCase):
    """2026-11-04..05: four by-PNU kinds were defined (regulation zones, sub-parcels, energy,
    demolitions) before their Gold was ever built. The catalog answered 404 for each, the gate said
    it "cannot tell" and the 공매 lane refused every bake for two days, serving its pre-photo
    documents of 2026-10-28 (photos: [] on every round)."""

    def setUp(self):
        self.environ = {"FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": "https://catalog.invalid",
                        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": "w", "TOKEN": "t"}

    def catalog(self, missing: set[str], kind: str = "NoSuchTableException"):
        def get(_self, url):
            if "/v1/config" in url:
                return {"overrides": {"prefix": "p"}}
            if url.rsplit("/", 1)[1] in missing:
                raise not_found(kind)
            return {"metadata": {"current-snapshot-id": -1, "snapshots": []}}
        return mock.patch.object(iceberg_metadata.Catalog, "_get", get)

    def test_a_table_never_created_reads_as_never_committed_only_when_asked(self):
        catalog = iceberg_metadata.Catalog("TOKEN", "test", self.environ)
        with self.catalog({"parcel_energy"}):
            self.assertEqual(catalog.table("gold.parcel_energy", absent_ok=True), {"head": None, "snapshots": []})
            with self.assertRaises(urllib.error.HTTPError):
                catalog.table("gold.parcel_energy")

    def test_a_missing_namespace_is_never_an_absent_table(self):
        catalog = iceberg_metadata.Catalog("TOKEN", "test", self.environ)
        with self.catalog({"parcel_energy"}, "NoSuchNamespaceException"), self.assertRaises(urllib.error.HTTPError):
            catalog.table("gold.parcel_energy", absent_ok=True)

    def run_gate(self, missing: set[str]) -> int:
        with self.catalog(missing), mock.patch.dict("os.environ", self.environ | {
                "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN": "t"}),                 mock.patch("sys.stdout", io.StringIO()) as out, mock.patch("sys.stderr", io.StringIO()):
            code = gate.main(["parcel-public-auction"])
        self.output = out.getvalue().strip()
        return code

    def test_a_kind_whose_gold_was_never_built_does_not_stop_the_other_lanes(self):
        kinds = {table.split(".", 1)[1] for lane, table in gate.lockstep_tables().items() if lane != gate.PARCEL_LANE}
        self.assertEqual(self.run_gate(set(sorted(kinds)[:4])), 0)
        self.assertEqual(self.output, "ok")

    def test_an_absent_parcel_panel_is_a_wrong_catalog_and_the_gate_cannot_tell(self):
        self.assertEqual(self.run_gate({gate.PARCEL_PANEL.split(".", 1)[1]}), 65)


if __name__ == "__main__":
    unittest.main()
