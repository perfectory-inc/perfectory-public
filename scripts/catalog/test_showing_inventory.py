#!/usr/bin/env python3
"""The showing inventory credits a Silver only with what its own Gold serves, and judges by the job."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location("render_showing_inventory", HERE / "render-showing-inventory.py")
inv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inv)
dd = inv.dd


def node(id_, type_, **extra):
    return {"id": id_, "type": type_, "title": id_, **({"table_name": f"silver.{id_}"} if type_ == "silver_table" else {}), **extra}


def edge(a, b, relation="feeds"):
    return {"from": a, "to": b, "relation": relation, "status": "implemented"}


def documents(enabled):
    nodes = [node("edition", "silver_table"), node("factory", "silver_table"), node("energy", "silver_table"),
             node("permit", "silver_table"), node("bronze-permit", "bronze_source"),
             node("gold-panel", "gold_table"), node("gold-energy", "gold_table"),
             node("panel-packs", "serving_surface"), node("energy-packs", "serving_surface"),
             node(inv.PANEL, "serving_surface")]
    edges = [edge("edition", "gold-panel"), edge("gold-panel", "panel-packs"), edge("panel-packs", inv.PANEL, "serves"),
             edge("gold-panel", "gold-energy"), edge("energy", "gold-energy"), edge("gold-energy", "energy-packs")]
    jobs = [{"id": "panel_bake", "enabled": True, "pipeline_graph_edges": ["gold-panel-to-panel-packs"]},
            {"id": "energy_bake", "enabled": enabled, "pipeline_graph_edges": ["gold-energy-to-energy-packs"]},
            {"id": "permit_silver", "enabled": enabled, "pipeline_graph_edges": ["bronze-permit-to-permit"]}]
    registry = {"subjects": [{"owner": {"lane": "parcel-energy"}, "consumers": [{"name": "web"}]}]}
    return {dd.GRAPH: {"nodes": nodes, "edges": edges}, dd.JOBS: {"jobs": jobs}, dd.REGISTRY: registry}


LANES = {"lanes": [{"lane": "parcel", "serving_nodes": ["panel-packs"]},
                   {"lane": "parcel-energy", "serving_nodes": ["energy-packs"]}]}


class ShowingInventoryTest(unittest.TestCase):
    def rows(self, enabled):
        with mock.patch.object(dd, "lane_index", return_value=LANES):
            return {r["table"]: r for r in inv.inventory(documents(enabled))}

    def test_a_silver_is_not_credited_with_a_lane_behind_a_second_gold(self):
        self.assertEqual(self.rows(False)["silver.edition"]["lanes"], ["parcel"])

    def test_the_verdict_follows_the_job_that_fills_the_surface(self):
        self.assertEqual(self.rows(False)["silver.energy"]["verdict"], "길 있음·꺼짐")
        self.assertEqual(self.rows(True)["silver.energy"]["verdict"], "화면")
        self.assertEqual(self.rows(True)["silver.factory"]["verdict"], "없음")

    def test_a_silver_without_a_path_whose_loader_is_off_waits_for_its_loader(self):
        self.assertEqual(self.rows(False)["silver.permit"]["verdict"], "적재 꺼짐")
        self.assertEqual(self.rows(True)["silver.permit"]["verdict"], "없음")

    def test_the_block_is_replaced_between_its_markers_only(self):
        text = f"before\n{inv.BEGIN}\nold\n{inv.END}\nafter\n"
        self.assertEqual(inv.replace(text, "NEW"), "before\nNEW\nafter\n")
        with self.assertRaises(ValueError):
            inv.replace("no markers", "NEW")


if __name__ == "__main__":
    unittest.main()
