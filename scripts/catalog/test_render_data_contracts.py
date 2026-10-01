"""The rendered data contracts keep to the ODCS v3 fields they use and come only from their sources
(root ADR-0123). The full official-schema validation runs where the contracts are registered."""

import copy
import importlib.util
import json
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("render_data_contracts", HERE / "render-data-contracts.py")
render = importlib.util.module_from_spec(spec)
spec.loader.exec_module(render)

# The values ODCS v3.2.0 allows for the fields the renderer writes (schema/odcs-json-schema-v3.2.0.json).
ODCS_API_VERSIONS = {"v3.2.0", "v3.1.0", "v3.0.2", "v3.0.1", "v3.0.0"}
ODCS_LOGICAL_TYPES = {"string", "date", "timestamp", "time", "number", "integer", "object", "array", "boolean", "map", "vector"}
ODCS_QUALITY_TYPES = {"text", "library", "sql", "custom"}
ODCS_STATUSES = {"proposed", "draft", "active", "deprecated", "retired"}


def sources():
    return (
        json.loads(render.LAKEHOUSE_CONTRACTS.read_text(encoding="utf-8")),
        json.loads(render.GRAPH.read_text(encoding="utf-8")),
    )


class TheRenderedContracts(unittest.TestCase):
    def setUp(self):
        self.lakehouse, self.graph = sources()
        self.documents = render.documents_for(self.lakehouse, self.graph)

    def test_one_contract_per_lakehouse_table(self):
        self.assertEqual(
            {doc["id"] for doc in self.documents.values()}, set(self.lakehouse["contracts"])
        )

    def test_required_odcs_fields_and_allowed_values(self):
        for name, doc in self.documents.items():
            with self.subTest(name):
                for field in ("version", "apiVersion", "kind", "id"):
                    self.assertIn(field, doc)
                self.assertIn(doc["apiVersion"], ODCS_API_VERSIONS)
                self.assertEqual(doc["kind"], "DataContract")
                self.assertIn(doc["status"], ODCS_STATUSES)
                (table,) = doc["schema"]
                self.assertEqual(table["logicalType"], "object")
                for prop in table["properties"]:
                    self.assertIn("name", prop)
                    if "logicalType" in prop:
                        self.assertIn(prop["logicalType"], ODCS_LOGICAL_TYPES)
                for rule in table.get("quality", []):
                    self.assertIn(rule["type"], ODCS_QUALITY_TYPES)

    def test_every_column_and_gate_comes_from_the_rust_contract(self):
        for doc in self.documents.values():
            contract = self.lakehouse["contracts"][doc["id"]]
            (table,) = doc["schema"]
            self.assertEqual([p["name"] for p in table["properties"]], [c["name"] for c in contract["columns"]])
            self.assertEqual([p["physicalType"] for p in table["properties"]], [c["logical_type"] for c in contract["columns"]])
            self.assertEqual([q["description"] for q in table.get("quality", [])], contract.get("quality_gates", []))

    def test_the_committed_files_are_the_rendering(self):
        rendered = render.contracts_for(self.lakehouse, self.graph)
        for name, text in rendered.items():
            with self.subTest(name):
                self.assertEqual((render.OUTPUT / name).read_text(encoding="utf-8"), text)


class WhatTheRendererRefuses(unittest.TestCase):
    def test_a_table_only_one_source_knows(self):
        lakehouse, graph = sources()
        graph = copy.deepcopy(graph)
        graph["nodes"] = [node for node in graph["nodes"] if node.get("table_name") != "silver.industrial_complexes"]
        with self.assertRaises(render.RenderError):
            render.documents_for(lakehouse, graph)

    def test_a_physical_type_with_no_odcs_word(self):
        lakehouse, graph = sources()
        lakehouse = copy.deepcopy(lakehouse)
        lakehouse["contracts"]["silver.industrial_complexes"]["columns"][0]["logical_type"] = "geometry"
        with self.assertRaises(render.RenderError):
            render.documents_for(lakehouse, graph)

    def test_a_declared_status_with_no_odcs_status(self):
        lakehouse, graph = sources()
        graph = copy.deepcopy(graph)
        for node in graph["nodes"]:
            if node.get("table_name") == "silver.industrial_complexes":
                node["status"] = "someday"
        with self.assertRaises(render.RenderError):
            render.documents_for(lakehouse, graph)


if __name__ == "__main__":
    unittest.main()
