"""What the Lakekeeper catalog holds is generated from the pipeline graph (root ADR-0624 §2, ADR-0626 §9; W109e).

scripts/catalog/declared_definitions.py lakekeeper_declaration: the tables a definition marks `catalog: lakekeeper`,
each AWS lane's grants on them (select on a table its edges read, write on a table its load feeds) and the Bronze
sources whose ledger rows move with those lanes. What failing this prevents: the first switch found that a host job
touching a switched table would, under the table's final name, make a stray table in the old catalog and have its
collectors ask their sources' whole history again (about 3,200 requests to factory-on), so such a job is refused at
generation, and a switched table that no lane loads would stand empty.
"""

from __future__ import annotations

import copy
import importlib.util
import pathlib
import sys
import unittest

REPOSITORY = pathlib.Path(__file__).resolve().parents[4]


def load(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, REPOSITORY / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


declared = load("declared_definitions_for_lakekeeper", "scripts/catalog/declared_definitions.py")


def documents(collector_enabled=False, load_enabled=False, marked=True):
    nodes = [
        {"id": "source-x", "type": "source_group"},
        {"id": "silver-x-attempt", "type": "silver_table", "table_name": "silver.x_attempt"},
        {"id": "silver-x-file", "type": "silver_table", "table_name": "silver.x_file"},
    ]
    if marked:
        for node in nodes[1:]:
            node["catalog"] = "lakekeeper"
    edges = [
        {"id": "source-x-to-silver-x-attempt", "from": "source-x", "to": "silver-x-attempt", "relation": "feeds"},
        {"id": "source-x-to-silver-x-file", "from": "source-x", "to": "silver-x-file", "relation": "feeds"},
        {"id": "silver-x-attempt-to-source-x", "from": "silver-x-attempt", "to": "source-x", "relation": "schedules"},
    ]
    jobs = [
        {"id": "x_collect", "dagster_group": "collect", "enabled": collector_enabled,
         "pipeline_graph_edges": [edge["id"] for edge in edges]},
        {"id": "x_silver", "dagster_group": "silver", "enabled": load_enabled,
         "pipeline_graph_edges": [edges[0]["id"], edges[1]["id"]]},
    ]
    return {declared.GRAPH: {"nodes": nodes, "edges": edges}, declared.JOBS: {"jobs": jobs}}


AWS = {"collect": {"lanes": {
    "x_collect": {"write_prefixes": ["bronze/source=xorkr__x_detail/", "media/display/"]},
    "x_silver": {"write_prefixes": ["control/media_index/silver/x_file/"]},
}}}


class TheDeclaration(unittest.TestCase):
    def test_collectors_select_what_they_read_and_the_load_writes_what_it_feeds(self):
        made = declared.lakekeeper_declaration(documents(), AWS)
        self.assertEqual(made["tables"], ["silver.x_attempt", "silver.x_file"])
        self.assertEqual(made["lanes"], {"x_collect": {"select": ["silver.x_attempt"]},
                                         "x_silver": {"write": ["silver.x_attempt", "silver.x_file"]}})
        # Only a collector's Bronze sources move their ledger rows; a load writes no Bronze.
        self.assertEqual(made["ledger_sources"], {"x_collect": ["xorkr__x_detail"]})

    def test_a_host_job_that_touches_a_switched_table_is_refused(self):
        for enabled in ({"collector_enabled": True}, {"load_enabled": True}):
            with self.assertRaises(declared.DefinitionError, msg=str(enabled)):
                declared.lakekeeper_declaration(documents(**enabled), AWS)

    def test_a_switched_table_that_no_lane_loads_is_refused(self):
        aws = copy.deepcopy(AWS)
        del aws["collect"]["lanes"]["x_silver"]
        with self.assertRaises(declared.DefinitionError):
            declared.lakekeeper_declaration(documents(), aws)

    def test_before_any_switch_the_catalog_holds_nothing(self):
        made = declared.lakekeeper_declaration(documents(collector_enabled=True, load_enabled=True, marked=False), AWS)
        self.assertEqual((made["tables"], made["lanes"], made["ledger_sources"]), ([], {}, {}))

    def test_only_a_table_lives_in_the_catalog(self):
        docs = documents()
        docs[declared.GRAPH]["nodes"][0]["catalog"] = "lakekeeper"
        with self.assertRaises(declared.DefinitionError):
            declared.lakekeeper_declaration(docs, AWS)


if __name__ == "__main__":
    unittest.main()
