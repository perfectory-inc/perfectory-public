"""The Silver load result checks (orchestration/dagster/load_checks.py): the names the code location
implements are the names the court auction DuckDB load reports, and every table that load writes
declares the three."""

from __future__ import annotations

import ast
import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
LOAD_CHECKS = PLATFORM / "orchestration" / "dagster" / "load_checks.py"
LOAD = PLATFORM / "services" / "foundation-outbox-publisher" / "src" / "courtauction_silver_load.rs"
STAGE = PLATFORM / "crates" / "publisher" / "foundation-publisher-silver" / "src" / "courtauction_silver_stage.rs"
DEFINITION = PLATFORM / "definitions" / "court-auction-collect.definition.json"


def check_names() -> set[str]:
    """The check names load_checks.py defines (read as source: Dagster is not needed to say them)."""
    tree = ast.parse(LOAD_CHECKS.read_text(encoding="utf-8"))
    return {node.value.value for node in tree.body
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str) and node.targets[0].id in ("ROWS", "KEY_UNIQUE", "REQUIRED_PRESENT")}


class LoadChecks(unittest.TestCase):
    def test_the_load_reports_the_checks_the_code_location_implements(self):
        names = check_names()
        self.assertEqual(len(names), 3)
        reported = set(re.findall(r'"(silver_load_[a-z_]+)"', LOAD.read_text(encoding="utf-8")))
        self.assertEqual(reported, names)

    def test_every_table_the_duckdb_load_writes_declares_the_three(self):
        tables = set(re.findall(r'"(silver\.court_auction_[a-z_]+)"', STAGE.read_text(encoding="utf-8")))
        self.assertGreaterEqual(len(tables), 26)
        definition = json.loads(DEFINITION.read_text(encoding="utf-8"))
        node_of = {asset.get("table_name"): asset["id"] for asset in definition["assets"]}
        declared = {(check["asset"], check["check"]) for check in definition["asset_checks"]}
        for table in sorted(tables):
            for name in check_names():
                self.assertIn((node_of[table], name), declared, f"{table} {name}")


if __name__ == "__main__":
    unittest.main()
