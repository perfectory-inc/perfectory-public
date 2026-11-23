"""The data contracts' machine rules are Dagster asset checks the quality job reports (root ADR-0609 §2).

contract_checks.py declares them from the contract files on the graph node of each contract's table;
only the quality job's run takes them from its unit's journal, on tables others write, at the severity
the unit gives. Tables, columns and values in the synthetic cases are made up.
"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))

try:
    import dagster  # noqa: F401
except ImportError:  # the default runner; CI runs these in the code location's image
    dagster = None

GRAPH = {"nodes": [
    {"id": "silver-example", "table_name": "silver.example"},
    {"id": "example-surface"},
]}
CONTRACT = """id: "silver.example"
schema:
  - physicalName: "silver.example"
    properties:
      - name: "pnu"
      - name: "kind"
        quality:
          - metric: "invalidValues"
            arguments:
              validValues: ["a", "b"]
"""


@unittest.skipIf(dagster is None, "needs Dagster")
class TheContractsDeclareTheChecks(unittest.TestCase):
    def rules(self, *texts):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        for index, text in enumerate(texts):
            (pathlib.Path(directory.name) / f"c{index}.odcs.yaml").write_text(text, encoding="utf-8")
        import contract_checks

        return contract_checks.contract_rules(pathlib.Path(directory.name))

    def test_a_contract_declares_its_columns_and_one_check_per_allowed_values_rule(self):
        self.assertEqual(self.rules(CONTRACT), {"silver.example": ["contract_columns_present",
                                                                   "contract_allowed_values_kind"]})

    def test_the_checks_land_on_the_node_of_the_contract_s_table_and_never_block(self):
        import contract_checks
        import observe_assets

        keys = observe_assets.node_keys(GRAPH)
        declared = contract_checks.declared(GRAPH, keys, self.rules(CONTRACT))
        self.assertEqual([(item.key().to_user_string(), item.check, item.kind, item.blocking) for item in declared], [
            ("iceberg/silver/example", "contract_columns_present", "contract_columns_present", False),
            ("iceberg/silver/example", "contract_allowed_values_kind", "contract_allowed_values", False)])

    def test_a_contract_whose_table_no_node_names_is_refused(self):
        import contract_checks
        import observe_assets

        with self.assertRaisesRegex(observe_assets.job_specs.JobListError, "silver.nowhere"):
            contract_checks.declared(GRAPH, observe_assets.node_keys(GRAPH),
                                     self.rules(CONTRACT.replace("silver.example", "silver.nowhere")))

    def test_two_contracts_of_one_table_are_refused(self):
        import observe_assets

        with self.assertRaisesRegex(observe_assets.job_specs.JobListError, "two contracts"):
            self.rules(CONTRACT, CONTRACT)

    def test_every_real_contract_is_a_check_of_the_code_location(self):
        import asset_checks
        import contract_checks
        import observe_assets

        _, graph = observe_assets.contracts()
        rules = contract_checks.contract_rules()
        self.assertTrue(rules, "the release's contracts are read")
        specs = {(spec.asset_key, spec.name): spec for check in asset_checks.build(graph) for spec in check.check_specs}
        keys = observe_assets.node_keys(graph)
        nodes = {node["table_name"]: node["id"] for node in graph["nodes"] if node.get("table_name")}
        for table, checks in rules.items():
            for check in checks:
                spec = specs[(keys[nodes[table]], check)]
                self.assertFalse(spec.blocking, (table, check))


@unittest.skipIf(dagster is None, "needs Dagster")
class OnlyTheQualityJobReportsThem(unittest.TestCase):
    def test_the_quality_job_reports_every_contract_check_at_the_severity_its_unit_gives(self):
        import contract_checks
        import observe_assets
        import run_jobs

        _, graph = observe_assets.contracts()
        reported = run_jobs.reported_checks([], graph, job_id=contract_checks.REPORTER)
        contract = {key: severity for key, severity in reported.items() if contract_checks.kind_of(key[1])}
        self.assertEqual(len(contract), sum(len(checks) for checks in contract_checks.contract_rules().values()))
        self.assertEqual(set(contract.values()), {None})

    def test_no_other_job_reports_a_contract_check(self):
        import contract_checks
        import observe_assets
        import run_jobs

        _, graph = observe_assets.contracts()
        # Even asked for every dataset there is, a job that is not the quality job reports none.
        everything = list(observe_assets.node_keys(graph).values())
        reported = run_jobs.reported_checks(everything, graph, job_id="gold_panel_rebuild")
        self.assertFalse([key for key in reported if contract_checks.kind_of(key[1])])


if __name__ == "__main__":
    unittest.main()
