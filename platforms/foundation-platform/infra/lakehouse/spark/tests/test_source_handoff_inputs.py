import importlib.util
import json
from pathlib import Path
import unittest

SPARK = Path(__file__).resolve().parents[1]
CONTRACTS = SPARK.parent / "contracts"


class SourceHandoffInputsTest(unittest.TestCase):
    def planner(self):
        path = SPARK / "jobs/source_handoff_inputs.py"
        self.assertTrue(path.is_file(), "contract-driven input planner is missing")
        spec = importlib.util.spec_from_file_location("source_handoff_inputs", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.plan_inputs

    def test_existing_sido_contract_uses_declared_count_and_propagates_missing_regions(self):
        c = json.loads((CONTRACTS / "vworld-land-right-registration-source-objects.json").read_text(encoding="utf-8"))
        plan = self.planner()
        self.assertEqual(len(plan(c, "test", None)), 17)
        c["objects"] = [o for o in c["objects"] if not (o["region_code"] == "11" and o["vintage"] == c["selected_vintage"])]
        with self.assertRaises(ValueError):
            plan(c, "test", None)

    def test_a_lane_whose_release_the_ledger_picks_is_not_planned_here(self):
        # The hub lanes load themselves from the Bronze ledger's newest complete release (root
        # ADR-0169); a measured-object plan of them would be a second, hand-picked answer.
        plan = self.planner()
        for name in sorted(CONTRACTS.glob("hub-building-register-*-source-objects.json")):
            with self.subTest(contract=name.name):
                c = json.loads(name.read_text(encoding="utf-8"))
                with self.assertRaisesRegex(ValueError, "Bronze ledger"):
                    plan(c, "test")


if __name__ == "__main__":
    unittest.main()
