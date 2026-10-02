"""Engine contract v3 acceptance matches the native Rust reader's envelope."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import lakehouse_engine as engine


class NativeEnginePolicyTests(unittest.TestCase):
    def setUp(self):
        self.contract = json.loads(engine.DEFAULT_CONTRACT_PATH.read_text(encoding="utf-8"))

    def load(self, contract):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "engine.json"
            path.write_text(json.dumps(contract), encoding="utf-8")
            with patch.dict(os.environ, {engine.CONTRACT_PATH_ENV: str(path)}):
                return engine.load_engine_contract()

    def test_current_contract_and_packages_resolve(self):
        self.assertEqual(self.load(self.contract), self.contract)
        self.assertTrue(engine.iceberg_packages())
        self.assertEqual(self.contract["schema_version"], engine.CONTRACT_SCHEMA_VERSION)

    def test_other_schemas_fail(self):
        for version in (engine.CONTRACT_SCHEMA_VERSION - 1, engine.CONTRACT_SCHEMA_VERSION + 1,
                        float(engine.CONTRACT_SCHEMA_VERSION), str(engine.CONTRACT_SCHEMA_VERSION), True):
            with self.subTest(version=version):
                contract = copy.deepcopy(self.contract)
                contract["schema_version"] = version
                with self.assertRaises(ValueError):
                    self.load(contract)

    def test_native_profile_rejects_invalid_and_legacy_fields(self):
        for field, value in (
            ("memory_mib", 0), ("memory_mib", 1), ("memory_mib", 1 << 31),
            ("cpu_slots", 0), ("pids_limit", 0), ("swap_mib", 1),
            ("memory_mib", True), ("memory_mib", "4096"),
            ("memory_mib", -1), ("memory_mib", 2.5), ("admission_gid", 1),
        ):
            with self.subTest(field=field, value=value):
                contract = copy.deepcopy(self.contract)
                contract["execution_profile"][field] = value
                with self.assertRaises(ValueError):
                    self.load(contract)
        contract = copy.deepcopy(self.contract)
        contract["execution_profile"]["pids_limit"] = contract["execution_profile"]["cpu_slots"] - 1
        with self.assertRaises(ValueError):
            self.load(contract)

    def test_required_sections_and_fields_cannot_disappear(self):
        for section in ("execution_profile", "serving_parquet"):
            contract = copy.deepcopy(self.contract)
            del contract[section]
            with self.assertRaises(ValueError):
                self.load(contract)
            for field in self.contract[section]:
                contract = copy.deepcopy(self.contract)
                del contract[section][field]
                with self.assertRaises(ValueError):
                    self.load(contract)

    def test_parquet_bounds_reject_wrong_types_zero_overflow_and_inversion(self):
        for field in self.contract["serving_parquet"]:
            for value in (0, -1, True, "1", 1.5, 1 << 31):
                contract = copy.deepcopy(self.contract)
                contract["serving_parquet"][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.load(contract)
        for target, maximum in (("row_group_target_bytes", "max_row_group_bytes"),
                                ("row_group_check_min_records", "row_group_check_max_records")):
            contract = copy.deepcopy(self.contract)
            contract["serving_parquet"][target] = contract["serving_parquet"][maximum] + 1
            with self.assertRaises(ValueError):
                self.load(contract)


if __name__ == "__main__":
    unittest.main()
