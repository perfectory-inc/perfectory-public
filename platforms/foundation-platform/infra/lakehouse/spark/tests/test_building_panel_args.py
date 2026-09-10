"""Validation follows the same guards as writing, except for write authorization."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # Reuse the parcel lane's import-only Spark stub.
import building_panel_silver_to_gold as job
from lakehouse_engine import required_catalog_env


class BuildingPanelArgumentsTest(unittest.TestCase):
    def parse(self, *extra):
        return job.parse_args(["--input-mode", "iceberg", "--write-mode", "iceberg",
                               "--iceberg-snapshot-id", "1", *extra])

    def test_write_authorization_is_required_only_for_writes(self):
        with patch.dict(os.environ, {k: "probe" for k in required_catalog_env("r2")}):
            with self.assertRaisesRegex(ValueError, "allow-non-smoke-overwrite"):
                job.validate_args(self.parse())
            job.validate_args(self.parse("--validate-only"))
            job.validate_args(self.parse("--allow-non-smoke-overwrite"))

    def test_validate_only_still_checks_source_configuration_and_scope(self):
        with self.assertRaisesRegex(ValueError, "pnu-prefix"):
            job.validate_args(self.parse("--validate-only", "--pnu-prefix", "bad"))
        with self.assertRaisesRegex(ValueError, "inside"):
            job.validate_args(self.parse("--validate-only", "--region-prefix", "99", "--pnu-prefix", "88"))
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                job.validate_args(self.parse("--validate-only"))

    def test_snapshot_file_must_cover_every_source_and_match_title_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshots.json"
            path.write_text(json.dumps({k: "1" for k in job.ALL_SOURCES}), encoding="utf-8")
            args = self.parse("--source-snapshots-path", str(path))
            self.assertEqual(set(job.load_snapshot_pins(args)), set(job.ALL_SOURCES))
            path.write_text(json.dumps({job.TITLE_SOURCE: "1"}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "every Silver source"):
                job.load_snapshot_pins(args)


if __name__ == "__main__":
    unittest.main()
