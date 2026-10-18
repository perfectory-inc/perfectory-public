"""The ADR-0275 measurement writes only to a scratch namespace and fingerprints content, not lineage."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import silver_append_measure as measure  # noqa: E402


class SilverAppendMeasureTest(unittest.TestCase):
    def args(self, *extra):
        return measure.parse_args(["--old-snapshot-id", "123", *extra])

    def test_writes_go_only_to_a_scratch_namespace(self):
        self.assertEqual(self.args().scratch_namespace, "scratch_adr0275")
        for namespace in ("silver", "gold", "reference", "scratch-x"):
            with self.subTest(namespace), self.assertRaises(ValueError):
                self.args("--scratch-namespace", namespace)
        with self.assertRaises(ValueError):
            self.args("--source-namespace", "scratch_a", "--scratch-namespace", "scratch_a")

    def test_names_and_the_snapshot_id_are_plain(self):
        for extra in (("--table", "x; DROP"), ("--iceberg-catalog-name", "a.b")):
            with self.subTest(extra), self.assertRaises(ValueError):
                self.args(*extra)
        with self.assertRaises(ValueError):
            measure.parse_args(["--old-snapshot-id", "main"])

    def test_the_fingerprint_reads_the_declared_content_columns_only(self):
        columns = list(measure.CONTENT_COLUMNS["land_transfer_history"])
        everything = [*columns, "data_reference_date", "source_record_id", "source_snapshot_id", "ingested_at_utc"]
        content = measure.content_columns("land_transfer_history", everything, everything)
        self.assertEqual(content, columns)
        # 2026-10-18: the release's own date differs on every row, so a fingerprint over it counts every
        # row as changed; no per-release column may be content.
        for per_release in ("data_reference_date", "source_record_id", "source_snapshot_id", "ingested_at_utc"):
            self.assertNotIn(per_release, content)
        with self.assertRaisesRegex(ValueError, "lacks declared content columns"):
            measure.content_columns("land_transfer_history", everything[1:], everything)

    def test_a_table_without_declared_content_columns_is_refused(self):
        with self.assertRaisesRegex(ValueError, "declares no content columns"):
            self.args("--table", "land_use_plan")


if __name__ == "__main__":
    unittest.main()
