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

    def test_the_fingerprint_reads_shared_content_columns_only(self):
        old = ["pnu", "seq", "source_snapshot_id", "ingested_at_utc", "dropped"]
        new = ["pnu", "seq", "added", "source_snapshot_id", "ingested_at_utc"]
        self.assertEqual(measure.content_columns(old, new), ["pnu", "seq"])


if __name__ == "__main__":
    unittest.main()
