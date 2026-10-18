"""The content fingerprint's comparison, without Spark (root ADR-0274 §1)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # noqa: F401  Import-only PySpark stub.
import by_pnu_panel_delta as delta  # noqa: E402
import gold_fingerprint  # noqa: E402


class UnchangedRows(unittest.TestCase):
    def test_the_same_fingerprint_on_both_snapshots_is_their_row_count(self):
        same = gold_fingerprint.value(39_861_511, 123456789)
        self.assertEqual(delta.unchanged_rows(same, same), 39_861_511)

    def test_a_different_or_missing_fingerprint_is_compared(self):
        self.assertIsNone(delta.unchanged_rows(gold_fingerprint.value(2, 5), gold_fingerprint.value(2, 6)))
        self.assertIsNone(delta.unchanged_rows(gold_fingerprint.value(2, 5), gold_fingerprint.value(3, 5)))
        self.assertIsNone(delta.unchanged_rows(None, gold_fingerprint.value(2, 5)))
        self.assertIsNone(delta.unchanged_rows(gold_fingerprint.value(2, 5), None))

    def test_an_unreadable_fingerprint_is_refused_not_read_as_no_change(self):
        with self.assertRaises(ValueError):
            delta.unchanged_rows("garbage", "garbage")


if __name__ == "__main__":
    unittest.main()
