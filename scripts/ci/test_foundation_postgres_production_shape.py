"""The production-shape seed reads a CHECK's own values and keeps its gap list honest (root ADR-0259 §8).

The database half runs in the Postgres integration job, where a planted migration that adds a
required column must fail on the seeded rows; these are the parts that need no database.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import foundation_postgres_production_shape as shape  # noqa: E402


def check(definition):
    return {"definition": definition}


class CheckValues(unittest.TestCase):
    def test_the_first_value_a_list_check_allows(self):
        definition = "CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text])))"
        self.assertEqual(shape.check_value("status", [check(definition)]), "'planned'")

    def test_an_example_of_an_anchored_regex(self):
        self.assertEqual(shape.check_value("sha", [check("CHECK ((sha ~ '^[0-9a-f]{64}$'::text))")]), "'" + "0" * 64 + "'")
        self.assertEqual(shape.regex_example("^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$"), "iceberg:AAA")
        self.assertEqual(shape.regex_example("^[1-9][0-9]*$"), "1")

    def test_a_regex_it_cannot_read_gives_nothing(self):
        self.assertIsNone(shape.regex_example("^(a|b)$"))
        self.assertIsNone(shape.regex_example("[0-9]+"))

    def test_a_value_inside_a_function_call_is_not_the_columns(self):
        definition = "CHECK ((jsonb_typeof(request_params) = 'object'::text))"
        self.assertIsNone(shape.check_value("request_params", [check(definition)]))

    def test_another_columns_value_is_not_this_ones(self):
        definition = "CHECK (((status <> 'failed'::text) OR (error_message IS NOT NULL)))"
        self.assertIsNone(shape.check_value("error_message", [check(definition)]))


class TheGapListOnlyShrinks(unittest.TestCase):
    TABLES = ["catalog.a", "catalog.b"]

    def test_a_named_gap_is_not_a_problem(self):
        self.assertEqual(shape.seed_problems(["catalog.a: ERROR x"], {"catalog.a": "why"}, self.TABLES), [])

    def test_an_unnamed_failure_is_one(self):
        self.assertIn("catalog.b: ERROR x", shape.seed_problems(["catalog.b: ERROR x"], {}, self.TABLES)[0])

    def test_a_named_table_that_seeds_now_must_leave_the_list(self):
        self.assertIn("seeds now", shape.seed_problems([], {"catalog.a": "why"}, self.TABLES)[0])

    def test_a_named_table_production_holds_no_rows_in_must_leave_the_list(self):
        self.assertIn("holds no rows", shape.seed_problems([], {"catalog.z": "why"}, self.TABLES)[0])


class TheCommittedShape(unittest.TestCase):
    def test_every_gap_is_a_table_production_holds_rows_in(self):
        import json

        tables = json.loads(shape.SHAPE.read_text(encoding="utf-8"))["tables_with_rows"]
        gaps = json.loads(shape.HINTS.read_text(encoding="utf-8"))["not_yet_seeded"]
        self.assertEqual(sorted(set(gaps) - set(tables)), [])
        self.assertTrue(all(reason.strip() for reason in gaps.values()))


if __name__ == "__main__":
    unittest.main()
