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


class ASelfVerifyingRecordIsMadeAsProductionMakesIt(unittest.TestCase):
    """The audit chain (root ADR-0584 §2): an event row leaves its transaction id to its default, the
    digest is made by its own function after the events hold a row, and the witness follows it."""

    class Database:
        def __init__(self):
            self.ran = []

        def column(self, table, name, category, *, generated=False, has_default=False):
            return {"schema": "audit", "table": table, "column": name, "not_null": True, "has_default": has_default,
                    "generated": generated, "udt": None, "element": None, "category": category, "length": None}

        def rows(self, sql):
            if sql == shape.COLUMNS:
                return [self.column("event", "seq", "bigint", generated=True, has_default=True),
                        self.column("event", "txid", "xid8", has_default=True),
                        self.column("event", "object_id", "text"),
                        self.column("digest", "digest_no", "bigint"),
                        self.column("digest_witness", "digest_no", "bigint")]
            if sql == shape.FOREIGN_KEYS:
                return [{"schema": "audit", "table": "digest_witness", "parent_schema": "audit", "parent": "digest",
                         "columns": ["digest_no"], "parent_columns": ["digest_no"]}]
            if sql in (shape.CHECKS, shape.ENUM_FIRST):
                return [] if sql == shape.CHECKS else {}
            return [1] if any("make_digest" in ran for ran in self.ran) else []

        def run(self, sql, **_):
            self.ran.append(sql)
            return type("Result", (), {"returncode": 0, "stderr": ""})()

    def test_the_digest_is_made_after_the_events_and_before_its_witness(self):
        db = self.Database()
        made_by = {"audit.digest": {"sql": "select count(*) from audit.make_digest();", "after": ["audit.event"]}}
        failures = shape.seed(db, ["audit.digest_witness", "audit.event"], {}, {}, made_by)
        self.assertEqual(failures, [])
        event, digest, witness = db.ran
        self.assertTrue(event.startswith("insert into audit.event (object_id)"), event)
        self.assertEqual(digest, "select count(*) from audit.make_digest();")
        self.assertIn("(select digest_no from audit.digest limit 1)", witness)

    def test_a_type_with_no_value_and_no_default_still_names_itself(self):
        db = self.Database()
        column = db.column("event", "txid", "xid8")
        with self.assertRaises(LookupError):
            shape.column_value(column, [], {}, {})


class TheCommittedShape(unittest.TestCase):
    def test_every_gap_is_a_table_production_holds_rows_in(self):
        import json

        tables = json.loads(shape.SHAPE.read_text(encoding="utf-8"))["tables_with_rows"]
        gaps = json.loads(shape.HINTS.read_text(encoding="utf-8"))["not_yet_seeded"]
        self.assertEqual(sorted(set(gaps) - set(tables)), [])
        self.assertTrue(all(reason.strip() for reason in gaps.values()))


if __name__ == "__main__":
    unittest.main()
