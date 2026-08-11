"""Self-test for the foundation baseline generator.

Every case here is a bug this generator actually had. It reported zero unreachable status values
(it searched the whole tree, so `'failed'` written for another table counted), then reported a
status column on a table whose own comment says it deliberately has none (it attributed an
`ALTER TABLE`'s constraint to the nearest preceding `CREATE TABLE`), then read a constraint's own
list of admitted values as proof that something writes them (its window crossed the statement
boundary), then counted one re-declared constraint twice (the name sat on the previous line and
parsed as unnamed). A measurement that fails these silently reports a flattering number, which is
worse than no measurement at all.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("render-foundation-baseline.py")
SPEC = importlib.util.spec_from_file_location("render_foundation_baseline", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class StatusCheckAttributionTests(unittest.TestCase):
    def test_alter_table_constraint_belongs_to_the_table_it_names(self) -> None:
        sql = {
            Path("m.sql"): """CREATE TABLE catalog.publication_revision (
    id uuid NOT NULL
);

ALTER TABLE catalog.administrative_boundary_revision
    ADD CONSTRAINT administrative_boundary_revision_status_check
        CHECK (status IN ('candidate', 'validated'));
"""
        }
        checks = MODULE.status_checks(sql)
        owners = {owner for owner, _ in checks.values()}
        self.assertEqual(owners, {"catalog.administrative_boundary_revision"})

    def test_a_redeclared_constraint_is_counted_once(self) -> None:
        sql = {
            Path("a.sql"): "CREATE TABLE catalog.job (\n"
            "    CONSTRAINT job_status_check CHECK (status IN ('planned', 'running'))\n);\n",
            Path("b.sql"): "ALTER TABLE catalog.job\n"
            "    ADD CONSTRAINT job_status_check\n"
            "        CHECK (status IN ('planned', 'running', 'failed'));\n",
        }
        checks = MODULE.status_checks(sql)
        self.assertEqual(len(checks), 1)
        # The later migration wins: an append-only set means the last declaration is the live one.
        self.assertEqual(checks["job_status_check"][1], ["planned", "running", "failed"])


class UnreachableValueTests(unittest.TestCase):
    SQL = {
        Path("m.sql"): "CREATE TABLE catalog.job (\n"
        "    CONSTRAINT job_status_check CHECK (status IN ('planned', 'failed'))\n);\n"
    }

    def test_a_value_written_to_another_table_does_not_count(self) -> None:
        sources = [(Path("other.rs"), "\"UPDATE catalog.unrelated SET status = 'failed'\"")]
        _, unreachable = MODULE.unreachable_status_values(self.SQL, sources)
        self.assertEqual(
            {value for _, _, value in unreachable}, {"planned", "failed"}
        )

    def test_a_value_written_to_the_table_counts(self) -> None:
        sources = [(Path("w.rs"), "\"UPDATE catalog.job SET status = 'failed'\"")]
        _, unreachable = MODULE.unreachable_status_values(self.SQL, sources)
        self.assertEqual({value for _, _, value in unreachable}, {"planned"})

    def test_the_constraint_definition_is_not_evidence_of_a_write(self) -> None:
        # An `UPDATE` followed by the constraint that lists every admitted value. Reading past the
        # statement's end made the constraint vouch for values nothing writes.
        sources = [
            (
                Path("m2.sql"),
                "UPDATE catalog.job SET status = 'planned';\n"
                "ALTER TABLE catalog.job ADD CONSTRAINT job_status_check\n"
                "    CHECK (status IN ('planned', 'failed'));\n",
            )
        ]
        _, unreachable = MODULE.unreachable_status_values(self.SQL, sources)
        self.assertEqual({value for _, _, value in unreachable}, {"failed"})

    def test_a_comment_naming_a_value_is_not_evidence_of_a_write(self) -> None:
        sources = [
            (
                Path("m3.sql"),
                "-- `failed` has no writer anywhere in this platform.\n"
                "UPDATE catalog.job SET status = 'planned';\n",
            )
        ]
        _, unreachable = MODULE.unreachable_status_values(self.SQL, sources)
        self.assertEqual({value for _, _, value in unreachable}, {"failed"})


class ProducerTests(unittest.TestCase):
    SQL = {
        Path("m.sql"): "CREATE TABLE catalog.parcel (\n    id uuid\n);\n"
        "CREATE TABLE catalog.parcel_identifier (\n    id uuid\n);\n"
    }

    def test_a_longer_table_name_does_not_satisfy_a_shorter_one(self) -> None:
        sources = [(Path("w.rs"), '"INSERT INTO catalog.parcel_identifier (id) VALUES ($1)"')]
        tables, missing = MODULE.tables_without_producer(self.SQL, sources)
        self.assertEqual(len(tables), 2)
        self.assertEqual(missing, ["catalog.parcel"])


if __name__ == "__main__":
    unittest.main()
