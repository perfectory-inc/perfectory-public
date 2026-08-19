import json
import sys
import tempfile
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from industrial_complex_boundaries_silver_to_postgis_handoff import (  # noqa: E402
    EXPORT_COLUMNS,
    GEOMETRY_SRID,
    OFFICIAL_BOUNDARY_KIND,
    assert_contract_columns,
    build_summary,
    export_lines,
    export_row,
    needs_iceberg_catalog,
    parse_args,
    validate_args,
    write_create_only,
)


def silver_row(**overrides):
    row = {
        "boundary_id": "vworldkr-sandan-boundary:complex-boundary:official:999ZZ0",
        "complex_id": "7df3859c-0000-51fa-8000-000000000001",
        "official_complex_code": "999ZZ0",
        "boundary_kind": OFFICIAL_BOUNDARY_KIND,
        "geometry_srid": GEOMETRY_SRID,
        "geometry_wkb_hex": "0103000000",
        "area_sqm_calculated": "10000.00",
        "geometry_checksum_sha256": "a" * 64,
        "source_record_id": "bronze/vworldkr__sandan_boundary/test.zip",
        "source_snapshot_id": "vworldkr__sandan_boundary-test",
    }
    row.update(overrides)
    return row


def iceberg_args(*extra: str):
    return parse_args(["--output", "out.jsonl", *extra])


class ExportRowTest(unittest.TestCase):
    def test_a_row_projects_to_exactly_the_columns_the_publisher_reads(self):
        exported = export_row(silver_row())
        self.assertEqual(tuple(sorted(exported)), tuple(sorted(EXPORT_COLUMNS)))
        self.assertIsInstance(exported["geometry_srid"], int)
        self.assertEqual(exported["area_sqm_calculated"], "10000.00")

    def test_a_missing_value_is_named_rather_than_invented(self):
        with self.assertRaises(ValueError) as raised:
            export_row(silver_row(complex_id=None))
        self.assertIn("complex_id", str(raised.exception))
        self.assertIn("cannot invent", str(raised.exception))

    def test_a_boundary_that_is_not_official_has_no_serving_projection(self):
        with self.assertRaises(ValueError) as raised:
            export_row(silver_row(boundary_kind="draft"))
        self.assertIn("draft", str(raised.exception))

    def test_a_row_declaring_another_crs_is_refused(self):
        with self.assertRaises(ValueError) as raised:
            export_row(silver_row(geometry_srid=4326))
        self.assertIn(f"EPSG:{GEOMETRY_SRID}", str(raised.exception))


class ExportLinesTest(unittest.TestCase):
    def test_lines_are_ordered_and_compact(self):
        body = export_lines(
            [
                silver_row(official_complex_code="999ZZ1", complex_id="7df3859c-0000-51fa-8000-000000000002"),
                silver_row(),
            ]
        )
        lines = body.splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["official_complex_code"], "999ZZ0")
        self.assertEqual(json.loads(lines[1])["official_complex_code"], "999ZZ1")
        self.assertTrue(body.endswith("\n"))

    def test_two_active_official_boundaries_for_one_complex_are_refused(self):
        with self.assertRaises(ValueError) as raised:
            export_lines([silver_row(), silver_row(boundary_id="other")])
        self.assertIn("at most one", str(raised.exception))

    def test_an_empty_snapshot_is_refused_rather_than_written_as_an_empty_file(self):
        with self.assertRaises(ValueError):
            export_lines([])


class OutputTest(unittest.TestCase):
    def test_an_existing_export_is_never_replaced(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "nested" / "boundaries.jsonl"
            write_create_only(path, "first\n")
            self.assertEqual(path.read_text(encoding="utf-8"), "first\n")
            with self.assertRaises(FileExistsError):
                write_create_only(path, "second\n")
            self.assertEqual(path.read_text(encoding="utf-8"), "first\n")


class ArgumentTest(unittest.TestCase):
    def test_iceberg_is_the_default_input_mode(self):
        args = iceberg_args()
        self.assertEqual(args.input_mode, "iceberg")
        self.assertTrue(needs_iceberg_catalog(args))

    def test_jsonl_input_mode_requires_both_inputs(self):
        args = parse_args(["--output", "out.jsonl", "--input-mode", "jsonl"])
        self.assertFalse(needs_iceberg_catalog(args))
        with self.assertRaises(ValueError):
            validate_args(args)

    def test_a_table_name_that_is_not_an_identifier_is_refused(self):
        with self.assertRaises(ValueError):
            validate_args(iceberg_args("--iceberg-table", "boundaries; DROP TABLE x"))

    def test_a_non_positive_row_bound_is_refused(self):
        with self.assertRaises(ValueError):
            validate_args(iceberg_args("--max-rows", "0"))


class ContractTest(unittest.TestCase):
    def test_every_column_this_job_selects_is_in_its_contract(self):
        assert_contract_columns()


class SummaryTest(unittest.TestCase):
    def test_the_summary_carries_what_the_publisher_needs(self):
        summary = build_summary(
            iceberg_args(),
            "841361364657368624",
            2,
            ["vworldkr__sandan_boundary-test"],
            "2026-08-19T00:00:00Z",
        )
        self.assertEqual(summary["canonical_iceberg_snapshot_id"], "841361364657368624")
        self.assertEqual(summary["source_snapshot_ids"], ["vworldkr__sandan_boundary-test"])
        self.assertEqual(summary["geometry_srid"], GEOMETRY_SRID)
        self.assertFalse(summary["completion_claim_allowed"])
        self.assertIn("does_not_write_postgis", summary["evidence_limitations"])

    def test_a_dry_run_says_it_cannot_name_a_canonical_snapshot(self):
        summary = build_summary(
            parse_args(
                [
                    "--output",
                    "out.jsonl",
                    "--input-mode",
                    "jsonl",
                    "--boundaries-input",
                    "b.jsonl",
                    "--complexes-input",
                    "c.jsonl",
                ]
            ),
            None,
            1,
            ["vworldkr__sandan_boundary-test"],
            "2026-08-19T00:00:00Z",
        )
        self.assertIsNone(summary["canonical_iceberg_snapshot_id"])
        self.assertIn(
            "canonical_iceberg_snapshot_id_is_unknown_outside_iceberg_input_mode",
            summary["evidence_limitations"],
        )


if __name__ == "__main__":
    unittest.main()
