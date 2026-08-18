import sys
import unittest
from pathlib import Path
from unittest.mock import patch


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from industrial_complex_boundaries_handoff_to_silver import (  # noqa: E402
    COMPLEX_JOIN_COLUMNS,
    GEOMETRY_SRID,
    HANDOFF_INPUT_COLUMNS,
    JOIN_COUNT_METRICS,
    OFFICIAL_BOUNDARY_KIND,
    SILVER_COLUMNS,
    build_run_summary,
    merge_join_counts,
    merge_transport_metrics,
    needs_iceberg_catalog,
    parse_args,
    run_summary_complexes_source,
    run_summary_disposition,
    validate_args,
)

ICEBERG_ENV = {
    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": "https://catalog.invalid",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": "warehouse",
    "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": "token",
}


def local_args(*extra: str):
    return parse_args(
        [
            "--input",
            "handoff.jsonl",
            "--output",
            "out",
            "--complexes-mode",
            "jsonl",
            "--complexes-input",
            "complexes.jsonl",
            *extra,
        ]
    )


class ContractShapeTest(unittest.TestCase):
    def test_srid_comes_from_the_contract_not_the_job(self) -> None:
        self.assertEqual(GEOMETRY_SRID, 5186)

    def test_handoff_carries_the_contract_columns_plus_the_join_key_and_geometry(self) -> None:
        for column in SILVER_COLUMNS:
            self.assertIn(column, HANDOFF_INPUT_COLUMNS)
        for column in ("official_complex_code", "geometry_wkb_hex", "geometry_wkb_encoding"):
            self.assertIn(column, HANDOFF_INPUT_COLUMNS)

    def test_the_two_joined_columns_are_contract_columns(self) -> None:
        self.assertIn("complex_id", SILVER_COLUMNS)
        self.assertIn("sido_code", SILVER_COLUMNS)

    def test_only_identity_and_province_are_read_off_the_complex(self) -> None:
        self.assertEqual(
            COMPLEX_JOIN_COLUMNS,
            ("official_complex_code", "complex_id", "sido_code"),
        )

    def test_this_job_loads_the_official_boundary_kind(self) -> None:
        self.assertEqual(OFFICIAL_BOUNDARY_KIND, "official")


class ValidateArgsTest(unittest.TestCase):
    def test_local_run_needs_no_catalog(self) -> None:
        args = local_args()

        self.assertFalse(needs_iceberg_catalog(args))
        validate_args(args)

    def test_jsonl_complexes_without_a_path_is_refused(self) -> None:
        args = parse_args(
            ["--input", "handoff.jsonl", "--output", "out", "--complexes-mode", "jsonl"]
        )

        with self.assertRaisesRegex(ValueError, "--complexes-input is required"):
            validate_args(args)

    def test_parquet_write_without_an_output_is_refused(self) -> None:
        args = parse_args(
            [
                "--input",
                "handoff.jsonl",
                "--complexes-mode",
                "jsonl",
                "--complexes-input",
                "complexes.jsonl",
            ]
        )

        with self.assertRaisesRegex(ValueError, "--output is required"):
            validate_args(args)

    def test_reading_complexes_from_iceberg_needs_the_catalog_even_when_writing_parquet(
        self,
    ) -> None:
        args = parse_args(["--input", "handoff.jsonl", "--output", "out"])

        self.assertTrue(needs_iceberg_catalog(args))
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Missing required environment variable"):
                validate_args(args)
        with patch.dict("os.environ", ICEBERG_ENV, clear=True):
            validate_args(args)

    def test_overwriting_a_non_smoke_table_needs_the_flag(self) -> None:
        args = parse_args(
            [
                "--input",
                "handoff.jsonl",
                "--write-mode",
                "iceberg",
                "--iceberg-write-mode",
                "overwrite",
                "--complexes-mode",
                "jsonl",
                "--complexes-input",
                "complexes.jsonl",
            ]
        )

        with patch.dict("os.environ", ICEBERG_ENV, clear=True):
            with self.assertRaisesRegex(ValueError, "allow-non-smoke-overwrite"):
                validate_args(args)


class RunSummaryTest(unittest.TestCase):
    """The join counts describe the source, so they have to survive into the summary.

    They are properties of the run rather than of the written rows: re-measuring them off the
    persisted frame would report zero and read as "nothing was dropped".
    """

    def test_join_counts_reach_the_summary(self) -> None:
        merged = merge_join_counts(
            {"row_count": 1344},
            {
                "orphan_boundary_count": 19,
                "complex_without_boundary_count": 98,
                "complex_without_sido_code_count": 0,
            },
        )

        self.assertEqual(merged["row_count"], 1344)
        self.assertEqual(merged["orphan_boundary_count"], 19)
        self.assertEqual(merged["complex_without_boundary_count"], 98)
        self.assertEqual(merged["complex_without_sido_code_count"], 0)

    def test_every_join_count_is_reported_even_when_absent(self) -> None:
        merged = merge_join_counts({"row_count": 0}, {})

        for metric in JOIN_COUNT_METRICS:
            self.assertEqual(merged[metric], 0, metric)

    def test_transport_metrics_come_from_the_candidate_frame(self) -> None:
        merged = merge_transport_metrics(
            {"invalid_geometry_encoding_count": 0, "invalid_geometry_wkb_hex_count": 0},
            {"invalid_geometry_encoding_count": 3, "invalid_geometry_wkb_hex_count": 5},
        )

        self.assertEqual(merged["invalid_geometry_encoding_count"], 3)
        self.assertEqual(merged["invalid_geometry_wkb_hex_count"], 5)

    def test_summary_names_the_complexes_source(self) -> None:
        args = local_args()

        source = run_summary_complexes_source(args)

        self.assertEqual(source["kind"], "jsonl")
        self.assertEqual(source["contract"], "silver.industrial_complexes")
        self.assertEqual(source["path"], "complexes.jsonl")

    def test_summary_records_the_srid_and_the_counts(self) -> None:
        args = local_args("--validate-only")

        summary = build_run_summary(
            args,
            row_count=1344,
            persisted_row_count=None,
            quality_metrics=merge_join_counts(
                {"row_count": 1344}, {"orphan_boundary_count": 19}
            ),
            source_snapshot_summary={
                "source_snapshot_count": 1,
                "source_snapshot_ids": ["synthetic-snapshot-0001"],
                "source_snapshot_truncated": False,
            },
        )

        self.assertEqual(summary["contract"], "silver.industrial_complex_boundaries")
        self.assertEqual(summary["geometry_srid"], 5186)
        self.assertEqual(summary["row_count"], 1344)
        self.assertEqual(summary["quality_metrics"]["orphan_boundary_count"], 19)
        self.assertEqual(summary["write_disposition"], "validate_only")
        self.assertEqual(summary["complexes_source"]["kind"], "jsonl")

    def test_disposition_names_the_write(self) -> None:
        self.assertEqual(run_summary_disposition(local_args()), "parquet_overwrite")


if __name__ == "__main__":
    unittest.main()
