"""validate-only 는 덮어쓰기 승인 없이 지나간다 (root ADR-0096 서울 실증의 교훈).

첫 검증 실행이 덮어쓰기 가드에 걸려 죽었다: 아무것도 쓰지 않는 실행에
--allow-non-smoke-overwrite 를 요구했다. 검증이 승인 플래그를 의례처럼 들고 다니면
그 플래그는 실제 덮어쓰기 자리에서 아무것도 못 막는다. 가드는 쓰기가 일어날
실행에서만 선다.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest import mock

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_panel_silver_to_gold as job  # noqa: E402
from lakehouse_engine import required_catalog_env  # noqa: E402


def parse(*extra: str):
    argv = [
        "parcel_panel_silver_to_gold.py",
        "--input-mode",
        "iceberg",
        "--write-mode",
        "iceberg",
        "--iceberg-snapshot-id",
        "1",
        *extra,
    ]
    with mock.patch.object(sys, "argv", argv):
        return job.parse_args()


class NonSmokeOverwriteGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.pins_path = Path(directory.name) / "pins.json"
        self.pins_path.write_text(json.dumps({k: "1" for k in job.input_sources(parse())}), encoding="utf-8")
        catalog = parse().iceberg_catalog_name
        env = {name: "probe" for name in required_catalog_env(catalog)}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_write_without_approval_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-overwrite"):
            job.validate_args(parse())

    def test_a_write_with_approval_passes(self) -> None:
        job.validate_args(parse("--allow-non-smoke-overwrite", "--source-snapshots-path", str(self.pins_path)))

    def test_validate_only_needs_no_approval(self) -> None:
        job.validate_args(parse("--validate-only", "--source-snapshots-path", str(self.pins_path)))


def parquet_args(*extra: str):
    return parse("--input-mode", "parquet", "--input-root", "/unused-silver",
                 "--write-mode", "parquet", "--output", "/unused-gold", *extra)


class ParcelPanelLineageTest(unittest.TestCase):
    def event(self, *extra: str):
        args = parquet_args(*extra)
        summary = job.build_run_summary(args, 1, 1, {}, {}, {job.PARCEL_SOURCE: "snapshot"})
        return job.build_lineage_event(args, summary)

    def test_completion_lineage_covers_every_current_contract_column(self) -> None:
        event = self.event()
        self.assertEqual(
            tuple(entry["output_column"] for entry in event["column_lineage"]),
            job.column_names(job.load_lakehouse_contract(job.GOLD_CONTRACT_NAME)),
        )
        for entry in event["column_lineage"]:
            with self.subTest(column=entry["output_column"]):
                self.assertTrue(entry["inputs"])
                for source in entry["inputs"]:
                    self.assertTrue(all(source.get(key) for key in ("dataset", "column", "transform")))
                    if source["dataset"] != "foundation-platform.job_arguments":
                        declared = job.column_names(job.load_lakehouse_contract(source["dataset"]))
                        self.assertLessEqual(set(source["column"].split(",")), set(declared))

    def test_attached_paths_depend_on_resolved_links_and_section_availability(self) -> None:
        event = self.event()
        columns = {entry["output_column"]: entry["inputs"] for entry in event["column_lineage"]}
        inputs = columns["attached_via_json"]
        link_fields = {
            column for source in inputs if source["dataset"] == job.LINEAGE_SOURCE
            for column in source["column"].split(",")
        }
        self.assertEqual(link_fields, {
            "predecessor_pnu", "successor_pnu", "relation", "grade", "evidence_kind", "evidence_ref",
        })
        for dataset in (job.PARCEL_SOURCE, *job.ATTRIBUTE_SOURCES):
            self.assertTrue(any(source["dataset"] == dataset and "pnu" in source["column"].split(",")
                                for source in inputs), dataset)
        self.assertIn(job.LINEAGE_SOURCE,
                      {source["qualified_name"] for source in event["additional_input_datasets"]})

    def test_digest_traces_every_content_dependency_without_a_self_reference(self) -> None:
        columns = {entry["output_column"]: entry["inputs"]
                   for entry in self.event()["column_lineage"]}
        expected = {(source["dataset"], source["column"])
                    for column in job.CONTENT_DIGEST_COLUMNS for source in columns[column]}
        inputs = columns["row_digest"]
        self.assertEqual({(source["dataset"], source["column"]) for source in inputs}, expected)
        self.assertTrue(all(source["dataset"] != job.GOLD_CONTRACT_NAME for source in inputs))
        self.assertTrue(all("sha256" in source["transform"] for source in inputs))
        self.assertFalse(any("source_snapshot_id" in source["column"].split(",")
                             or "published_at_utc" in source["column"].split(",") for source in inputs))

    def test_disabled_carry_has_no_lineage_input_and_declares_literal_null(self) -> None:
        event = self.event("--no-carry-lineage")
        self.assertNotIn(job.LINEAGE_SOURCE,
                         {source["qualified_name"] for source in event["additional_input_datasets"]})
        columns = {entry["output_column"]: entry["inputs"] for entry in event["column_lineage"]}
        self.assertEqual(columns["attached_via_json"], [{
            "dataset": "foundation-platform.job_arguments", "column": "carry_lineage",
            "transform": "literal_null_when_disabled",
        }])
        self.assertFalse(any(source["dataset"] == job.LINEAGE_SOURCE
                             for inputs in columns.values() for source in inputs))

    def test_future_unmapped_column_is_rejected_before_spark_for_writes_and_validation(self) -> None:
        for extra in ((), ("--validate-only",)):
            with self.subTest(extra=extra), \
                    mock.patch.object(job, "parse_args", return_value=parquet_args(*extra)), \
                    mock.patch.object(job, "GOLD_COLUMNS", (*job.GOLD_COLUMNS, "future_content")), \
                    mock.patch.object(job, "CONTENT_DIGEST_COLUMNS", (*job.CONTENT_DIGEST_COLUMNS, "future_content")), \
                    mock.patch.object(job, "build_spark_session") as start:
                start.side_effect = AssertionError("Spark started before lineage contract validation")
                with self.assertRaisesRegex(ValueError, "future_content"):
                    job.main()
                start.assert_not_called()

    def test_main_finishes_after_readback_with_and_without_lineage_output(self) -> None:
        # Isolate Spark I/O and transformations; completion metadata and the control flow are real.
        for write_mode in ("parquet", "iceberg"):
            for output_requested in (False, True):
                with self.subTest(write_mode=write_mode, output_requested=output_requested), \
                        tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                    summary_path = Path(directory) / "summary.json"
                    lineage_path = Path(directory) / "lineage.json"
                    args = parquet_args("--summary-output", str(summary_path))
                    args.write_mode = write_mode
                    args.allow_non_smoke_overwrite = True
                    args.lineage_output = str(lineage_path) if output_requested else None
                    stack.enter_context(mock.patch.dict(os.environ, {
                        name: "probe" for name in required_catalog_env(args.iceberg_catalog_name)
                    }))
                    stack.enter_context(mock.patch.object(job, "parse_args", return_value=args))
                    frame, spark = mock.MagicMock(), mock.MagicMock()
                    for name in ("read_source", "filtered_by_region", "with_lineage_sources",
                                 "build_zonings", "build_price", "build_characteristics",
                                 "build_forest_ledger", "build_transfer_history", "build_land_rights",
                                 "carry_section", "build_gold_panel_frame"):
                        stack.enter_context(mock.patch.object(job, name, return_value=frame))
                    stack.enter_context(mock.patch.object(job, "build_spark_session", return_value=spark))
                    stack.enter_context(mock.patch.object(job, "assert_single_snapshot", return_value="snapshot"))
                    stack.enter_context(mock.patch.object(job, "read_carry_candidates", return_value=None))
                    stack.enter_context(mock.patch.object(job, "resolve_zoning_anchors", return_value={}))
                    validation = stack.enter_context(mock.patch.object(job, "validate_gold_frame", return_value=(1, {})))
                    parquet_write = stack.enter_context(mock.patch.object(job, "write_gold_parquet"))
                    iceberg_write = stack.enter_context(mock.patch.object(job, "write_gold_iceberg", return_value=()))
                    stack.enter_context(mock.patch.object(job.F, "expr", create=True))
                    stack.enter_context(mock.patch.object(job.F, "col", create=True))
                    stack.enter_context(mock.patch.object(job.StorageLevel, "MEMORY_AND_DISK", "disk", create=True))
                    stdout = stack.enter_context(redirect_stdout(io.StringIO()))

                    self.assertEqual(job.main(), 0)

                    (parquet_write if write_mode == "parquet" else iceberg_write).assert_called_once()
                    self.assertEqual(validation.call_count, 2, "candidate and persisted rows were checked")
                    self.assertEqual(json.loads(summary_path.read_text())["persisted_row_count"], 1)
                    self.assertIn("gold-parcel-panel-write-ok rows=1", stdout.getvalue())
                    self.assertEqual(lineage_path.exists(), output_requested)
                    if output_requested:
                        event = json.loads(lineage_path.read_text())
                        self.assertEqual(event["openlineage_mapping"]["event_type"], "COMPLETE")
                        self.assertEqual(len(event["column_lineage"]), len(job.GOLD_COLUMNS))
                    frame.persist.return_value.unpersist.assert_called_once()
                    spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
