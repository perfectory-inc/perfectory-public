"""Every panel read uses the same complete, once-read Iceberg input selection (ADR-0130)."""
import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
import io
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # Import-only Spark fixture.
import building_panel_silver_to_gold as building
import parcel_panel_silver_to_gold as parcel
from lakehouse_engine import required_catalog_env


class PanelSnapshotPinsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "pins.json"
        env = mock.patch.dict(os.environ, {k: "probe" for k in required_catalog_env("r2")})
        env.start()
        self.addCleanup(env.stop)

    def args(self, job):
        argv = ["job", "--input-mode", "iceberg", "--write-mode", "iceberg",
                "--iceberg-snapshot-id", "11", "--validate-only"]
        with mock.patch.object(sys, "argv", argv):
            args = job.parse_args()
        args.source_snapshots_path = str(self.path)
        return args

    def pins(self, job, carry=True):
        sources = list(job.ALL_SOURCES)
        if job is parcel and carry:
            sources.append(parcel.LINEAGE_SOURCE)
        return {source: "11" for source in sources}

    def write(self, pins):
        self.path.write_text(json.dumps(pins), encoding="utf-8")

    def test_missing_file_is_refused_by_both_validation_entrypoints(self):
        for job in (parcel, building):
            with self.subTest(job=job.JOB_NAME):
                args = self.args(job)
                args.source_snapshots_path = None
                with self.assertRaisesRegex(ValueError, "source-snapshots-path"):
                    job.validate_args(args)

    def test_every_source_is_required_and_extra_or_duplicate_keys_are_refused(self):
        for job in (parcel, building):
            good = self.pins(job)
            for label, value in (("missing", dict(list(good.items())[:-1])),
                                 ("extra", {**good, "silver.unused": "11"})):
                with self.subTest(job=job.JOB_NAME, case=label):
                    self.write(value)
                    with self.assertRaisesRegex(ValueError, "every Silver source"):
                        job.validate_args(self.args(job))
            key = next(iter(good))
            self.path.write_text(json.dumps(good)[:-1] + f',"{key}":"12"' + '}', encoding="utf-8")
            with self.subTest(job=job.JOB_NAME, case="duplicate"):
                with self.assertRaisesRegex(ValueError, "duplicate"):
                    job.validate_args(self.args(job))

    def test_snapshot_ids_are_exact_positive_longs_not_json_booleans_or_floats(self):
        for job in (parcel, building):
            for invalid in (True, 11.0, 0, -1, None, "11 ", "01", "1e2", 2**63):
                with self.subTest(job=job.JOB_NAME, value=invalid):
                    pins = self.pins(job)
                    pins[list(pins)[-1]] = invalid
                    self.write(pins)
                    with self.assertRaisesRegex(ValueError, "snapshot"):
                        job.validate_args(self.args(job))

    def test_anchor_pin_must_match_the_cli(self):
        for job, anchor in ((parcel, parcel.PARCEL_SOURCE), (building, building.TITLE_SOURCE)):
            with self.subTest(job=job.JOB_NAME):
                pins = self.pins(job)
                pins[anchor] = "12"
                self.write(pins)
                with self.assertRaisesRegex(ValueError, "iceberg-snapshot-id"):
                    job.validate_args(self.args(job))

    def test_carry_disabled_changes_the_required_set_without_silently_ignoring_a_pin(self):
        args = self.args(parcel)
        args.carry_lineage = False
        self.write(self.pins(parcel))
        with self.assertRaisesRegex(ValueError, "every Silver source"):
            parcel.validate_args(args)
        self.write(self.pins(parcel, carry=False))
        self.assertNotIn(parcel.LINEAGE_SOURCE, parcel.validate_args(args))

    def test_validated_pins_are_reused_even_if_the_file_changes(self):
        for job in (parcel, building):
            with self.subTest(job=job.JOB_NAME):
                args, pins = self.args(job), self.pins(job)
                self.write(pins)
                selected = job.validate_args(args)
                self.assertEqual(selected, pins)
                self.write({key: "22" for key in pins})
                for name in pins:
                    spark = mock.MagicMock()
                    reader = spark.read.format.return_value
                    reader.option.return_value = reader
                    reader.load.return_value.columns = job.column_names(job.load_lakehouse_contract(name))
                    with mock.patch.object(job.F, "expr", create=True):
                        job.read_source(spark, args, name, selected)
                    reader.option.assert_called_once_with("snapshot-id", "11")
                    reader.load.assert_called_once_with(f"r2.silver.{name.split('.', 1)[1]}")
                    spark.table.assert_not_called()

    def test_missing_pin_cannot_fall_back_to_current_head_at_the_read_boundary(self):
        for job, source in ((parcel, parcel.PARCEL_SOURCE), (building, building.TITLE_SOURCE)):
            spark = mock.MagicMock()
            with self.subTest(job=job.JOB_NAME), self.assertRaisesRegex(ValueError, "snapshot"):
                job.read_source(spark, self.args(job), source, {})
            spark.read.format.assert_not_called()
            spark.table.assert_not_called()

    def test_parquet_does_not_claim_to_have_used_an_iceberg_pin_file(self):
        for job in (parcel, building):
            with self.subTest(job=job.JOB_NAME):
                args = self.args(job)
                args.input_mode, args.input_root = "parquet", "/fixture"
                self.write(self.pins(job))
                with self.assertRaisesRegex(ValueError, "parquet"):
                    job.validate_args(args)
                args.source_snapshots_path = None
                self.assertEqual(job.validate_args(args), {})

    def test_large_pin_files_are_refused_and_integer_ids_are_normalized(self):
        for job in (parcel, building):
            with self.subTest(job=job.JOB_NAME):
                self.path.write_bytes(b" " * (64 * 1024 + 1))
                with self.assertRaisesRegex(ValueError, "64 KiB"):
                    job.validate_args(self.args(job))
                self.write({name: 11 for name in self.pins(job)})
                self.assertEqual(job.validate_args(self.args(job)), self.pins(job))

    def test_main_uses_one_selection_for_reads_and_summary_after_file_replacement(self):
        for job in (parcel, building):
            with self.subTest(job=job.JOB_NAME), ExitStack() as stack:
                args, pins = self.args(job), self.pins(job)
                args.summary_output = str(self.path.with_name("summary.json"))
                self.write(pins)
                stack.enter_context(mock.patch.object(job, "parse_args", return_value=args))
                spark, frame = mock.MagicMock(), mock.MagicMock()

                def start(*unused):
                    self.write({name: "22" for name in pins})
                    return spark

                if job is parcel:
                    stack.enter_context(mock.patch.object(job, "build_spark_session", side_effect=start))
                    for name in ("filtered_by_region", "with_lineage_sources", "build_zonings", "build_price",
                                 "build_characteristics", "build_forest_ledger", "build_transfer_history",
                                 "build_land_rights", "carry_section"):
                        stack.enter_context(mock.patch.object(job, name, return_value=frame))
                    carry = stack.enter_context(mock.patch.object(job, "read_carry_candidates", return_value=None))
                    stack.enter_context(mock.patch.object(job, "resolve_zoning_anchors", return_value={}))
                else:
                    session = stack.enter_context(mock.patch.object(job, "SparkSession"))
                    builder = session.builder.appName.return_value.config.return_value
                    stack.enter_context(mock.patch.object(job, "apply_catalog_settings", return_value=builder))
                    builder.getOrCreate.side_effect = start
                    stack.enter_context(mock.patch.object(job, "assert_iceberg_runtime_loaded"))
                read = stack.enter_context(mock.patch.object(job, "read_source", return_value=frame))
                stack.enter_context(mock.patch.object(job, "assert_single_snapshot", return_value="logical-batch"))
                stack.enter_context(mock.patch.object(job, "build_gold_panel_frame", return_value=frame))
                stack.enter_context(mock.patch.object(job, "validate_gold_frame", return_value=(1, {})))
                stack.enter_context(mock.patch.object(job.F, "expr", create=True))
                stack.enter_context(mock.patch.object(job.StorageLevel, "MEMORY_AND_DISK", "disk", create=True))
                stack.enter_context(redirect_stdout(io.StringIO()))

                self.assertEqual(job.main(), 0)
                self.assertTrue(read.call_args_list)
                for call in read.call_args_list:
                    self.assertEqual(call.args[-1], pins)
                if job is parcel:
                    self.assertEqual(carry.call_args.args[-1], pins)
                summary = json.loads(Path(args.summary_output).read_text())
                self.assertEqual(summary["source_iceberg_snapshots_by_dataset"], pins)
                self.assertEqual(set(summary["source_snapshot_ids"]), {"logical-batch"})


if __name__ == "__main__":
    unittest.main()
