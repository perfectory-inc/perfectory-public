"""The ordinary-land PNU NULL-share guard refuses the load shape that 2026-09-27 committed."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import pnu_null_share_guard as guard  # noqa: E402
from platform_contracts import (  # noqa: E402
    declared_ordinary_land_pnu_null_share_tolerance,
    load_lakehouse_contract,
)
from silver_scalar_handoff_to_lakehouse import (  # noqa: E402
    enforce_ordinary_land_pnu_null_share,
    main,
    run_batched_input,
)

HUB_REGISTER_TABLES = (
    "silver.building_register_titles",
    "silver.building_register_units",
    "silver.building_register_unit_areas",
)


def has_pyspark() -> bool:
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


class DecideTest(unittest.TestCase):
    def test_a_whole_sido_losing_its_pnu_is_refused(self) -> None:
        # Planted violation: the 09-27 shape in round numbers — one in a million before,
        # 11.6 percent after, because every row of one merged 시도 lost its PNU.
        baseline = guard.OrdinaryLandPnuNulls(rows=8_000_000, nulls=8)
        candidate = guard.OrdinaryLandPnuNulls(rows=8_000_000, nulls=928_000)
        with self.assertRaisesRegex(ValueError, "Refusing the Silver load"):
            guard.decide(candidate, baseline, 0.001, "r2.silver.example")

    def test_a_rise_just_past_the_tolerance_is_refused_and_one_at_it_passes(self) -> None:
        baseline = guard.OrdinaryLandPnuNulls(rows=1_000_000, nulls=0)
        with self.assertRaises(ValueError):
            guard.decide(guard.OrdinaryLandPnuNulls(1_000_000, 1_001), baseline, 0.001, "t")
        outcome = guard.decide(guard.OrdinaryLandPnuNulls(1_000_000, 1_000), baseline, 0.001, "t")
        self.assertEqual(outcome["outcome"], "within_tolerance")

    def test_a_falling_share_is_the_correction_and_passes(self) -> None:
        outcome = guard.decide(
            guard.OrdinaryLandPnuNulls(rows=8_000_000, nulls=8),
            guard.OrdinaryLandPnuNulls(rows=8_000_000, nulls=928_000),
            0.001,
            "t",
        )
        self.assertEqual(outcome["outcome"], "within_tolerance")
        self.assertLess(outcome["increase"], 0)

    def test_no_baseline_is_a_named_outcome_not_a_silent_pass(self) -> None:
        candidate = guard.OrdinaryLandPnuNulls(rows=10, nulls=10)
        for baseline in (None, guard.OrdinaryLandPnuNulls(rows=0, nulls=0)):
            self.assertEqual(guard.decide(candidate, baseline, 0.001, "t")["outcome"], "no_baseline")


class ContractTest(unittest.TestCase):
    def test_every_hub_register_table_declares_the_tolerance(self) -> None:
        for table in HUB_REGISTER_TABLES:
            tolerance = declared_ordinary_land_pnu_null_share_tolerance(load_lakehouse_contract(table))
            self.assertIsNotNone(tolerance, table)
            self.assertGreater(tolerance, 0.0, table)
            self.assertLess(tolerance, 0.116, f"{table} would have let the 09-27 snapshot through")

    def test_tables_without_the_gate_declare_none(self) -> None:
        contract = load_lakehouse_contract("silver.building_register_floors")
        self.assertIsNone(declared_ordinary_land_pnu_null_share_tolerance(contract))

    def test_a_malformed_or_repeated_gate_is_refused(self) -> None:
        for gates in (
            ["ordinary_land_pnu_null_share_increase <= lots"],
            ["ordinary_land_pnu_null_share_increase <= 2"],
            ["ordinary_land_pnu_null_share_increase <= 0.001", "ordinary_land_pnu_null_share_increase <= 0.01"],
        ):
            with self.assertRaises(ValueError, msg=str(gates)):
                declared_ordinary_land_pnu_null_share_tolerance({"table_name": "t", "quality_gates": gates})


def iceberg_args(**overrides: object) -> MagicMock:
    args = MagicMock()
    args.write_mode = "iceberg"
    args.iceberg_catalog_name = "r2"
    args.iceberg_namespace = "silver"
    args.iceberg_table = "building_register_titles"
    args.contract = "silver.building_register_titles"
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


class LoaderWiringTest(unittest.TestCase):
    def test_enforce_measures_the_current_table_and_raises_on_regression(self) -> None:
        contract = load_lakehouse_contract("silver.building_register_titles")
        with patch.object(guard, "measure", return_value=guard.OrdinaryLandPnuNulls(100, 50)), \
                patch.object(guard, "table_baseline",
                             return_value=guard.OrdinaryLandPnuNulls(100, 0)) as baseline:
            with self.assertRaisesRegex(ValueError, "Refusing the Silver load"):
                enforce_ordinary_land_pnu_null_share(MagicMock(), MagicMock(), iceberg_args(), contract, MagicMock())
        self.assertEqual(baseline.call_args.args[1], "`r2`.`silver`.`building_register_titles`")

    def test_enforce_is_a_no_op_for_tables_without_the_gate(self) -> None:
        contract = load_lakehouse_contract("silver.building_register_floors")
        with patch.object(guard, "measure") as measure:
            self.assertIsNone(enforce_ordinary_land_pnu_null_share(
                MagicMock(), MagicMock(), iceberg_args(), contract, MagicMock()))
        measure.assert_not_called()

    def test_single_load_refusal_happens_before_any_write(self) -> None:
        args = iceberg_args(
            input="/tmp/input", input_format="jsonl", input_file_batch_size=0,
            contract="silver.building_register_titles", iceberg_write_mode="append",
            validate_only=False, expected_count=None, summary_output=None,
            defer_iceberg_readback_validation=False, derivation=None,
        )
        frame = MagicMock()
        frame.persist.return_value = frame
        with ExitStack() as stack:
            for name, value in {
                "parse_args": args,
                "validate_args": None,
                "load_lakehouse_contract": load_lakehouse_contract(args.contract),
                "validate_scalar_contract": None,
                "load_pyspark": (object(), MagicMock(), object(), MagicMock()),
                "build_spark_session": MagicMock(),
                "read_handoff": frame,
                "cast_handoff_frame": frame,
                "validate_frame": (2, {"row_count": 2}),
                "collect_source_snapshot_summary": {"source_snapshot_ids": ["s"]},
            }.items():
                stack.enter_context(
                    patch(f"silver_scalar_handoff_to_lakehouse.{name}", return_value=value))
            stack.enter_context(patch(
                "silver_scalar_handoff_to_lakehouse.enforce_ordinary_land_pnu_null_share",
                side_effect=ValueError("Refusing the Silver load: planted"),
            ))
            write = stack.enter_context(patch("silver_scalar_handoff_to_lakehouse.write_silver_iceberg"))
            with self.assertRaisesRegex(ValueError, "planted"):
                main()
        write.assert_not_called()

    def test_batched_load_judges_every_batch_before_the_first_write(self) -> None:
        args = iceberg_args(
            input="/tmp/input", input_format="jsonl", input_file_batch_size=1,
            contract="silver.building_register_titles", iceberg_write_mode="append",
            validate_only=False, expected_count=None,
        )
        contract = load_lakehouse_contract(args.contract)
        read = MagicMock(return_value=MagicMock())
        with ExitStack() as stack:
            stack.enter_context(patch("silver_scalar_handoff_to_lakehouse.collect_input_batches",
                                      return_value=[["/tmp/a.jsonl"], ["/tmp/b.jsonl"]]))
            stack.enter_context(patch("silver_scalar_handoff_to_lakehouse.read_handoff", read))
            stack.enter_context(patch("silver_scalar_handoff_to_lakehouse.cast_handoff_frame",
                                      side_effect=lambda frame, *_: frame))
            stack.enter_context(patch(
                "silver_scalar_handoff_to_lakehouse.enforce_ordinary_land_pnu_null_share",
                side_effect=ValueError("Refusing the Silver load: planted"),
            ))
            preflight = stack.enter_context(
                patch("silver_scalar_handoff_to_lakehouse.preflight_file_batches"))
            write = stack.enter_context(patch("silver_scalar_handoff_to_lakehouse.write_silver_iceberg"))
            with self.assertRaisesRegex(ValueError, "planted"):
                run_batched_input(MagicMock(), args, contract, MagicMock(), MagicMock(), MagicMock())
        self.assertEqual(read.call_args.args[1], ["/tmp/a.jsonl", "/tmp/b.jsonl"])
        preflight.assert_not_called()
        write.assert_not_called()


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class MeasureSparkTest(unittest.TestCase):
    def test_measure_counts_only_ordinary_land_rows(self) -> None:
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("pnu-null-share").getOrCreate()
        try:
            frame = spark.createDataFrame(
                [
                    ("9999900101000010000", "9999900101100010000"),  # 대지, PNU present
                    ("9999900101000020000", None),                   # 대지, PNU lost
                    ("9999900101200030000", None),                   # 블록: no PNU by design
                    ("9999900101100040000", "9999900101200040000"),  # 산
                ],
                "register_parcel_key string, pnu string",
            )
            self.assertEqual(guard.measure(frame, F), guard.OrdinaryLandPnuNulls(rows=2, nulls=1))
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
