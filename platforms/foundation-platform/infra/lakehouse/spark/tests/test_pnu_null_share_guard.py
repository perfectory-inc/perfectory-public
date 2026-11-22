"""The ordinary-land PNU NULL-share guard refuses the load shape that 2026-09-27 committed."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import pnu_null_share_guard as guard  # noqa: E402
from platform_contracts import (  # noqa: E402
    OrdinaryLandPnuNullShareBounds,
    declared_ordinary_land_pnu_null_share_bounds,
    load_lakehouse_contract,
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


# The contract's bounds, read from the contract rather than copied, so a changed number is a
# decision and not a test edit.
BOUNDS = declared_ordinary_land_pnu_null_share_bounds(
    load_lakehouse_contract("silver.building_register_titles")
)
# Measured ordinary-land counts of silver.building_register_titles (ADR-0142).
GOOD_2026_09_03 = guard.OrdinaryLandPnuNulls(rows=7_927_468, nulls=8)
BAD_2026_09_27 = guard.OrdinaryLandPnuNulls(rows=7_939_750, nulls=916_469)
CORRECTED_DRY_RUN = guard.OrdinaryLandPnuNulls(rows=7_939_750, nulls=8)
# The same reload once the hub placeholder codes compose NULL: 8 + 958 (99999) + 1 (`0`).
CORRECTED_WITH_PLACEHOLDERS = guard.OrdinaryLandPnuNulls(rows=7_939_750, nulls=967)


class DecideTest(unittest.TestCase):
    def test_a_whole_sido_losing_its_pnu_is_refused(self) -> None:
        # Planted violation: the 09-27 snapshot loaded over the good 09-03 one.
        with self.assertRaisesRegex(ValueError, "Refusing the Silver load"):
            guard.decide(BAD_2026_09_27, GOOD_2026_09_03, BOUNDS, "r2.silver.example")

    def test_replaying_the_bad_snapshot_over_itself_is_refused(self) -> None:
        # Planted violation: the 09-27 snapshot is still current, so reloading the same loss
        # rises by zero. The increase bound alone lets this through; the ceiling must not.
        with self.assertRaisesRegex(ValueError, "above the contract ceiling"):
            guard.decide(BAD_2026_09_27, BAD_2026_09_27, BOUNDS, "r2.silver.example")

    def test_the_corrected_reload_over_the_bad_snapshot_passes(self) -> None:
        for corrected in (CORRECTED_DRY_RUN, CORRECTED_WITH_PLACEHOLDERS):
            outcome = guard.decide(corrected, BAD_2026_09_27, BOUNDS, "r2.silver.example")
            self.assertEqual(outcome["outcome"], "within_bounds")
            self.assertLess(outcome["increase"], 0)
        # And over the good 09-03 snapshot, which it replaces in readers' eyes.
        outcome = guard.decide(CORRECTED_WITH_PLACEHOLDERS, GOOD_2026_09_03, BOUNDS, "t")
        self.assertEqual(outcome["outcome"], "within_bounds")

    def test_the_ceiling_holds_without_a_baseline(self) -> None:
        with self.assertRaisesRegex(ValueError, "above the contract ceiling"):
            guard.decide(BAD_2026_09_27, None, BOUNDS, "t")
        self.assertEqual(guard.decide(GOOD_2026_09_03, None, BOUNDS, "t")["outcome"], "no_baseline")

    def test_each_bound_refuses_just_past_it_and_passes_at_it(self) -> None:
        bounds = OrdinaryLandPnuNullShareBounds(ceiling=0.002, increase=0.001)
        baseline = guard.OrdinaryLandPnuNulls(rows=1_000_000, nulls=0)
        with self.assertRaisesRegex(ValueError, "more than the contract tolerance"):
            guard.decide(guard.OrdinaryLandPnuNulls(1_000_000, 1_001), baseline, bounds, "t")
        outcome = guard.decide(guard.OrdinaryLandPnuNulls(1_000_000, 1_000), baseline, bounds, "t")
        self.assertEqual(outcome["outcome"], "within_bounds")
        high = guard.OrdinaryLandPnuNulls(rows=1_000_000, nulls=1_900)
        self.assertEqual(guard.decide(high, high, bounds, "t")["outcome"], "within_bounds")
        higher = guard.OrdinaryLandPnuNulls(rows=1_000_000, nulls=2_001)
        with self.assertRaisesRegex(ValueError, "above the contract ceiling"):
            guard.decide(higher, higher, bounds, "t")

    def test_no_baseline_is_a_named_outcome_not_a_silent_pass(self) -> None:
        candidate = guard.OrdinaryLandPnuNulls(rows=10, nulls=0)
        for baseline in (None, guard.OrdinaryLandPnuNulls(rows=0, nulls=0)):
            self.assertEqual(guard.decide(candidate, baseline, BOUNDS, "t")["outcome"], "no_baseline")


class ContractTest(unittest.TestCase):
    def test_every_hub_register_table_declares_both_bounds(self) -> None:
        for table in HUB_REGISTER_TABLES:
            bounds = declared_ordinary_land_pnu_null_share_bounds(load_lakehouse_contract(table))
            self.assertIsNotNone(bounds, table)
            assert bounds is not None
            # Properties, not the numbers: the measured good share passes, the 09-27 one cannot.
            self.assertGreater(bounds.ceiling, CORRECTED_WITH_PLACEHOLDERS.share, table)
            self.assertLess(bounds.ceiling, BAD_2026_09_27.share, f"{table} would hold the 09-27 loss")
            self.assertGreater(bounds.increase, 0.0, table)
            self.assertLess(bounds.increase, BAD_2026_09_27.share, table)

    def test_tables_without_the_gate_declare_none(self) -> None:
        contract = load_lakehouse_contract("silver.building_register_floors")
        self.assertIsNone(declared_ordinary_land_pnu_null_share_bounds(contract))

    def test_a_malformed_partial_or_repeated_gate_is_refused(self) -> None:
        for gates in (
            ["ordinary_land_pnu_null_share <= lots and increase <= 0.001"],
            ["ordinary_land_pnu_null_share <= 0.0001 and increase <= 2"],
            # The rise alone is the shape that ratcheted onto 09-27; it is no longer a gate.
            ["ordinary_land_pnu_null_share_increase <= 0.001"],
            ["ordinary_land_pnu_null_share <= 0.0001"],
            ["ordinary_land_pnu_null_share <= 0.0001 and increase <= 0.001",
             "ordinary_land_pnu_null_share <= 0.001 and increase <= 0.01"],
        ):
            with self.assertRaises(ValueError, msg=str(gates)):
                declared_ordinary_land_pnu_null_share_bounds({"table_name": "t", "quality_gates": gates})


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
