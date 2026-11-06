"""A batched load reads back only the commit it appended (root ADR-0391).

The readback filtered the whole ref by the batch's (source_snapshot_id, ingested_at_utc) pairs, so
each batch of a staged release read every batch staged before it: on 2026-10-20 the apartment-price
lane's readbacks took 8, 13, 18, 24 and 33 minutes for its 1st to 5th 40M-row batch, and its
150-minute unit was killed. A batch it appended is now read back from that commit's own files.
"""

import sys
import unittest
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from platform_contracts import load_lakehouse_contract  # noqa: E402
from silver_scalar_handoff_to_lakehouse import committed_append_snapshot, run_batched_input  # noqa: E402

MODULE = "silver_scalar_handoff_to_lakehouse"
CONTRACT = "silver.building_register_apartment_price"


def batched_args(mode: str = "append") -> MagicMock:
    args = MagicMock()
    for key, value in {
        "write_mode": "iceberg", "iceberg_catalog_name": "r2", "iceberg_namespace": "silver",
        "iceberg_table": "building_register_apartment_price", "contract": CONTRACT,
        "input": "/tmp/input", "input_format": "jsonl", "input_file_batch_size": 40,
        "iceberg_write_mode": mode, "validate_only": False, "expected_count": None,
        "defer_iceberg_readback_validation": False, "summary_output": None, "derivation": None,
    }.items():
        setattr(args, key, value)
    return args


class ReadbackReadsItsOwnCommit(unittest.TestCase):
    def run_load(self, args: MagicMock, outcome: dict, recorded: dict[str, int]) -> dict[str, MagicMock]:
        frame = MagicMock()
        frame.persist.return_value = frame
        with ExitStack() as stack:
            replacements = {
                "collect_input_batches": [["/tmp/part-0001.jsonl.gz"]],
                "read_handoff": frame,
                "preflight_file_batches": [("snapshot-a", datetime(2026, 10, 20, 4))],
                "collect_source_snapshot_summary": {"source_snapshot_ids": ["snapshot-a"]},
                "write_silver_iceberg": outcome,
                "read_ingested_objects": recorded,
                "read_recorded_append": frame,
                "read_iceberg_snapshot_for_pairs": frame,
                "emit_run_summary": None,
                "exit_after_success_if_requested": None,
                "enforce_ordinary_land_pnu_null_share": None,
            }
            mocks = {name: stack.enter_context(patch(f"{MODULE}.{name}", return_value=value))
                     for name, value in replacements.items()}
            stack.enter_context(patch(f"{MODULE}.cast_handoff_frame", side_effect=lambda handoff, *_: handoff))
            stack.enter_context(patch(f"{MODULE}.validate_frame", return_value=(2, {"row_count": 2})))
            self.assertEqual(run_batched_input(MagicMock(), args, load_lakehouse_contract(CONTRACT),
                                               MagicMock(), MagicMock(), MagicMock()), 0)
        return mocks

    def test_an_appended_batch_is_read_back_from_its_own_commit_not_the_whole_branch(self) -> None:
        mocks = self.run_load(batched_args(), {"appended": True, "record_ids": ["part-0001"], "token": "t",
                                               "existing_snapshot": None, "ref": "load_0123456789abcdef"},
                              {"part-0000": 41, "part-0001": 77})
        self.assertEqual(mocks["read_recorded_append"].call_args.args[2], 77)
        self.assertEqual(mocks["read_ingested_objects"].call_args.args[3], "load_0123456789abcdef")
        mocks["read_iceberg_snapshot_for_pairs"].assert_not_called()

    def test_an_overwritten_or_already_held_batch_keeps_the_pair_readback(self) -> None:
        for mode, outcome in (
            ("overwrite", {"appended": True, "record_ids": ["part-0001"], "token": "t", "existing_snapshot": None}),
            ("append", {"appended": False, "record_ids": ["part-0001"], "token": "t", "existing_snapshot": 41}),
        ):
            with self.subTest(mode=mode, appended=outcome["appended"]):
                mocks = self.run_load(batched_args(mode), outcome, {"part-0001": 41})
                mocks["read_recorded_append"].assert_not_called()
                mocks["read_iceberg_snapshot_for_pairs"].assert_called_once()

    def test_a_batch_whose_objects_no_single_commit_records_is_refused(self) -> None:
        # Synthetic failure: the registry names two commits (or none) for the batch's objects, so
        # no one commit's files are the batch.
        for recorded in ({"part-0001": 77, "part-0002": 78}, {"part-0001": 77}):
            with self.subTest(recorded=recorded), \
                    patch(f"{MODULE}.read_ingested_objects", return_value=recorded), \
                    patch(f"{MODULE}.qualified_iceberg_table", return_value="`r2`.`silver`.`t`"), \
                    self.assertRaisesRegex(ValueError, "not recorded by one append"):
                committed_append_snapshot(MagicMock(), batched_args(),
                                          {"record_ids": ["part-0001", "part-0002"], "ref": "main"})


if __name__ == "__main__":
    unittest.main()
