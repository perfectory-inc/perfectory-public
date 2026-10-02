"""A later month's retry must authenticate its own append, including derivations."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from building_register_floor_history import registered_snapshot, verify_registered_lineage
from lakehouse_ingest import (
    INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY,
    identities_under_derivation, ingest_batch_token,
)


def receipt():
    return {"source_snapshot_id": "building-register-floor-content-v1-" + "b" * 64,
            "bronze_object_key": "bronze/source=floor/OPN20991020SYNTHETIC-FLOOR.zip",
            "valid_from_utc": "2026-10-01T00:00:00Z",
            "ingested_at_utc": "2026-10-22T00:00:00Z"}


def append(source, derivation=None):
    identity = identities_under_derivation([source["source_snapshot_id"]], derivation)[0]
    return {"snapshot_id": 20, "parent_id": 10, "operation": "append",
            "summary": {INGEST_BATCH_OBJECTS_KEY: identity,
                        INGEST_BATCH_TOKEN_KEY: ingest_batch_token([identity]),
                        "added-records": "3"}}


class RegisteredHistoryTests(unittest.TestCase):
    def test_retry_uses_source_and_derivation_not_wall_clock(self):
        source = receipt()
        record = append(source)
        later = {"snapshot_id": 21, "parent_id": 20, "operation": "append", "summary": {}}
        self.assertEqual(registered_snapshot(source, None, 21, [record, later]), record)
        self.assertIsNone(registered_snapshot(source, "normalizer-v2", 21, [record, later]))
        derived = append(source, "normalizer-v2")
        self.assertEqual(registered_snapshot(source, "normalizer-v2", 20, [derived]), derived)

    def test_registry_ambiguity_tampering_and_lost_ancestry_are_errors(self):
        source = receipt()
        original = append(source)
        cases = []
        for key in [INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY, "added-records"]:
            changed = copy.deepcopy(original)
            changed["summary"][key] = (source["source_snapshot_id"] + ",other") if key == INGEST_BATCH_OBJECTS_KEY else "bad"
            cases.append((20, [changed]))
        cases.extend([(20, [original, dict(original, snapshot_id=21)]),
                      (19, [original]), (21, [original]),
                      (20, [dict(original, operation="overwrite")])])
        for operation in ["delete", "overwrite", "replace"]:
            cases.append((21, [original, {"snapshot_id": 21, "parent_id": 20,
                                          "operation": operation, "summary": {}}]))
        for head, records in cases:
            with self.subTest(head=head, records=records), self.assertRaises(ValueError):
                registered_snapshot(source, None, head, records)

    def test_actual_append_time_is_recovered_but_mixed_rows_are_refused(self):
        source = receipt()
        row = {key: source[key] for key in ["source_snapshot_id", "bronze_object_key", "valid_from_utc"]}
        row.update(ingested_at_utc="2026-10-21T12:34:56Z", count=3)
        self.assertEqual(verify_registered_lineage(source, append(source), [row]).isoformat(),
                         "2026-10-21T12:34:56+00:00")
        for rows in [[], [row, row], [dict(row, count=2)],
                     [dict(row, source_snapshot_id="other")],
                     [dict(row, bronze_object_key="other.zip")],
                     [dict(row, valid_from_utc="2026-11-01T00:00:00Z")],
                     [dict(row, ingested_at_utc=None)]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                verify_registered_lineage(source, append(source), rows)


if __name__ == "__main__":
    unittest.main()
