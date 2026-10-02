"""Recovered Bronze identities must not turn retained FLOOR rows into a new load."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from unittest import mock
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from building_register_floor_history import (  # noqa: E402
    validate_receipt, verify_metadata, verify_lineage, load_history_witness,
)
from lakehouse_ingest import INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY, ingest_batch_token


def binding():
    return json.loads((Path(__file__).parent / "fixtures/building_register_floor_history.json").read_text(encoding="utf-8"))


def receipt(witness):
    return {"schema_version": 2, "source_snapshot_id": witness["source_snapshot_id"],
            "bronze_object_key": witness["bronze_object_key"],
            "valid_from_utc": witness["valid_from_utc"],
            "ingested_at_utc": "2099-10-01T00:00:00Z", "inputs": witness["inputs"],
            "historical_binding": copy.deepcopy(witness)}


def snapshots(witness):
    source = witness["source_snapshot_id"]
    return [{"snapshot_id": 10, "parent_id": 1, "operation": "overwrite",
             "summary": {INGEST_BATCH_OBJECTS_KEY: source,
                         INGEST_BATCH_TOKEN_KEY: ingest_batch_token([source]),
                         "total-records": "3"}}]


class FloorHistoryTests(unittest.TestCase):
    def test_runtime_witness_uses_explicit_path_and_exact_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / "witness.json"
            data = json.dumps(binding()).encode()
            path.write_bytes(data)
            self.assertEqual(load_history_witness(path, hashlib.sha256(data).hexdigest()), binding())
            path.write_bytes(data + b" ")
            with self.assertRaisesRegex(ValueError, "hash"):
                load_history_witness(path, hashlib.sha256(data).hexdigest())

    def test_runtime_witness_rejects_missing_relative_oversized_and_invalid_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            cases = [(root / "missing.json", "a" * 64), (Path("relative.json"), "a" * 64), (root, "a" * 64)]
            path = root / "oversized.json"
            path.write_bytes(b" " * 65537)
            cases.append((path, hashlib.sha256(path.read_bytes()).hexdigest()))
            for path, digest in cases:
                with self.subTest(path=path), self.assertRaises((ValueError, OSError)):
                    load_history_witness(path, digest)

    def test_runtime_witness_rejects_duplicate_fields_and_changed_structure(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / "witness.json"
            malformed = [b'{"schema_version":1,"schema_version":1}']
            for key, value in [("schema_version", True), ("table_name", "other.table"),
                               ("table_uuid", "00000000-0000-0000-0000-000000000000"),
                               ("snapshot_id", "0"), ("operation", "append"),
                               ("row_count", False), ("extra", "unrecognized")]:
                witness = binding()
                witness[key] = value
                malformed.append(json.dumps(witness).encode())
            for data in malformed:
                path.write_bytes(data)
                with self.subTest(data=data[:80]), self.assertRaises(ValueError):
                    load_history_witness(path, hashlib.sha256(data).hexdigest())

    def test_runtime_witness_canonicalizes_typed_values_without_changing_byte_pin(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / "witness.json"
            witness = binding()
            witness["valid_from_utc"] = "2099-09-20T09:00:00+09:00"
            witness["ingested_at_utc"] = "2099-09-29T17:53:49.231000Z"
            data = json.dumps(witness).encode()
            path.write_bytes(data)
            canonical = load_history_witness(path, hashlib.sha256(data).hexdigest())
            self.assertEqual(canonical, binding())
            self.assertEqual(validate_receipt(receipt(binding()), canonical), canonical)
            witness["valid_from_utc"] = "2099-09-20T00:00:00.000000001Z"
            data = json.dumps(witness).encode()
            path.write_bytes(data)
            with self.assertRaisesRegex(ValueError, "precision"):
                load_history_witness(path, hashlib.sha256(data).hexdigest())

    def test_runtime_witness_requires_a_valid_content_pin(self):
        path = Path(__file__).resolve().parent / "fixtures/building_register_floor_history.json"
        for digest in (None, "", "x" * 64, "a" * 63, "a" * 64):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                load_history_witness(path, digest)

    @unittest.skipIf(os.name == "nt", "symlink fixture needs POSIX permissions")
    def test_runtime_witness_rejects_symlink_and_fifo_without_blocking(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            target = root / "actual.json"
            data = json.dumps(binding()).encode()
            target.write_bytes(data)
            link = root / "link.json"
            link.symlink_to(target)
            fifo = root / "fifo"
            os.mkfifo(fifo)
            for path in (link, fifo):
                with self.subTest(path=path), self.assertRaises((ValueError, OSError)):
                    load_history_witness(path, hashlib.sha256(data).hexdigest())

    def test_exact_receipt_resolves_only_fixed_witness(self):
        witness = binding()
        self.assertEqual(validate_receipt(receipt(witness), witness), witness)

    def test_mutated_receipt_cannot_claim_retained(self):
        witness = binding()
        for field in ["source_snapshot_id", "valid_from_utc", "schema_version"]:
            changed = receipt(witness)
            changed[field] = "changed"
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_receipt(changed, witness)
        for role in ["floor", "title"]:
            for field in witness["inputs"][role]:
                changed = copy.deepcopy(receipt(witness))
                changed["inputs"][role][field] = "changed"
                with self.subTest(role=role, field=field), self.assertRaises(ValueError):
                    validate_receipt(changed, witness)

    def test_removed_binding_cannot_fall_through_reserved_month(self):
        witness = binding()
        changed = receipt(witness)
        changed["historical_binding"] = None
        with self.assertRaises(ValueError):
            validate_receipt(changed, witness)
        changed = copy.deepcopy(changed)
        changed["source_snapshot_id"] = "building-register-floor-content-v1-" + "b" * 64
        for value in changed["inputs"].values():
            value["provider_month"] = "2099-07-01"
        with self.assertRaisesRegex(ValueError, "historical provider month"):
            validate_receipt(changed, witness)

    def test_new_month_has_no_historical_skip(self):
        witness = binding()
        changed = copy.deepcopy(receipt(witness))
        changed["historical_binding"] = None
        changed["source_snapshot_id"] = "building-register-floor-content-v1-" + "b" * 64
        for value in changed["inputs"].values():
            value["provider_month"] = "2099-10-01"
        self.assertIsNone(validate_receipt(changed, witness))

    def test_unknown_receipt_fields_are_not_silently_accepted(self):
        witness = binding()
        changed = receipt(witness)
        changed["skip"] = True
        with self.assertRaises(ValueError):
            validate_receipt(changed, witness)

    def test_pinned_overwrite_and_append_descendant_are_valid(self):
        witness = binding()
        records = snapshots(witness)
        verify_metadata(witness, witness["table_uuid"], 10, records)
        records.append({"snapshot_id": 11, "parent_id": 10,
                        "operation": "append", "summary": {}})
        verify_metadata(witness, witness["table_uuid"], 11, records)

    def test_wrong_table_missing_pin_and_changed_registry_fail(self):
        witness = binding()
        cases = [ ("different", 10, snapshots(witness)),
                  (witness["table_uuid"], 10, []),
                  (witness["table_uuid"], 99, snapshots(witness)) ]
        for key in [INGEST_BATCH_OBJECTS_KEY, INGEST_BATCH_TOKEN_KEY, "total-records"]:
            records = snapshots(witness)
            records[0]["summary"][key] = "wrong"
            cases.append((witness["table_uuid"], 10, records))
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                verify_metadata(witness, *args)

    def test_rollback_delete_replace_or_duplicate_registry_fail(self):
        witness = binding()
        for operation in ["overwrite", "replace", "delete"]:
            records = snapshots(witness) + [{"snapshot_id": 11, "parent_id": 10,
                        "operation": operation, "summary": {}}]
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                verify_metadata(witness, witness["table_uuid"], 11, records)
        records = snapshots(witness)
        duplicate = copy.deepcopy(records[0])
        duplicate.update(snapshot_id=11, parent_id=10, operation="append")
        with self.assertRaises(ValueError):
            verify_metadata(witness, witness["table_uuid"], 11, records + [duplicate])

    def test_actual_lineage_counts_and_times_are_required(self):
        witness = binding()
        row = {key: witness[key] for key in ["source_snapshot_id", "bronze_object_key",
                                            "valid_from_utc", "ingested_at_utc"]}
        row["count"] = 3
        verify_lineage(witness, [row])
        for changed in [[], [row, row], [dict(row, count=2)],
                        [dict(row, bronze_object_key="other.zip")],
                        [dict(row, ingested_at_utc="2099-09-29T11:09:43.585677Z")]]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                verify_lineage(witness, changed)


if __name__ == "__main__":
    unittest.main()
