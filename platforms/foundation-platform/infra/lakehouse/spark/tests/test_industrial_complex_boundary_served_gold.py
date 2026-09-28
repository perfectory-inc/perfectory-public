import json
import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from industrial_complex_boundary_served_gold import (  # noqa: E402
    BAKE_HANDOFF_COLUMNS,
    GEOMETRY_SRID,
    LEDGER_COLUMNS,
    SERVED_COLUMNS,
    apply_edits,
    bake_handoff_lines,
    contract_schema,
    edit_fingerprint,
    new_ledger_rows,
    parse_args,
    read_edit_handoff,
    validate_args,
    LEDGER_CONTRACT,
)

A = "00000000-0000-5000-8000-000000000001"
B = "00000000-0000-5000-8000-000000000002"
C = "00000000-0000-5000-8000-000000000003"


def base_row(complex_id, code):
    return {
        "complex_id": complex_id,
        "official_complex_code": code,
        "geometry_wkb_hex": "01" + code.encode().hex(),
        "geometry_srid": GEOMETRY_SRID,
        "geometry_checksum_sha256": "a" * 64,
        "source_snapshot_id": "synthetic-source-1",
    }


def edit(seq, feature_id, op, code="SYN-EDIT"):
    upsert = op == "upsert"
    return {
        "unit": "complex",
        "change_seq": seq,
        "feature_id": feature_id,
        "op": op,
        "geometry_geojson": '{"type":"Polygon"}' if upsert else None,
        "geometry_wkb_hex": f"0106{seq:04x}" if upsert else None,
        "geometry_srid": GEOMETRY_SRID,
        "geometry_checksum_sha256": f"{seq:064x}" if upsert else None,
        "properties_json": json.dumps({"official_complex_code": code}) if upsert else "{}",
        "editor": "00000000-0000-0000-0000-000000000000",
        "edited_at": "2099-01-01T00:00:00Z",
    }


def lines(*rows):
    return [json.dumps(row) for row in rows]


class ServedGoldContractTest(unittest.TestCase):
    def test_the_tables_come_from_the_contract_artifact(self):
        self.assertIn("change_seq", LEDGER_COLUMNS)
        self.assertIn("edits_through_change_seq", SERVED_COLUMNS)
        self.assertEqual(GEOMETRY_SRID, 5186)
        self.assertIn("change_seq BIGINT", contract_schema(LEDGER_CONTRACT))
        self.assertIn("geometry_wkb BINARY", contract_schema(LEDGER_CONTRACT))
        self.assertEqual(
            BAKE_HANDOFF_COLUMNS,
            ("complex_id", "official_complex_code", "geometry_wkb_hex", "geometry_srid",
             "geometry_checksum_sha256", "origin"),
        )

    def test_real_tables_need_an_explicit_flag(self):
        args = parse_args(["--edits-input", "e.jsonl", "--output", "o.jsonl"])
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)


class EditHandoffTest(unittest.TestCase):
    def test_edits_are_read_in_change_order(self):
        rows = read_edit_handoff(lines(edit(3, A, "delete"), edit(2, B, "upsert")))
        self.assertEqual([row["change_seq"] for row in rows], [2, 3])

    def test_malformed_edits_are_refused(self):
        wrong_unit = dict(edit(1, A, "upsert"), unit="parcels")
        wrong_crs = dict(edit(1, A, "upsert"), geometry_srid=4326)
        delete_with_geometry = dict(edit(1, A, "delete"), geometry_wkb_hex="01")
        upsert_without_geometry = dict(edit(1, A, "upsert"), geometry_wkb_hex=None)
        upsert_without_code = dict(edit(1, A, "upsert"), properties_json="{}")
        for bad in (wrong_unit, wrong_crs, delete_with_geometry, upsert_without_geometry, upsert_without_code):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                read_edit_handoff(lines(bad))
        with self.assertRaisesRegex(ValueError, "change_seq"):
            read_edit_handoff(lines(edit(1, A, "upsert"), edit(1, B, "upsert")))


class LedgerTest(unittest.TestCase):
    def test_only_unledgered_edits_are_appended(self):
        first, second = edit(1, A, "upsert"), edit(2, B, "delete")
        ledgered = {1: edit_fingerprint(first)}
        self.assertEqual(new_ledger_rows(ledgered, [first, second]), [second])

    def test_a_ledgered_change_seq_cannot_change_content(self):
        ledgered = {1: edit_fingerprint(edit(1, A, "upsert", code="SYN-OLD"))}
        with self.assertRaisesRegex(ValueError, "append-only"):
            new_ledger_rows(ledgered, [edit(1, A, "upsert", code="SYN-NEW")])


class ApplyEditsTest(unittest.TestCase):
    def test_every_ledgered_edit_is_applied_in_change_order(self):
        base = [base_row(A, "SYN-A"), base_row(B, "SYN-B")]
        ledger = [
            edit(4, A, "upsert", code="SYN-A2"),
            edit(1, A, "upsert", code="SYN-A1"),
            edit(2, B, "delete"),
            edit(3, C, "upsert", code="SYN-C"),
        ]
        served, counts = apply_edits(base, ledger)
        self.assertEqual([row["complex_id"] for row in served], [A, C])
        by_id = {row["complex_id"]: row for row in served}
        self.assertEqual(by_id[A]["official_complex_code"], "SYN-A2", "the newest edit wins")
        self.assertEqual(by_id[A]["origin"], "edit")
        self.assertEqual(by_id[A]["source_snapshot_id"], "map-edit-4")
        self.assertEqual(by_id[C]["origin"], "edit")
        self.assertEqual(counts, {"upserts": 3, "deletes": 1, "deletes_of_absent_features": 0})

    def test_an_untouched_boundary_is_served_from_source(self):
        served, _ = apply_edits([base_row(A, "SYN-A")], [])
        self.assertEqual(served[0]["origin"], "source")
        self.assertEqual(served[0]["source_snapshot_id"], "synthetic-source-1")

    def test_deleting_an_absent_feature_is_counted_not_hidden(self):
        _, counts = apply_edits([base_row(A, "SYN-A")], [edit(1, C, "delete")])
        self.assertEqual(counts["deletes_of_absent_features"], 1)

    def test_two_complexes_cannot_end_up_with_one_code(self):
        with self.assertRaisesRegex(ValueError, "SYN-B"):
            apply_edits([base_row(A, "SYN-A"), base_row(B, "SYN-B")], [edit(1, A, "upsert", code="SYN-B")])


class BakeHandoffTest(unittest.TestCase):
    def test_an_empty_served_set_is_refused(self):
        with self.assertRaisesRegex(ValueError, "erase the layer"):
            bake_handoff_lines([])

    def test_lines_carry_exactly_the_bake_columns(self):
        served, _ = apply_edits([base_row(A, "SYN-A")], [])
        line = json.loads(bake_handoff_lines(served))
        self.assertEqual(tuple(sorted(line)), tuple(sorted(BAKE_HANDOFF_COLUMNS)))


if __name__ == "__main__":
    unittest.main()
