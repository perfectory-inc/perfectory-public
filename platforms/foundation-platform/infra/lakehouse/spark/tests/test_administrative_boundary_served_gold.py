import json
import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from administrative_boundary_served_gold import (  # noqa: E402
    GEOMETRY_SRID,
    SERVED_COLUMNS,
    TILE_PROPERTIES,
    apply_edits,
    bake_handoff_lines,
    parse_args,
    read_edit_handoff,
    validate_args,
)

A = "00000000-0000-5000-8000-000000000001"
B = "00000000-0000-5000-8000-000000000002"
C = "00000000-0000-5000-8000-000000000003"


def base_row(unit_id, code):
    return {
        "administrative_unit_id": unit_id,
        "scope_kind": "legal_dong",
        "canonical_code": code,
        "display_name": f"SYN-{code}",
        "geometry_wkb_hex": "01" + code.encode().hex(),
        "geometry_srid": GEOMETRY_SRID,
        "geometry_checksum_sha256": "a" * 64,
        "source_snapshot_id": "synthetic-source-1",
    }


def edit(seq, feature_id, op, code="9999999900"):
    upsert = op == "upsert"
    properties = {"scope_kind": "legal_dong", "canonical_code": code, "display_name": "SYN-EDIT"}
    return {
        "unit": "admin",
        "change_seq": seq,
        "feature_id": feature_id,
        "op": op,
        "geometry_geojson": '{"type":"Polygon"}' if upsert else None,
        "geometry_wkb_hex": f"0106{seq:04x}" if upsert else None,
        "geometry_srid": GEOMETRY_SRID,
        "geometry_checksum_sha256": f"{seq:064x}" if upsert else None,
        "properties_json": json.dumps(properties) if upsert else "{}",
        "editor": "00000000-0000-0000-0000-000000000000",
        "edited_at": "2099-01-01T00:00:00Z",
    }


def lines(*rows):
    return [json.dumps(row) for row in rows]


class ServedGoldContractTest(unittest.TestCase):
    def test_the_table_comes_from_the_contract_artifact(self):
        self.assertEqual(GEOMETRY_SRID, 4326)
        for name in ("administrative_unit_id", *TILE_PROPERTIES, "edits_through_change_seq"):
            self.assertIn(name, SERVED_COLUMNS)

    def test_real_tables_need_an_explicit_flag(self):
        args = parse_args(["--edits-input", "e.jsonl", "--output", "o.jsonl"])
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)


class EditHandoffTest(unittest.TestCase):
    def test_only_admin_edits_in_epsg_4326_with_every_tile_property_are_read(self):
        self.assertEqual(len(read_edit_handoff(lines(edit(1, A, "upsert")))), 1)
        wrong_unit = dict(edit(1, A, "upsert"), unit="complex")
        wrong_crs = dict(edit(1, A, "upsert"), geometry_srid=5186)
        no_name = dict(
            edit(1, A, "upsert"),
            properties_json=json.dumps({"scope_kind": "legal_dong", "canonical_code": "9999999900"}),
        )
        for bad in (wrong_unit, wrong_crs, no_name):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                read_edit_handoff(lines(bad))


class ApplyEditsTest(unittest.TestCase):
    def test_every_ledgered_edit_is_applied_in_change_order(self):
        base = [base_row(A, "1111111100"), base_row(B, "2222222200")]
        ledger = [
            edit(3, A, "upsert", code="3333333300"),
            edit(1, A, "upsert", code="4444444400"),
            edit(2, B, "delete"),
            edit(4, C, "upsert", code="5555555500"),
        ]
        served, counts = apply_edits(base, ledger)
        self.assertEqual([row["administrative_unit_id"] for row in served], [A, C])
        self.assertEqual(served[0]["canonical_code"], "3333333300", "the newest edit wins")
        self.assertEqual(served[0]["display_name"], "SYN-EDIT")
        self.assertEqual(served[0]["source_snapshot_id"], "map-edit-3")
        self.assertEqual(counts, {"upserts": 3, "deletes": 1, "deletes_of_absent_features": 0})

    def test_two_boundaries_cannot_end_up_with_one_code(self):
        base = [base_row(A, "1111111100"), base_row(B, "2222222200")]
        with self.assertRaisesRegex(ValueError, "2222222200"):
            apply_edits(base, [edit(1, A, "upsert", code="2222222200")])


class BakeHandoffTest(unittest.TestCase):
    def test_lines_carry_the_id_every_tile_property_and_the_crs(self):
        served, _ = apply_edits([base_row(A, "1111111100")], [])
        line = json.loads(bake_handoff_lines(served))
        self.assertEqual(line["feature_id"], A)
        self.assertEqual(
            line["properties"],
            {"scope_kind": "legal_dong", "canonical_code": "1111111100", "display_name": "SYN-1111111100"},
        )
        self.assertEqual((line["geometry_srid"], line["origin"]), (4326, "source"))

    def test_an_empty_served_set_is_refused(self):
        with self.assertRaisesRegex(ValueError, "erase the layer"):
            bake_handoff_lines([])


if __name__ == "__main__":
    unittest.main()
