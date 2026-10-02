"""The parcel served-Gold job's rules that need no Spark runtime (root ADR-0133 §2).

The Spark half — the duplicate PNU refusal, the served arithmetic and the parts the executors
write — is in `test_parcel_boundary_served_gold_spark.py`.
"""

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import served_gold_common as common  # noqa: E402
from parcel_boundary_served_gold import (  # noqa: E402
    FEATURE_ID_PROPERTY,
    GEOMETRY_SRID,
    SERVED_COLUMNS,
    TILE_PROPERTIES,
    check_ledger,
    fold_edits,
    parse_args,
    validate_args,
)

# Synthetic PNUs: the 99999 prefix is no real district.
P1 = "9999910100100010000"
P2 = "9999910100100020000"
P3 = "9999910100100030000"


def edit(seq, pnu, op, srid=GEOMETRY_SRID):
    upsert = op == "upsert"
    return {
        "change_seq": seq,
        "feature_id": pnu,
        "op": op,
        "geometry_srid": srid,
        "geometry_wkb_hex": f"0106{seq:04x}" if upsert else None,
        "geometry_checksum_sha256": f"{seq:064x}" if upsert else None,
    }


def v1_args(served):
    return dict(
        job="parcel_boundary_served_gold", unit="parcels", feature_id_property="pnu", srid=GEOMETRY_SRID,
        gold_snapshot="1", through=0, handoff=0, appended=0, ledger=0, served=served, base=served,
        output=Path("parts"), generated_at="2099-01-01T00:00:00Z", counts={}, extra={},
    )


class ContractTest(unittest.TestCase):
    def test_the_table_comes_from_the_contract_artifact(self):
        self.assertEqual(GEOMETRY_SRID, 4326)
        self.assertEqual(FEATURE_ID_PROPERTY, "pnu")
        self.assertEqual(TILE_PROPERTIES, (), "the tile carries the PNU and nothing else")
        for name in ("pnu", "geometry_wkb", "origin", "source_snapshot_id", "edits_through_change_seq"):
            self.assertIn(name, SERVED_COLUMNS)

    def test_a_real_table_needs_an_explicit_flag(self):
        args = parse_args(["--source-snapshot-id", "synthetic-1", "--output-dir", "parts"])
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)

    def test_a_snapshot_id_that_could_break_out_of_the_query_is_refused(self):
        args = parse_args(
            ["--source-snapshot-id", "x' OR '1'='1", "--output-dir", "parts", "--allow-non-smoke-write"]
        )
        with self.assertRaisesRegex(ValueError, "snapshot id"):
            validate_args(args)


class LedgerTest(unittest.TestCase):
    def test_an_edit_in_another_crs_is_refused(self):
        with self.assertRaisesRegex(ValueError, "EPSG:5186"):
            check_ledger([edit(1, P1, "upsert", srid=5186)])

    def test_an_edit_that_names_no_pnu_or_carries_no_geometry_is_refused(self):
        for bad in (
            dict(edit(1, P1, "upsert"), feature_id="not-a-pnu"),
            dict(edit(1, P1, "upsert"), geometry_wkb_hex=None),
            dict(edit(1, P1, "upsert"), geometry_checksum_sha256=""),
            dict(edit(1, P1, "delete"), op="move"),
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                check_ledger([bad])

    def test_a_repeated_change_seq_is_refused(self):
        with self.assertRaisesRegex(ValueError, "change_seq 1"):
            check_ledger([edit(1, P1, "delete"), edit(1, P2, "delete")])


class FoldTest(unittest.TestCase):
    def test_edits_apply_in_change_order_and_the_last_one_wins(self):
        final, counts = fold_edits(
            [edit(3, P1, "upsert"), edit(1, P1, "delete"), edit(2, P2, "delete"), edit(4, P3, "upsert")],
            present={P1, P2},
        )
        self.assertEqual(final[P1]["change_seq"], 3, "deleted at 1, upserted again at 3")
        self.assertIsNone(final[P2])
        self.assertEqual(final[P3]["change_seq"], 4)
        self.assertEqual(counts, {"upserts": 2, "deletes": 2, "deletes_of_absent_features": 0})

    def test_a_delete_of_an_absent_parcel_is_counted(self):
        _, counts = fold_edits([edit(1, P3, "delete")], present=set())
        self.assertEqual(counts["deletes_of_absent_features"], 1)

    def test_a_second_delete_counts_the_parcel_the_first_one_removed_as_absent(self):
        _, counts = fold_edits([edit(1, P1, "delete"), edit(2, P1, "delete")], present={P1})
        self.assertEqual(counts["deletes_of_absent_features"], 1)


class HandoffLineTest(unittest.TestCase):
    def test_a_parcel_line_is_the_v1_row_with_no_properties(self):
        line = common.bake_handoff_line(P1, {}, "0106", GEOMETRY_SRID, "source")
        self.assertEqual(
            line,
            '{"feature_id":"9999910100100010000","geometry_srid":4326,"geometry_wkb_hex":"0106",'
            '"origin":"source","properties":{}}',
        )
        self.assertEqual(
            common.bake_handoff_lines(
                [{"pnu": P1, "geometry_wkb_hex": "0106", "origin": "source"}], "pnu", (), GEOMETRY_SRID
            ),
            line + "\n",
            "the v1 file and a v2 part are made of the same line",
        )


class PartsTest(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="parcel-parts-")
        self.addCleanup(self.workspace.cleanup)
        self.directory = Path(self.workspace.name) / "parts"
        self.directory.mkdir()

    def part(self, index, lines):
        written = common.write_part(str(self.directory), index, iter(lines))
        return [{"path": name, "rows": rows, "sha256": sha} for name, rows, sha in written]

    def test_a_part_records_its_rows_and_the_digest_of_its_bytes(self):
        [part] = self.part(0, ["a", "b"])
        self.assertEqual(part["path"], "part-00000.jsonl")
        self.assertEqual(part["rows"], 2)
        self.assertEqual(part["sha256"], hashlib.sha256(b"a\nb\n").hexdigest())
        common.verify_handoff_parts(self.directory, [part], served=2)

    def test_an_empty_partition_writes_no_part(self):
        self.assertEqual(self.part(1, []), [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_a_part_is_never_written_twice(self):
        self.part(0, ["a"])
        with self.assertRaises(FileExistsError):
            self.part(0, ["b"])

    def test_parts_that_disagree_with_their_record_are_refused(self):
        [part] = self.part(0, ["a", "b"])
        for recorded, served, message in (
            (dict(part, rows=3), 3, "recorded 3 rows"),
            (dict(part, sha256="0" * 64), 2, "sha256"),
            (part, 5, "the parts hold 2 rows, the served table 5"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                common.verify_handoff_parts(self.directory, [recorded], served=served)
        (self.directory / "part-00009.jsonl").write_bytes(b"c\n")
        with self.assertRaisesRegex(ValueError, "the parts directory holds"):
            common.verify_handoff_parts(self.directory, [part], served=2)


class SummaryTest(unittest.TestCase):
    def test_the_v2_summary_is_the_v1_summary_plus_the_snapshot_and_the_parts(self):
        parts = [{"path": "part-00000.jsonl", "rows": 2, "sha256": "a" * 64},
                 {"path": "part-00001.jsonl", "rows": 1, "sha256": "b" * 64}]
        v1 = common.summary(**v1_args(3))
        v2 = common.summary_v2(source_snapshot_id="synthetic-1", parts=parts, **v1_args(3))
        self.assertEqual(v1["schema_version"], "foundation-platform.polygon_served_gold.v1")
        self.assertEqual(v2["schema_version"], "foundation-platform.polygon_served_gold.v2")
        self.assertEqual(set(v2) - set(v1), {"source_snapshot_id", "handoff_parts"})
        self.assertEqual({k: v for k, v in v2.items() if k in v1 and k != "schema_version"},
                         {k: v for k, v in v1.items() if k != "schema_version"})
        self.assertEqual(v2["handoff_parts"], parts)
        self.assertEqual(v2["source_snapshot_id"], "synthetic-1")
        json.dumps(v2)

    def test_parts_that_do_not_add_up_or_escape_the_directory_are_refused(self):
        with self.assertRaisesRegex(ValueError, "add up"):
            common.summary_v2(source_snapshot_id="s", parts=[{"path": "part-00000.jsonl", "rows": 2,
                                                               "sha256": "a" * 64}], **v1_args(3))
        for path in ("../part-00000.jsonl", "/data/part-00000.jsonl", "part-0.jsonl"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "relative"):
                common.summary_v2(source_snapshot_id="s", parts=[{"path": path, "rows": 3,
                                                                   "sha256": "a" * 64}], **v1_args(3))


if __name__ == "__main__":
    unittest.main()
