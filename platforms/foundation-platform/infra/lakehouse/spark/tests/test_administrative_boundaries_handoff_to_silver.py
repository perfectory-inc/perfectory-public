import hashlib
import struct
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from administrative_boundaries_handoff_to_silver import (  # noqa: E402
    COLUMNS,
    GEOMETRY_SRID,
    administrative_unit_id,
    resolve_unit_ids,
    multipolygon_wkb,
    parse_args,
    read_predecessors,
    silver_rows,
    validate_args,
)

# The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py (half-open).
SQUARE = [[[127.1231, 36.1231], [127.1234, 36.1231], [127.1234, 36.1234], [127.1231, 36.1234], [127.1231, 36.1231]]]
NOW = datetime(2099, 1, 1, tzinfo=timezone.utc)


def feature(emd="99999101", emd_nm="합성동", sgg="99999", sgg_nm="합성구", geometry=None):
    return {
        "type": "Feature",
        "properties": {"EMD_CD": emd, "EMD_NM": emd_nm, "SIGUNGU_CD": sgg, "SIGUNGU_NM": sgg_nm},
        "geometry": geometry or {"type": "Polygon", "coordinates": SQUARE},
    }


class IdentityTest(unittest.TestCase):
    def test_the_id_is_the_v5_of_the_stable_key_the_registry_already_used(self):
        self.assertEqual(
            administrative_unit_id("9999910100"),
            str(uuid.uuid5(uuid.NAMESPACE_URL, "scope:legal-dong:9999910100")),
        )
        self.assertEqual(uuid.UUID(administrative_unit_id("9999910100")).version, 5)


class UnitIdAcrossRenumberingTest(unittest.TestCase):
    def test_a_renumbered_dong_keeps_the_id_of_the_dong_it_came_from(self):
        previous = {"9999910100": administrative_unit_id("9999910100"), "9999910200": administrative_unit_id("9999910200")}
        ids, counts = resolve_unit_ids(
            ["9999920100", "9999910200", "9999930100"],
            {"9999920100": "9999910100"},
            previous,
        )
        self.assertEqual(ids["9999920100"], previous["9999910100"], "renumbered: same place, same id")
        self.assertEqual(ids["9999910200"], previous["9999910200"], "unchanged code keeps its id")
        self.assertEqual(ids["9999930100"], administrative_unit_id("9999930100"), "a new place gets its own")
        self.assertEqual(counts, {"kept": 1, "inherited": 1, "new": 1, "collisions": 0})

    def test_a_first_snapshot_with_a_known_predecessor_uses_the_predecessor_code(self):
        ids, _ = resolve_unit_ids(["9999920100"], {"9999920100": "9999910100"}, {})
        self.assertEqual(ids["9999920100"], administrative_unit_id("9999910100"))

    def test_an_id_is_never_given_to_two_dongs(self):
        previous = {"9999910100": administrative_unit_id("9999910100")}
        ids, counts = resolve_unit_ids(
            ["9999910100", "9999920100"], {"9999920100": "9999910100"}, previous
        )
        self.assertNotEqual(ids["9999910100"], ids["9999920100"])
        self.assertEqual(ids["9999910100"], previous["9999910100"], "the continuing code keeps it")
        self.assertEqual(counts["collisions"], 1)


class WkbTest(unittest.TestCase):
    def test_a_polygon_becomes_a_little_endian_multipolygon(self):
        wkb = multipolygon_wkb({"type": "Polygon", "coordinates": SQUARE})
        self.assertEqual(wkb[:9], struct.pack("<BII", 1, 6, 1))
        self.assertEqual(wkb[9:18], struct.pack("<BII", 1, 3, 1))
        self.assertEqual(struct.unpack("<I", wkb[18:22])[0], 5)
        self.assertEqual(struct.unpack("<dd", wkb[22:38]), (127.1231, 36.1231))
        self.assertEqual(len(wkb), 22 + 5 * 16)

    def test_open_rings_and_other_types_are_refused(self):
        with self.assertRaisesRegex(ValueError, "closed"):
            multipolygon_wkb({"type": "Polygon", "coordinates": [SQUARE[0][:4] + [[127.1232, 36.1232]]]})
        with self.assertRaisesRegex(ValueError, "Polygon or MultiPolygon"):
            multipolygon_wkb({"type": "LineString", "coordinates": SQUARE[0]})


class SilverRowsTest(unittest.TestCase):
    def test_rows_carry_every_contract_column_and_the_derived_facts(self):
        rows = silver_rows([feature()], "synthetic-snapshot-1", "bronze/source=synthetic/1.zip", NOW)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(set(row), set(COLUMNS))
        self.assertEqual(row["canonical_code"], "9999910100")
        self.assertEqual(row["scope_kind"], "legal_dong")
        self.assertEqual(row["display_name"], "합성동")
        self.assertEqual(row["parent_canonical_code"], "99999")
        self.assertEqual(row["geometry_srid"], GEOMETRY_SRID)
        self.assertEqual(row["geometry_checksum_sha256"], hashlib.sha256(row["geometry_wkb"]).hexdigest())

    def test_a_legal_dong_in_a_non_autonomous_gu_names_its_city(self):
        rows = silver_rows([feature(emd="99993101", sgg="99990", sgg_nm="합성시")], "s", "r", NOW)
        self.assertEqual(rows[0]["parent_canonical_code"], "99990")

    def test_bad_codes_names_parents_and_repeats_are_refused(self):
        for bad in (
            feature(emd="9999910"),
            feature(emd_nm=""),
            feature(sgg="88888"),
            feature(emd="99993101", sgg="99992"),
            feature(sgg_nm=""),
        ):
            with self.subTest(bad=bad["properties"]), self.assertRaises(ValueError):
                silver_rows([bad], "s", "r", NOW)
        with self.assertRaisesRegex(ValueError, "twice"):
            silver_rows([feature(), feature()], "s", "r", NOW)
        with self.assertRaisesRegex(ValueError, "no legal dong"):
            silver_rows([], "s", "r", NOW)


class ArgsTest(unittest.TestCase):
    def test_the_real_table_needs_the_explicit_flag(self):
        args = parse_args(["--input", "a.geojson", "--source-snapshot-id", "s1", "--source-record-id", "r"])
        with self.assertRaisesRegex(ValueError, "allow-non-smoke-write"):
            validate_args(args)

    def test_validate_only_needs_no_catalog(self):
        args = parse_args(["--input", "a.geojson", "--source-snapshot-id", "s1", "--source-record-id", "r", "--validate-only"])
        validate_args(args)

    def test_the_predecessor_flags_are_checked_and_the_file_input_is_gone(self):
        base = ["--input", "a.geojson", "--source-snapshot-id", "s1", "--source-record-id", "r", "--validate-only"]
        validate_args(parse_args(base + ["--legal-dong-change-table", "reference.legal_dong_code_change",
                                         "--predecessor-parcel-snapshot-id", "vworldkr__parcel:209906"]))
        table, snapshot = ["--legal-dong-change-table", "reference.legal_dong_code_change"], ["--predecessor-parcel-snapshot-id", "p"]
        for extra in (["--legal-dong-change-table", "reference.x;DROP", *snapshot], table, snapshot,
                      [*table, "--predecessor-parcel-snapshot-id", "p;DROP"]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                validate_args(parse_args(base + extra))
        with self.assertRaises(SystemExit):
            parse_args(base + ["--predecessor-map", "map.json"])


class _Row(dict):
    def asDict(self):
        return dict(self)


class _Spark:
    """Answers the three reads `read_predecessors` makes: the change table, its ref, the parcels."""

    def __init__(self, changes, pnus):
        self.changes, self.pnus, self.queries = changes, pnus, []

    def sql(self, query):
        self.queries.append(query)
        if ".refs" in query:
            rows = [_Row(snapshot_id=7)]
        elif "parcel_boundaries" in query:
            rows = [_Row(pnu=value) for value in self.pnus]
        else:
            rows = [_Row(row) for row in self.changes]
        return type("Result", (), {"collect": lambda _self: rows})()

    def createDataFrame(self, rows, schema):
        return type("Frame", (), {"createOrReplaceTempView": lambda _self, name: None})()


class PredecessorsFromTheChangeTableTest(unittest.TestCase):
    """The id's predecessors are a view of the code change table (root ADR-0145), not a CLI file."""

    def change(self, old, new, level="eupmyeondong"):
        return {"kind": "pair", "old_code": old, "new_code": new, "level": level, "effective_date": "20990701", "source": "s"}

    def test_merged_dongs_are_weighed_by_the_named_parcel_snapshot(self):
        changes = [self.change("9999910100", "9999930100"), self.change("9999910200", "9999930100")]
        pnus = ["9999910100100010000", "9999910200100010000", "9999910200100020000"]
        args = parse_args(["--input", "a", "--source-snapshot-id", "s1", "--source-record-id", "r",
                           "--legal-dong-change-table", "reference.legal_dong_code_change",
                           "--predecessor-parcel-snapshot-id", "june"])
        spark = _Spark(changes, pnus)
        predecessors, summary = read_predecessors(spark, args)
        self.assertEqual(predecessors, {"9999930100": "9999910200"})
        self.assertEqual(summary["change_table_snapshot_id"], "7")
        self.assertIn("FROM `lakehouse`.`reference`.`legal_dong_code_change`", spark.queries[0])
        self.assertIn("level IN ('eupmyeondong', 'ri')", spark.queries[0])

    def test_a_parcel_snapshot_holding_none_of_the_old_codes_is_refused(self):
        # Every weight would be zero and the merger's id would go to whichever code sorts first.
        changes = [self.change("9999910100", "9999930100"), self.change("9999910200", "9999930100")]
        args = parse_args(["--input", "a", "--source-snapshot-id", "s1", "--source-record-id", "r",
                           "--legal-dong-change-table", "reference.legal_dong_code_change",
                           "--predecessor-parcel-snapshot-id", "september"])
        with self.assertRaisesRegex(ValueError, "holds no parcel under any of the 2 old codes"):
            read_predecessors(_Spark(changes, ["9999930100100010000"]), args)


if __name__ == "__main__":
    unittest.main()
