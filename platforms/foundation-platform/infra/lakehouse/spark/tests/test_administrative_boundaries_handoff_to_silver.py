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
    multipolygon_wkb,
    parse_args,
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

    def test_bad_codes_names_parents_and_repeats_are_refused(self):
        for bad in (feature(emd="9999910"), feature(emd_nm=""), feature(sgg="88888"), feature(sgg_nm="")):
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


if __name__ == "__main__":
    unittest.main()
