"""The parcel edit fold's plan (root ADR-0112 §7·§8, ADR-0375 §2): `jobs/parcel_boundary_fold.py`.

Pure Python: no Spark, no catalog. Synthetic PNUs (99999…) and coordinates near 127°E 37°N only.
"""

import math
import struct
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import land_use_zone_area_by_tile as by_tile  # noqa: E402
import parcel_boundary_fold as fold  # noqa: E402


def pnu(n):
    return f"99999101001{n:04d}0000"


def square_wkb(lon, lat, side, order="<", srid=None):
    kind = fold.WKB_POLYGON | (fold.EWKB_SRID_FLAG if srid else 0)
    out = struct.pack(order + "BI", 1 if order == "<" else 0, kind)
    if srid:
        out += struct.pack(order + "I", srid)
    ring = [(0, 0), (side, 0), (side, side), (0, side), (0, 0)]
    out += struct.pack(order + "II", 1, len(ring))
    for x, y in ring:
        out += struct.pack(order + "dd", lon + x, lat + y)
    return out


def edit(seq, n, op="upsert", lon=127.0, lat=37.0, side=0.0001):
    row = {"change_seq": seq, "feature_id": pnu(n), "op": op, "geometry_srid": 4326,
           "geometry_wkb_hex": None, "geometry_checksum_sha256": None}
    if op == "upsert":
        row["geometry_wkb_hex"] = square_wkb(lon, lat, side).hex()
        row["geometry_checksum_sha256"] = f"{seq:064x}"
    return row


class WkbBoxTest(unittest.TestCase):
    def test_a_polygon_box_is_read_in_either_byte_order_and_with_an_srid(self):
        expected = (127.0, 37.0, 127.001, 37.001)
        for wkb in (square_wkb(127.0, 37.0, 0.001), square_wkb(127.0, 37.0, 0.001, ">"),
                    square_wkb(127.0, 37.0, 0.001, srid=4326)):
            self.assertEqual(tuple(round(v, 9) for v in fold.wkb_bbox(wkb)), expected)

    def test_a_multipolygon_box_covers_every_part(self):
        parts = [square_wkb(127.0, 37.0, 0.001), square_wkb(127.01, 37.02, 0.001)]
        wkb = struct.pack("<BII", 1, fold.WKB_MULTIPOLYGON, 2) + b"".join(parts)
        self.assertEqual(tuple(round(v, 9) for v in fold.wkb_bbox(wkb)), (127.0, 37.0, 127.011, 37.021))

    def test_a_truncated_or_padded_or_non_polygon_wkb_is_refused(self):
        wkb = square_wkb(127.0, 37.0, 0.001)
        for bad in (wkb[:-3], wkb + b"\x00", struct.pack("<BIdd", 1, 1, 127.0, 37.0)):
            with self.assertRaises(ValueError):
                fold.wkb_bbox(bad)


class EditStatesTest(unittest.TestCase):
    def test_before_is_the_last_edit_up_to_the_served_sequence_and_after_the_last_of_all(self):
        ledger = [edit(1, 1, lon=127.0), edit(2, 2), edit(3, 1, lon=127.5), edit(4, 3, op="delete")]
        before, after = fold.edit_states(ledger, through=2)
        self.assertEqual(sorted(after), [pnu(1), pnu(3)], "only PNUs edited after 2 change")
        self.assertEqual(before[pnu(1)]["change_seq"], 1)
        self.assertNotIn(pnu(3), before, "never edited before: its Silver row stands")
        self.assertEqual(after[pnu(1)]["change_seq"], 3)
        self.assertIsNone(after[pnu(3)], "a delete leaves nothing served")
        self.assertEqual(fold.change_list(after), [{"feature_id": pnu(1), "op": "upsert"},
                                                   {"feature_id": pnu(3), "op": "delete"}])


class NeighbourhoodTest(unittest.TestCase):
    def test_a_box_meets_a_tiles_widened_rectangle_exactly_when_tile_range_names_the_tile(self):
        # The Silver predicate (rectangles) and the tile math (tile_range) agree box by box.
        tiles = fold.neighbourhood_tiles([(127.0, 37.0, 127.0001, 37.0001)])
        self.assertTrue(tiles)
        for dx in (-0.03, -0.0215, -0.02, 0.0, 0.02, 0.0215, 0.03):
            for dy in (-0.03, -0.017, 0.0, 0.017, 0.03):
                box = (127.0 + dx, 37.0 + dy, 127.0001 + dx, 37.0001 + dy)
                span = by_tile.tile_range(box, fold.NEIGHBOURHOOD_ZOOM, by_tile.MEMBERSHIP_BUFFER_PIXELS)
                named = {(x, y) for x in range(span[0], span[2] + 1) for y in range(span[1], span[3] + 1)}
                self.assertEqual(fold.touches_any(box, tiles), bool(named & set(tiles)), box)

    def test_the_predicate_is_one_rectangle_test_per_tile(self):
        tiles = fold.neighbourhood_tiles([(127.0, 37.0, 127.0001, 37.0001)])
        predicate = fold.silver_predicate(tiles)
        self.assertEqual(predicate.count(" OR ") + 1, len(tiles))
        self.assertEqual(fold.silver_predicate([]), "false")

    def test_a_tiles_envelope_is_its_own_square_widened_by_the_pixels(self):
        left, bottom, right, top = fold.tile_envelope(0, 0, 1, 0.0)
        self.assertEqual((left, right), (-180.0, 0.0))
        self.assertAlmostEqual(top, by_tile.MAX_LATITUDE, places=6)
        self.assertAlmostEqual(bottom, 0.0, places=9)
        wide = fold.tile_envelope(0, 0, 1, 256.0)
        self.assertLess(wide[0], -180.0)
        self.assertTrue(math.isfinite(wide[1]))


class PlanTest(unittest.TestCase):
    def test_the_neighbourhood_covers_the_old_and_the_new_box_of_a_moved_parcel(self):
        silver = {pnu(1): (127.0, 37.0, 127.0001, 37.0001)}
        ledger = [edit(1, 1, lon=127.2, lat=37.2)]
        planned = fold.plan(ledger, 0, silver)
        old = fold.neighbourhood_tiles([silver[pnu(1)]])
        new = fold.neighbourhood_tiles([fold.edit_box(ledger[0])])
        self.assertTrue(set(old) <= set(planned["tiles"]) and set(new) <= set(planned["tiles"]))
        self.assertEqual(planned["edited_in_neighbourhood"], [pnu(1)])
        self.assertEqual(planned["changes"], [{"feature_id": pnu(1), "op": "upsert"}])

    def test_an_earlier_edit_inside_the_neighbourhood_is_handed_from_its_edit(self):
        # PNU 2 was edited in an earlier fold to a place beside PNU 1's change: the served row of
        # PNU 2 is its edit, which the handoff must carry; its Silver row is not served.
        ledger = [edit(1, 2, lon=127.00005), edit(2, 1, lon=127.0)]
        planned = fold.plan(ledger, 1, {pnu(1): (127.0, 37.0, 127.0001, 37.0001)})
        self.assertEqual(planned["ledger_pnus"], [pnu(1), pnu(2)])
        self.assertEqual(planned["edited_in_neighbourhood"], [pnu(1), pnu(2)])
        self.assertEqual([line["feature_id"] for line in planned["changes"]], [pnu(1)])

    def test_a_deleted_parcel_dirties_its_old_box_and_is_not_handed(self):
        ledger = [edit(1, 1, op="delete")]
        planned = fold.plan(ledger, 0, {pnu(1): (127.0, 37.0, 127.0001, 37.0001)})
        self.assertTrue(planned["tiles"])
        self.assertEqual(planned["edited_in_neighbourhood"], [])
        self.assertEqual(planned["changes"], [{"feature_id": pnu(1), "op": "delete"}])

    def test_a_fold_past_its_tile_bound_is_not_partial(self):
        ledger = [edit(n, n, lon=124.0 + n * 0.05, lat=34.0 + (n % 40) * 0.05) for n in range(1, 600)]
        planned = fold.plan(ledger, 0, {})
        self.assertIn("full_reason", planned)


if __name__ == "__main__":
    unittest.main()
