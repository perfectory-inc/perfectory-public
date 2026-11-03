"""The overlap kernel of the regulation zones kind (root ADR-0375): synthetic shapes only.

Pins: the intersection area of two regions is exact for touching, contained, partly overlapping,
holed and multi-part shapes, in any ring orientation; cutting a zone into grid cells keeps its area
and every overlap; WKB of both byte orders and EWKB parse to rings with the region on their left.
"""

import random
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import zone_overlap as z  # noqa: E402

# Near the country, so the tests see coordinates of the real magnitude.
X0, Y0 = 127.0, 37.0


def square(x, y, side, clockwise=False):
    ring = [(X0 + x, Y0 + y), (X0 + x + side, Y0 + y), (X0 + x + side, Y0 + y + side), (X0 + x, Y0 + y + side)]
    return ring[::-1] if clockwise else ring


def polygon_wkb(polygons, order="<", srid=None):
    """A Polygon (one part) or MultiPolygon (more), closing each ring as the format does."""

    def head(kind, with_srid=False):
        out = struct.pack("B", 1 if order == "<" else 0) + struct.pack(order + "I", kind | (0x20000000 if with_srid else 0))
        return out + (struct.pack(order + "I", srid) if with_srid else b"")

    def body(rings):
        out = struct.pack(order + "I", len(rings))
        for ring in rings:
            closed = [*ring, ring[0]]
            out += struct.pack(order + "I", len(closed)) + b"".join(struct.pack(order + "dd", *p) for p in closed)
        return out

    if len(polygons) == 1:
        return head(3, srid is not None) + body(polygons[0])
    parts = b"".join(head(3) + body(p) for p in polygons)
    return head(6, srid is not None) + struct.pack(order + "I", len(polygons)) + parts


def region(*polygons):
    return z.rings_of(polygon_wkb(list(polygons)))


class RingsTest(unittest.TestCase):
    def test_outer_rings_turn_left_and_holes_right_whatever_the_file_says(self):
        rings = region([square(0, 0, 4, clockwise=True), square(1, 1, 1)])
        self.assertGreater(z.signed_area(rings[0]), 0)
        self.assertLess(z.signed_area(rings[1]), 0)
        self.assertAlmostEqual(z.region_area(rings), 15)

    def test_big_endian_and_ewkb_read_the_same(self):
        little = z.rings_of(polygon_wkb([[square(0, 0, 2)]]))
        self.assertEqual(z.rings_of(polygon_wkb([[square(0, 0, 2)]], order=">")), little)
        self.assertEqual(z.rings_of(polygon_wkb([[square(0, 0, 2)]], srid=4326)), little)

    def test_a_multipolygon_keeps_every_part(self):
        self.assertAlmostEqual(z.region_area(region([square(0, 0, 1)], [square(3, 0, 2)])), 5)

    def test_a_point_is_refused(self):
        with self.assertRaises(ValueError):
            z.rings_of(struct.pack("<BIdd", 1, 1, X0, Y0))


class OverlapTest(unittest.TestCase):
    def overlap(self, a, b):
        both = (z.overlap_area(a, b), z.overlap_area(b, a))
        self.assertAlmostEqual(both[0], both[1], places=9)
        return both[0]

    def test_disjoint_and_corner_or_edge_touching_shapes_do_not_overlap(self):
        a = region([square(0, 0, 1)])
        for b in (square(5, 5, 1), square(1, 0, 1), square(1, 1, 1), square(1, 0.25, 0.5), square(-1, 0.5, 1)):
            with self.subTest(b=b):
                self.assertEqual(self.overlap(a, region([b])), 0.0)

    def test_a_contained_shape_overlaps_by_its_own_area(self):
        self.assertAlmostEqual(self.overlap(region([square(0, 0, 10)]), region([square(2, 3, 1)])), 1, places=9)

    def test_a_shape_inside_and_touching_the_boundary_from_within(self):
        # Shares part of one edge with the outer square, the inside on the same side.
        self.assertAlmostEqual(self.overlap(region([square(0, 0, 4)]), region([square(0, 1, 1)])), 1, places=9)

    def test_identical_shapes_overlap_whole(self):
        self.assertAlmostEqual(self.overlap(region([square(0, 0, 2)]), region([square(0, 0, 2)])), 4, places=9)

    def test_partly_overlapping_squares(self):
        self.assertAlmostEqual(self.overlap(region([square(0, 0, 2)]), region([square(1, 1, 2)])), 1, places=9)

    def test_a_crossing_bar(self):
        bar = [(X0 - 1, Y0 + 0.5), (X0 + 3, Y0 + 0.5), (X0 + 3, Y0 + 1), (X0 - 1, Y0 + 1)]
        self.assertAlmostEqual(self.overlap(region([square(0, 0, 2)]), region([bar])), 1, places=9)

    def test_a_shape_inside_a_hole_does_not_overlap_and_one_across_its_edge_does_in_part(self):
        holed = region([square(0, 0, 10), square(2, 2, 4)])
        self.assertEqual(self.overlap(holed, region([square(3, 3, 1)])), 0.0)
        self.assertEqual(self.overlap(holed, region([square(2, 2, 4)])), 0.0)
        self.assertAlmostEqual(self.overlap(holed, region([square(1, 3, 2)])), 2, places=9)

    def test_a_multipolygon_overlaps_by_the_sum_of_its_parts(self):
        parts = region([square(0, 0, 1)], [square(3, 0, 1)])
        self.assertAlmostEqual(self.overlap(parts, region([square(0.5, 0, 3)])), 1.0, places=9)

    def test_a_concave_shape(self):
        # An L: the square in its notch only touches it.
        ell = [(X0, Y0), (X0 + 2, Y0), (X0 + 2, Y0 + 1), (X0 + 1, Y0 + 1), (X0 + 1, Y0 + 2), (X0, Y0 + 2)]
        self.assertEqual(self.overlap(region([ell]), region([square(1, 1, 1)])), 0.0)
        self.assertAlmostEqual(self.overlap(region([ell]), region([square(0.5, 0.5, 1)])), 0.75, places=9)

    def test_a_sliver_along_a_shared_edge_is_a_small_area_not_nothing_or_everything(self):
        a = region([square(0, 0, 1)])
        b = region([square(0.999999, 0, 1)])
        self.assertAlmostEqual(self.overlap(a, b), 1e-6, places=12)

    def test_a_piece_cut_from_the_parcel_itself_overlaps_by_its_own_area(self):
        # The clip rounds a cut edge a hair off the parcel's own edge, nearly parallel to it: the
        # edges are judged by their distance, not by an angle the rounding makes up (a fuzz against
        # shapely found this, 2026-11-04).
        parcel = region([[(X0 + 0.000678307907, Y0 + 0.000282538156), (X0 + 0.0000496113857, Y0 + 0.000577505757),
                          (X0, Y0)]])
        for i, j, piece in z.cell_pieces(parcel, 0.0002):
            with self.subTest(cell=(i, j)):
                # The piece's own area summed next to it: at 127 degrees the shoelace rounds at 1e-13.
                x0, y0, _, _ = z.bbox(piece)
                area = z.region_area([[(x - x0, y - y0) for x, y in r] for r in piece])
                self.assertLess(abs(z.overlap_area(parcel, piece) - area), 1e-9 * area)

    def test_random_rectangles_match_their_exact_intersection(self):
        rng = random.Random(20261104)
        for _ in range(200):
            x, y, w, h = (rng.uniform(0, 3) for _ in range(4))
            u, v, s, t = (rng.uniform(0, 3) for _ in range(4))
            a = [(X0 + x, Y0 + y), (X0 + x + w, Y0 + y), (X0 + x + w, Y0 + y + h), (X0 + x, Y0 + y + h)]
            b = [(X0 + u, Y0 + v), (X0 + u + s, Y0 + v), (X0 + u + s, Y0 + v + t), (X0 + u, Y0 + v + t)]
            exact = max(0, min(x + w, u + s) - max(x, u)) * max(0, min(y + h, v + t) - max(y, v))
            self.assertAlmostEqual(z.overlap_area(region([a]), region([b])), exact, places=9)


class CellPiecesTest(unittest.TestCase):
    def test_pieces_keep_the_area_and_stay_inside_their_cells(self):
        holed = region([square(0.05, 0.05, 0.9), square(0.3, 0.3, 0.2)])
        pieces = z.cell_pieces(holed, 0.25)
        self.assertAlmostEqual(sum(z.region_area(p) for _, _, p in pieces), z.region_area(holed), places=9)
        for i, j, piece in pieces:
            x0, y0, x1, y1 = z.bbox(piece)
            self.assertGreaterEqual(x0, i * 0.25 - 1e-9)
            self.assertLessEqual(x1, (i + 1) * 0.25 + 1e-9)
            self.assertGreaterEqual(y0, j * 0.25 - 1e-9)
            self.assertLessEqual(y1, (j + 1) * 0.25 + 1e-9)

    def test_a_cell_inside_the_hole_has_no_piece(self):
        holed = region([square(0, 0, 3), square(0.9, 0.9, 1.2)])
        cells = {(i, j) for i, j, _ in z.cell_pieces(holed, 1)}
        self.assertNotIn((128, 38), cells)  # the cell (127+1, 37+1) lies wholly in the hole
        self.assertIn((127, 37), cells)

    def test_overlaps_summed_over_pieces_equal_the_whole_zones(self):
        ell = [(X0, Y0), (X0 + 2, Y0), (X0 + 2, Y0 + 1), (X0 + 1, Y0 + 1), (X0 + 1, Y0 + 2), (X0, Y0 + 2)]
        zone = region([ell], [square(2.5, 2.5, 0.4)])
        pieces = z.cell_pieces(zone, 0.3)
        for parcel in (square(0.4, 0.4, 1.0), square(0.95, 0.95, 0.1), square(1.9, 0.9, 0.2), square(2.6, 2.6, 0.5)):
            with self.subTest(parcel=parcel):
                a = region([parcel])
                cells = set(z.cells_of(z.bbox(a), 0.3))
                summed = sum(z.overlap_area(a, p) for i, j, p in pieces if (i, j) in cells)
                self.assertAlmostEqual(summed, z.overlap_area(a, zone), places=9)

    def test_one_cell_s_pairs_are_the_pairs_overlap_area_measures(self):
        parcels = [region([square(0.1, 0.1, 0.2)]), [], region([square(0.5, 0.5, 0.2)]), region([square(0.9, 0.1, 0.05)])]
        pieces = [region([square(0, 0, 0.25)]), region([square(0.55, 0.55, 0.4), square(0.6, 0.6, 0.05)])]
        found = z.cell_overlaps((X0, Y0), parcels, pieces)
        self.assertEqual([(p, q) for p, q, _ in found], [(0, 0), (2, 1)])
        for p, q, area in found:
            self.assertAlmostEqual(area, z.overlap_area(parcels[p], pieces[q]), places=12)

    def test_parcel_cells_cover_its_box(self):
        self.assertEqual(z.cells_of((0.05, 0.05, 0.15, 0.05), 0.1), [(0, 0), (1, 0)])

    def test_square_metres_scale_by_latitude(self):
        self.assertAlmostEqual(z.square_metres(1e-8, 0) / z.square_metres(1e-8, 60), 2, places=6)


if __name__ == "__main__":
    unittest.main()
