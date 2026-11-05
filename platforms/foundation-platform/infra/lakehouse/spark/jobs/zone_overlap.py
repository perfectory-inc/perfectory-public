"""How much of a parcel a regulation zone covers, for the regulation zones by-PNU kind (root ADR-0375).

Standard library only, and nothing read at import: Spark ships this file to its executors
(`SparkContext.addPyFile`) to run its functions as column functions there, as `parcel_land.py`. The
stock Spark image has no Sedona or shapely, and a zone can hold 155,000 vertices (measured
2026-11-03), so a zone is first cut into grid cells (`cell_pieces`) and a parcel is compared only with
the pieces of the cells its bounding box touches.

Geometry is EPSG:4326 longitude/latitude, as `silver.parcel_boundaries` and
`silver.land_use_zone_area` write it. A region is a list of rings, each a list of (x, y) without the
closing point, oriented with the region on its left (outer rings counter-clockwise, holes clockwise),
so an area is the shoelace sum over all its rings.

`overlap_area` is the area of the intersection of two regions, by Green's theorem over the boundary of
the intersection: the pieces of A's boundary inside B, of B's boundary inside A, and the pieces both
share where the two regions lie on the same side. Boundaries that only touch add no area, so a parcel
whose edge runs along a zone's edge is not in the zone; how much overlap counts is the producer's
rule (`regulation_zones_silver_to_gold.counts`), not this module's.
"""

from __future__ import annotations

import math
import struct
from typing import Any

EARTH_RADIUS_M = 6371008.8
# Coordinates are shifted next to the parcel before comparing; a point this close (degrees, about
# 0.1 µm) to a line is on it.
EPSILON = 1e-12

Point = tuple[float, float]
Ring = list[Point]


def rings_of(wkb: bytes) -> list[Ring]:
    """The rings of a (E)WKB Polygon or MultiPolygon, oriented with the region on their left (the
    first ring of each polygon counter-clockwise, its holes clockwise). Any other geometry is refused."""

    data = bytes(wkb)
    pos = 0
    out: list[Ring] = []

    def take(fmt: str) -> tuple[Any, ...]:
        nonlocal pos
        values = struct.unpack_from(fmt, data, pos)
        pos += struct.calcsize(fmt)
        return values

    def ring(order: str, dims: int, outer: bool) -> None:
        (count,) = take(order + "I")
        points = [take(order + "d" * dims)[:2] for _ in range(count)]
        if len(points) > 1 and points[0] == points[-1]:
            points.pop()
        if len(points) < 3:
            return
        if (signed_area(points) > 0) != outer:
            points.reverse()
        out.append(points)

    def geometry() -> None:
        order = "<" if take("B")[0] == 1 else ">"
        (kind,) = take(order + "I")
        if kind & 0x20000000:  # EWKB SRID
            take(order + "I")
        base = (kind & 0x0FFFFFFF) % 1000
        dims = 2 + bool(kind & 0x80000000) + bool(kind & 0x40000000) + {1: 1, 2: 1, 3: 2}.get((kind & 0x0FFFFFFF) // 1000, 0)
        if base == 3:
            (rings,) = take(order + "I")
            for index in range(rings):
                ring(order, dims, index == 0)
        elif base == 6:
            (parts,) = take(order + "I")
            for _ in range(parts):
                geometry()
        else:
            raise ValueError(f"WKB geometry type {base} is not a polygon")

    geometry()
    return out


def signed_area(points: Ring, origin: Point | None = None) -> float:
    """The shoelace area of one ring: positive when counter-clockwise. Summed relative to `origin`
    (default its first point): at 127 degrees the products round at about 1e-13, a parcel's area."""

    ox, oy = origin or points[0]
    total = 0.0
    for (x1, y1), (x2, y2) in zip(points, points[1:] + points[:1]):
        total += (x1 - ox) * (y2 - oy) - (x2 - ox) * (y1 - oy)
    return total / 2


def region_area(rings: list[Ring]) -> float:
    return sum(signed_area(r) for r in rings)


def bbox(rings: list[Ring]) -> tuple[float, float, float, float]:
    xs = [x for r in rings for x, _ in r]
    ys = [y for r in rings for _, y in r]
    return min(xs), min(ys), max(xs), max(ys)


def square_metres(area_deg2: float, latitude: float) -> float:
    """An area in squared degrees near `latitude`, in m² (equirectangular, mean Earth radius), as
    `parcel_land.wkb_area_m2` measures a parcel."""

    scale = math.radians(1) * EARTH_RADIUS_M
    return area_deg2 * scale * scale * math.cos(math.radians(latitude))


def cells_of(box: tuple[float, float, float, float], size: float) -> list[tuple[int, int]]:
    """The grid cells (column, row of `size` degrees) a bounding box touches."""

    x0, y0, x1, y1 = box
    return [(i, j) for i in range(math.floor(x0 / size), math.floor(x1 / size) + 1)
            for j in range(math.floor(y0 / size), math.floor(y1 / size) + 1)]


def _clip(points: Ring, axis: int, at: float, keep_below: bool) -> Ring:
    """Sutherland-Hodgman: `points` cut to one side of the line `axis` = `at`. A concave ring may
    come back with zero-width edges along the line; they cancel in every area and crossing count."""

    def inside(p: Point) -> bool:
        return p[axis] <= at if keep_below else p[axis] >= at

    out: Ring = []
    for index, current in enumerate(points):
        previous = points[index - 1]
        if inside(current) != inside(previous):
            t = (at - previous[axis]) / (current[axis] - previous[axis])
            crossing = (previous[0] + t * (current[0] - previous[0]), previous[1] + t * (current[1] - previous[1]))
            crossing = (at, crossing[1]) if axis == 0 else (crossing[0], at)
            out.append(crossing)
        if inside(current):
            out.append(current)
    return out


def cell_pieces(rings: list[Ring], size: float) -> list[tuple[int, int, list[Ring]]]:
    """The region cut into grid cells of `size` degrees: (column, row, the rings inside that cell) for
    every cell the region covers with some area. The cut halves the cell range each time, so a zone
    of n vertices over c cells costs about n log c, never n x c."""

    out: list[tuple[int, int, list[Ring]]] = []
    if not rings:
        return out
    x0, y0, x1, y1 = bbox(rings)
    pending = [(rings, math.floor(x0 / size), math.floor(y0 / size), math.floor(x1 / size), math.floor(y1 / size))]
    while pending:
        part, i0, j0, i1, j1 = pending.pop()
        if i0 == i1 and j0 == j1:
            if region_area(part) > 0:
                out.append((i0, j0, part))
            continue
        axis, low, high = (0, i0, i1) if i1 - i0 >= j1 - j0 else (1, j0, j1)
        middle = (low + high) // 2
        at = (middle + 1) * size
        for keep_below in (True, False):
            cut = [c for c in (_clip(r, axis, at, keep_below) for r in part) if len(c) >= 3]
            if not cut or region_area(cut) <= 0:
                continue
            if axis == 0:
                pending.append((cut, i0, j0, middle, j1) if keep_below else (cut, middle + 1, j0, i1, j1))
            else:
                pending.append((cut, i0, j0, i1, middle) if keep_below else (cut, i0, middle + 1, i1, j1))
    return out


def _edges(rings: list[Ring]) -> list[tuple[float, float, float, float]]:
    return [(*p, *q) for r in rings for p, q in zip(r, r[1:] + r[:1]) if p != q]


def _cross(ax: float, ay: float, bx: float, by: float) -> float:
    return ax * by - ay * bx


def _splits(edge: tuple[float, float, float, float], others: list[tuple[float, float, float, float]]) -> list[float]:
    """The parameters along `edge` where it meets any of `others` (a crossing, a touch, or the ends of
    a collinear overlap), 0 and 1 included, sorted."""

    px, py, qx, qy = edge
    dx, dy = qx - px, qy - py
    length = math.hypot(dx, dy)
    found = {0.0, 1.0}

    def along(x: float, y: float) -> None:
        t = ((x - px) * dx + (y - py) * dy) / (length * length)
        if 0 < t < 1:
            found.add(t)

    for ax, ay, bx, by in others:
        # Distances from the edge's line, so nearly parallel edges are judged by how far apart they
        # are, not by an angle a clip's rounding makes up.
        da, db = _cross(dx, dy, ax - px, ay - py) / length, _cross(dx, dy, bx - px, by - py) / length
        if abs(da) <= EPSILON or abs(db) <= EPSILON:
            # An end on the line (both: a collinear overlap): where it lies along the edge.
            if abs(da) <= EPSILON:
                along(ax, ay)
            if abs(db) <= EPSILON:
                along(bx, by)
            continue
        if (da > 0) == (db > 0):
            continue
        # The other edge crosses the line; where along the edge, if within it.
        s = da / (da - db)
        along(ax + s * (bx - ax), ay + s * (by - ay))
    return sorted(found)


def _on(x: float, y: float, edge: tuple[float, float, float, float]) -> bool:
    """Whether (x, y) lies on `edge`, ends excluded."""

    ax, ay, bx, by = edge
    ex, ey = bx - ax, by - ay
    length = math.hypot(ex, ey)
    if abs(_cross(ex, ey, x - ax, y - ay)) > EPSILON * length:
        return False
    return 0 < ((x - ax) * ex + (y - ay) * ey) / (length * length) < 1


def _inside(x: float, y: float, edges: list[tuple[float, float, float, float]]) -> bool:
    """Even-odd: a ray from (x, y) towards +x crosses an odd number of edges."""

    inside = False
    for ax, ay, bx, by in edges:
        if (ay > y) != (by > y) and x < ax + (y - ay) * (bx - ax) / (by - ay):
            inside = not inside
    return inside


def _shared(x: float, y: float, dx: float, dy: float, edges: list[tuple[float, float, float, float]]
            ) -> tuple[int, list[tuple[float, float, float, float]]]:
    """For a point on a boundary piece of direction (dx, dy): the edges of `edges` it lies on, and
    their net direction (same as the piece +1, opposite -1)."""

    net, under = 0, []
    for edge in edges:
        if _on(x, y, edge):
            under.append(edge)
            net += 1 if (edge[2] - edge[0]) * dx + (edge[3] - edge[1]) * dy > 0 else -1
    return net, under


def _boundary_inside(own: list[tuple[float, float, float, float]], other: list[tuple[float, float, float, float]],
                     near: list[tuple[float, float, float, float]], shared_counts: bool) -> float:
    """Twice the shoelace sum of the pieces of `own` inside the region of `other` (`near`: the edges of
    `other` that can meet them). A piece on `other`'s boundary counts when `shared_counts` and both
    regions lie on its left: the edges under it run its way (net +1), or cancel and the region is
    there anyway."""

    total = 0.0
    for edge in own:
        px, py, qx, qy = edge
        dx, dy = qx - px, qy - py
        ts = _splits(edge, near)
        for t0, t1 in zip(ts, ts[1:]):
            if t1 - t0 <= 0:
                continue
            x0, y0, x1, y1 = px + t0 * dx, py + t0 * dy, px + t1 * dx, py + t1 * dy
            mx, my = (x0 + x1) / 2, (y0 + y1) / 2
            net, under = _shared(mx, my, dx, dy, near)
            if under:
                if not shared_counts:
                    continue
                if net < 0 or (net == 0 and not _inside(mx, my, [e for e in other if e not in under])):
                    continue
            elif not _inside(mx, my, other):
                continue
            total += x0 * y1 - x1 * y0
    return total


def _overlap(a: list[Ring], edges_a: list, box: tuple[float, float, float, float], edges_b: list) -> float:
    """`overlap_area` of `a` (its edges and box given) and the region of `edges_b`."""

    reach = EPSILON * 4
    x0, y0, x1, y1 = box[0] - reach, box[1] - reach, box[2] + reach, box[3] + reach
    near = [e for e in edges_b if max(e[0], e[2]) >= x0 and min(e[0], e[2]) <= x1
            and max(e[1], e[3]) >= y0 and min(e[1], e[3]) <= y1]
    if not near:
        # The boundaries never meet: either all of `a` is inside `b`, or none of it is.
        return region_area(a) if _inside(*a[0][0], edges_b) else 0.0
    twice = _boundary_inside(edges_a, edges_b, near, True) + _boundary_inside(near, edges_a, edges_a, False)
    return max(twice / 2, 0.0)


def overlap_area(a: list[Ring], b: list[Ring]) -> float:
    """The area (squared degrees) of the intersection of regions `a` and `b`; 0 when they only touch."""

    if not a or not b:
        return 0.0
    ox, oy, _, _ = bbox(a)
    # Next to the parcel, so the tests on differences of nearby coordinates keep their precision.
    a = [[(x - ox, y - oy) for x, y in r] for r in a]
    b = [[(x - ox, y - oy) for x, y in r] for r in b]
    return _overlap(a, _edges(a), bbox(a), _edges(b))


def cell_overlaps(origin: Point, parcels: list[list[Ring]], pieces: list[list[Ring]]) -> list[tuple[int, int, float]]:
    """(parcel index, piece index, overlap area in squared degrees) of every pair of one grid cell's
    parcels and zone pieces that overlap by some area. Each shape is shifted next to the cell's
    `origin` and its edges and box are taken once, not once per pair."""

    def local(rings: list[Ring]) -> list[Ring]:
        return [[(x - origin[0], y - origin[1]) for x, y in r] for r in rings]

    shapes = [(rings, _edges(rings), bbox(rings)) for rings in (local(p) for p in parcels) if rings]
    zones = [(index, _edges(rings), bbox(rings)) for index, rings in enumerate(local(p) for p in pieces) if rings]
    indices = [index for index, p in enumerate(parcels) if p]
    out = []
    for index, (rings, edges, (x0, y0, x1, y1)) in zip(indices, shapes):
        for piece, piece_edges, (u0, v0, u1, v1) in zones:
            if u0 > x1 or x0 > u1 or v0 > y1 or y0 > v1:
                continue
            area = _overlap(rings, edges, (x0, y0, x1, y1), piece_edges)
            if area > 0:
                out.append((index, piece, area))
    return out
