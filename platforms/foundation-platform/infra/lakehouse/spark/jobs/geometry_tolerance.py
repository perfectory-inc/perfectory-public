"""Whether two (E)WKB polygons in EPSG:4326 are the same shape within a tolerance in metres: the
compare's equality for a geometry that a provider re-projected by a few centimetres
(`silver_release_compare`, the lane contract's `compare.geometry_tolerance`).

The rule is JTS `Geometry.equalsExact(other, tolerance)`: the same type and the same parts, rings
and vertices in the same order, each vertex within the tolerance of its partner. A vertex's
distance is measured on the sphere by local scaling (latitude × 111,320 m, longitude × 111,320 m ×
cos latitude). Rounding coordinates before a checksum cannot do this: a vertex near a grid line
rounds either way (measured 2026-11-05, 1e-6° rounding made only ~1% of shifted zones equal).

Standard library only, and nothing read at import: Spark ships this file to its executors
(`SparkContext.addPyFile`), as it ships `parcel_land`. Its WKB reading follows `parcel_land`'s;
shortcut: two readers of one format, merge them when `parcel_land` is next changed (its bytes are
part of the legal-dong pairing's code version, so a refactor alone would re-run that step).
"""

from __future__ import annotations

import math
import struct
from typing import Any

METRES_PER_DEGREE = 111_320.0
POLYGON, MULTIPOLYGON = 3, 6


def polygons(wkb: bytes) -> tuple[int, list[list[list[tuple[float, float]]]]]:
    """The geometry type (3 Polygon, 6 MultiPolygon) and its polygons, each a list of rings of
    (longitude, latitude) vertices; Z and M are read past. Any other type is refused."""

    data = bytes(wkb)
    pos = 0

    def take(fmt: str) -> tuple[Any, ...]:
        nonlocal pos
        values = struct.unpack_from(fmt, data, pos)
        pos += struct.calcsize(fmt)
        return values

    def header() -> tuple[str, int, int]:
        order = "<" if take("B")[0] == 1 else ">"
        (kind,) = take(order + "I")
        if kind & 0x20000000:  # EWKB SRID
            take(order + "I")
        base = (kind & 0x0FFFFFFF) % 1000
        dims = 2 + bool(kind & 0x80000000) + bool(kind & 0x40000000) + {1: 1, 2: 1, 3: 2}.get((kind & 0x0FFFFFFF) // 1000, 0)
        return order, base, dims

    def polygon(order: str, dims: int) -> list[list[tuple[float, float]]]:
        (rings,) = take(order + "I")
        out = []
        for _ in range(rings):
            (count,) = take(order + "I")
            out.append([take(order + "d" * dims)[:2] for _ in range(count)])
        return out

    order, base, dims = header()
    if base == POLYGON:
        return base, [polygon(order, dims)]
    if base == MULTIPOLYGON:
        (parts,) = take(order + "I")
        found = []
        for _ in range(parts):
            part_order, part_base, part_dims = header()
            if part_base != POLYGON:
                raise ValueError(f"WKB MultiPolygon part type {part_base} is not a polygon")
            found.append(polygon(part_order, part_dims))
        return base, found
    raise ValueError(f"WKB geometry type {base} is not a polygon")


def within_tolerance(old: bytes | None, new: bytes | None, metres: float) -> bool:
    """True when `new` is `old` with every vertex moved by at most `metres`; False for any other
    difference (type, part, ring or vertex count), for a missing geometry, and for one that is not
    a polygon: those stay changes, as an exact compare would call them."""

    if old is None or new is None:
        return False
    try:
        old_type, old_parts = polygons(old)
        new_type, new_parts = polygons(new)
    except (ValueError, struct.error):
        return False
    if old_type != new_type or len(old_parts) != len(new_parts):
        return False
    limit = metres * metres
    for old_rings, new_rings in zip(old_parts, new_parts):
        if len(old_rings) != len(new_rings):
            return False
        for old_ring, new_ring in zip(old_rings, new_rings):
            if len(old_ring) != len(new_ring):
                return False
            for (x1, y1), (x2, y2) in zip(old_ring, new_ring):
                dy = (y2 - y1) * METRES_PER_DEGREE
                dx = (x2 - x1) * METRES_PER_DEGREE * math.cos(math.radians(y1))
                if dx * dx + dy * dy > limit:
                    return False
    return True
