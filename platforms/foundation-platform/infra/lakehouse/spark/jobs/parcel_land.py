"""What a parcel boundary row says about its land, for the legal-dong pairing's 지번 step (root
ADR-0144, owner decision 2026-10-05): its 지목 and its area.

Standard library only, and nothing read at import: Spark ships this file to its executors
(`SparkContext.addPyFile`) to run both as column functions there, where the job's directory is not
on the path.
"""

from __future__ import annotations

import math
import re
import struct
from typing import Any

JIMOK = re.compile(r"([가-힣]+)\s*$")
EARTH_RADIUS_M = 6371008.8


def jimok_of(jibun_text: str | None) -> str:
    """The 지목 the 연속지적도 writes after the lot number (`123-4대` → `대`), or "" when it has none."""

    found = JIMOK.search(jibun_text or "")
    return found.group(1) if found else ""


def wkb_area_m2(wkb: bytes) -> float:
    """The area in m² of a (E)WKB Polygon or MultiPolygon in longitude/latitude degrees
    (`silver.parcel_boundaries` is EPSG:4326).

    Each ring is laid on a plane at its own mean latitude (equirectangular, mean Earth radius) and
    measured by the shoelace formula, holes subtracted. For a parcel that is well inside the 지번
    step's tolerance, and both editions are measured the same way. Any other geometry is refused.
    """

    data = bytes(wkb)
    pos = 0

    def take(fmt: str) -> tuple[Any, ...]:
        nonlocal pos
        values = struct.unpack_from(fmt, data, pos)
        pos += struct.calcsize(fmt)
        return values

    def ring_area(order: str, dims: int) -> float:
        (count,) = take(order + "I")
        points = [take(order + "d" * dims)[:2] for _ in range(count)]
        if count < 3:
            return 0.0
        lat0 = math.radians(sum(lat for _, lat in points) / count)
        scale = math.radians(1) * EARTH_RADIUS_M
        xy = [(lon * scale * math.cos(lat0), lat * scale) for lon, lat in points]
        return abs(sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(xy, xy[1:] + xy[:1]))) / 2

    def geometry() -> float:
        order = "<" if take("B")[0] == 1 else ">"
        (kind,) = take(order + "I")
        if kind & 0x20000000:  # EWKB SRID
            take(order + "I")
        base = (kind & 0x0FFFFFFF) % 1000
        dims = 2 + bool(kind & 0x80000000) + bool(kind & 0x40000000) + {1: 1, 2: 1, 3: 2}.get((kind & 0x0FFFFFFF) // 1000, 0)
        if base == 3:
            (rings,) = take(order + "I")
            areas = [ring_area(order, dims) for _ in range(rings)]
            return (areas[0] - sum(areas[1:])) if areas else 0.0
        if base == 6:
            (parts,) = take(order + "I")
            return sum(geometry() for _ in range(parts))
        raise ValueError(f"WKB geometry type {base} is not a polygon")

    return geometry()


