"""Fold staff parcel edits into `gold.parcel_boundary_served` and hand the bake only their neighbourhood.

Root ADR-0112 §7·§8, ADR-0375 §2. A parcel edit changes one or a few of about forty million rows, so a
fold does not rebuild the served table or hand the bake every parcel:

- **Changed PNUs.** The ledgered `parcels` edits after the served Gold's `edits_through_change_seq`
  name them; each one's state is the last edit of the whole ledger (`fold_edits`), and its state
  before is the last edit up to the served sequence, or its Silver row when none.
- **Gold.** Only those PNUs are written: a keyed `MERGE` (the table is `replaced` by `pnu`), a delete
  removes the row, an upsert puts the edit's geometry. Every other row stays as served.
- **Neighbourhood.** Every z14 tile (the parcel layer's minzoom) that the old or the new box of a
  changed parcel touches, widened by the bake's patch buffer plus one pixel (`land_use_zone_area_by_tile`
  `MEMBERSHIP_BUFFER_PIXELS`, the Rust `BUFFER_PIXELS` + 1), so it covers every minzoom ancestor of
  every tile the patch dirties. The handoff is every served row whose box touches one of those tiles:
  Silver rows by their `bbox_*` columns (the edited PNUs excepted) and the edited PNUs by their edit's
  box. A superset is safe: the bake keeps only the rows its neighbourhood asks for.
- **Change list.** Every changed PNU, `upsert` when it is served after the fold and `delete` when not.

A fold larger than `MAX_FOLD_TILES` neighbourhood tiles is not partial: the caller runs the full
served Gold, the declared exception (root ADR-0314), and the bake patches or rebases from every row.
"""

from __future__ import annotations

import math
import struct
from typing import Any

import land_use_zone_area_by_tile as by_tile

# The parcel layer's minzoom (`r2-connections.contract.json` vector tile layer `parcels`).
NEIGHBOURHOOD_ZOOM = 14
# Past this many z14 tiles (about 2.4 km each) a fold hands every row: the predicate stays one scan.
MAX_FOLD_TILES = 512
WKB_POLYGON = 3
WKB_MULTIPOLYGON = 6
EWKB_SRID_FLAG = 0x2000_0000
# `silver.parcel_boundaries` box columns, longitude/latitude (EPSG:4326, the parcels' one CRS).
SILVER_BOX: tuple[str, ...] = ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y")


def wkb_bbox(wkb: bytes) -> tuple[float, float, float, float]:
    """The longitude/latitude box of a 2D Polygon or MultiPolygon WKB (ISO or EWKB with an SRID)."""

    at = 0

    def take(fmt: str, order: str) -> tuple[Any, ...]:
        nonlocal at
        size = struct.calcsize(order + fmt)
        if at + size > len(wkb):
            raise ValueError("the WKB ends inside a value")
        values = struct.unpack_from(order + fmt, wkb, at)
        at += size
        return values

    boxes: list[tuple[float, float]] = []

    def geometry(expected: tuple[int, ...]) -> None:
        (flag,) = take("B", "<")
        order = "<" if flag == 1 else ">" if flag == 0 else None
        if order is None:
            raise ValueError("the WKB has no byte order")
        (kind,) = take("I", order)
        if kind & EWKB_SRID_FLAG:
            take("I", order)
            kind &= ~EWKB_SRID_FLAG
        if kind not in expected:
            raise ValueError(f"the WKB is geometry type {kind}, not a 2D polygon")
        if kind == WKB_MULTIPOLYGON:
            (parts,) = take("I", order)
            for _ in range(parts):
                geometry((WKB_POLYGON,))
            return
        (rings,) = take("I", order)
        for _ in range(rings):
            (points,) = take("I", order)
            for _ in range(points):
                boxes.append(take("dd", order))

    geometry((WKB_POLYGON, WKB_MULTIPOLYGON))
    if at != len(wkb):
        raise ValueError("the WKB has bytes after its geometry")
    if not boxes:
        raise ValueError("the WKB polygon has no point")
    xs, ys = [x for x, _ in boxes], [y for _, y in boxes]
    box = (min(xs), min(ys), max(xs), max(ys))
    if not all(math.isfinite(value) for value in box):
        raise ValueError("the WKB has a coordinate that is not a finite number")
    return box


def edit_states(
    ledger: list[dict[str, Any]], through: int
) -> tuple[dict[str, dict[str, Any] | None], dict[str, dict[str, Any] | None]]:
    """For every PNU an edit after `through` names: its last edit up to `through` (absent when none:
    its Silver row stands), and its last edit of the whole ledger (None for a delete)."""

    ordered = sorted(ledger, key=lambda edit: int(edit["change_seq"]))
    changed = {str(edit["feature_id"]) for edit in ordered if int(edit["change_seq"]) > through}
    before: dict[str, dict[str, Any] | None] = {}
    after: dict[str, dict[str, Any] | None] = {}
    for edit in ordered:
        pnu = str(edit["feature_id"])
        if pnu not in changed:
            continue
        state = edit if edit["op"] == "upsert" else None
        if int(edit["change_seq"]) <= through:
            before[pnu] = state
        after[pnu] = state
    return before, after


def edit_box(edit: dict[str, Any] | None) -> tuple[float, float, float, float] | None:
    return None if edit is None else wkb_bbox(bytes.fromhex(edit["geometry_wkb_hex"]))


def neighbourhood_tiles(boxes: list[tuple[float, float, float, float]]) -> list[tuple[int, int]]:
    """Every z14 tile a box touches, widened by the bake's buffer plus one pixel, in order."""

    tiles: set[tuple[int, int]] = set()
    for box in boxes:
        span = by_tile.tile_range(box, NEIGHBOURHOOD_ZOOM, by_tile.MEMBERSHIP_BUFFER_PIXELS)
        if span is None:
            continue
        x0, y0, x1, y1 = span
        tiles.update((x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1))
    return sorted(tiles)


def tile_envelope(x: int, y: int, zoom: int, pixels: float) -> tuple[float, float, float, float]:
    """The longitude/latitude rectangle of tile `(x, y)` widened by `pixels` on every side: a box
    touches the tile with that buffer exactly when it meets this rectangle (`tile_range`)."""

    scale = float(1 << zoom)
    margin = pixels / 256.0

    def lon(tile_x: float) -> float:
        return tile_x / scale * 360.0 - 180.0

    def lat(tile_y: float) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * tile_y / scale))))

    return (lon(x - margin), lat(y + 1 + margin), lon(x + 1 + margin), lat(y - margin))


def touches_any(box: tuple[float, float, float, float], tiles: list[tuple[int, int]]) -> bool:
    """Whether `box` meets one of the tiles' widened rectangles: what the Silver predicate asks."""

    for x, y in tiles:
        left, bottom, right, top = tile_envelope(x, y, NEIGHBOURHOOD_ZOOM, by_tile.MEMBERSHIP_BUFFER_PIXELS)
        if box[2] >= left and box[0] <= right and box[3] >= bottom and box[1] <= top:
            return True
    return False


def silver_predicate(tiles: list[tuple[int, int]]) -> str:
    """The Spark SQL filter of `touches_any` over Silver's `bbox_*` columns."""

    if not tiles:
        return "false"
    terms = []
    for x, y in tiles:
        left, bottom, right, top = tile_envelope(x, y, NEIGHBOURHOOD_ZOOM, by_tile.MEMBERSHIP_BUFFER_PIXELS)
        terms.append(
            f"(bbox_max_x >= {left!r} AND bbox_min_x <= {right!r} "
            f"AND bbox_max_y >= {bottom!r} AND bbox_min_y <= {top!r})"
        )
    return "(" + " OR ".join(terms) + ")"


def change_list(after: dict[str, dict[str, Any] | None]) -> list[dict[str, str]]:
    return [
        {"feature_id": pnu, "op": "upsert" if state is not None else "delete"}
        for pnu, state in sorted(after.items())
    ]


def plan(
    ledger: list[dict[str, Any]], through: int, silver_boxes: dict[str, tuple[float, float, float, float]]
) -> dict[str, Any]:
    """The fold of the edits after `through`: the changed PNUs' states, the neighbourhood tiles and
    the edited PNUs (of the whole ledger) the neighbourhood holds, or the reason it is not partial.

    `silver_boxes` is the Silver box of every changed PNU the snapshot holds.
    """

    before, after = edit_states(ledger, through)
    boxes = []
    for pnu, state in after.items():
        old = edit_box(before[pnu]) if pnu in before else silver_boxes.get(pnu)
        new = edit_box(state)
        boxes.extend(box for box in (old, new) if box is not None)
    tiles = neighbourhood_tiles(boxes)
    if len(tiles) > MAX_FOLD_TILES:
        return {"full_reason": f"the fold touches {len(tiles)} z{NEIGHBOURHOOD_ZOOM} tiles, past {MAX_FOLD_TILES}"}
    # Every PNU the ledger ever edited is served from its edit, not its Silver row: those whose
    # served box meets the neighbourhood are handed from the edit's box.
    served_edits: dict[str, dict[str, Any] | None] = {}
    for edit in sorted(ledger, key=lambda edit: int(edit["change_seq"])):
        served_edits[str(edit["feature_id"])] = edit if edit["op"] == "upsert" else None
    edited_in_neighbourhood = sorted(
        pnu for pnu, state in served_edits.items() if state is not None and touches_any(edit_box(state), tiles)
    )
    return {
        "after": after,
        "tiles": tiles,
        "ledger_pnus": sorted(served_edits),
        "edited_in_neighbourhood": edited_in_neighbourhood,
        "changes": change_list(after),
    }
