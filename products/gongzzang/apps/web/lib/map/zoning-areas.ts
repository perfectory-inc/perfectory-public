import { sortBy, uniqBy } from "es-toolkit";
import { type ZoningFamilyKey, zoningFamilyOf } from "@/lib/map/zoning-families";

/** One regulation zone the map draws over a parcel, in the provider's words. */
export type ZoningArea = {
  zoneId: string;
  layerCode: string;
  layerName: string;
  zoneName: string;
  family: ZoningFamilyKey | undefined;
};

/** The part of mapbox-gl this module reads: features of tiles already loaded. */
export type ZoningSourceReader = {
  getSource?: (id: string) => unknown;
  querySourceFeatures?: (
    sourceId: string,
    options?: { sourceLayer?: string; filter?: unknown[] },
  ) => readonly unknown[];
};

type Position = readonly number[];
type Ring = readonly Position[];
/** A polygon as rings in metres around the parcel: outer first, holes after. */
type Polygon = readonly Ring[];
type SourceFeature = {
  id?: string | number;
  properties?: Record<string, unknown> | null;
  geometry?: { type?: string; coordinates?: unknown } | null;
};

/**
 * Overlap under this many metres is the two sources disagreeing about a shared line, not a zone
 * the parcel is in. Zone boundaries often follow parcel boundaries, and the tiles round both to
 * about 0.6 m at z14 (2.4 km / 4096), so an exact test would list every zone next door.
 */
const BOUNDARY_TOLERANCE_M = 1;

/**
 * The zones whose polygons overlap the parcel's, read from tiles the map already holds.
 *
 * Returns `null` when the map carries no `zoning` source (the unit is not promoted, or the map is
 * not up), so a caller can tell "not served" from "no zone here". Pieces of one zone cut by tile
 * edges come back once (by `zone_id`), sorted by layer then zone name.
 *
 * shortcut: reads only tiles already loaded, so a parcel opened from a link before the map has
 * drawn its spot lists nothing until the map loads there (callers re-read on `idle`); upgrade to a
 * by-PNU Foundation read if the panel must answer without the map.
 */
export function findParcelZoningAreas(mb: ZoningSourceReader, pnu: string): ZoningArea[] | null {
  if (!mb.getSource?.("zoning") || typeof mb.querySourceFeatures !== "function") return null;
  const parcelPieces = mb.querySourceFeatures("parcels", {
    sourceLayer: "parcels",
    filter: ["==", ["get", "pnu"], pnu],
  }) as readonly SourceFeature[];
  const parcelLonLat = parcelPieces.flatMap((f) => polygonsOf(f.geometry));
  if (parcelLonLat.length === 0) return [];
  const origin = parcelLonLat[0]?.[0]?.[0];
  if (!origin) return [];
  const project = metresAround(origin);
  const parcel = parcelLonLat.map((polygon) => polygon.map((ring) => ring.map(project)));
  const parcelBox = boundsOf(parcel);

  const zones = (
    mb.querySourceFeatures("zoning", { sourceLayer: "zoning" }) as readonly SourceFeature[]
  ).filter((feature) => {
    const zone = polygonsOf(feature.geometry).map((polygon) =>
      polygon.map((ring) => ring.map(project)),
    );
    return (
      zone.length > 0 &&
      boxesOverlap(parcelBox, boundsOf(zone)) &&
      parcel.some((a) => zone.some((b) => interiorsOverlap(a, b)))
    );
  });
  const areas = zones.map(toZoningArea).filter((area): area is ZoningArea => area !== null);
  return sortBy(
    uniqBy(areas, (area) => area.zoneId),
    [(area) => area.layerCode, (area) => area.zoneName],
  );
}

function toZoningArea(feature: SourceFeature): ZoningArea | null {
  const p = feature.properties ?? {};
  const text = (key: string) => (typeof p[key] === "string" ? (p[key] as string) : "");
  const zoneId = text("zone_id") || (typeof feature.id === "string" ? feature.id : "");
  const layerCode = text("layer_code");
  if (!zoneId || !layerCode) return null;
  return {
    zoneId,
    layerCode,
    layerName: text("layer_name"),
    zoneName: text("zone_name"),
    family: zoningFamilyOf(layerCode),
  };
}

function polygonsOf(geometry: SourceFeature["geometry"]): Polygon[] {
  if (geometry?.type === "Polygon") return [geometry.coordinates as Polygon];
  if (geometry?.type === "MultiPolygon") return geometry.coordinates as Polygon[];
  return [];
}

/** Equirectangular metres around `origin`: exact enough across one parcel and its neighbours. */
function metresAround(origin: Position): (p: Position) => Position {
  const [lon0 = 0, lat0 = 0] = origin;
  const kx = 111_320 * Math.cos((lat0 * Math.PI) / 180);
  const ky = 110_540;
  return ([lon = 0, lat = 0]) => [(lon - lon0) * kx, (lat - lat0) * ky];
}

type Box = [number, number, number, number];

function boundsOf(polygons: readonly Polygon[]): Box {
  const box: Box = [Infinity, Infinity, -Infinity, -Infinity];
  for (const polygon of polygons)
    for (const [x = 0, y = 0] of polygon[0] ?? []) {
      box[0] = Math.min(box[0], x);
      box[1] = Math.min(box[1], y);
      box[2] = Math.max(box[2], x);
      box[3] = Math.max(box[3], y);
    }
  return box;
}

function boxesOverlap(a: Box, b: Box): boolean {
  return a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3];
}

/**
 * Whether the two polygons share area wider than {@link BOUNDARY_TOLERANCE_M}.
 *
 * If the interiors overlap, then either one's boundary runs through the other's interior, or they
 * are the same shape. So: split every edge of each where the other's edges cross it, and ask
 * whether the middle of any piece lies inside the other well clear of its boundary; for the
 * same-shape case, also ask that of each polygon's own interior point.
 */
export function interiorsOverlap(a: Polygon, b: Polygon): boolean {
  return (
    boundaryEntersInterior(a, b) ||
    boundaryEntersInterior(b, a) ||
    deepInside(interiorPoint(a), b) ||
    deepInside(interiorPoint(b), a)
  );
}

function boundaryEntersInterior(a: Polygon, b: Polygon): boolean {
  const bEdges = edgesOf(b);
  for (const [p, q] of edgesOf(a)) {
    const cuts = [0, 1];
    for (const [r, s] of bEdges) {
      const t = crossingParameter(p, q, r, s);
      if (t !== null) cuts.push(t);
    }
    cuts.sort((x, y) => x - y);
    for (let i = 1; i < cuts.length; i++) {
      const t = ((cuts[i - 1] ?? 0) + (cuts[i] ?? 0)) / 2;
      if (deepInside(lerp(p, q, t), b)) return true;
    }
  }
  return false;
}

function deepInside(point: Position | null, polygon: Polygon): boolean {
  return (
    point !== null &&
    contains(polygon, point) &&
    distanceToBoundary(polygon, point) > BOUNDARY_TOLERANCE_M
  );
}

/** Even-odd over every ring, so a hole is outside. */
function contains(polygon: Polygon, [x = 0, y = 0]: Position): boolean {
  let inside = false;
  for (const ring of polygon) {
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [xi = 0, yi = 0] = ring[i] ?? [];
      const [xj = 0, yj = 0] = ring[j] ?? [];
      if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
    }
  }
  return inside;
}

function distanceToBoundary(polygon: Polygon, point: Position): number {
  let best = Infinity;
  for (const [p, q] of edgesOf(polygon)) best = Math.min(best, segmentDistance(point, p, q));
  return best;
}

/** A point inside the polygon: the middle of the widest span of the scanline through its centre. */
function interiorPoint(polygon: Polygon): Position | null {
  const [, minY, , maxY] = boundsOf([polygon]);
  const y = (minY + maxY) / 2;
  const xs: number[] = [];
  for (const [[px = 0, py = 0], [qx = 0, qy = 0]] of edgesOf(polygon)) {
    if (py > y !== qy > y) xs.push(px + ((y - py) * (qx - px)) / (qy - py));
  }
  xs.sort((m, n) => m - n);
  let best: Position | null = null;
  let width = 0;
  for (let i = 0; i + 1 < xs.length; i += 2) {
    const [l = 0, r = 0] = [xs[i], xs[i + 1]];
    if (r - l > width) {
      width = r - l;
      best = [(l + r) / 2, y];
    }
  }
  return best;
}

function edgesOf(polygon: Polygon): [Position, Position][] {
  return polygon.flatMap((ring) =>
    ring.slice(1).map((q, i) => [ring[i] ?? q, q] as [Position, Position]),
  );
}

/** Where segment r–s crosses segment p–q, as a fraction along p–q; null when it does not. */
function crossingParameter(p: Position, q: Position, r: Position, s: Position): number | null {
  const [px = 0, py = 0] = p;
  const [qx = 0, qy = 0] = q;
  const [rx = 0, ry = 0] = r;
  const [sx = 0, sy = 0] = s;
  const dx = qx - px;
  const dy = qy - py;
  const ex = sx - rx;
  const ey = sy - ry;
  const denom = dx * ey - dy * ex;
  if (denom === 0) return null;
  const t = ((rx - px) * ey - (ry - py) * ex) / denom;
  const u = ((rx - px) * dy - (ry - py) * dx) / denom;
  return t > 0 && t < 1 && u >= 0 && u <= 1 ? t : null;
}

function lerp([px = 0, py = 0]: Position, [qx = 0, qy = 0]: Position, t: number): Position {
  return [px + (qx - px) * t, py + (qy - py) * t];
}

function segmentDistance(
  [x = 0, y = 0]: Position,
  [px = 0, py = 0]: Position,
  [qx = 0, qy = 0]: Position,
): number {
  const dx = qx - px;
  const dy = qy - py;
  const len2 = dx * dx + dy * dy;
  const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, ((x - px) * dx + (y - py) * dy) / len2));
  return Math.hypot(x - (px + t * dx), y - (py + t * dy));
}
