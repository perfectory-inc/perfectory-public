import { z } from "zod";

import {
  FOUNDATION_VECTOR_FILL_STYLES,
  foundationVectorFillLayerId,
  foundationVectorOutlineLayerId,
} from "@/lib/map/foundation-vector-fill-layers";

/**
 * Admin polygon edits customers see before the next bake folds them into the tiles (root ADR-0112).
 *
 * The base tiles stay what they are. For each edited feature the map hides the base polygon with a
 * layer filter (a feature tombstone) and draws the fresh polygon from the edit store's overlay in
 * the same style, directly above the base layer. The overlay only ever holds edits the tiles do not
 * contain yet, so after a bake it empties by itself.
 */

const FeatureIdSchema = z.string().min(1).max(128);
const GeometrySchema = z
  .object({
    type: z.enum(["Polygon", "MultiPolygon"]),
    coordinates: z.array(z.unknown()).min(1),
  })
  .strict();

const MapEditOverlaySchema = z
  .object({
    unit: z.string().min(1),
    feature_id_property: z.string().min(1),
    folded_through_change_seq: z.number().int().nonnegative(),
    latest_change_seq: z.number().int().nonnegative(),
    hidden_feature_ids: z.array(FeatureIdSchema),
    features: z
      .object({
        type: z.literal("FeatureCollection"),
        features: z.array(
          z
            .object({
              type: z.literal("Feature"),
              properties: z.record(z.string(), z.string()),
              geometry: GeometrySchema,
            })
            .strict(),
        ),
      })
      .strict(),
  })
  .strict();

export type MapEditOverlay = z.infer<typeof MapEditOverlaySchema>;

/** Parses one overlay response and checks it belongs to `unit` and hides every feature it draws. */
export function parseMapEditOverlay(value: unknown, unit: string): MapEditOverlay {
  const overlay = MapEditOverlaySchema.parse(value);
  if (overlay.unit !== unit) {
    throw new Error(`map edit overlay is for ${overlay.unit}, expected ${unit}`);
  }
  const hidden = new Set(overlay.hidden_feature_ids);
  for (const feature of overlay.features.features) {
    const id = feature.properties[overlay.feature_id_property];
    // A drawn feature whose base polygon is not hidden would show twice.
    if (id === undefined || !hidden.has(id)) {
      throw new Error(`map edit overlay draws ${String(id)} without hiding it`);
    }
  }
  return overlay;
}

export function mapEditOverlayUrl(baseUrl: string, unit: string): string {
  return `${baseUrl.replace(/\/+$/, "")}/overlay/${encodeURIComponent(unit)}`;
}

/**
 * Fetches the overlay. A 304 answers the previous overlay unchanged.
 *
 * @throws on any other non-2xx answer or a body that does not match the contract, so the caller
 * keeps what it last drew instead of drawing something wrong.
 */
export async function fetchMapEditOverlay(
  fetcher: typeof fetch,
  baseUrl: string,
  unit: string,
  options: { signal?: AbortSignal; previous?: MapEditOverlay; etag?: string } = {},
): Promise<{ manifest: MapEditOverlay; etag?: string }> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (options.etag && options.previous) headers["If-None-Match"] = options.etag;
  const init: RequestInit = { headers, cache: "no-store" };
  if (options.signal) init.signal = options.signal;
  const response = await fetcher(mapEditOverlayUrl(baseUrl, unit), init);
  const etag = response.headers.get("ETag") ?? undefined;
  if (response.status === 304 && options.previous) {
    return { manifest: options.previous, ...(options.etag ? { etag: options.etag } : {}) };
  }
  if (!response.ok) throw new Error(`map edit overlay answered ${response.status}`);
  return { manifest: parseMapEditOverlay(await response.json(), unit), ...(etag ? { etag } : {}) };
}

/** The mapbox-gl surface the overlay needs. */
export type MapEditOverlayBridge = {
  addSource?: (id: string, source: Record<string, unknown>) => void;
  addLayer?: (layer: Record<string, unknown>, beforeId?: string) => void;
  getSource?: (id: string) => unknown;
  getLayer?: (id: string) => unknown;
  setFilter?: (layerId: string, filter: unknown[] | null) => void;
};

type GeoJsonSourceLike = { setData?: (data: unknown) => void };

export function mapEditOverlaySourceId(unit: string): string {
  return `${unit}-edits`;
}

export function mapEditOverlayFillLayerId(unit: string): string {
  return foundationVectorFillLayerId(mapEditOverlaySourceId(unit));
}

export function mapEditOverlayOutlineLayerId(unit: string): string {
  return foundationVectorOutlineLayerId(mapEditOverlaySourceId(unit));
}

const EMPTY_COLLECTION = { type: "FeatureCollection", features: [] } as const;

/**
 * Adds the unit's (empty) edit layers directly above its base layers, in the base style.
 *
 * Called once after the base layers exist, so a click handler can be bound to the edit layer before
 * the first overlay arrives. Returns the edit layer ids, or none when the base unit is absent.
 */
export function registerMapEditOverlayLayers(mb: MapEditOverlayBridge, unit: string): string[] {
  const style = FOUNDATION_VECTOR_FILL_STYLES[unit];
  const baseFill = foundationVectorFillLayerId(unit);
  if (!style || !mb.getLayer?.(baseFill) || !mb.addSource || !mb.addLayer) return [];
  const sourceId = mapEditOverlaySourceId(unit);
  if (mb.getSource?.(sourceId)) return [];
  mb.addSource(sourceId, { type: "geojson", data: EMPTY_COLLECTION });
  const beforeId = style.insertBelowLayerIds?.find((layerId) => mb.getLayer?.(layerId));
  const fillLayerId = mapEditOverlayFillLayerId(unit);
  mb.addLayer(
    {
      id: fillLayerId,
      type: "fill",
      source: sourceId,
      paint: {
        "fill-color": style.fillColor,
        "fill-opacity": style.fillOpacity,
        "fill-outline-color": style.outlineColor,
      },
    },
    beforeId,
  );
  const layerIds = [fillLayerId];
  if (style.outlineWidth !== undefined) {
    const outlineLayerId = mapEditOverlayOutlineLayerId(unit);
    mb.addLayer(
      {
        id: outlineLayerId,
        type: "line",
        source: sourceId,
        paint: {
          "line-color": style.outlineColor,
          "line-width": style.outlineWidth,
          "line-opacity": 0.9,
        },
      },
      beforeId,
    );
    layerIds.push(outlineLayerId);
  }
  return layerIds;
}

/** The filter that hides `hiddenIds` from a base layer, or `null` to show everything. */
export function mapEditTombstoneFilter(
  featureIdProperty: string,
  hiddenIds: readonly string[],
): unknown[] | null {
  if (hiddenIds.length === 0) return null;
  return ["!", ["in", ["get", featureIdProperty], ["literal", [...hiddenIds]]]];
}

/** Hides the edited base polygons and draws the fresh ones. */
export function applyMapEditOverlay(mb: MapEditOverlayBridge, overlay: MapEditOverlay): void {
  const filter = mapEditTombstoneFilter(overlay.feature_id_property, overlay.hidden_feature_ids);
  for (const layerId of [
    foundationVectorFillLayerId(overlay.unit),
    foundationVectorOutlineLayerId(overlay.unit),
  ]) {
    if (mb.getLayer?.(layerId)) mb.setFilter?.(layerId, filter);
  }
  const source = mb.getSource?.(mapEditOverlaySourceId(overlay.unit)) as
    | GeoJsonSourceLike
    | undefined;
  source?.setData?.(overlay.features);
}
