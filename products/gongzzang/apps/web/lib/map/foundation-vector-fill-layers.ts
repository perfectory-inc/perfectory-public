import { MAP_LAYER_COLORS } from "@gongzzang/ui/tokens.js";
import {
  buildFoundationVectorSource,
  FOUNDATION_VECTOR_LAYER_REGISTRY,
} from "@/lib/map/foundation-vector-layer-registry";
import {
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
} from "@/lib/map/marker-tile-style";
import type { VectorTileRuntimeManifest, VectorTileSource } from "@/lib/map/vector-tile-manifest";
import { zoningColorExpression, zoningLayerFilter } from "@/lib/map/zoning-families";

/**
 * How one Foundation polygon unit is painted, and where it sits in the stack.
 *
 * The registry next door owns the unit's *identity* — what the server must call it — and this owns
 * what the client draws for it. Every Foundation polygon unit reaches the map through
 * {@link registerFoundationVectorFillLayers}, so the opacity a unit is drawn at has one answer here
 * rather than one per call site: `parcels`, `admin` and `complex` each arrive over both a v2 runtime
 * manifest and a v1 static manifest, which is six registrations and was six chances to disagree.
 */
export type FoundationVectorFillStyle = {
  /** A colour, or a Mapbox expression for a unit whose features differ in kind (`zoning`). */
  fillColor: string | unknown[];
  fillOpacity: number;
  outlineColor: string | unknown[];
  /**
   * The filter both layers start with. `zoning` starts matching nothing and its toggles widen it:
   * a filter, not `visibility: none`, so the source's tiles still load and the parcel panel can
   * read the zones under a parcel while every toggle is off.
   */
  filter?: unknown[];
  /**
   * Draws a separate `line` layer at this width on top of the fill.
   *
   * `fill-outline-color` is fixed at one screen pixel and reads as a smudge against a satellite or
   * dense street basemap. A unit whose boundary is the *subject* — a designation line a user is
   * trying to see the edge of — needs a width; a unit that is merely tinted background does not.
   */
  outlineWidth?: number;
  /**
   * Layer ids this unit must stay underneath, listed bottom-first.
   *
   * The first one that already exists on the map is what the unit is inserted before. Registration
   * order alone already puts polygons under markers today, and that is exactly the kind of
   * invariant that holds until someone reorders two calls — so the units that must not cover
   * anything say so here instead.
   */
  insertBelowLayerIds?: readonly string[];
};

const FILL_LAYER_SUFFIX = "-fill";
const OUTLINE_LAYER_SUFFIX = "-outline";

/**
 * How many tile zooms one source of a unit serves (root ADR-0489).
 *
 * Mapbox GL draws a missing tile from a loaded ancestor up to `SourceCache.maxOverzooming` (10)
 * levels above it, so one z6..16 source showed a z8 complex tile at z15 while the z15 tiles loaded:
 * edges 36 m (19 px) off (W41, 남동산단, 2026-11-09). An ancestor is never taken below the source's
 * `minzoom`, so a unit is served as one source per band of three zooms and a placeholder is at most
 * two levels coarser than the tile it stands in for.
 */
export const FOUNDATION_VECTOR_ZOOM_BAND_LEVELS = 3;

/** Every tile zoom a band can start at: a lower band is named by its first tile zoom. */
const ZOOM_BAND_CELLS = Array.from({ length: 25 }, (_, zoom) => zoom);

/**
 * One source of a unit: the tile zooms it requests and the zooms its layers are drawn at.
 *
 * `cell` is `null` for the top band, which keeps the unit's own source and layer ids, so every
 * reader that names the unit's layer by id still finds the detailed one; a lower band is named by
 * its first tile zoom.
 */
export type FoundationVectorZoomBand = {
  cell: number | null;
  tileMinZoom: number;
  tileMaxZoom: number;
  renderMinZoom: number;
  renderMaxZoom: number;
};

/**
 * The bands a layer's tile window splits into, counted down from `tile_max_zoom`: the detailed
 * top band is always a full three zooms, and only the coarsest band may be shorter.
 */
export function foundationVectorZoomBands(layer: {
  tile_min_zoom: number;
  tile_max_zoom: number;
  render_min_zoom: number;
  render_max_zoom: number;
}): FoundationVectorZoomBand[] {
  const bands: FoundationVectorZoomBand[] = [];
  for (let end = layer.tile_max_zoom; end >= layer.tile_min_zoom; ) {
    const start = Math.max(end - FOUNDATION_VECTOR_ZOOM_BAND_LEVELS + 1, layer.tile_min_zoom);
    const top = end === layer.tile_max_zoom;
    const renderMinZoom = Math.max(start, layer.render_min_zoom);
    const renderMaxZoom = top ? layer.render_max_zoom : end + 1;
    if (renderMinZoom < renderMaxZoom) {
      bands.unshift({
        cell: top ? null : start,
        tileMinZoom: start,
        tileMaxZoom: end,
        renderMinZoom,
        renderMaxZoom,
      });
    }
    end = start - 1;
  }
  return bands;
}

function banded(id: string, cell: number | null | undefined): string {
  return cell === null || cell === undefined ? id : `${id}@z${cell}`;
}

/** The Mapbox layer id this repo draws `unitName`'s polygons as, in band `cell` when given. */
export function foundationVectorFillLayerId(unitName: string, cell?: number | null): string {
  return `${banded(unitName, cell)}${FILL_LAYER_SUFFIX}`;
}

/** The Mapbox layer id for a unit's crisp boundary line, when its style asks for one. */
export function foundationVectorOutlineLayerId(unitName: string, cell?: number | null): string {
  return `${banded(unitName, cell)}${OUTLINE_LAYER_SUFFIX}`;
}

/** The Mapbox source id of `sourceId`'s band `cell`. */
export function foundationVectorBandSourceId(sourceId: string, cell: number | null): string {
  return banded(sourceId, cell);
}

/** Every band source id a unit's source could have, the unit's own first. */
export function foundationVectorBandSourceIds(sourceId: string): string[] {
  return [sourceId, ...ZOOM_BAND_CELLS.map((cell) => banded(sourceId, cell))];
}

/**
 * The fill (or outline) layers of every band of `unitName` on the map, the top band first.
 *
 * A reader that acts on a unit's polygons (a click, a filter, a tombstone) acts on all of them:
 * the same feature is drawn by a different band's layer at a different zoom.
 */
export function foundationVectorUnitLayerIds(
  mb: { getLayer?: (id: string) => unknown },
  unitName: string,
  kind: "fill" | "outline" | "both" = "both",
): string[] {
  const ids: string[] = [];
  for (const cell of [null, ...ZOOM_BAND_CELLS]) {
    if (kind !== "outline") ids.push(foundationVectorFillLayerId(unitName, cell));
    if (kind !== "fill") ids.push(foundationVectorOutlineLayerId(unitName, cell));
  }
  return ids.filter((id) => mb.getLayer?.(id));
}

/**
 * Layers whose feature owns a click that also lands on the `complex` boundary.
 *
 * A designation boundary is kilometres across and covers everything inside it, so without this a
 * click on a parcel inside a complex opens both panels. `complex` is the context, not the subject
 * (see the `insertBelowLayerIds` note below): if anything more specific is drawn at the point, that
 * is what the user aimed at.
 *
 * Deliberately **not** `insertBelowLayerIds`, even though the members overlap. That list answers
 * "what must this be drawn under", and it includes `admin-fill` — a nationwide background tint that
 * is under the cursor everywhere and answers nothing when clicked. Yielding to it would mean the
 * complex boundary is never clickable at all.
 */
export const COMPLEX_CLICK_YIELDS_TO_LAYER_IDS: readonly string[] = [
  PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
];

/** The units whose polygons, in every zoom band, own a click that also lands on `complex`. */
export const COMPLEX_CLICK_YIELDS_TO_UNITS: readonly string[] = ["parcels"];

export const FOUNDATION_VECTOR_FILL_STYLES: Record<string, FoundationVectorFillStyle> = {
  parcels: {
    fillColor: MAP_LAYER_COLORS.parcel.fill,
    fillOpacity: 0.1,
    outlineColor: MAP_LAYER_COLORS.parcel.outline,
  },
  admin: {
    fillColor: MAP_LAYER_COLORS.admin.fill,
    fillOpacity: 0.05,
    outlineColor: MAP_LAYER_COLORS.admin.outline,
  },
  // An industrial complex is context for a listing, not the thing a user came to click, so the fill
  // is lighter than `parcels` (0.1) — a marker sitting on a complex must not read as a different
  // colour from the same marker sitting off one — while the boundary itself is the informative part
  // and gets a real line. Both colours are the existing `complex` palette tokens; the layer
  // introduces no new one.
  complex: {
    fillColor: MAP_LAYER_COLORS.complex.fill,
    fillOpacity: 0.08,
    outlineColor: MAP_LAYER_COLORS.complex.outline,
    outlineWidth: 1.5,
    insertBelowLayerIds: [
      foundationVectorFillLayerId("parcels"),
      foundationVectorFillLayerId("admin"),
      PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
      LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
      LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
    ],
  },
  // Regulation zones sit above the base fills (`complex`, registered before them) and under the
  // parcels: the parcel a user clicks stays the top polygon, and a zone tints it from below.
  zoning: {
    fillColor: zoningColorExpression(),
    fillOpacity: 0.2,
    outlineColor: zoningColorExpression(),
    outlineWidth: 1.2,
    filter: zoningLayerFilter(new Set()),
    insertBelowLayerIds: [
      foundationVectorFillLayerId("parcels"),
      foundationVectorFillLayerId("admin"),
      PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
      LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
      LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
    ],
  },
};

/** The mapbox-gl surface this module needs, so callers can pass their own bridge type. */
export type FoundationVectorLayerBridge = {
  addSource: (id: string, source: Record<string, unknown>) => void;
  addLayer: (layer: Record<string, unknown>, beforeId?: string) => void;
  getSource?: (id: string) => unknown;
  getLayer?: (id: string) => unknown;
};

/** One already-resolved vector source plus the render window the manifest declared for it. */
export type FoundationVectorFillRegistration = {
  sourceId: string;
  source: VectorTileSource;
  sourceLayer: string;
  minzoom: number;
  maxzoom: number;
  promoteId?: string;
  /** The zoom band this registration draws; absent is the unit's own (top) layers. */
  cell?: number | null;
};

/**
 * Adds `unitName`'s source and polygon layers, and returns the layer ids it added.
 *
 * Returns an empty array when the source is already registered, so a caller can tell a fresh
 * registration from a repeat one without asking the map again.
 *
 * @throws when the unit has no style, which is what a unit added to the registry and nowhere else
 * looks like.
 */
export function registerFoundationVectorFillLayers(
  mb: FoundationVectorLayerBridge,
  unitName: string,
  registration: FoundationVectorFillRegistration,
): string[] {
  const style = FOUNDATION_VECTOR_FILL_STYLES[unitName];
  if (!style) throw new Error(`Foundation vector fill style is missing: ${unitName}`);
  if (mb.getSource?.(registration.sourceId)) return [];

  mb.addSource(registration.sourceId, registration.source);
  const beforeId = resolveInsertBeforeId(mb, style.insertBelowLayerIds);
  const shared = {
    source: registration.sourceId,
    "source-layer": registration.sourceLayer,
    minzoom: registration.minzoom,
    maxzoom: registration.maxzoom,
    ...(style.filter ? { filter: style.filter } : {}),
  };

  const fillLayerId = foundationVectorFillLayerId(unitName, registration.cell);
  const fillLayer: Record<string, unknown> = {
    ...shared,
    id: fillLayerId,
    type: "fill",
    paint: {
      "fill-color": style.fillColor,
      "fill-opacity": style.fillOpacity,
      "fill-outline-color": style.outlineColor,
    },
  };
  if (registration.promoteId) fillLayer.promoteId = registration.promoteId;
  mb.addLayer(fillLayer, beforeId);
  const layerIds = [fillLayerId];

  if (style.outlineWidth !== undefined) {
    const outlineLayerId = foundationVectorOutlineLayerId(unitName, registration.cell);
    mb.addLayer(
      {
        ...shared,
        id: outlineLayerId,
        type: "line",
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

/**
 * Registers one v2 runtime unit, reading its source identity from the registry and its render
 * window from the manifest, as one source and layer pair per zoom band
 * ({@link foundationVectorZoomBands}). The top band goes first and keeps the unit's ids; the lower
 * bands are inserted above it, so the unit's own fill stays the lowest of its layers and a unit
 * inserted below it lands below every band.
 *
 * Returns an empty array when the manifest carries no such unit, which is the normal state of an
 * environment where the unit has not been promoted yet.
 *
 * @throws when the unit is unregistered, when the manifest's unit is missing the layer the registry
 * names, or when {@link buildFoundationVectorSource} finds the manifest disagreeing with the
 * registry.
 */
export function registerFoundationVectorRuntimeUnit(
  mb: FoundationVectorLayerBridge,
  manifest: VectorTileRuntimeManifest,
  unitName: string,
): string[] {
  const definition = FOUNDATION_VECTOR_LAYER_REGISTRY[unitName];
  if (!definition) throw new Error(`unregistered Foundation vector layer: ${unitName}`);
  const unit = manifest.publication_units[unitName];
  if (!unit) return [];
  const layer = unit.layers[definition.layerName];
  if (!layer) {
    throw new Error(`v2 ${unitName} unit is missing the ${definition.layerName} layer`);
  }
  const source = buildFoundationVectorSource(manifest, unitName, definition.layerName);
  const bands = foundationVectorZoomBands(layer);
  const top = bands.at(-1);
  const ordered = top ? [top, ...bands.slice(0, -1)] : [];
  return ordered.flatMap((band) =>
    registerFoundationVectorFillLayers(mb, unitName, {
      sourceId: foundationVectorBandSourceId(definition.sourceId, band.cell),
      // Each band requests only its own tile zooms, so no tile is fetched twice; the top band ends
      // at `tile_max_zoom`, the range Martin cuts, and its layers draw that tile overzoomed.
      source: { ...source, minzoom: band.tileMinZoom, maxzoom: band.tileMaxZoom },
      sourceLayer: layer.source_layer,
      minzoom: band.renderMinZoom,
      maxzoom: band.renderMaxZoom,
      promoteId: layer.feature_id_property,
      cell: band.cell,
    }),
  );
}

function resolveInsertBeforeId(
  mb: FoundationVectorLayerBridge,
  candidates: readonly string[] | undefined,
): string | undefined {
  if (!candidates || !mb.getLayer) return undefined;
  return candidates.find((candidate) => mb.getLayer?.(candidate));
}
