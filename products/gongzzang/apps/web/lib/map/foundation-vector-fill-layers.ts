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
  fillColor: string;
  fillOpacity: number;
  outlineColor: string;
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

/** The Mapbox layer id this repo draws `unitName`'s polygons as. */
export function foundationVectorFillLayerId(unitName: string): string {
  return `${unitName}${FILL_LAYER_SUFFIX}`;
}

/** The Mapbox layer id for a unit's crisp boundary line, when its style asks for one. */
export function foundationVectorOutlineLayerId(unitName: string): string {
  return `${unitName}${OUTLINE_LAYER_SUFFIX}`;
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
  foundationVectorFillLayerId("parcels"),
  PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
];

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
  };

  const fillLayerId = foundationVectorFillLayerId(unitName);
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
    const outlineLayerId = foundationVectorOutlineLayerId(unitName);
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
 * window from the manifest.
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
  return registerFoundationVectorFillLayers(mb, unitName, {
    sourceId: definition.sourceId,
    source: buildFoundationVectorSource(manifest, unitName, definition.layerName),
    sourceLayer: layer.source_layer,
    // The render window, not the tile window: the source already carries `tile_max_zoom`, so a
    // request never leaves the range Martin cuts, and a client asking for z18 gets the z16 tile
    // overzoomed rather than an empty one.
    minzoom: layer.render_min_zoom,
    maxzoom: layer.render_max_zoom,
    promoteId: layer.feature_id_property,
  });
}

function resolveInsertBeforeId(
  mb: FoundationVectorLayerBridge,
  candidates: readonly string[] | undefined,
): string | undefined {
  if (!candidates || !mb.getLayer) return undefined;
  return candidates.find((candidate) => mb.getLayer?.(candidate));
}
