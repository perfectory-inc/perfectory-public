import { LISTING_TYPE_COLOR_FALLBACK } from "@gongzzang/ui/tokens.js";
import {
  LISTING_MARKER_RENDER_MAX_ZOOM,
  LISTING_MARKER_RENDER_MIN_ZOOM,
} from "@/lib/map/map-zoom-policy";
import {
  buildListingMarkerDeltaTileSource,
  buildListingMarkerTileSource,
  LISTING_MARKER_DELTA_TILE_LAYER,
  LISTING_MARKER_TILE_LAYER,
  type MarkerTileSource,
} from "@/lib/map/marker-tile-contract";
// The PNU anchor markers' layer id, which a complex click yields to. The markers come back from
// the v2 runtime manifest's `parcel_anchor*` units (root ADR-0225 step 1); until then no such
// layer is on the map.
export const PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID = "parcel-anchor-markers-circle";
export const LISTING_MARKER_TILE_SOURCE_ID = LISTING_MARKER_TILE_LAYER;
export const LISTING_MARKER_TILE_CIRCLE_LAYER_ID = "listing-markers-circle";
export const LISTING_MARKER_DELTA_TILE_SOURCE_ID = LISTING_MARKER_DELTA_TILE_LAYER;
export const LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID = "listing-marker-deltas-circle";

type MarkerCircleLayer<
  LayerId extends string,
  SourceId extends string,
  SourceLayer extends string,
> = {
  id: LayerId;
  type: "circle";
  source: SourceId;
  "source-layer": SourceLayer;
  minzoom: number;
  maxzoom: number;
  paint: {
    "circle-color": string;
    "circle-opacity": number;
    "circle-radius": unknown[];
    "circle-stroke-color": string;
    "circle-stroke-opacity": number;
    "circle-stroke-width": unknown[];
  };
};

type ListingMarkerLayer = MarkerCircleLayer<
  typeof LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  typeof LISTING_MARKER_TILE_SOURCE_ID,
  typeof LISTING_MARKER_TILE_LAYER
>;

type ListingMarkerDeltaLayer = MarkerCircleLayer<
  typeof LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
  typeof LISTING_MARKER_DELTA_TILE_SOURCE_ID,
  typeof LISTING_MARKER_DELTA_TILE_LAYER
>;

export type ListingMarkerLayerRegistration = {
  sourceId: typeof LISTING_MARKER_TILE_SOURCE_ID;
  source: MarkerTileSource;
  layers: [ListingMarkerLayer];
};

export type ListingMarkerDeltaLayerRegistration = {
  sourceId: typeof LISTING_MARKER_DELTA_TILE_SOURCE_ID;
  source: MarkerTileSource;
  layers: [ListingMarkerDeltaLayer];
};

export type BuildListingMarkerLayerRegistrationInput = {
  filterHash: string;
  projectionVersion: number | null;
  minzoom: number;
  maxzoom: number;
  origin?: string;
};

export type BuildListingMarkerDeltaLayerRegistrationInput = {
  baseVersion: number | null;
  minzoom: number;
  maxzoom: number;
  origin?: string;
};

export function buildListingMarkerLayerRegistration(
  input: BuildListingMarkerLayerRegistrationInput,
): ListingMarkerLayerRegistration {
  return {
    sourceId: LISTING_MARKER_TILE_SOURCE_ID,
    source: buildListingMarkerTileSource(input),
    layers: [
      {
        id: LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
        type: "circle",
        source: LISTING_MARKER_TILE_SOURCE_ID,
        "source-layer": LISTING_MARKER_TILE_LAYER,
        minzoom: input.minzoom,
        maxzoom: input.maxzoom,
        paint: {
          "circle-color": LISTING_TYPE_COLOR_FALLBACK,
          "circle-opacity": 0.96,
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 4, 14, 6, 18, 9],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-opacity": 0.96,
          "circle-stroke-width": ["interpolate", ["linear"], ["zoom"], 8, 1, 14, 1.5, 18, 2],
        },
      },
    ],
  };
}

export function buildListingMarkerDeltaLayerRegistration(
  input: BuildListingMarkerDeltaLayerRegistrationInput,
): ListingMarkerDeltaLayerRegistration {
  return {
    sourceId: LISTING_MARKER_DELTA_TILE_SOURCE_ID,
    source: buildListingMarkerDeltaTileSource(input),
    layers: [
      {
        id: LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
        type: "circle",
        source: LISTING_MARKER_DELTA_TILE_SOURCE_ID,
        "source-layer": LISTING_MARKER_DELTA_TILE_LAYER,
        minzoom: input.minzoom,
        maxzoom: input.maxzoom,
        paint: {
          "circle-color": LISTING_TYPE_COLOR_FALLBACK,
          "circle-opacity": 0.96,
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 4, 14, 6, 18, 9],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-opacity": 0.96,
          "circle-stroke-width": ["interpolate", ["linear"], ["zoom"], 8, 1, 14, 1.5, 18, 2],
        },
      },
    ],
  };
}

export type ListingMarkerSourceState = {
  filterHash: string;
  projectionVersion: number | undefined;
};

/** Registers both listing marker sources at one server state (the initial one before the server answers). */
export function buildListingMarkerSourceRegistrations(
  state: ListingMarkerSourceState,
  origin?: string,
): [ListingMarkerLayerRegistration, ListingMarkerDeltaLayerRegistration] {
  const version = state.projectionVersion ?? null;
  return [
    buildListingMarkerLayerRegistration({
      filterHash: state.filterHash,
      projectionVersion: version,
      minzoom: LISTING_MARKER_RENDER_MIN_ZOOM,
      maxzoom: LISTING_MARKER_RENDER_MAX_ZOOM,
      origin,
    }),
    buildListingMarkerDeltaLayerRegistration({
      baseVersion: version,
      minzoom: 0,
      maxzoom: LISTING_MARKER_RENDER_MAX_ZOOM,
      origin,
    }),
  ];
}

/**
 * Points the live listing marker sources at a newer server state: base tiles carry its filter hash
 * and projection version (a new browser cache key), deltas are asked for changes after it.
 */
export function retargetListingMarkerSources(
  mapbox: { getSource?: (id: string) => unknown },
  state: ListingMarkerSourceState,
  origin?: string,
): void {
  for (const registration of buildListingMarkerSourceRegistrations(state, origin)) {
    const source = mapbox.getSource?.(registration.sourceId) as
      | { setTiles?: (tiles: string[]) => void }
      | undefined;
    if (!source) continue; // never registered: the setup failure is already logged
    if (!source.setTiles) {
      throw new Error(`listing marker source cannot be retargeted: ${registration.sourceId}`);
    }
    source.setTiles(registration.source.tiles);
  }
}
