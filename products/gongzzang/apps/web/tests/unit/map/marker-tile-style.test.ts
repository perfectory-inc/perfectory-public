// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { GONGZZANG_MAP_ZOOM_POLICY } from "@/lib/map/map-zoom-policy";
import {
  buildListingMarkerDeltaLayerRegistration,
  buildListingMarkerLayerRegistration,
  LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_DELTA_TILE_SOURCE_ID,
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  LISTING_MARKER_TILE_SOURCE_ID,
  retargetListingMarkerSources,
} from "@/lib/map/marker-tile-style";

describe("listing marker tile map style", () => {
  it("registers Gongzzang listing marker source and circle layer without coordinate inputs", () => {
    const registration = buildListingMarkerLayerRegistration({
      filterHash: "all-active-v1",
      projectionVersion: null,
      minzoom: GONGZZANG_MAP_ZOOM_POLICY.markers.listing.minZoom,
      maxzoom: GONGZZANG_MAP_ZOOM_POLICY.markers.listing.maxZoom,
      origin: "http://localhost:3900",
    });

    expect(registration.sourceId).toBe(LISTING_MARKER_TILE_SOURCE_ID);
    expect(registration.source).toEqual({
      type: "vector",
      tiles: [
        "http://localhost:3900/api/proxy/map/v1/marker-tiles/listing/{z}/{x}/{y}.pbf?filter_hash=all-active-v1",
      ],
      minzoom: 14,
      maxzoom: 22,
    });
    expect(registration.layers[0].id).toBe(LISTING_MARKER_TILE_CIRCLE_LAYER_ID);
    expect(registration.layers[0]["source-layer"]).toBe("listing");
    expect(registration.layers[0].minzoom).toBe(14);
    expect(registration.layers[0].maxzoom).toBe(22);
    expect(registration.source.tiles[0]).not.toContain("bbox=");
    expect(registration.source.tiles[0]).not.toContain("bounds=");
    expect(registration.source.tiles[0]).not.toContain("lat=");
    expect(registration.source.tiles[0]).not.toContain("lng=");
  });

  it("registers Gongzzang listing marker delta source with the listing delta layer", () => {
    const registration = buildListingMarkerDeltaLayerRegistration({
      baseVersion: 41,
      minzoom: 0,
      maxzoom: GONGZZANG_MAP_ZOOM_POLICY.markers.listing.maxZoom,
      origin: "http://localhost:3900",
    });

    expect(registration.sourceId).toBe(LISTING_MARKER_DELTA_TILE_SOURCE_ID);
    expect(registration.source.tiles[0]).toBe(
      "http://localhost:3900/api/proxy/map/v1/marker-deltas/listing/{z}/{x}/{y}.pbf?base_version=41",
    );
    expect(registration.layers[0].id).toBe(LISTING_MARKER_DELTA_TILE_CIRCLE_LAYER_ID);
    expect(registration.layers[0]["source-layer"]).toBe("listing_delta");
  });
});

describe("retargetListingMarkerSources", () => {
  it("re-keys base tiles by the server projection version and bases deltas on it", () => {
    const base = { setTiles: vi.fn() };
    const delta = { setTiles: vi.fn() };
    const sources: Record<string, unknown> = {
      [LISTING_MARKER_TILE_SOURCE_ID]: base,
      [LISTING_MARKER_DELTA_TILE_SOURCE_ID]: delta,
    };
    const registeredHash = `lst_filter_v1_${"b".repeat(64)}`;

    retargetListingMarkerSources(
      { getSource: (id) => sources[id] },
      { filterHash: registeredHash, projectionVersion: 7 },
      "http://localhost:3900",
    );

    expect(base.setTiles).toHaveBeenCalledWith([
      `http://localhost:3900/api/proxy/map/v1/marker-tiles/listing/{z}/{x}/{y}.pbf?filter_hash=${registeredHash}&projection_version=7`,
    ]);
    expect(delta.setTiles).toHaveBeenCalledWith([
      "http://localhost:3900/api/proxy/map/v1/marker-deltas/listing/{z}/{x}/{y}.pbf?base_version=7",
    ]);
  });

  it("refuses a registered source it cannot retarget instead of serving the old version", () => {
    expect(() =>
      retargetListingMarkerSources(
        { getSource: () => ({}) },
        { filterHash: "all-active-v1", projectionVersion: 7 },
        "http://localhost:3900",
      ),
    ).toThrow(/cannot be retargeted/);
  });
});
