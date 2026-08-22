// @vitest-environment node
import { MAP_LAYER_COLORS } from "@gongzzang/ui/tokens.js";
import { describe, expect, it, vi } from "vitest";
import {
  FOUNDATION_VECTOR_FILL_STYLES,
  foundationVectorFillLayerId,
  foundationVectorOutlineLayerId,
  registerFoundationVectorRuntimeUnit,
} from "@/lib/map/foundation-vector-fill-layers";
import {
  FOUNDATION_VECTOR_LAYER_REGISTRY,
  validateFoundationVectorManifest,
} from "@/lib/map/foundation-vector-layer-registry";
import { setupPolygonLayers } from "@/lib/map/listing-map-runtime";
import {
  LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
  PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
} from "@/lib/map/marker-tile-style";
import {
  parseVectorTileRuntimeManifest,
  type VectorTileRuntimeManifest,
} from "@/lib/map/vector-tile-manifest";

const MARTIN_BASE = "http://127.0.0.1:3110";

/**
 * The `complex` unit exactly as `promote-industrial-complex-boundary-runtime` publishes it:
 * `feature_id_property` is `complex_id`, the tiles are cut over z6..16 and drawn to z22.
 */
function complexUnit() {
  return {
    data_revision: "0196e7e0-3c20-7000-8000-000000000071",
    serving_generation: 1,
    active_release_id: "0196e7e0-3c20-7000-8000-000000000072",
    canonical_iceberg_snapshot_id: "841361364657368625",
    source: {
      kind: "dynamic_postgis",
      martin_source_id: "complex",
      tiles_url_template: `${MARTIN_BASE}/complex/{z}/{x}/{y}`,
      cache_policy: "no_store",
    },
    layers: {
      complex: {
        source_layer: "complex",
        feature_id_property: "complex_id",
        tile_min_zoom: 6,
        tile_max_zoom: 16,
        render_min_zoom: 6,
        render_max_zoom: 22,
        feature_filter_properties: { official_complex_code: "official_complex_code" },
      },
    },
    lineage: {
      source_record_id: "0196e7e0-3c20-7000-8000-000000000074",
      source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000075"],
    },
  };
}

function parcelsUnit() {
  return {
    data_revision: "0196e7e0-3c20-7000-8000-000000000061",
    serving_generation: 1,
    active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
    canonical_iceberg_snapshot_id: "70000000000000001",
    source: {
      kind: "dynamic_postgis",
      martin_source_id: "parcels",
      tiles_url_template: `${MARTIN_BASE}/parcels/{z}/{x}/{y}`,
      cache_policy: "no_store",
    },
    layers: {
      parcels: {
        source_layer: "parcels",
        feature_id_property: "pnu",
        tile_min_zoom: 14,
        tile_max_zoom: 16,
        render_min_zoom: 14,
        render_max_zoom: 22,
        feature_filter_properties: { pnu: "pnu" },
      },
    },
    lineage: {
      source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
      source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
    },
  };
}

function runtimeManifest(units: Record<string, unknown>): VectorTileRuntimeManifest {
  return parseVectorTileRuntimeManifest({
    schema_version: 2,
    current_version: "0196e7e0-3c20-7000-8000-000000000052",
    manifest_generation: 1,
    refresh_after_seconds: 4,
    published_at: "2026-07-24T00:00:00Z",
    publication_units: units,
  });
}

type AddedLayer = { layer: Record<string, unknown>; beforeId?: string };

function fakeMapbox(existingLayerIds: string[] = []) {
  const sources = new Map<string, Record<string, unknown>>();
  const added: AddedLayer[] = [];
  const existing = new Set(existingLayerIds);
  return {
    sources,
    added,
    layerIds: () => added.map((entry) => entry.layer.id as string),
    layer: (id: string) => added.find((entry) => entry.layer.id === id)?.layer,
    entry: (id: string) => added.find((entry) => entry.layer.id === id),
    addSource: (id: string, source: Record<string, unknown>) => {
      sources.set(id, source);
    },
    addLayer: (layer: Record<string, unknown>, beforeId?: string) => {
      added.push({ layer, beforeId });
    },
    getSource: (id: string) => sources.get(id),
    getLayer: (id: string) =>
      existing.has(id) ? { id } : added.find((entry) => entry.layer.id === id)?.layer,
  };
}

describe("Foundation vector fill layers", () => {
  it("draws the industrial-complex boundary from the unit the promotion published", () => {
    const mb = fakeMapbox();
    const registered = registerFoundationVectorRuntimeUnit(
      mb,
      runtimeManifest({ complex: complexUnit() }),
      "complex",
    );

    expect(registered).toEqual(["complex-fill", "complex-outline"]);
    // The source stops at the zoom Martin stops cutting at, so nothing ever requests a tile
    // outside 6..16; the layers are drawn past it from the overzoomed z16 tile.
    expect(mb.sources.get("complex")).toEqual({
      type: "vector",
      tiles: [`${MARTIN_BASE}/complex/{z}/{x}/{y}`],
      minzoom: 6,
      maxzoom: 16,
      promoteId: "complex_id",
    });
    expect(mb.layer("complex-fill")).toMatchObject({
      type: "fill",
      source: "complex",
      "source-layer": "complex",
      minzoom: 6,
      maxzoom: 22,
      promoteId: "complex_id",
      paint: {
        "fill-color": MAP_LAYER_COLORS.complex.fill,
        "fill-outline-color": MAP_LAYER_COLORS.complex.outline,
      },
    });
    expect(mb.layer("complex-outline")).toMatchObject({
      type: "line",
      source: "complex",
      paint: { "line-color": MAP_LAYER_COLORS.complex.outline, "line-width": 1.5 },
    });
  });

  it("keeps the complex fill lighter than the parcel fill it is context for", () => {
    const complex = FOUNDATION_VECTOR_FILL_STYLES.complex;
    const parcels = FOUNDATION_VECTOR_FILL_STYLES.parcels;
    if (!complex || !parcels) throw new Error("both units must be styled");
    expect(complex.fillOpacity).toBeLessThan(parcels.fillOpacity);
  });

  it("inserts the complex layers under the lowest layer that must stay above them", () => {
    for (const above of [
      foundationVectorFillLayerId("parcels"),
      PARCEL_ANCHOR_MARKER_TILE_CIRCLE_LAYER_ID,
      LISTING_MARKER_TILE_CIRCLE_LAYER_ID,
    ]) {
      const mb = fakeMapbox([above]);
      registerFoundationVectorRuntimeUnit(
        mb,
        runtimeManifest({ complex: complexUnit() }),
        "complex",
      );
      expect(mb.entry(foundationVectorFillLayerId("complex"))?.beforeId).toBe(above);
      expect(mb.entry(foundationVectorOutlineLayerId("complex"))?.beforeId).toBe(above);
    }
  });

  it("registers nothing for a unit the manifest does not carry", () => {
    const mb = fakeMapbox();
    const registered = registerFoundationVectorRuntimeUnit(
      mb,
      runtimeManifest({ parcels: parcelsUnit() }),
      "complex",
    );

    expect(registered).toEqual([]);
    expect(mb.sources.size).toBe(0);
    expect(mb.added).toEqual([]);
  });

  it("refuses a manifest whose complex layer keys features on something else", () => {
    const unit = complexUnit();
    const drifted = runtimeManifest({
      complex: {
        ...unit,
        layers: {
          complex: { ...unit.layers.complex, feature_id_property: "official_complex_code" },
        },
      },
    });

    expect(() => registerFoundationVectorRuntimeUnit(fakeMapbox(), drifted, "complex")).toThrow(
      /contract drift/,
    );
  });

  it("styles every unit the registry registers", () => {
    for (const unitName of Object.keys(FOUNDATION_VECTOR_LAYER_REGISTRY)) {
      expect(FOUNDATION_VECTOR_FILL_STYLES[unitName]).toBeDefined();
    }
  });
});

describe("Foundation polygon layer setup", () => {
  it("puts the complex polygons under the parcel polygons", () => {
    const mb = fakeMapbox();
    setupPolygonLayers(
      mb,
      () => undefined,
      null,
      runtimeManifest({ parcels: parcelsUnit(), complex: complexUnit() }),
    );

    expect(mb.layerIds()).toEqual(["complex-fill", "complex-outline", "parcels-fill"]);
    expect(mb.sources.has("complex")).toBe(true);
    expect(mb.sources.has("parcels")).toBe(true);
  });

  it("leaves the rest of the map intact when nothing has promoted the complex unit", () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const mb = fakeMapbox();
    try {
      setupPolygonLayers(mb, () => undefined, null, runtimeManifest({ parcels: parcelsUnit() }));

      expect(mb.layerIds()).toEqual(["parcels-fill"]);
      expect(mb.sources.has("complex")).toBe(false);
      // Silence is what makes "not promoted yet" indistinguishable from "registration threw".
      expect(info.mock.calls.map(([, payload]) => String(payload))).toContainEqual(
        expect.stringContaining('"unit":"complex"'),
      );
    } finally {
      info.mockRestore();
    }
  });

  it("registers no runtime unit at all when the manifest carries one this client does not own", () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const mb = fakeMapbox();
    const unowned = runtimeManifest({
      parcels: parcelsUnit(),
      complex: complexUnit(),
      anchors: {
        ...parcelsUnit(),
        active_release_id: "0196e7e0-3c20-7000-8000-000000000066",
        source: {
          ...parcelsUnit().source,
          martin_source_id: "anchors",
          tiles_url_template: `${MARTIN_BASE}/anchors/{z}/{x}/{y}`,
        },
        layers: { anchors: { ...parcelsUnit().layers.parcels, source_layer: "anchors" } },
      },
    });
    try {
      expect(() => validateFoundationVectorManifest(unowned)).toThrow(
        /unregistered Foundation publication unit/,
      );
      setupPolygonLayers(mb, () => undefined, null, unowned);

      expect(mb.added).toEqual([]);
      expect(mb.sources.size).toBe(0);
      expect(error).toHaveBeenCalled();
    } finally {
      error.mockRestore();
      info.mockRestore();
    }
  });
});
