// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  buildVectorTileRuntimeSource,
  fetchVectorTileRuntimeManifest,
  parseVectorTileRuntimeManifest,
  resolveVectorTileAllowedOrigins,
  resolveVectorTileRuntimeEnv,
  resolveVectorTileRuntimeManifestUrl,
} from "@/lib/map/vector-tile-manifest";

describe("foundation-platform vector tile runtime manifest consumer", () => {
  it("returns CSP origins for the Catalog and an environment's tile host", () => {
    const origins = resolveVectorTileAllowedOrigins({
      NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL: "https://foundation-platform.example.com/api",
      NEXT_PUBLIC_TILES_MANIFEST_URL: "https://static.example.com/gold/manifest.json",
    });

    expect(origins).toEqual([
      "https://foundation-platform.example.com",
      "https://static.example.com",
    ]);
  });

  it("builds the default browser runtime env from direct public env references", () => {
    const previous = process.env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL;
    process.env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL =
      "https://foundation-platform.example.com";
    try {
      expect(resolveVectorTileRuntimeManifestUrl(resolveVectorTileRuntimeEnv())).toBe(
        "https://foundation-platform.example.com/catalog/v1/vector-tiles/runtime-manifest",
      );
    } finally {
      process.env.NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL = previous;
    }
  });

  it("dispatches the strict v2 runtime manifest without object-key substitution", () => {
    const manifest = parseVectorTileRuntimeManifest({
      schema_version: 2,
      current_version: "0196e7e0-3c20-7000-8000-000000000052",
      manifest_generation: 108,
      refresh_after_seconds: 4,
      published_at: "2026-07-24T00:00:00Z",
      publication_units: {
        parcels: {
          data_revision: "0196e7e0-3c20-7000-8000-000000000061",
          serving_generation: 42,
          active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
          canonical_iceberg_snapshot_id: "70000000000000001",
          source: {
            kind: "static_pmtiles",
            martin_source_id: "parcels-0196e7e0-3c20-7000-8000-000000000062",
            tiles_url_template:
              "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
            pmtiles_object_key:
              "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
            pmtiles_file_asset_id: "0196e7e0-3c20-7000-8000-000000000063",
            pmtiles_sha256: "a".repeat(64),
            pmtiles_bytes: 123,
          },
          layers: {
            parcels: {
              source_layer: "parcels",
              feature_id_property: "pnu",
              tile_min_zoom: 8,
              tile_max_zoom: 16,
              render_min_zoom: 10,
              render_max_zoom: 22,
              feature_filter_properties: { pnu: "pnu" },
            },
          },
          lineage: {
            source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
            source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
          },
        },
      },
    });
    const parcelsUnit = manifest.publication_units.parcels;
    if (!parcelsUnit) throw new Error("runtime fixture must contain parcels");
    expect(buildVectorTileRuntimeSource(parcelsUnit, "parcels")).toEqual({
      type: "vector",
      tiles: ["https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}"],
      minzoom: 8,
      maxzoom: 16,
      promoteId: "pnu",
    });
  });

  it("rejects duplicate Martin source ids and unsafe publication unit names", () => {
    const base = {
      schema_version: 2,
      current_version: "0196e7e0-3c20-7000-8000-000000000052",
      manifest_generation: 108,
      refresh_after_seconds: 4,
      published_at: "2026-07-24T00:00:00Z",
      publication_units: {
        parcels: {
          data_revision: "0196e7e0-3c20-7000-8000-000000000061",
          serving_generation: 42,
          active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
          canonical_iceberg_snapshot_id: "70000000000000001",
          source: {
            kind: "dynamic_postgis",
            martin_source_id: "parcels",
            tiles_url_template: "http://127.0.0.1:3110/parcels/{z}/{x}/{y}",
            cache_policy: "no_store",
          },
          layers: {
            parcels: {
              source_layer: "parcels",
              feature_id_property: "pnu",
              tile_min_zoom: 8,
              tile_max_zoom: 16,
              render_min_zoom: 10,
              render_max_zoom: 22,
              feature_filter_properties: { pnu: "pnu" },
            },
          },
          lineage: {
            source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
            source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
          },
        },
      },
    };
    expect(() =>
      parseVectorTileRuntimeManifest({
        ...base,
        publication_units: {
          parcels: base.publication_units.parcels,
          anchors: {
            ...base.publication_units.parcels,
            source: { ...base.publication_units.parcels.source },
          },
        },
      }),
    ).toThrow();
    expect(() =>
      parseVectorTileRuntimeManifest({
        ...base,
        publication_units: { "bad/name": base.publication_units.parcels },
      }),
    ).toThrow(/safe Martin identifiers/);
  });

  it("rejects static routes whose PMTiles object name is not the release identity", () => {
    expect(() =>
      parseVectorTileRuntimeManifest({
        schema_version: 2,
        current_version: "0196e7e0-3c20-7000-8000-000000000052",
        manifest_generation: 108,
        refresh_after_seconds: 4,
        published_at: "2026-07-24T00:00:00Z",
        publication_units: {
          parcels: {
            data_revision: "0196e7e0-3c20-7000-8000-000000000061",
            serving_generation: 42,
            active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
            canonical_iceberg_snapshot_id: "70000000000000001",
            source: {
              kind: "static_pmtiles",
              martin_source_id: "parcels-0196e7e0-3c20-7000-8000-000000000062",
              tiles_url_template:
                "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
              pmtiles_object_key: "gold/vector-tiles/releases/old/parcels.pmtiles",
              pmtiles_file_asset_id: "0196e7e0-3c20-7000-8000-000000000063",
              pmtiles_sha256: "a".repeat(64),
              pmtiles_bytes: 123,
            },
            layers: {
              parcels: {
                source_layer: "parcels",
                feature_id_property: "pnu",
                tile_min_zoom: 8,
                tile_max_zoom: 16,
                render_min_zoom: 10,
                render_max_zoom: 22,
                feature_filter_properties: { pnu: "pnu" },
              },
            },
            lineage: {
              source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
              source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
            },
          },
        },
      }),
    ).toThrow(/release-addressed filename stem/);
  });

  it("rejects a tile URL whose route does not match its declared Martin source", () => {
    const value = {
      schema_version: 2,
      current_version: "0196e7e0-3c20-7000-8000-000000000052",
      manifest_generation: 108,
      refresh_after_seconds: 4,
      published_at: "2026-07-24T00:00:00Z",
      publication_units: {
        parcels: {
          data_revision: "0196e7e0-3c20-7000-8000-000000000061",
          serving_generation: 42,
          active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
          canonical_iceberg_snapshot_id: "70000000000000001",
          source: {
            kind: "dynamic_postgis",
            martin_source_id: "parcels",
            tiles_url_template: "http://127.0.0.1:3110/not-parcels/{z}/{x}/{y}",
            cache_policy: "no_store",
          },
          layers: {
            parcels: {
              source_layer: "parcels",
              feature_id_property: "pnu",
              tile_min_zoom: 8,
              tile_max_zoom: 16,
              render_min_zoom: 10,
              render_max_zoom: 22,
              feature_filter_properties: { pnu: "pnu" },
            },
          },
          lineage: {
            source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
            source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
          },
        },
      },
    };
    expect(() => parseVectorTileRuntimeManifest(value)).toThrow(
      /route does not match|Martin tile URL/,
    );
  });

  it("reads the v2 endpoint and preserves ETag on 304", async () => {
    const env = { NEXT_PUBLIC_FOUNDATION_PLATFORM_BASE_URL: "https://foundation.example/" };
    expect(resolveVectorTileRuntimeManifestUrl(env)).toBe(
      "https://foundation.example/catalog/v1/vector-tiles/runtime-manifest",
    );
    const previous = parseVectorTileRuntimeManifest({
      schema_version: 2,
      current_version: "0196e7e0-3c20-7000-8000-000000000052",
      manifest_generation: 1,
      refresh_after_seconds: 4,
      published_at: "2026-07-24T00:00:00Z",
      publication_units: {
        parcels: {
          data_revision: "0196e7e0-3c20-7000-8000-000000000061",
          serving_generation: 1,
          active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
          canonical_iceberg_snapshot_id: "1",
          source: {
            kind: "dynamic_postgis",
            martin_source_id: "parcels",
            tiles_url_template: "http://127.0.0.1:3101/parcels/{z}/{x}/{y}",
            cache_policy: "no_store",
          },
          layers: {
            parcels: {
              source_layer: "parcels",
              feature_id_property: "pnu",
              tile_min_zoom: 8,
              tile_max_zoom: 16,
              render_min_zoom: 10,
              render_max_zoom: 22,
              feature_filter_properties: { pnu: "pnu" },
            },
          },
          lineage: {
            source_record_id: "0196e7e0-3c20-7000-8000-000000000064",
            source_file_asset_ids: ["0196e7e0-3c20-7000-8000-000000000065"],
          },
        },
      },
    });
    const result = await fetchVectorTileRuntimeManifest(
      async (_input, init) => {
        const headers = init?.headers as Record<string, string> | undefined;
        expect(headers?.["if-none-match"]).toBe('"manifest-1"');
        return new Response(null, { status: 304 });
      },
      env,
      { etag: '"manifest-1"', previous },
    );
    expect(result.notModified).toBe(true);
    expect(result.manifest).toBe(previous);
  });

  it("rejects future schema, unsafe generation, and unknown URL placeholders", () => {
    expect(() => parseVectorTileRuntimeManifest({ schema_version: 3 })).toThrow();
    expect(() =>
      parseVectorTileRuntimeManifest({
        schema_version: 2,
        current_version: "0196e7e0-3c20-7000-8000-000000000052",
        manifest_generation: 9007199254740992,
        refresh_after_seconds: 4,
        published_at: "2026-07-24T00:00:00Z",
        publication_units: {},
      }),
    ).toThrow();
  });
});
