// @vitest-environment node
import { describe, expect, it } from "vitest";
import { validateFoundationVectorManifest } from "@/lib/map/foundation-vector-layer-registry";
import { refreshFoundationVectorSources } from "@/lib/map/foundation-vector-source-refresh";
import { parseVectorTileRuntimeManifest } from "@/lib/map/vector-tile-manifest";

function manifest(servingGeneration: number, url: string) {
  return parseVectorTileRuntimeManifest({
    schema_version: 2,
    current_version: "0196e7e0-3c20-7000-8000-000000000052",
    manifest_generation: servingGeneration,
    refresh_after_seconds: 4,
    published_at: "2026-07-24T00:00:00Z",
    publication_units: {
      parcels: {
        data_revision: "0196e7e0-3c20-7000-8000-000000000061",
        serving_generation: servingGeneration,
        active_release_id: "0196e7e0-3c20-7000-8000-000000000062",
        canonical_iceberg_snapshot_id: "70000000000000001",
        source: {
          kind: "dynamic_postgis",
          martin_source_id: "parcels",
          tiles_url_template: url,
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
      },
    },
  });
}

describe("Foundation vector source refresh", () => {
  it("retargets the existing source only when its serving generation changes", () => {
    const tiles = ["http://127.0.0.1:3110/parcels/{z}/{x}/{y}"];
    const source = { setTiles: (next: string[]) => tiles.splice(0, tiles.length, ...next) };
    const mapbox = { getSource: () => source };
    const previous = manifest(1, "http://127.0.0.1:3110/parcels/{z}/{x}/{y}");
    const unchanged = refreshFoundationVectorSources(mapbox, previous, previous);
    expect(unchanged).toEqual([]);
    expect(tiles).toEqual(["http://127.0.0.1:3110/parcels/{z}/{x}/{y}"]);

    const next = manifest(2, "https://tiles.example.com/parcels/{z}/{x}/{y}");
    const changed = refreshFoundationVectorSources(mapbox, previous, next);
    expect(changed).toEqual(["parcels"]);
    expect(tiles).toEqual(["https://tiles.example.com/parcels/{z}/{x}/{y}"]);
  });

  it("fails closed when the manifest layer identity drifts from the registry", () => {
    const previous = manifest(1, "http://127.0.0.1:3110/parcels/{z}/{x}/{y}");
    const previousParcels = previous.publication_units.parcels;
    if (!previousParcels) throw new Error("test manifest must contain parcels");
    const previousLayer = previousParcels.layers.parcels;
    if (!previousLayer) throw new Error("test manifest must contain parcels layer");
    const drifted = parseVectorTileRuntimeManifest({
      ...previous,
      manifest_generation: 2,
      publication_units: {
        ...previous.publication_units,
        parcels: {
          ...previousParcels,
          serving_generation: 2,
          layers: {
            parcels: {
              ...previousLayer,
              source_layer: "parcel-boundaries",
            },
          },
        },
      },
    });
    expect(() =>
      refreshFoundationVectorSources(
        { getSource: () => ({ setTiles: () => undefined }) },
        previous,
        drifted,
      ),
    ).toThrow(/contract drift/);
  });

  it("fails closed when Catalog publishes an unregistered unit", () => {
    const previous = manifest(1, "http://127.0.0.1:3110/parcels/{z}/{x}/{y}");
    const parcels = previous.publication_units.parcels;
    if (!parcels) throw new Error("test manifest must contain parcels");
    const parcelLayer = parcels.layers.parcels;
    if (!parcelLayer) throw new Error("test manifest must contain parcels layer");
    const unknown = parseVectorTileRuntimeManifest({
      ...previous,
      manifest_generation: 2,
      publication_units: {
        ...previous.publication_units,
        anchors: {
          ...parcels,
          active_release_id: "0196e7e0-3c20-7000-8000-000000000066",
          serving_generation: 1,
          source: {
            ...parcels.source,
            martin_source_id: "anchors",
            tiles_url_template: "https://tiles.example.com/anchors/{z}/{x}/{y}",
          },
          layers: {
            anchors: {
              ...parcelLayer,
              source_layer: "anchors",
            },
          },
        },
      },
    });
    expect(() =>
      refreshFoundationVectorSources(
        { getSource: () => ({ setTiles: () => undefined }) },
        previous,
        unknown,
      ),
    ).toThrow(/unregistered Foundation publication unit/);
  });

  it("requires static Martin source ids to match the immutable release filename", () => {
    const dynamic = manifest(1, "http://127.0.0.1:3110/parcels/{z}/{x}/{y}");
    const parcels = dynamic.publication_units.parcels;
    if (!parcels) throw new Error("test manifest must contain parcels");
    const staticManifest = parseVectorTileRuntimeManifest({
      ...dynamic,
      publication_units: {
        parcels: {
          ...parcels,
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
        },
      },
    });
    expect(() => validateFoundationVectorManifest(staticManifest)).not.toThrow();

    const staticParcels = staticManifest.publication_units.parcels;
    if (!staticParcels) throw new Error("static manifest must contain parcels");
    const drifted = {
      ...staticManifest,
      publication_units: {
        ...staticManifest.publication_units,
        parcels: {
          ...staticParcels,
          source: {
            kind: "static_pmtiles" as const,
            martin_source_id: "parcels",
            tiles_url_template:
              "https://tiles.example.com/parcels-0196e7e0-3c20-7000-8000-000000000062/{z}/{x}/{y}",
            pmtiles_object_key:
              "gold/vector-tiles/releases/0196e7e0-3c20-7000-8000-000000000062/parcels-0196e7e0-3c20-7000-8000-000000000062.pmtiles",
            pmtiles_file_asset_id: "0196e7e0-3c20-7000-8000-000000000063",
            pmtiles_sha256: "a".repeat(64),
            pmtiles_bytes: 123,
          },
        },
      },
    };
    expect(() => validateFoundationVectorManifest(drifted)).toThrow(
      /static Martin source identity/,
    );
  });
});
