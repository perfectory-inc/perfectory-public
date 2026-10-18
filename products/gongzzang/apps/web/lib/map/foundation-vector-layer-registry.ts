import {
  buildVectorTileRuntimeSource,
  type VectorTileRuntimeManifest,
} from "@/lib/map/vector-tile-manifest";

/** One Foundation v2 layer's stable Mapbox source identity and style contract. */
export type FoundationVectorLayerDefinition = {
  sourceId: string;
  dynamicMartinSourceId: string;
  layerName: string;
  sourceLayer: string;
  promoteId: string;
};

/** Single source of truth for v2 Foundation vector layer registration. */
export const FOUNDATION_VECTOR_LAYER_REGISTRY: Record<string, FoundationVectorLayerDefinition> = {
  parcels: {
    sourceId: "parcels",
    dynamicMartinSourceId: "parcels",
    layerName: "parcels",
    sourceLayer: "parcels",
    promoteId: "pnu",
  },
  admin: {
    sourceId: "admin",
    dynamicMartinSourceId: "admin",
    layerName: "admin",
    sourceLayer: "admin",
    promoteId: "administrative_unit_id",
  },
  // `promoteId` is the property the publisher writes into
  // `catalog.vector_tile_release_layer.feature_id_property`, and that value is
  // `feature_id_property: "complex_id"` in
  // `platforms/foundation-platform/services/foundation-outbox-publisher/src/industrial_complex_boundary_runtime_promote.rs`.
  // `official_complex_code` also rides in the tile, but it is the code a human quotes, not the
  // identity the lakehouse and every Gold artifact key on, and `buildFoundationVectorSource` below
  // rejects the manifest outright if the two disagree.
  complex: {
    sourceId: "complex",
    dynamicMartinSourceId: "complex",
    layerName: "complex",
    sourceLayer: "complex",
    promoteId: "complex_id",
  },
};

/** Builds the source descriptor using the registry's identity rather than runtime string copies. */
export function buildFoundationVectorSource(
  manifest: VectorTileRuntimeManifest,
  unitName: string,
  layerName = FOUNDATION_VECTOR_LAYER_REGISTRY[unitName]?.layerName,
) {
  const definition = FOUNDATION_VECTOR_LAYER_REGISTRY[unitName];
  if (!definition || definition.layerName !== layerName) {
    throw new Error(`unregistered Foundation vector layer: ${unitName}/${layerName}`);
  }
  const unit = manifest.publication_units[unitName];
  if (!unit) throw new Error(`Foundation publication unit is missing: ${unitName}`);
  const layer = unit.layers[layerName];
  if (
    !layer ||
    layer.source_layer !== definition.sourceLayer ||
    layer.feature_id_property !== definition.promoteId
  ) {
    throw new Error(`Foundation vector layer contract drift: ${unitName}/${layerName}`);
  }
  return buildVectorTileRuntimeSource(unit, layerName);
}

/**
 * Fails closed on a unit the client owns whose contract drifted. A unit the registry does not name
 * is never registered (every registration and refresh walks the registry) and is skipped here: the
 * Catalog publishes a unit before a web release knows it (root ADR-0225 step 1b), and rejecting the
 * whole manifest for it would take parcels, admin and complex off the map with it.
 */
export function validateFoundationVectorManifest(manifest: VectorTileRuntimeManifest): void {
  for (const [unitName, unit] of Object.entries(manifest.publication_units)) {
    const definition = FOUNDATION_VECTOR_LAYER_REGISTRY[unitName];
    if (!definition) continue;
    const layerNames = Object.keys(unit.layers);
    if (layerNames.length !== 1 || layerNames[0] !== definition.layerName) {
      throw new Error(`Foundation vector layer contract drift: ${unitName}`);
    }
    if (unit.source.kind === "dynamic_postgis") {
      if (unit.source.martin_source_id !== definition.dynamicMartinSourceId) {
        throw new Error(`Foundation dynamic Martin source id drift: ${unitName}`);
      }
      continue;
    }
    const expectedFilename = `${unitName}-${unit.active_release_id}.pmtiles`;
    const objectFilename = unit.source.pmtiles_object_key.split("/").at(-1);
    if (
      objectFilename !== expectedFilename ||
      unit.source.martin_source_id !== expectedFilename.slice(0, -".pmtiles".length)
    ) {
      throw new Error(`Foundation static Martin source identity drift: ${unitName}`);
    }
  }
}
