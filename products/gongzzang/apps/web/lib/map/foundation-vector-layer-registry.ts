import {
  buildVectorTileRuntimeSource,
  type VectorTileRuntimeManifest,
} from "@/lib/map/vector-tile-manifest";

/** One Foundation v2 layer's stable Mapbox source identity and style contract. */
export type FoundationVectorLayerDefinition = {
  sourceId: string;
  sourceLayer: string;
  promoteId: string;
};

/** Single source of truth for v2 Foundation vector layer registration. */
export const FOUNDATION_VECTOR_LAYER_REGISTRY: Record<string, FoundationVectorLayerDefinition> = {
  parcels: { sourceId: "parcels", sourceLayer: "parcels", promoteId: "pnu" },
};

/** Builds the source descriptor using the registry's identity rather than runtime string copies. */
export function buildFoundationVectorSource(
  manifest: VectorTileRuntimeManifest,
  unitName: string,
  layerName: string,
) {
  const definition = FOUNDATION_VECTOR_LAYER_REGISTRY[layerName];
  if (!definition || definition.sourceId !== unitName) {
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
