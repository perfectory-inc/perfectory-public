import { MAP_LAYER_COLORS } from "@gongzzang/ui/tokens.js";

/**
 * The 용도지역지구 layers the Foundation `zoning` tile unit carries, grouped the way a buyer toggles
 * them (root ADR-0362 §2, ADR-0373). A feature's `layer_code` property is the provider's four-letter
 * layer (`D032`); its family is the one toggle that shows or hides it.
 *
 * The labels a user reads are the provider's words and live in `map.zoning.families` (ko.json).
 */
export const ZONING_FAMILIES = [
  { key: "greenbelt", layerCodes: ["D032"] },
  {
    key: "urbanPlanningFacility",
    layerCodes: ["D142", "D143", "D144", "D145", "D146", "D147", "D148"],
  },
  { key: "factoryLocation", layerCodes: ["D335", "D336", "D327"] },
  { key: "agriculturalPromotion", layerCodes: ["D036"] },
  { key: "useDistrict", layerCodes: ["D124", "D125", "D126", "D127"] },
  { key: "forest", layerCodes: ["D054"] },
] as const satisfies readonly { key: keyof typeof MAP_LAYER_COLORS.zoning; layerCodes: string[] }[];

export type ZoningFamilyKey = (typeof ZONING_FAMILIES)[number]["key"];

/**
 * Layers a family names but the tiles do not carry yet: files over 500 MB that only the RAON lane
 * fetches, and that lane has not run (root ADR-0362 §2). The screen says "일부 구역 수집 대기" for
 * them instead of leaving them out.
 *
 * A cross-area guard compares this list with `pending_regions` in
 * `platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-use-zone-area-source-objects.json`
 * (codes there are without the `D`), so keep it a literal array.
 */
export const ZONING_PENDING_LAYER_CODES = ["D054", "D125"] as const;

const PENDING: ReadonlySet<string> = new Set(ZONING_PENDING_LAYER_CODES);

/** The family a feature's `layer_code` belongs to, or undefined for a layer no toggle owns. */
export function zoningFamilyOf(layerCode: string): ZoningFamilyKey | undefined {
  return ZONING_FAMILIES.find((family) =>
    (family.layerCodes as readonly string[]).includes(layerCode),
  )?.key;
}

/** How much of a family is still waiting for collection: none, some layers, or all of them. */
export function zoningFamilyPending(key: ZoningFamilyKey): "none" | "partial" | "all" {
  const codes = ZONING_FAMILIES.find((family) => family.key === key)?.layerCodes ?? [];
  const pending = codes.filter((code) => PENDING.has(code)).length;
  if (pending === 0) return "none";
  return pending === codes.length ? "all" : "partial";
}

/** Shows exactly the layers of the enabled families; with none enabled, matches no feature. */
export function zoningLayerFilter(enabled: ReadonlySet<ZoningFamilyKey>): unknown[] {
  const codes = ZONING_FAMILIES.filter((family) => enabled.has(family.key)).flatMap((family) => [
    ...family.layerCodes,
  ]);
  return ["in", ["get", "layer_code"], ["literal", codes]];
}

/** One colour per family, picked by `layer_code`; a layer no family owns is drawn grey. */
export function zoningColorExpression(): unknown[] {
  return [
    "match",
    ["get", "layer_code"],
    ...ZONING_FAMILIES.flatMap((family) => [
      [...family.layerCodes],
      MAP_LAYER_COLORS.zoning[family.key],
    ]),
    MAP_LAYER_COLORS.admin.outline,
  ];
}
