// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  ZONING_FAMILIES,
  ZONING_PENDING_LAYER_CODES,
  zoningColorExpression,
  zoningFamilyOf,
  zoningFamilyPending,
  zoningLayerFilter,
} from "@/lib/map/zoning-families";

describe("zoning families", () => {
  it("puts every served and pending layer under exactly one toggle", () => {
    const codes = ZONING_FAMILIES.flatMap((family) => [...family.layerCodes]);
    expect(new Set(codes).size).toBe(codes.length);
    expect(codes).toHaveLength(17);
    for (const pending of ZONING_PENDING_LAYER_CODES) expect(codes).toContain(pending);
    expect(zoningFamilyOf("D032")).toBe("greenbelt");
    expect(zoningFamilyOf("D327")).toBe("factoryLocation");
    expect(zoningFamilyOf("D125")).toBe("useDistrict");
    expect(zoningFamilyOf("D999")).toBeUndefined();
  });

  it("says which toggles still wait for collection", () => {
    expect(ZONING_PENDING_LAYER_CODES).toEqual(["D054", "D125"]);
    expect(zoningFamilyPending("useDistrict")).toBe("partial");
    expect(zoningFamilyPending("forest")).toBe("all");
    expect(zoningFamilyPending("greenbelt")).toBe("none");
  });

  it("filters to the enabled families' layers, and to nothing with none enabled", () => {
    expect(zoningLayerFilter(new Set())).toEqual(["in", ["get", "layer_code"], ["literal", []]]);
    expect(zoningLayerFilter(new Set(["factoryLocation", "greenbelt"]))).toEqual([
      "in",
      ["get", "layer_code"],
      ["literal", ["D032", "D335", "D336", "D327"]],
    ]);
  });

  it("colours each family by its layer codes", () => {
    const expression = zoningColorExpression();
    expect(expression.slice(0, 2)).toEqual(["match", ["get", "layer_code"]]);
    // a label array and a colour per family, then the fallback
    expect(expression).toHaveLength(2 + ZONING_FAMILIES.length * 2 + 1);
    expect(expression[2]).toEqual(["D032"]);
  });
});

describe("zoning pending layers (root ADR-0373 §4)", () => {
  // The Foundation lane contract is the one list of layers it does not load yet; the map's list
  // must be the same, or a collected layer would still say 수집 대기 (or a missing one would not).
  const contract = JSON.parse(
    readFileSync(
      resolve(
        __dirname,
        "../../../../../../../platforms/foundation-platform/infra/lakehouse/contracts/vworld-land-use-zone-area-source-objects.json",
      ),
      "utf8",
    ),
  ) as { pending_regions: { region: string }[] };
  it("matches the lane contract's pending_regions", () => {
    const fromContract = contract.pending_regions.map((item) => `D${item.region}`).sort();
    expect([...ZONING_PENDING_LAYER_CODES].sort()).toEqual(fromContract);
  });
});
