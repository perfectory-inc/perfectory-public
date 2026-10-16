import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  CATALOG_OPENAPI,
  OUTPUT,
  render,
  valueLists,
} from "../../../../scripts/generate-catalog-values";

const document = JSON.parse(readFileSync(CATALOG_OPENAPI, "utf8"));

describe("catalog value lists — generated from the pinned Catalog contract", () => {
  it("are exactly what the generator writes from the pinned document", () => {
    // Stale after a contract change, or edited by hand: regenerate with
    // `node products/gongzzang/scripts/generate-catalog-values.ts`.
    expect(readFileSync(OUTPUT, "utf8").replaceAll("\r\n", "\n")).toBe(render(document));
  });

  it("fails loudly when the contract stops publishing a value list", () => {
    const { IndustrialComplexKindValue: _dropped, ...schemas } = document.components.schemas;
    expect(() => valueLists({ components: { schemas } })).toThrow(/IndustrialComplexKindValue/);
  });
});
