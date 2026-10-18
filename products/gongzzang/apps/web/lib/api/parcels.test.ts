import { describe, expect, it } from "vitest";

import { jibunOf, parseParcelDocument } from "@/lib/api/parcels";

// Repository-reserved synthetic PNUs (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";

describe("parseParcelDocument", () => {
  it("keeps the sections the panel reads and ignores the rest", () => {
    const document = parseParcelDocument({
      schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
      pnu: PNU,
      source: { table: "gold.parcel_panel" },
      zonings: [{ zone_code: "UQA122", zone_name: "제2종일반주거지역", inclusion_code: "1" }],
      price: {
        price_per_m2: 1_720_000,
        base_year: 2026,
        base_month: 1,
        announced_date: "2026-04-30",
      },
      characteristics: { land_category: "대", area_m2: 120.5 },
      forest_ledger: { ownership_kind: "06", co_owner_count: 0 },
    });

    expect(document.zonings?.[0]?.inclusion_code).toBe("1");
    expect(document.price?.announced_date).toBe("2026-04-30");
    // The ledger's code is not the panel's: only the source's name for it is read.
    expect(document.forest_ledger).toEqual({ co_owner_count: 0 });
    expect(document.legal_dong).toBeUndefined();
  });

  it("tolerates a parcel whose optional sections are absent", () => {
    const document = parseParcelDocument({
      schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
      pnu: PNU,
    });
    expect(document.price).toBeUndefined();
    expect(document.zonings).toBeUndefined();
  });
});

describe("jibunOf", () => {
  it.each([
    ["9999900000100120000", "12"],
    ["9999900000100120003", "12-3"],
    ["9999900000204450003", "산 445-3"],
  ])("%s reads as %s", (pnu, expected) => {
    expect(jibunOf(pnu, "산")).toBe(expected);
  });
});
