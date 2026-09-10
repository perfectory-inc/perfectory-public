import { describe, expect, it } from "vitest";

import { type EdgeParcelProfile, toParcelInfo } from "@/lib/api/parcels";

// PNU in the repository-reserved synthetic namespace.
// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";

describe("toParcelInfo — edge document maps to the panel view model", () => {
  it("carries zoning, price, and land category from the baked sections", () => {
    const profile: EdgeParcelProfile = {
      schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
      pnu: PNU,
      zonings: [
        { zone_code: "UQA121", zone_name: "제1종일반주거지역" },
        { zone_code: "UQA122", zone_name: "제2종일반주거지역" },
      ],
      price: { price_per_m2: 1_720_000, base_year: 2026, base_month: 1 },
      characteristics: { land_category: "대" },
    };

    const info = toParcelInfo(profile);

    expect(info.pnu).toBe(PNU);
    // Codes are derived from the PNU; the first ten digits are the legal-dong code.
    expect(info.sido_code).toBe("99");
    expect(info.sigungu_code).toBe("99999");
    expect(info.eupmyeondong_code).toBe("99999000");
    expect(info.zoning).toBe("제1종일반주거지역"); // the anchor zoning, first in the array
    expect(info.official_land_price_per_m2).toBe(1_720_000);
    expect(info.gosi_year_month).toBe("2026-01"); // month is zero-padded
    expect(info.land_use_type).toBe("대");
    // Admin names are not in the public document — the panel falls back to the code heading.
    expect(info.sido_name).toBe("");
  });

  it("tolerates a parcel whose optional sections are absent", () => {
    const info = toParcelInfo({
      schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
      pnu: PNU,
    });

    expect(info.zoning).toBeNull();
    expect(info.official_land_price_per_m2).toBeNull();
    expect(info.gosi_year_month).toBeNull();
    expect(info.land_use_type).toBe("");
  });
});
