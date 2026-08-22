import { describe, expect, it } from "vitest";
import {
  COMPLEX_PAGE_SIZE,
  ComplexesResponseSchema,
  ComplexListItemSchema,
  toComplexSearchParams,
} from "@/lib/complexes/api";
import { DEFAULT_COMPLEX_FILTERS } from "@/lib/complexes/filters";

describe("toComplexSearchParams", () => {
  it("asks for a page size the route serves", () => {
    // The screen never asks for more than the route's maximum, so `size` is a constant rather than
    // something a caller supplies. The route refuses anything above 100 independently — this is
    // the client half, not the guard.
    const sp = toComplexSearchParams(DEFAULT_COMPLEX_FILTERS);
    expect(sp.get("size")).toBe(String(COMPLEX_PAGE_SIZE));
    expect(Number(sp.get("size"))).toBeLessThanOrEqual(100);
    expect(sp.get("page")).toBe("0");
    expect(sp.get("sort")).toBe("name_asc");
    expect(sp.has("q")).toBe(false);
  });

  it("sends a Korean search word and the comma-separated status list", () => {
    const sp = toComplexSearchParams({
      q: "  반월  ",
      sidoCode: "41",
      statuses: ["operating", "developing"],
      sort: "area_desc",
      page: 2,
    });

    expect(sp.get("q")).toBe("반월");
    expect(sp.get("sido_code")).toBe("41");
    expect(sp.get("status")).toBe("operating,developing");
    expect(sp.get("page")).toBe("2");
    // URLSearchParams percent-encodes on serialization; the API decodes it back.
    expect(sp.toString()).toContain("q=%EB%B0%98%EC%9B%94");
  });
});

describe("ComplexesResponseSchema", () => {
  it("accepts a row whose absent columns are absent keys", () => {
    const row = ComplexListItemSchema.parse({
      official_complex_code: "141010",
      name: "반월특수지역",
      kind: "national",
    });

    expect(row.address_text).toBeUndefined();
    expect(row.status).toBeUndefined();
    expect(row.lakehouse_complex_id).toBeUndefined();
  });

  it("keeps the total so the screen can say how much it is showing", () => {
    const page = ComplexesResponseSchema.parse({
      complexes: [],
      total: 1442,
      page: 0,
      size: 20,
      has_next: true,
      unexpected_column: "dropped",
    });

    expect(page.total).toBe(1442);
    expect(page).not.toHaveProperty("unexpected_column");
  });
});
