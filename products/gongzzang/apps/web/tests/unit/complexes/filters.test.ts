import { describe, expect, it } from "vitest";
import {
  applyComplexFiltersToSearchParams,
  DEFAULT_COMPLEX_FILTERS,
  narrowingChanged,
  parseComplexFiltersFromSearchParams,
} from "@/lib/complexes/filters";

describe("parseComplexFiltersFromSearchParams", () => {
  it("returns the defaults for a bare URL", () => {
    expect(parseComplexFiltersFromSearchParams(new URLSearchParams())).toEqual(
      DEFAULT_COMPLEX_FILTERS,
    );
  });

  it("reads a Korean search word back out of the query string", () => {
    const sp = new URLSearchParams("q=%EB%B0%98%EC%9B%94");
    expect(parseComplexFiltersFromSearchParams(sp).q).toBe("반월");
  });

  it("drops values the screen does not serve rather than passing them on", () => {
    // A hand-edited URL is a caller like any other. What it must not do is reach the API as a
    // filter nobody offers and come back as an error the reader cannot act on.
    const sp = new URLSearchParams("sido_code=99&status=operating,sold_out&sort=price_asc&page=-3");
    const filters = parseComplexFiltersFromSearchParams(sp);

    expect(filters.sidoCode).toBeUndefined();
    expect(filters.statuses).toEqual(["operating"]);
    expect(filters.sort).toBe("name_asc");
    expect(filters.page).toBe(0);
  });
});

describe("applyComplexFiltersToSearchParams", () => {
  it("round-trips every filter", () => {
    const filters = {
      q: "반월",
      sidoCode: "41" as const,
      statuses: ["operating" as const, "developing" as const],
      sort: "area_desc" as const,
      page: 3,
    };

    const sp = applyComplexFiltersToSearchParams(new URLSearchParams(), filters);
    expect(parseComplexFiltersFromSearchParams(sp)).toEqual(filters);
  });

  it("leaves a bare URL bare", () => {
    const sp = applyComplexFiltersToSearchParams(new URLSearchParams(), DEFAULT_COMPLEX_FILTERS);
    expect(sp.toString()).toBe("");
  });

  it("keeps the open panel when a filter changes", () => {
    // `?p=` is the panel stack. A filter write that dropped it would close the panel the reader is
    // looking at, which is the one thing changing a filter must not do.
    const current = new URLSearchParams("p=complex%3A024905cb-8a9f-54ef-ac7d-ca30d796a6b9.summary");
    const sp = applyComplexFiltersToSearchParams(current, {
      ...DEFAULT_COMPLEX_FILTERS,
      sidoCode: "41",
    });

    expect(sp.get("p")).toBe("complex:024905cb-8a9f-54ef-ac7d-ca30d796a6b9.summary");
    expect(sp.get("sido_code")).toBe("41");
  });

  it("removes a filter that went back to its default", () => {
    const current = new URLSearchParams("sort=area_desc&page=4&q=%EB%B0%98%EC%9B%94");
    const sp = applyComplexFiltersToSearchParams(current, DEFAULT_COMPLEX_FILTERS);
    expect(sp.toString()).toBe("");
  });
});

describe("narrowingChanged", () => {
  it("is true when the matching set changes, so the reader returns to the first page", () => {
    // Staying on page 40 of a collection that just became three rows long renders an empty screen,
    // and an empty screen reads as "there are none".
    expect(
      narrowingChanged(DEFAULT_COMPLEX_FILTERS, { ...DEFAULT_COMPLEX_FILTERS, q: "반월" }),
    ).toBe(true);
    expect(
      narrowingChanged(DEFAULT_COMPLEX_FILTERS, { ...DEFAULT_COMPLEX_FILTERS, sidoCode: "41" }),
    ).toBe(true);
    expect(
      narrowingChanged(DEFAULT_COMPLEX_FILTERS, {
        ...DEFAULT_COMPLEX_FILTERS,
        statuses: ["operating"],
      }),
    ).toBe(true);
  });

  it("is false for a page move", () => {
    expect(narrowingChanged(DEFAULT_COMPLEX_FILTERS, { ...DEFAULT_COMPLEX_FILTERS, page: 2 })).toBe(
      false,
    );
  });
});
