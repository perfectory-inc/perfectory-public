import { describe, expect, it } from "vitest";
import type { ComplexIndex } from "@/lib/api/complexes";
import {
  COMPLEX_PAGE_SIZE,
  ComplexesResponseSchema,
  ComplexListItemSchema,
  pageOfComplexes,
} from "@/lib/complexes/api";
import { DEFAULT_COMPLEX_FILTERS } from "@/lib/complexes/filters";

// Synthetic complexes in the reserved 99 시도 (no real code, name or id).
function row(n: number, overrides: Partial<ComplexIndex["complexes"][number]> = {}) {
  return {
    complex_id: `0196e7e0-3c20-5000-8000-${String(n).padStart(12, "0")}`,
    official_complex_code: `9999${String(n).padStart(2, "0")}`,
    name: `합성${n}일반산업단지`,
    kind: "general",
    status: "operating",
    sido_code: "99",
    sido_name: "합성시도",
    address_text: null,
    official_area_sqm: `${n}000.00`,
    profile_object_key: `gold/industrial-complex/profiles/0196e7e0-3c20-5000-9000-${String(n).padStart(12, "0")}.json`,
    ...overrides,
  };
}

function index(rows: ReturnType<typeof row>[]): ComplexIndex {
  return {
    schema_version: "foundation-platform.industrial_complex_index.v1",
    generation: 1,
    complexes: rows,
  } as ComplexIndex;
}

describe("pageOfComplexes", () => {
  const rows = Array.from({ length: COMPLEX_PAGE_SIZE + 5 }, (_, n) =>
    row(n + 1, n % 2 === 0 ? {} : { status: "developing", sido_code: "98", sido_name: null }),
  );

  it("pages the index, newest page flags has_next", () => {
    const first = pageOfComplexes(index(rows), DEFAULT_COMPLEX_FILTERS);
    expect(first.total).toBe(rows.length);
    expect(first.complexes).toHaveLength(COMPLEX_PAGE_SIZE);
    expect(first.has_next).toBe(true);
    const second = pageOfComplexes(index(rows), { ...DEFAULT_COMPLEX_FILTERS, page: 1 });
    expect(second.complexes).toHaveLength(5);
    expect(second.has_next).toBe(false);
  });

  it("searches name and code, filters status and 시도, and counts the facet before the 시도 filter", () => {
    const page = pageOfComplexes(index(rows), {
      ...DEFAULT_COMPLEX_FILTERS,
      statuses: ["operating"],
      sidoCode: "98",
    });
    // Every operating row is in 99, so the 98 filter leaves none, but the facet still offers 99.
    expect(page.total).toBe(0);
    expect(page.sido_facet).toEqual([
      { sido_code: "99", sido_name: "합성시도", complex_count: 13 },
    ]);
    const byCode = pageOfComplexes(index(rows), { ...DEFAULT_COMPLEX_FILTERS, q: " 999903 " });
    expect(byCode.complexes.map((c) => c.name)).toEqual(["합성3일반산업단지"]);
  });

  it("sorts by area, the unstated last", () => {
    const page = pageOfComplexes(index([row(1), row(2, { official_area_sqm: null }), row(3)]), {
      ...DEFAULT_COMPLEX_FILTERS,
      sort: "area_desc",
    });
    expect(page.complexes.map((c) => c.official_complex_code)).toEqual([
      "999903",
      "999901",
      "999902",
    ]);
  });
});

describe("ComplexesResponseSchema", () => {
  it("accepts a row whose absent columns are absent keys", () => {
    const row = ComplexListItemSchema.parse({
      lakehouse_complex_id: "024905cb-8a9f-54ef-ac7d-ca30d796a6b9",
      official_complex_code: "141010",
      name: "반월특수지역",
      kind: "national",
    });

    expect(row.address_text).toBeUndefined();
    expect(row.status).toBeUndefined();
  });

  it("keeps the total so the screen can say how much it is showing", () => {
    const page = ComplexesResponseSchema.parse({
      complexes: [],
      total: 1442,
      page: 0,
      size: 20,
      has_next: true,
      sido_facet: [
        { sido_code: "99", sido_name: "합성시도", complex_count: 3 },
        { sido_code: "98", complex_count: 1 },
      ],
      unexpected_column: "dropped",
    });

    expect(page.total).toBe(1442);
    expect(page.sido_facet.map((entry) => entry.sido_name ?? entry.sido_code)).toEqual([
      "합성시도",
      "98",
    ]);
    expect(page).not.toHaveProperty("unexpected_column");
  });
});
