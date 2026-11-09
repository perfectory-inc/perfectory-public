// apps/web/lib/complexes/api.ts
import { z } from "zod";
import { type ComplexIndex, fetchComplexIndex, wholeSquareMetres } from "@/lib/api/complexes";
import type { ComplexFilters } from "@/lib/complexes/filters";

/** Rows per page. */
export const COMPLEX_PAGE_SIZE = 20;

/**
 * One row of the industrial-complex list.
 *
 * `.nullish()` where the API omits an absent column entirely (`skip_serializing_if`), so one branch
 * in the row handles both spellings of "the source did not state this". Zod's default object mode
 * strips undeclared keys, so a column the API grows tomorrow reaches no row until it is named here.
 *
 * `lakehouse_complex_id` is required: every complex has one (root ADR-0250).
 */
export const ComplexListItemSchema = z.object({
  lakehouse_complex_id: z.string(),
  official_complex_code: z.string(),
  name: z.string(),
  kind: z.string(),
  status: z.string().nullish(),
  address_text: z.string().nullish(),
});

export type ComplexListItem = z.infer<typeof ComplexListItemSchema>;

/** One 시도 filter option: code, its current name from the 법정동 code table, complex count. */
export const SidoFacetEntrySchema = z.object({
  sido_code: z.string(),
  sido_name: z.string().nullish(),
  complex_count: z.number().int(),
});

export type SidoFacetEntry = z.infer<typeof SidoFacetEntrySchema>;

export const ComplexesResponseSchema = z.object({
  complexes: z.array(ComplexListItemSchema),
  sido_facet: z.array(SidoFacetEntrySchema),
  total: z.number().int(),
  page: z.number().int(),
  size: z.number().int(),
  has_next: z.boolean(),
});

export type ComplexesResponse = z.infer<typeof ComplexesResponseSchema>;

type IndexRow = ComplexIndex["complexes"][number];

const byName = new Intl.Collator("ko-KR");

const SORTS: Record<ComplexFilters["sort"], (a: IndexRow, b: IndexRow) => number> = {
  name_asc: (a, b) => byName.compare(a.name, b.name),
  // A complex whose area the source did not state sorts last.
  area_desc: (a, b) =>
    (wholeSquareMetres(b.official_area_sqm) ?? -1) - (wholeSquareMetres(a.official_area_sqm) ?? -1),
  official_complex_code_asc: (a, b) =>
    a.official_complex_code.localeCompare(b.official_complex_code),
};

/**
 * One page of the list, read from the index on the parcel edge (root ADR-0456): every complex is in
 * the one document (1,442 on 2026-11-09), so the search, the filters, the 시도 facet and the pages
 * run here. The facet counts the complexes the search and the status filter leave, before the 시도
 * filter, so choosing a 시도 does not hide the others.
 */
export function pageOfComplexes(index: ComplexIndex, filters: ComplexFilters): ComplexesResponse {
  const q = filters.q.trim().toLowerCase();
  const matching = index.complexes.filter(
    (row) =>
      (q === "" || row.name.toLowerCase().includes(q) || row.official_complex_code.includes(q)) &&
      (filters.statuses.length === 0 ||
        (filters.statuses as readonly string[]).includes(row.status)),
  );
  const facet = new Map<string, SidoFacetEntry>();
  for (const row of matching) {
    if (!row.sido_code) continue;
    const entry = facet.get(row.sido_code) ?? {
      sido_code: row.sido_code,
      sido_name: row.sido_name,
      complex_count: 0,
    };
    entry.complex_count += 1;
    facet.set(row.sido_code, entry);
  }
  const rows = matching
    .filter((row) => filters.sidoCode === undefined || row.sido_code === filters.sidoCode)
    .sort(SORTS[filters.sort]);
  const start = filters.page * COMPLEX_PAGE_SIZE;
  return {
    complexes: rows.slice(start, start + COMPLEX_PAGE_SIZE).map((row) => ({
      lakehouse_complex_id: row.complex_id,
      official_complex_code: row.official_complex_code,
      name: row.name,
      kind: row.kind,
      status: row.status,
      address_text: row.address_text,
    })),
    sido_facet: [...facet.values()].sort((a, b) => a.sido_code.localeCompare(b.sido_code)),
    total: rows.length,
    page: filters.page,
    size: COMPLEX_PAGE_SIZE,
    has_next: start + COMPLEX_PAGE_SIZE < rows.length,
  };
}

export async function fetchComplexes(
  filters: ComplexFilters,
  signal?: AbortSignal,
): Promise<ComplexesResponse> {
  return pageOfComplexes(await fetchComplexIndex(signal), filters);
}
