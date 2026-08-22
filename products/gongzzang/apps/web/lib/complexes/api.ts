// apps/web/lib/complexes/api.ts
import { z } from "zod";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";
import type { ComplexFilters } from "@/lib/complexes/filters";

/** Rows per page. Also the API default; stated here so the URL and the request cannot disagree. */
export const COMPLEX_PAGE_SIZE = 20;

/**
 * One row of the industrial-complex list.
 *
 * `.nullish()` where the API omits an absent column entirely (`skip_serializing_if`), so one branch
 * in the row handles both spellings of "the source did not state this". Zod's default object mode
 * strips undeclared keys, so a column the API grows tomorrow reaches no row until it is named here.
 *
 * `lakehouse_complex_id` is optional because it genuinely is: six of the 1,448 canonical complexes
 * were registered through the write API and carry none. That is what makes a row unopenable, and it
 * is the reason this is a nullish field rather than a required one — see `complex-row.tsx`.
 */
export const ComplexListItemSchema = z.object({
  lakehouse_complex_id: z.string().nullish(),
  official_complex_code: z.string(),
  name: z.string(),
  kind: z.string(),
  status: z.string().nullish(),
  address_text: z.string().nullish(),
});

export type ComplexListItem = z.infer<typeof ComplexListItemSchema>;

export const ComplexesResponseSchema = z.object({
  complexes: z.array(ComplexListItemSchema),
  total: z.number().int(),
  page: z.number().int(),
  size: z.number().int(),
  has_next: z.boolean(),
});

export type ComplexesResponse = z.infer<typeof ComplexesResponseSchema>;

export function toComplexSearchParams(filters: ComplexFilters): URLSearchParams {
  const sp = new URLSearchParams();
  const q = filters.q.trim();
  if (q !== "") sp.set("q", q);
  if (filters.sidoCode) sp.set("sido_code", filters.sidoCode);
  if (filters.statuses.length > 0) sp.set("status", filters.statuses.join(","));
  sp.set("sort", filters.sort);
  sp.set("page", String(filters.page));
  sp.set("size", String(COMPLEX_PAGE_SIZE));
  return sp;
}

export async function fetchComplexes(
  filters: ComplexFilters,
  signal?: AbortSignal,
): Promise<ComplexesResponse> {
  const json = await apiProxyClient.complexesCollectionRead.getJson<unknown>({
    searchParams: toComplexSearchParams(filters),
    signal,
  });
  return ComplexesResponseSchema.parse(json);
}
