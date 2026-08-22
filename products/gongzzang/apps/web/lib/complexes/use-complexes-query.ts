// apps/web/lib/complexes/use-complexes-query.ts
"use client";

import { useQuery } from "@tanstack/react-query";
import { type ComplexesResponse, fetchComplexes } from "@/lib/complexes/api";
import type { ComplexFilters } from "@/lib/complexes/filters";

/**
 * One page of complexes for the current filters.
 *
 * `useQuery`, not `useInfiniteQuery`: this screen pages with buttons, and the page number is in the
 * URL. An infinite query would keep pages the URL no longer describes, so a reader arriving on
 * `?page=3` would see either page 3 alone (and the accumulated-pages type for nothing) or pages 0–3
 * concatenated, which is not what the URL says.
 *
 * `placeholderData: keepPreviousData` in spirit is deliberately NOT used: a page change replaces the
 * whole list, and showing the previous page under a new page number is the one thing a pager must
 * not do. The list renders its skeleton instead.
 */
export function useComplexesQuery(filters: ComplexFilters) {
  return useQuery<ComplexesResponse>({
    queryKey: ["complexes", filters],
    queryFn: ({ signal }) => fetchComplexes(filters, signal),
    // A designation record changes on the order of years. Same 5 minutes the complex panel uses.
    staleTime: 5 * 60_000,
  });
}
