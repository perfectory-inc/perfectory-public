// apps/web/lib/complexes/use-complex-filters.ts
"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useCallback, useMemo } from "react";
import {
  applyComplexFiltersToSearchParams,
  type ComplexFilters,
  narrowingChanged,
  parseComplexFiltersFromSearchParams,
} from "./filters";

/**
 * The screen's filters, read from and written to the URL.
 *
 * The URL is the state — there is no store copy, the same way `lib/panel/use-panel-stack.ts` keeps
 * the panel stack. That is what makes Back work: the reader opens a complex (a `?p=` push), presses
 * Back, and lands on the list they were reading rather than on a screen that reset itself.
 *
 * Two navigation modes, and the difference is whether the reader performed an act they might want
 * undone:
 * - `replace` for typing. A debounced search box would otherwise leave one history entry per pause,
 *   and Back would walk the reader backwards through their own word.
 * - `push` for choosing a province, a status, an order, or a page. Those are decisions, and Back is
 *   how a reader undoes a decision.
 */
export interface UseComplexFiltersResult {
  filters: ComplexFilters;
  /** Replaces the search word without adding a history entry. Resets to the first page. */
  setSearch: (q: string) => void;
  /** Applies a filter decision and adds a history entry. Resets to the first page when narrowed. */
  patch: (patch: Partial<ComplexFilters>) => void;
  /** Moves to a page and adds a history entry. */
  goToPage: (page: number) => void;
}

export function useComplexFilters(): UseComplexFiltersResult {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const search = searchParams.toString();

  const filters = useMemo(
    () => parseComplexFiltersFromSearchParams(new URLSearchParams(search)),
    [search],
  );

  const navigate = useCallback(
    (next: ComplexFilters, mode: "push" | "replace") => {
      const sp = applyComplexFiltersToSearchParams(new URLSearchParams(search), next);
      const qs = sp.toString();
      // Next.js typed routes cannot type a query string built at runtime.
      const url = `${pathname}${qs ? `?${qs}` : ""}` as never;
      if (mode === "push") router.push(url, { scroll: false });
      else router.replace(url, { scroll: false });
    },
    [pathname, router, search],
  );

  const setSearch = useCallback(
    (q: string) => {
      if (q.trim() === filters.q) return;
      navigate({ ...filters, q, page: 0 }, "replace");
    },
    [filters, navigate],
  );

  const patch = useCallback(
    (change: Partial<ComplexFilters>) => {
      const next = { ...filters, ...change };
      navigate(narrowingChanged(filters, next) ? { ...next, page: 0 } : next, "push");
    },
    [filters, navigate],
  );

  const goToPage = useCallback(
    (page: number) => {
      if (page === filters.page) return;
      navigate({ ...filters, page }, "push");
    },
    [filters, navigate],
  );

  return { filters, setSearch, patch, goToPage };
}
