// apps/web/lib/complexes/filters.ts
//
// The industrial-complex screen's state lives in the URL, not in a store.
//
// `lib/listings/filters.ts` is the shape this follows — same parse/serialize pair, same
// "a default is absent from the query string" rule. The difference is that this screen actually
// navigates on every change, because a complex is opened as a panel (`?p=…`) on top of the list and
// the browser Back button has to land the reader back on the page and filter they came from.
//
// Parameter names match the API (`q`, `sido_code`, `status`, `sort`, `page`) so a URL a reader
// copies out of the address bar is readable against the route it feeds.

import { isSidoCode, type SidoCode } from "./sido";
import { COMPLEX_STATUSES, type ComplexStatus } from "./wire-values";

export type ComplexSortKey = "name_asc" | "area_desc" | "official_complex_code_asc";

/** Orders this screen offers. The set the Catalog `listComplexes` route serves. */
export const COMPLEX_SORTS: readonly ComplexSortKey[] = [
  "name_asc",
  "area_desc",
  "official_complex_code_asc",
];

/**
 * Statuses the filter offers.
 *
 * Three of the domain's six. `changed`, `abolished` and `unknown` are not offered because no
 * canonical row carries them today (1,448 rows on 2026-08-23: 1,069 operating, 289 developing,
 * 84 planned, 6 with no status), and a filter chip that can only ever return nothing teaches the
 * reader that the screen is broken. The wire contract still admits all six — `wire-values.ts` is
 * the full set, and a row arriving with one of the other three still renders its own label.
 */
export const COMPLEX_STATUS_FILTERS: readonly ComplexStatus[] = [
  "operating",
  "developing",
  "planned",
];

export const DEFAULT_COMPLEX_SORT: ComplexSortKey = "name_asc";

export interface ComplexFilters {
  /** Name or official-code substring. Empty string means "no search word". */
  q: string;
  /** Province filter, or `undefined` for every province. */
  sidoCode: SidoCode | undefined;
  /** Development lifecycle filter. Empty means every value. */
  statuses: ComplexStatus[];
  /** Row order. */
  sort: ComplexSortKey;
  /** Zero-indexed page. */
  page: number;
}

export const DEFAULT_COMPLEX_FILTERS: ComplexFilters = {
  q: "",
  sidoCode: undefined,
  statuses: [],
  sort: DEFAULT_COMPLEX_SORT,
  page: 0,
};

function parseStatuses(raw: string | null): ComplexStatus[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value): value is ComplexStatus =>
      (COMPLEX_STATUSES as readonly string[]).includes(value),
    );
}

function parsePage(raw: string | null): number {
  if (raw === null || raw === "") return 0;
  const parsed = Number(raw);
  // A hand-edited `page=-1` or `page=abc` is page 0, not a request the API has to defend against.
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
}

export function parseComplexFiltersFromSearchParams(sp: URLSearchParams): ComplexFilters {
  const sortRaw = sp.get("sort");
  const sort: ComplexSortKey = COMPLEX_SORTS.includes(sortRaw as ComplexSortKey)
    ? (sortRaw as ComplexSortKey)
    : DEFAULT_COMPLEX_SORT;
  const sidoRaw = sp.get("sido_code");

  return {
    q: sp.get("q")?.trim() ?? "",
    sidoCode: sidoRaw !== null && isSidoCode(sidoRaw) ? sidoRaw : undefined,
    statuses: parseStatuses(sp.get("status")),
    sort,
    page: parsePage(sp.get("page")),
  };
}

/**
 * Writes the filters onto a copy of the current query string, leaving every other key alone.
 *
 * `?p=` — the panel stack — is the other key that lives here, and a filter change must not close an
 * open panel by dropping it. Defaults are deleted rather than written so a pristine screen has a
 * bare `/complexes` URL.
 */
export function applyComplexFiltersToSearchParams(
  current: URLSearchParams,
  filters: ComplexFilters,
): URLSearchParams {
  const next = new URLSearchParams(current.toString());
  setOrDelete(next, "q", filters.q.trim() === "" ? undefined : filters.q.trim());
  setOrDelete(next, "sido_code", filters.sidoCode);
  setOrDelete(next, "status", filters.statuses.length > 0 ? filters.statuses.join(",") : undefined);
  setOrDelete(next, "sort", filters.sort === DEFAULT_COMPLEX_SORT ? undefined : filters.sort);
  setOrDelete(next, "page", filters.page === 0 ? undefined : String(filters.page));
  return next;
}

function setOrDelete(sp: URLSearchParams, key: string, value: string | undefined): void {
  if (value === undefined) sp.delete(key);
  else sp.set(key, value);
}

/**
 * Whether a filter change should return the reader to the first page.
 *
 * Anything that changes which rows match does: staying on page 40 of a collection that just became
 * three rows long shows an empty screen and reads as "no results".
 */
export function narrowingChanged(before: ComplexFilters, after: ComplexFilters): boolean {
  return (
    before.q !== after.q ||
    before.sidoCode !== after.sidoCode ||
    before.sort !== after.sort ||
    before.statuses.join(",") !== after.statuses.join(",")
  );
}
