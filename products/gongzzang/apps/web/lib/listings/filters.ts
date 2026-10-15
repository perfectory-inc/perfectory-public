import {
  type CardSearchSort,
  cardSearchSortValues,
  type ListingType,
  listingTypeValues,
  type TransactionType,
  transactionTypeValues,
} from "@gongzzang/api-types";

export type { ListingType, TransactionType };
export type SortKey = CardSearchSort;

export interface ListingFilters {
  types: ListingType[];
  transactions: TransactionType[];
  minAreaM2: number | undefined;
  maxAreaM2: number | undefined;
  minPriceKrw: number | undefined;
  maxPriceKrw: number | undefined;
  sort: SortKey;
  /** 행정구역 코드 prefix (시도 2 / 시군구 5 / 읍면동 8자리). */
  adminCode: string | undefined;
  /** 지목 (예: `factory_site`). */
  landUseType: string | undefined;
}

function parseList<T extends string>(raw: string | null, valid: readonly T[]): T[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s): s is T => valid.includes(s as T));
}

function parseNumber(raw: string | null): number | undefined {
  if (raw === null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

export function parseFiltersFromSearchParams(sp: URLSearchParams): ListingFilters {
  const sortRaw = sp.get("sort");
  const sort: SortKey = cardSearchSortValues.includes(sortRaw as SortKey)
    ? (sortRaw as SortKey)
    : "created_at_desc";

  return {
    types: parseList(sp.get("types"), listingTypeValues),
    transactions: parseList(sp.get("transaction"), transactionTypeValues),
    minAreaM2: parseNumber(sp.get("min_area_m2")),
    maxAreaM2: parseNumber(sp.get("max_area_m2")),
    minPriceKrw: parseNumber(sp.get("min_price_krw")),
    maxPriceKrw: parseNumber(sp.get("max_price_krw")),
    sort,
    adminCode: sp.get("admin_code") ?? undefined,
    landUseType: sp.get("land_use_type") ?? undefined,
  };
}

export function toSearchParams(f: ListingFilters): URLSearchParams {
  const sp = new URLSearchParams();
  if (f.types.length > 0) sp.set("types", f.types.join(","));
  if (f.transactions.length > 0) sp.set("transaction", f.transactions.join(","));
  if (f.minAreaM2 !== undefined) sp.set("min_area_m2", String(f.minAreaM2));
  if (f.maxAreaM2 !== undefined) sp.set("max_area_m2", String(f.maxAreaM2));
  if (f.minPriceKrw !== undefined) sp.set("min_price_krw", String(f.minPriceKrw));
  if (f.maxPriceKrw !== undefined) sp.set("max_price_krw", String(f.maxPriceKrw));
  if (f.sort !== "created_at_desc") sp.set("sort", f.sort);
  if (f.adminCode) sp.set("admin_code", f.adminCode);
  if (f.landUseType) sp.set("land_use_type", f.landUseType);
  return sp;
}
