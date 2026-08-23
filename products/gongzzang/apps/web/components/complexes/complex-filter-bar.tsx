"use client";
import {
  MultiSelect,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@gongzzang/ui";
import { useTranslations } from "next-intl";
import {
  COMPLEX_SORTS,
  COMPLEX_STATUS_FILTERS,
  type ComplexFilters,
  type ComplexSortKey,
} from "@/lib/complexes/filters";
import { isSidoCode, SIDO_CODES } from "@/lib/complexes/sido";
import type { ComplexStatus } from "@/lib/complexes/wire-values";

/**
 * Radix Select refuses an empty option value, so "every province" needs a name of its own.
 * It is not a province code — `isSidoCode` rejects it — so it cannot be mistaken for one.
 */
const ANY_SIDO = "all";

/**
 * 시도 / 상태 / 정렬 filter row, plus the count.
 *
 * Same shape as the listing filter bar (`components/listings/filter-bar.tsx`): a horizontal row of
 * labelled controls, the sort pushed to the right, all colour and size from `var(--color-*)` and
 * `var(--text-*)` tokens.
 */
export function ComplexFilterBar({
  filters,
  total,
  shown,
  onPatch,
}: {
  filters: ComplexFilters;
  /** Complexes matching the filters, or `undefined` before the first page arrives. */
  total: number | undefined;
  /** Rows on the current page. */
  shown: number;
  onPatch: (patch: Partial<ComplexFilters>) => void;
}) {
  const t = useTranslations("complexes");
  const tSido = useTranslations("complexes.sido");
  const tStatus = useTranslations("complexValues.status");

  const statusOptions = COMPLEX_STATUS_FILTERS.map((value) => ({
    value,
    label: tStatus(value),
  }));

  const labelClass =
    "text-[length:var(--text-caption-uppercase)] font-medium tracking-[var(--tracking-uppercase)] uppercase text-[var(--color-muted)] whitespace-nowrap";

  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-3 bg-[var(--color-canvas)] px-6 py-3.5">
      <div className="flex items-center gap-3">
        <span className={labelClass}>{t("filter.sido")}</span>
        <Select
          value={filters.sidoCode ?? ANY_SIDO}
          onValueChange={(next) => onPatch({ sidoCode: isSidoCode(next) ? next : undefined })}
        >
          <SelectTrigger className="h-9 w-40" aria-label={t("filter.sido")}>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY_SIDO}>{t("filter.anySido")}</SelectItem>
            {SIDO_CODES.map((code) => (
              <SelectItem key={code} value={code}>
                {tSido(code)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center gap-3">
        <span className={labelClass}>{t("filter.status")}</span>
        <MultiSelect
          options={statusOptions}
          value={filters.statuses}
          onValueChange={(next) => onPatch({ statuses: next as ComplexStatus[] })}
        />
      </div>

      <div className="ml-auto flex items-center gap-4">
        {total !== undefined && (
          <span
            className="whitespace-nowrap text-[length:var(--text-body-sm)] text-[var(--color-muted)]"
            data-testid="complex-count"
          >
            {t("countOfTotal", {
              total: total.toLocaleString("ko-KR"),
              shown: shown.toLocaleString("ko-KR"),
            })}
          </span>
        )}
        <div className="flex items-center gap-3">
          <span className={labelClass}>{t("filter.sort")}</span>
          <Select
            value={filters.sort}
            onValueChange={(next) => onPatch({ sort: next as ComplexSortKey })}
          >
            <SelectTrigger className="h-9 w-44" aria-label={t("filter.sort")}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {COMPLEX_SORTS.map((sort) => (
                <SelectItem key={sort} value={sort}>
                  {t(`sort.${sort}`)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  );
}
