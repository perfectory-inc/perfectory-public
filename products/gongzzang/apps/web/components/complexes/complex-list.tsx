"use client";
import { Separator, Skeleton } from "@gongzzang/ui";
import { useTranslations } from "next-intl";
import { Fragment } from "react";
import { ComplexFilterBar } from "@/components/complexes/complex-filter-bar";
import { ComplexPager } from "@/components/complexes/complex-pager";
import { ComplexRow } from "@/components/complexes/complex-row";
import { ComplexSearchBar } from "@/components/complexes/complex-search-bar";
import { COMPLEX_PAGE_SIZE, type ComplexesResponse } from "@/lib/complexes/api";
import { useComplexFilters } from "@/lib/complexes/use-complex-filters";
import { useComplexesQuery } from "@/lib/complexes/use-complexes-query";

const SKELETON_KEYS = ["sk-0", "sk-1", "sk-2", "sk-3", "sk-4", "sk-5", "sk-6", "sk-7"] as const;

/**
 * The industrial-complex list: search box, filters, rows, pager.
 *
 * Header, filter bar and list follow `app/(authenticated)/listings/page.tsx` — same header/filter/
 * list stacking, same tokens, no new design vocabulary.
 */
export function ComplexList() {
  const t = useTranslations("complexes");
  const { filters, setSearch, patch, goToPage } = useComplexFilters();
  const query = useComplexesQuery(filters);
  const page = query.data;

  return (
    <div className="flex h-full flex-col bg-[var(--color-canvas)]">
      <header className="flex items-center justify-between gap-6 px-6 py-4">
        <h1 className="whitespace-nowrap text-[length:var(--text-title-lg)] font-semibold tracking-[var(--tracking-display-sm)] text-[var(--color-ink)]">
          {t("page.title")}
        </h1>
        <div className="max-w-md flex-1">
          <ComplexSearchBar value={filters.q} onSearch={setSearch} />
        </div>
      </header>
      <Separator />
      <ComplexFilterBar
        filters={filters}
        total={page?.total}
        shown={page?.complexes.length ?? 0}
        onPatch={patch}
      />
      <Separator />
      <div className="flex-1 overflow-y-auto">
        <ComplexListBody
          page={page}
          isPending={query.isPending}
          isError={query.isError}
          onGoToPage={goToPage}
        />
      </div>
    </div>
  );
}

/**
 * The three states the list can be in, and they are three different screens.
 *
 * "Nothing has arrived yet" is decided by `isPending`, never by an empty array: a skeleton drawn
 * over an empty result set says "keep waiting" about an answer that already arrived, and an
 * empty-state drawn while loading says "there are none" about a question nobody has answered.
 */
function ComplexListBody({
  page,
  isPending,
  isError,
  onGoToPage,
}: {
  page: ComplexesResponse | undefined;
  isPending: boolean;
  isError: boolean;
  onGoToPage: (page: number) => void;
}) {
  const t = useTranslations("complexes");

  if (isPending) {
    return (
      <div className="flex flex-col gap-3 p-5" data-testid="complex-list-loading">
        {SKELETON_KEYS.map((key) => (
          <Skeleton key={key} className="h-14 w-full" />
        ))}
      </div>
    );
  }

  if (isError || !page) {
    return (
      <p
        className="p-8 text-center text-[length:var(--text-body-sm)] text-[var(--color-error)]"
        data-testid="complex-list-error"
      >
        {t("errors.fetchFailed")}
      </p>
    );
  }

  if (page.complexes.length === 0) {
    return (
      <p
        className="p-8 text-center text-[length:var(--text-body-sm)] text-[var(--color-muted)]"
        data-testid="complex-list-empty"
      >
        {t("empty")}
      </p>
    );
  }

  const pageCount = Math.max(1, Math.ceil(page.total / COMPLEX_PAGE_SIZE));

  return (
    <>
      <ul className="flex flex-col">
        {page.complexes.map((complex, index) => (
          <Fragment key={complex.official_complex_code}>
            {index > 0 && <Separator />}
            <li>
              <ComplexRow item={complex} />
            </li>
          </Fragment>
        ))}
      </ul>
      <Separator />
      <ComplexPager page={page.page} pageCount={pageCount} onGoToPage={onGoToPage} />
    </>
  );
}
