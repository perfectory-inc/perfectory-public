"use client";
import { Button } from "@gongzzang/ui";
import { useTranslations } from "next-intl";

/**
 * Previous/next paging with the reader's position stated in words.
 *
 * Numbered page links are not offered: at 20 rows a page the collection is 73 pages, and a strip of
 * 73 links is a navigation problem of its own. What a reader of an alphabetical directory actually
 * does is search, then step — so stepping is what this offers, with "3 / 73 쪽" so the step has a
 * frame of reference.
 */
export function ComplexPager({
  page,
  pageCount,
  onGoToPage,
}: {
  /** Zero-indexed current page. */
  page: number;
  /** Total pages behind the current filters. Always at least 1. */
  pageCount: number;
  onGoToPage: (page: number) => void;
}) {
  const t = useTranslations("complexes.pager");
  if (pageCount <= 1) return null;

  return (
    <nav
      className="flex items-center justify-center gap-4 px-6 py-5"
      aria-label={t("label")}
      data-testid="complex-pager"
    >
      <Button
        variant="secondary"
        size="sm"
        disabled={page <= 0}
        onClick={() => onGoToPage(page - 1)}
      >
        {t("previous")}
      </Button>
      <span
        className="text-[length:var(--text-body-sm)] text-[var(--color-muted)]"
        aria-live="polite"
      >
        {t("position", { page: page + 1, pageCount })}
      </span>
      <Button
        variant="secondary"
        size="sm"
        disabled={page >= pageCount - 1}
        onClick={() => onGoToPage(page + 1)}
      >
        {t("next")}
      </Button>
    </nav>
  );
}
