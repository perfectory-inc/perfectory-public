"use client";
import { useTranslations } from "next-intl";
import type { ComplexListItem } from "@/lib/complexes/api";
import { complexPanelEntry } from "@/lib/complexes/panel-target";
import {
  COMPLEX_VALUES_NAMESPACE,
  complexKindLabel,
  complexStatusLabel,
  stated,
} from "@/lib/complexes/wire-values";
import { usePanelStack } from "@/lib/panel/use-panel-stack";

/**
 * One complex in the list.
 *
 * Two lines: the name, then the facts that tell one 산업단지 from another with the same word in it —
 * where it is, what kind it is, what state it is in. A fact the source did not state contributes no
 * separator and no gap; the row is the pieces that exist, joined. 1,442 complexes have an address
 * and six do not, and the six must not read as "address: (blank)".
 *
 * 준공일 is deliberately absent. 26% of the canonical rows carry none, and a date shown on three
 * rows out of four makes the fourth look broken rather than unstated — while not being what someone
 * scanning for a name is reading for. The panel this row opens shows it.
 */
export function ComplexRow({ item }: { item: ComplexListItem }) {
  const t = useTranslations("complexes.row");
  const tValues = useTranslations(COMPLEX_VALUES_NAMESPACE);
  const { push } = usePanelStack();

  const entry = complexPanelEntry(item);
  const meta = [
    stated(item.address_text),
    complexKindLabel(tValues, item.kind),
    complexStatusLabel(tValues, item.status),
  ].filter((piece): piece is string => piece !== undefined);

  const body = (
    <>
      <span className="text-[length:var(--text-body)] font-medium text-[var(--color-ink)]">
        {item.name}
      </span>
      {meta.length > 0 && (
        <span className="text-[length:var(--text-body-sm)] text-[var(--color-muted)]">
          {meta.join(" · ")}
        </span>
      )}
    </>
  );

  if (!entry) {
    return (
      <div className="flex flex-col gap-1 px-6 py-4" data-testid="complex-row-unopenable">
        {body}
        <span className="text-[length:var(--text-caption)] text-[var(--color-muted)]">
          {t("noDetail")}
        </span>
      </div>
    );
  }

  return (
    <button
      type="button"
      onClick={() => push(entry)}
      className="flex w-full flex-col gap-1 px-6 py-4 text-left transition-colors hover:bg-[var(--color-surface-soft)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-primary)]/30"
      data-testid="complex-row"
    >
      {body}
    </button>
  );
}
