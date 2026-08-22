// apps/web/lib/complexes/wire-values.ts
//
// The Catalog wire values an industrial complex carries, and the one place that turns them into
// Korean.
//
// Both surfaces that show a complex — the list row and the `complex` panel's summary card — have to
// say `national` as 국가산업단지 and `operating` as 운영중. Two copies of that mapping is a defect on
// its own terms (root AGENTS.md ★ 2): the copies drift, and nothing decides which one is right.
// So the value sets live here and the message keys live under one i18n namespace, `complexValues`.
//
// A value outside the known set falls through to the source's own string rather than rendering a
// message-key path. Showing what the source said is honest; inventing a label for it is not.

/** Canonical industrial-complex classifications, as `catalog_domain::IndustrialComplexKind`. */
export const COMPLEX_KINDS = ["national", "general", "agricultural", "urban_high_tech"] as const;

/**
 * Development lifecycle values, as `catalog_domain::IndustrialComplexStatus`.
 *
 * `unknown` is in the set on purpose: per the Catalog contract it means the source stated a
 * lifecycle this contract does not recognize, while an absent status means it stated none.
 */
export const COMPLEX_STATUSES = [
  "planned",
  "developing",
  "operating",
  "changed",
  "abolished",
  "unknown",
] as const;

/** Lot sale/lease progress values, as `catalog_domain::IndustrialComplexLotSalesStatus`. */
export const LOT_SALES_STATUSES = ["planned", "in_progress", "completed"] as const;

export type ComplexKind = (typeof COMPLEX_KINDS)[number];
export type ComplexStatus = (typeof COMPLEX_STATUSES)[number];
export type LotSalesStatus = (typeof LOT_SALES_STATUSES)[number];

/** The i18n namespace holding one label per wire value above. */
export const COMPLEX_VALUES_NAMESPACE = "complexValues";

export function isComplexKind(value: string): value is ComplexKind {
  return (COMPLEX_KINDS as readonly string[]).includes(value);
}

export function isComplexStatus(value: string): value is ComplexStatus {
  return (COMPLEX_STATUSES as readonly string[]).includes(value);
}

export function isLotSalesStatus(value: string): value is LotSalesStatus {
  return (LOT_SALES_STATUSES as readonly string[]).includes(value);
}

/**
 * A value the source actually stated. Blank-but-present text is not a statement.
 */
export function stated(value: string | null | undefined): string | undefined {
  if (value == null) return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

/** A translator scoped to {@link COMPLEX_VALUES_NAMESPACE}. */
type ValueTranslator = (key: string) => string;

export function complexKindLabel(
  t: ValueTranslator,
  raw: string | null | undefined,
): string | undefined {
  const value = stated(raw);
  if (value === undefined) return undefined;
  return isComplexKind(value) ? t(`kind.${value}`) : value;
}

export function complexStatusLabel(
  t: ValueTranslator,
  raw: string | null | undefined,
): string | undefined {
  const value = stated(raw);
  if (value === undefined) return undefined;
  return isComplexStatus(value) ? t(`status.${value}`) : value;
}

export function lotSalesStatusLabel(
  t: ValueTranslator,
  raw: string | null | undefined,
): string | undefined {
  const value = stated(raw);
  if (value === undefined) return undefined;
  return isLotSalesStatus(value) ? t(`lotSales.${value}`) : value;
}
