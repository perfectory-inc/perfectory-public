// apps/web/lib/complexes/sido.ts
//
// The seventeen province codes the 시도 filter offers, in administrative-code order.
//
// Codes here, names in `complexes.sido.<code>` of `lib/i18n/ko.json` — the same split the listing
// filter bar already uses for `type`/`transaction`/`sort`, and the reason is the same: a Korean
// label written in a component is a label no translation pass can find.
//
// `51` and `52` rather than `42` and `45`: 강원 and 전북 became 특별자치도 and took new codes, and the
// canonical `catalog.industrial_complex.sido_code` column carries the new ones (verified 2026-08-23
// against the 1,448 canonical rows — the distinct set is exactly this list).

/** 시도 codes, ascending, as the 행정안전부 standard assigns them. */
export const SIDO_CODES = [
  "11",
  "26",
  "27",
  "28",
  "29",
  "30",
  "31",
  "36",
  "41",
  "43",
  "44",
  "46",
  "47",
  "48",
  "50",
  "51",
  "52",
] as const;

export type SidoCode = (typeof SIDO_CODES)[number];

export function isSidoCode(value: string): value is SidoCode {
  return (SIDO_CODES as readonly string[]).includes(value);
}
