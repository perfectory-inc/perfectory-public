// apps/web/lib/complexes/panel-target.ts
//
// Which id a list row opens the `complex` panel with, and why it is not the obvious one.
//
// A complex carries two identifiers that are not computable from each other (root ADR-0043,
// ADR-0048): `catalog.industrial_complex.id`, minted with `Uuid::now_v7()`, and the lakehouse
// `complex_id`, derived as a UUIDv5 in the Bronze-to-Silver job. The panel is keyed on the second —
// it is what the `complex` vector tile publishes as its feature id and what
// `GET /api/complexes/:lakehouse_complex_id` looks up. Handing the panel the first one produces a
// 404 that reads as "no such complex".
//
// So the list response carries `lakehouse_complex_id` and this function is the only place that
// turns a row into a panel entry. It refuses anything that is not shaped like a lakehouse id, which
// is the same check `lib/panel/codec.ts` applies when the URL is read back — a row that could not
// survive the round trip must not be rendered as clickable in the first place.

import { LAKEHOUSE_COMPLEX_ID_PATTERN } from "@/lib/identity/patterns";
import type { PanelStackEntry } from "@/lib/panel/types";
import type { ComplexListItem } from "./api";

/**
 * The panel entry a row opens, or `undefined` when the row cannot open one.
 *
 * Two reasons a row cannot, and they are different facts:
 * - the complex has no lakehouse identity at all (six of the 1,448 canonical rows on 2026-08-23,
 *   registered through the Catalog write API rather than loaded from a Gold snapshot). Such a
 *   complex is in no tile and has no panel to open.
 * - the value is present but is not a UUIDv5. That is an identity-space mix-up, not a missing row,
 *   and rendering it as clickable would turn a contract break into a 404 for the reader.
 */
export function complexPanelEntry(
  item: Pick<ComplexListItem, "lakehouse_complex_id">,
): Extract<PanelStackEntry, { kind: "complex" }> | undefined {
  const id = item.lakehouse_complex_id?.trim();
  if (!id || !LAKEHOUSE_COMPLEX_ID_PATTERN.test(id)) return undefined;
  return { kind: "complex", id, view: "summary" };
}
