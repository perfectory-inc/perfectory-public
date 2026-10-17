// apps/web/lib/complexes/panel-target.ts
//
// Which id a list row opens the `complex` panel with.
//
// A complex has one id (root ADR-0250): the UUIDv5 the `complex` vector tile publishes as its feature
// id and `GET /api/complexes/:lakehouse_complex_id` looks up. This function is the only place that
// turns a row into a panel entry, and it refuses anything not shaped like that id — the same check
// `lib/panel/codec.ts` applies when the URL is read back, so a row that could not survive the round
// trip is never rendered as clickable.

import { LAKEHOUSE_COMPLEX_ID_PATTERN } from "@/lib/identity/patterns";
import type { PanelStackEntry } from "@/lib/panel/types";
import type { ComplexListItem } from "./api";

/**
 * The panel entry a row opens, or `undefined` when its id is not a UUIDv5 — a contract break that
 * rendering as clickable would turn into a 404 for the reader.
 */
export function complexPanelEntry(
  item: Pick<ComplexListItem, "lakehouse_complex_id">,
): Extract<PanelStackEntry, { kind: "complex" }> | undefined {
  const id = item.lakehouse_complex_id;
  if (!LAKEHOUSE_COMPLEX_ID_PATTERN.test(id)) return undefined;
  return { kind: "complex", id, view: "summary" };
}
