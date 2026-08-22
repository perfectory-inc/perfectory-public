import { describe, expect, it } from "vitest";
import { ComplexListItemSchema } from "@/lib/complexes/api";
import { complexPanelEntry } from "@/lib/complexes/panel-target";
import { g1Codec } from "@/lib/panel/codec";

/**
 * The id a list row opens the panel with.
 *
 * A complex carries two identifiers that are not computable from each other, and the list response
 * is the only place a screen ever sees both worlds meet. Using the wrong one is a 404 that reads as
 * "no such complex" (root ADR-0048), so this file pins which one a row hands the panel.
 */

/** As a Gold snapshot mints it and as the `complex` tile publishes it: UUIDv5. */
const LAKEHOUSE_ID = "024905cb-8a9f-54ef-ac7d-ca30d796a6b9";
/** As `catalog.industrial_complex.id` mints it: `Uuid::now_v7()`. Same complex, other id space. */
const CATALOG_ID = "01a0136d-1b7c-7d61-9acc-fb2d2f64a146";

/** Rows are built through the schema, so a test cannot assert a shape the API layer cannot make. */
function row(overrides: Record<string, unknown> = {}) {
  return ComplexListItemSchema.parse({
    lakehouse_complex_id: LAKEHOUSE_ID,
    official_complex_code: "141010",
    name: "반월특수지역",
    kind: "national",
    ...overrides,
  });
}

describe("complexPanelEntry", () => {
  it("opens the panel on the id the list row carries", () => {
    expect(complexPanelEntry(row())).toEqual({
      kind: "complex",
      id: LAKEHOUSE_ID,
      view: "summary",
    });
  });

  it("survives the URL round trip the panel stack performs", () => {
    // The entry is serialized into `?p=` and read back by `lib/panel/codec.ts`, which re-checks the
    // id pattern. A row that produced an entry the codec then rejected would open a panel that
    // vanishes on the next render, so the two have to agree.
    const entry = complexPanelEntry(row());
    if (!entry) throw new Error("the fixture row must be openable");

    const parsed = g1Codec.deserialize(g1Codec.serialize({ v: 1, entries: [entry] }));
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;
    expect(parsed.value.entries[0]).toEqual(entry);
  });

  it("refuses the catalog id for the same complex", () => {
    // The disabling experiment: return `{ kind: "complex", id, view: "summary" }` without the
    // pattern check in `panel-target.ts` and this assertion goes red — and in the browser, every
    // such row opens a panel that fetches a 404.
    expect(complexPanelEntry({ lakehouse_complex_id: CATALOG_ID })).toBeUndefined();

    // …and the reason it must be refused: the panel stack cannot carry it either.
    const forged = g1Codec.deserialize(`complex:${CATALOG_ID}.summary`);
    expect(forged.ok).toBe(false);
  });

  it("refuses a complex that has no lakehouse identity at all", () => {
    // Six of the 1,448 canonical rows on 2026-08-23: registered through the Catalog write API
    // rather than loaded from a Gold snapshot. Not an error — such a complex is in no tile and has
    // no panel to open, and the row renders unclickable rather than pretending otherwise.
    expect(complexPanelEntry(row({ lakehouse_complex_id: null }))).toBeUndefined();
    expect(complexPanelEntry({ lakehouse_complex_id: undefined })).toBeUndefined();
    expect(complexPanelEntry({ lakehouse_complex_id: "   " })).toBeUndefined();
  });
});
