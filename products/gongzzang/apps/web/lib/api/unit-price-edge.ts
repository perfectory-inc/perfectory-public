import { z } from "zod";
import { env } from "@/lib/env";
import { type OfficialPriceHistory, OfficialPriceHistorySchema, PnuSchema } from "./building-edge";
import { UnitPriceByPnuSchema as Generated } from "./unit-price.generated";

// A unit's official price history is its own by-PNU document, apart from the building document
// (root ADR-0190): a monthly price vintage never rebuilds the buildings. The two join here by
// unit id. A document is one building of one PNU (root ADR-0204: a large complex's PNU outgrew
// the edge's whole-read bound), keyed by the building's id, or the nil UUID for the units no
// building holds.
// The document's shape is the schema registry's (root ADR-0336), narrowed to what the panel reads;
// this file only tightens the PNU and the price history to the building document's.
export const UnitPriceDocumentSchema = Generated.extend({
  pnu: PnuSchema,
  units: z.array(
    z.object({ id: z.string().uuid(), official_price_history: OfficialPriceHistorySchema }),
  ),
});

/** Each priced unit's history by unit id; a unit absent from it has no published price. */
export type UnitPrices = ReadonlyMap<string, OfficialPriceHistory>;

/** A building none of whose units has a price is not served: 404 is "no prices", not a failure. */
export async function fetchUnitPrices(
  pnu: string,
  buildingId: string,
  signal?: AbortSignal,
): Promise<UnitPrices> {
  PnuSchema.parse(pnu);
  z.string().uuid().parse(buildingId);
  const base = env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/unit-prices/by-pnu/${pnu}/${buildingId}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return new Map();
  if (!response.ok) throw new Error(`unit price edge fetch failed: ${response.status}`);
  const document = UnitPriceDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu || document.building_id !== buildingId) {
    throw new Error("unit price edge response PNU or building disagrees with request");
  }
  return new Map(document.units.map((unit) => [unit.id, unit.official_price_history]));
}
