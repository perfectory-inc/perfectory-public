import { z } from "zod";
import { env } from "@/lib/env";
import { type OfficialPriceHistory, OfficialPriceHistorySchema, PnuSchema } from "./building-edge";

// A unit's official price history is its own document, apart from the building document (root
// ADR-0190): a monthly price vintage never rebuilds the buildings. A document is one building of
// one PNU (root ADR-0204), read when that building's unit list is opened; the two join by unit id.

/** The document of a PNU's units that no building carries (root ADR-0204 §1). */
export const UNLINKED_BUILDING_ID = "00000000-0000-0000-0000-000000000000";

// Any UUID-shaped catalog id, the nil UUID included; checked before it becomes part of a path.
const BuildingIdSchema = z
  .string()
  .regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);

export const UnitPriceDocumentSchema = z.object({
  schema_version: z.literal("foundation-platform.unit_price_by_pnu.v1"),
  pnu: PnuSchema,
  building_id: BuildingIdSchema,
  source: z.object({
    table: z.literal("gold.unit_price_panel"),
    iceberg_snapshot_id: z.string().min(1),
  }),
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
  BuildingIdSchema.parse(buildingId);
  const base = env.NEXT_PUBLIC_UNIT_PRICE_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/unit-prices/by-pnu/${pnu}/${buildingId}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return new Map();
  if (!response.ok) throw new Error(`unit price edge fetch failed: ${response.status}`);
  const document = UnitPriceDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("unit price edge response PNU disagrees with request");
  if (document.building_id !== buildingId) {
    throw new Error("unit price edge response building disagrees with request");
  }
  return new Map(document.units.map((unit) => [unit.id, unit.official_price_history]));
}
