import { z } from "zod";
import { env } from "@/lib/env";

// Published foundation-contracts building_panel / UnitResponse wire shapes (root ADR-0100).
export const PnuSchema = z.string().regex(/^[0-9]{10}[1289][0-9]{8}$/);
export const OfficialPriceHistorySchema = z.array(
  z.object({
    base_date: z.string().regex(/^[0-9]{8}$/),
    price_won: z.number().int().nonnegative(),
  }),
);
export type OfficialPriceHistory = z.infer<typeof OfficialPriceHistorySchema>;
export const EdgeUnitSchema = z.object({
  id: z.string().uuid(),
  parcel_id: z.string().uuid(),
  building_id: z.string().uuid().nullable(),
  building_name: z.string(),
  dong_name: z.string(),
  ho_name: z.string(),
  // The unit number the data normalized from the source's 호명칭 (units lane); null when it has none.
  ho_number: z.number().int().nullish(),
  // The source's floor kind and number (the kind lanes, ADR-0193 §9); the screen writes the floor
  // from them (`unit-floor.ts`). A bundle document has only the server-formatted `floor_label`.
  floor_kind: z.string().nullish(),
  floor_number: z.number().int().nullish(),
  floor_label: z.string().optional(),
  exclusive_area_m2: z.number().nonnegative().nullable(),
  usage_name: z.string(),
  structure_name: z.string(),
  // Only a v2 document embeds a unit's prices; they are served on their own now (`unit-price-edge.ts`,
  // root ADR-0190), and these are drawn only while that lane does not answer.
  official_price_history: OfficialPriceHistorySchema.optional(),
});
export const EdgeFloorSchema = z.object({
  floor_row_id: z.string(),
  floor_kind: z.string(),
  floor_number: z.number().int().nonnegative().nullable(),
  floor_index: z.number().int().nullable(),
});
export const EdgeBuildingProfileSchema = z.object({
  // v3 is v2 without the unit prices (root ADR-0190); both are read until v2 is no longer served.
  schema_version: z.enum([
    "foundation-platform.building_by_pnu_profile.v2",
    "foundation-platform.building_by_pnu_profile.v3",
  ]),
  pnu: PnuSchema,
  source: z.object({
    table: z.literal("gold.building_panel"),
    iceberg_snapshot_id: z.string().min(1),
  }),
  buildings: z.array(
    z.object({
      id: z.string().uuid(),
      parcel_id: z.string().uuid(),
      register_pk: z.string(),
      purpose_code: z.string().nullable(),
      structure_code: z.string().nullable(),
      // The register's own names for the two codes (표제부 주용도코드명·구조코드명), once the bake
      // carries them; the panel shows a name, never a code.
      purpose_name: z.string().nullish(),
      structure_name: z.string().nullish(),
      floor_area_m2: z.number().positive().nullable(),
      stories: z.number().int().nonnegative().nullable(),
      below_ground_floors: z.number().int().nonnegative(),
      has_rooftop: z.boolean(),
      rooftop_area_m2: z.number().nonnegative().nullish(),
      // A list of the source's usages in a kind lane; one server-joined string in a bundle document.
      rooftop_usage: z.union([z.array(z.string()), z.string()]),
      built_year: z.number().int().nullable(),
      floors: z.array(EdgeFloorSchema),
      units: z.array(EdgeUnitSchema),
    }),
  ),
  unlinked_units: z.array(EdgeUnitSchema),
});

export type EdgeBuildingProfile = z.infer<typeof EdgeBuildingProfileSchema>;

/** A valid parcel can have no building object in the currently published generation. */
export class BuildingNotServedError extends Error {
  constructor(readonly pnu: string) {
    super(`buildings for parcel ${pnu} are not in the current serving generation`);
    this.name = "BuildingNotServedError";
  }
}

export async function fetchBuildingProfile(
  pnu: string,
  signal?: AbortSignal,
): Promise<EdgeBuildingProfile> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_BUILDING_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/buildings/by-pnu/${pnu}?schema=2`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) throw new BuildingNotServedError(pnu);
  if (!response.ok) throw new Error(`building edge fetch failed: ${response.status}`);
  const profile = EdgeBuildingProfileSchema.parse(await response.json());
  if (profile.pnu !== pnu) throw new Error("building edge response PNU disagrees with request");
  return profile;
}
