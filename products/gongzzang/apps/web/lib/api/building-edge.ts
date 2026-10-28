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
const EdgeSourceSchema = z.object({
  table: z.literal("gold.building_panel"),
  iceberg_snapshot_id: z.string().min(1),
});
const EdgeBuildingSchema = z.object({
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
});
export const EdgeBuildingProfileSchema = z.object({
  // v3 is v2 without the unit prices (root ADR-0190); v4 is v2 with the register's names beside its
  // codes. All are read until v2 is no longer served.
  schema_version: z.enum([
    "foundation-platform.building_by_pnu_profile.v2",
    "foundation-platform.building_by_pnu_profile.v3",
    "foundation-platform.building_by_pnu_profile.v4",
  ]),
  pnu: PnuSchema,
  source: EdgeSourceSchema,
  buildings: z.array(EdgeBuildingSchema),
  unlinked_units: z.array(EdgeUnitSchema),
});

export type EdgeBuildingProfile = z.infer<typeof EdgeBuildingProfileSchema>;

const PAGE_SCHEMA_VERSION = "foundation-platform.building_by_pnu_page.v1";

// One page of a building document too large for one request (root ADR-0325): page 1 answers the
// parcel's own path, page n `<pnu>/<n>`; a building cut between pages appears on each, its floors
// on the first. `withheld` says the document could not be served at all, and why.
export const EdgeBuildingPageSchema = z.object({
  schema_version: z.literal(PAGE_SCHEMA_VERSION),
  pnu: PnuSchema,
  source: EdgeSourceSchema,
  page: z.object({ index: z.number().int().positive(), count: z.number().int().positive() }),
  buildings: z.array(EdgeBuildingSchema),
  unlinked_units: z.array(EdgeUnitSchema),
  withheld: z.string().optional(),
});

export type EdgeBuildingPage = z.infer<typeof EdgeBuildingPageSchema>;

/** A valid parcel can have no building object in the currently published generation. */
export class BuildingNotServedError extends Error {
  constructor(readonly pnu: string) {
    super(`buildings for parcel ${pnu} are not in the current serving generation`);
    this.name = "BuildingNotServedError";
  }
}

/** The parcel's building document is too large to serve; the bake named why (root ADR-0325 §4). */
export class BuildingWithheldError extends Error {
  constructor(
    readonly pnu: string,
    readonly reason: string,
  ) {
    super(`buildings for parcel ${pnu} are withheld: ${reason}`);
    this.name = "BuildingWithheldError";
  }
}

async function fetchEdge(pnu: string, page: number, signal?: AbortSignal): Promise<unknown> {
  const base = env.NEXT_PUBLIC_BUILDING_EDGE_BASE_URL.replace(/\/$/, "");
  const path = page === 1 ? pnu : `${pnu}/${page}`;
  const response = await fetch(`${base}/buildings/by-pnu/${path}?schema=2`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) throw new BuildingNotServedError(pnu);
  if (!response.ok) throw new Error(`building edge fetch failed: ${response.status}`);
  return response.json();
}

/**
 * The document its pages hold, joined in order: consecutive buildings of one id are one building,
 * its floors and units continued. Every page must be the same document's: a publish between two
 * page reads fails the read, and the query's retry starts again from page 1.
 */
export function joinBuildingPages(pnu: string, pages: EdgeBuildingPage[]): EdgeBuildingProfile {
  const first = pages[0];
  if (first === undefined) throw new Error("building edge document has no pages");
  if (first.withheld !== undefined) throw new BuildingWithheldError(pnu, first.withheld);
  const buildings: EdgeBuildingProfile["buildings"] = [];
  const unlinked: EdgeBuildingProfile["unlinked_units"] = [];
  pages.forEach((page, at) => {
    if (
      page.pnu !== pnu ||
      page.page.index !== at + 1 ||
      page.page.count !== pages.length ||
      page.source.iceberg_snapshot_id !== first.source.iceberg_snapshot_id ||
      page.withheld !== undefined
    ) {
      throw new Error("building edge pages are not one document's pages in order");
    }
    for (const building of page.buildings) {
      const last = buildings[buildings.length - 1];
      if (last?.id === building.id) {
        buildings[buildings.length - 1] = {
          ...last,
          floors: [...last.floors, ...building.floors],
          units: [...last.units, ...building.units],
        };
      } else {
        buildings.push(building);
      }
    }
    unlinked.push(...page.unlinked_units);
  });
  // A page carries the v4 document's fields, cut; joined, it is that document.
  return {
    schema_version: "foundation-platform.building_by_pnu_profile.v4",
    pnu,
    source: first.source,
    buildings,
    unlinked_units: unlinked,
  };
}

export async function fetchBuildingProfile(
  pnu: string,
  signal?: AbortSignal,
): Promise<EdgeBuildingProfile> {
  PnuSchema.parse(pnu);
  const body = await fetchEdge(pnu, 1, signal);
  const paged =
    (body as { schema_version?: unknown } | null)?.schema_version === PAGE_SCHEMA_VERSION;
  if (!paged) {
    const profile = EdgeBuildingProfileSchema.parse(body);
    if (profile.pnu !== pnu) throw new Error("building edge response PNU disagrees with request");
    return profile;
  }
  // A document too large for one request is its pages (root ADR-0325): never draw page 1 alone.
  const first = EdgeBuildingPageSchema.parse(body);
  if (first.pnu !== pnu) throw new Error("building edge response PNU disagrees with request");
  if (first.withheld !== undefined) throw new BuildingWithheldError(pnu, first.withheld);
  const rest = await Promise.all(
    Array.from({ length: first.page.count - 1 }, async (_, at) =>
      EdgeBuildingPageSchema.parse(await fetchEdge(pnu, at + 2, signal)),
    ),
  );
  return joinBuildingPages(pnu, [first, ...rest]);
}
