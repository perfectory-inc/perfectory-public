import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelBuildingPermitsByPnuSchema as Generated } from "./building-permits.generated";

// A parcel's 건축인허가 기본개요 (one per permit) is its own by-PNU document on the parcel edge
// (`parcel-building-permits` lane, root ADR-0451): each permit in the provider's words (건축구분명,
// 주용도명, 대지·건축·연면적, 건축허가·착공예정·착공연기·실제착공·사용승인일), and one list per other
// file of the permit family (ADR-0464). The document's shape is the schema registry's (ADR-0336).
export const BuildingPermitsDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type BuildingPermits = z.infer<typeof BuildingPermitsDocumentSchema>["building_permits"] & {
  total: number;
};
export type BuildingPermitRecord = BuildingPermits["records"][number];

/** A parcel the lane has no document for (not published yet, or no permit): null, not a failure. */
export async function fetchBuildingPermits(
  pnu: string,
  signal?: AbortSignal,
): Promise<BuildingPermits | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/building-permits/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`building permits edge fetch failed: ${response.status}`);
  const document = BuildingPermitsDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) {
    throw new Error("building permits edge response PNU disagrees with request");
  }
  return {
    ...document.building_permits,
    total: document.building_permit_total ?? document.building_permits.records.length,
  };
}

/**
 * The permit family's other files (root ADR-0464), in the order the panel lists them: each a list of the
 * parcel's rows `{total, records}`, null when it has none. `head` are the record's unlabelled heading
 * fields, `fields` its labelled ones, each in the provider's order; `dates` are YYYYMMDD.
 */
export const PERMIT_DETAILS = [
  {
    key: "building_outlines",
    head: ["main_annex_name", "main_purpose_name", "structure_name", "roof_name"],
    fields: [
      "building_area_m2",
      "total_floor_area_m2",
      "far_floor_area_m2",
      "unit_count",
      "family_count",
      "household_count",
      "passenger_elevator_count",
      "emergency_elevator_count",
    ],
  },
  {
    key: "floor_overviews",
    head: ["floor_kind_name", "floor_no", "main_purpose_name"],
    fields: ["floor_area_m2", "structure_name", "arch_kind_name"],
  },
  {
    key: "temporary_buildings",
    head: ["main_purpose_name", "structure_name"],
    fields: [
      "building_area_m2",
      "total_floor_area_m2",
      "plot_area_m2",
      "ground_floor_count",
      "existence_expiry_date",
    ],
  },
  {
    key: "structures",
    head: ["facility_kind_name", "structure_name"],
    fields: [
      "length",
      "height",
      "area_m2",
      "building_coverage_ratio",
      "zone_name",
      "district_name",
      "precinct_name",
    ],
  },
  {
    key: "major_repairs",
    head: ["major_repair_kind_name", "major_repair_change_name"],
    fields: [],
  },
  {
    key: "site_locations",
    head: ["main_building_name", "land_category_name"],
    fields: ["representative_flag"],
  },
  { key: "annex_parkings", head: ["land_category_name"], fields: ["related_jibun_name"] },
  {
    key: "regular_inspections",
    head: ["inspection_agency_name"],
    fields: ["inspection_start_date", "processed_date"],
  },
] as const satisfies readonly {
  key: keyof BuildingPermits;
  head: readonly string[];
  fields: readonly string[];
}[];

export const PERMIT_DETAIL_DATES: ReadonlySet<string> = new Set([
  "existence_expiry_date",
  "inspection_start_date",
  "processed_date",
]);

export type PermitDetailKey = (typeof PERMIT_DETAILS)[number]["key"];
export type PermitDetailRecord = Record<string, string | null | undefined>;

/** A file's list on the parcel, or null when the parcel has none of its rows. */
export function permitDetail(
  permits: BuildingPermits,
  key: PermitDetailKey,
): { total: number; records: PermitDetailRecord[] } | null {
  const listed = permits[key];
  return listed && listed.records.length > 0 ? listed : null;
}

/** Whether the parcel has anything for the 건축인허가 section: a permit or a row of another file. */
export function hasPermitContent(permits: BuildingPermits): boolean {
  return permits.records.length > 0 || PERMIT_DETAILS.some(({ key }) => permitDetail(permits, key));
}
