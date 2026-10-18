// apps/web/lib/api/buildings.ts
import { z } from "zod";
import { type EdgeBuildingProfile, fetchBuildingProfile } from "./building-edge";
import { BuildingUnitSchema } from "./building-units";

export const BuildingSchema = z.object({
  id: z.string(),
  name: z.string(),
  // 대장이 말하지 않은 값은 키가 생략된다 (root ADR-0078 §1) — 0 이나 빈 문자열로
  // 지어내지 않는다.
  purpose: z.string().nullish(),
  structure: z.string().nullish(),
  total_area_m2: z.number().nullish(),
  stories: z.number().int().nullish(),
  below_ground_floors: z.number().int().nullish(),
  built_year: z.number().int().nullish(),
  approved_at: z.string().nullish(),
  units: z.array(BuildingUnitSchema),
});

export type Building = z.infer<typeof BuildingSchema>;

export const BuildingsResponseSchema = z.object({
  buildings: z.array(BuildingSchema),
  unlinked_units: z.array(BuildingUnitSchema),
});

export type BuildingsResponse = z.infer<typeof BuildingsResponseSchema>;

export function toBuildingsResponse(profile: EdgeBuildingProfile): BuildingsResponse {
  return {
    buildings: profile.buildings.map((building) => ({
      id: building.id,
      name: "",
      // The register's name for the purpose, never its code (screen labels use source words).
      purpose: building.purpose_name ?? null,
      structure: building.structure_name ?? null,
      total_area_m2: building.floor_area_m2,
      stories: building.stories,
      below_ground_floors: building.below_ground_floors,
      built_year: building.built_year,
      // A source year is not an approval date. Neither a name nor date is invented.
      approved_at: null,
      units: building.units,
    })),
    unlinked_units: profile.unlinked_units,
  };
}

export async function fetchBuildings(
  parcelPnu: string,
  signal?: AbortSignal,
): Promise<BuildingsResponse> {
  return toBuildingsResponse(await fetchBuildingProfile(parcelPnu, signal));
}
