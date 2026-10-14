// apps/web/lib/api/floors.ts
import { z } from "zod";
import { EdgeFloorSchema } from "./building-edge";
import { type BuildingProfile, fetchBuildingProfile } from "./building-kinds";

export const FloorBuildingSchema = z.object({
  id: z.string(),
  name: z.string(),
  above_ground: z.number().int().nullable(),
  below_ground: z.number().int(),
  has_rooftop: z.boolean(),
  // 옥탑 공용부 allocated area (㎡); omitted by the API when there is no rooftop.
  rooftop_area_m2: z.number().optional(),
  // 옥탑 용도, one per source usage; empty when there is no rooftop. The screen joins them.
  rooftop_usages: z.array(z.string()),
  floors: z.array(EdgeFloorSchema),
});

export type FloorBuilding = z.infer<typeof FloorBuildingSchema>;

export const FloorsResponseSchema = z.object({
  buildings: z.array(FloorBuildingSchema),
});

export type FloorsResponse = z.infer<typeof FloorsResponseSchema>;

export function toFloorsResponse(profile: BuildingProfile): FloorsResponse {
  return {
    buildings: profile.buildings.map((building) => ({
      id: building.id,
      name: "",
      above_ground: building.stories,
      below_ground: building.below_ground_floors,
      has_rooftop: building.has_rooftop,
      rooftop_area_m2: building.rooftop_area_m2 ?? undefined,
      // A bundle document's one joined string is kept whole — never split back into usages.
      rooftop_usages: Array.isArray(building.rooftop_usage)
        ? building.rooftop_usage
        : [building.rooftop_usage].filter((usage) => usage !== ""),
      floors: building.floors,
    })),
  };
}

export async function fetchFloors(
  parcelPnu: string,
  signal?: AbortSignal,
): Promise<FloorsResponse> {
  return toFloorsResponse(await fetchBuildingProfile(parcelPnu, signal));
}
