// apps/web/lib/api/floors.ts
import { z } from "zod";
import { type EdgeBuildingProfile, EdgeFloorSchema, fetchBuildingProfile } from "./building-edge";

export const FloorBuildingSchema = z.object({
  id: z.string(),
  name: z.string(),
  above_ground: z.number().int().nullable(),
  below_ground: z.number().int(),
  has_rooftop: z.boolean(),
  // 옥탑 공용부 allocated area (㎡); omitted by the API when there is no rooftop.
  rooftop_area_m2: z.number().optional(),
  // 옥탑 용도 (주용도 · 기타용도); empty when there is no rooftop.
  rooftop_usage: z.string().default(""),
  floors: z.array(EdgeFloorSchema),
});

export type FloorBuilding = z.infer<typeof FloorBuildingSchema>;

export const FloorsResponseSchema = z.object({
  buildings: z.array(FloorBuildingSchema),
});

export type FloorsResponse = z.infer<typeof FloorsResponseSchema>;

export function toFloorsResponse(profile: EdgeBuildingProfile): FloorsResponse {
  return {
    buildings: profile.buildings.map((building) => ({
      id: building.id,
      name: "",
      above_ground: building.stories,
      below_ground: building.below_ground_floors,
      has_rooftop: building.has_rooftop,
      rooftop_area_m2: building.rooftop_area_m2 ?? undefined,
      rooftop_usage: building.rooftop_usage,
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
