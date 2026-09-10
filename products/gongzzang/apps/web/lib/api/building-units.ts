import { z } from "zod";
import { type EdgeBuildingProfile, EdgeUnitSchema, fetchBuildingProfile } from "./building-edge";

export const BuildingUnitSchema = EdgeUnitSchema;
export type BuildingUnit = z.infer<typeof BuildingUnitSchema>;
export const BuildingUnitsResponseSchema = z.object({ units: z.array(BuildingUnitSchema) });
export type BuildingUnitsResponse = z.infer<typeof BuildingUnitsResponseSchema>;

/** All units are already present; an absent title link remains a separate group. */
export function toBuildingUnitsResponse(
  profile: EdgeBuildingProfile,
  buildingId?: string | null,
): BuildingUnitsResponse {
  if (buildingId === null) return { units: profile.unlinked_units };
  if (buildingId !== undefined)
    return { units: profile.buildings.find((building) => building.id === buildingId)?.units ?? [] };
  return {
    units: [...profile.buildings.flatMap((building) => building.units), ...profile.unlinked_units],
  };
}

export async function fetchBuildingUnits(
  pnu: string,
  buildingId?: string | null,
  signal?: AbortSignal,
): Promise<BuildingUnitsResponse> {
  return toBuildingUnitsResponse(await fetchBuildingProfile(pnu, signal), buildingId);
}
