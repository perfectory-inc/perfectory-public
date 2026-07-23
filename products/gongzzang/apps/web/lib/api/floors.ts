// apps/web/lib/api/floors.ts
import { z } from "zod";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";

export const FloorBuildingSchema = z.object({
  id: z.string(),
  name: z.string(),
  above_ground: z.number().int(),
  below_ground: z.number().int(),
  has_rooftop: z.boolean(),
  // 옥탑 공용부 allocated area (㎡); omitted by the API when there is no rooftop.
  rooftop_area_m2: z.number().optional(),
  // 옥탑 용도 (주용도 · 기타용도); empty when there is no rooftop.
  rooftop_usage: z.string().default(""),
});

export type FloorBuilding = z.infer<typeof FloorBuildingSchema>;

export const FloorsResponseSchema = z.object({
  buildings: z.array(FloorBuildingSchema),
});

export type FloorsResponse = z.infer<typeof FloorsResponseSchema>;

export async function fetchFloors(
  parcelPnu: string,
  signal?: AbortSignal,
): Promise<FloorsResponse> {
  const searchParams = new URLSearchParams({ parcel_pnu: parcelPnu });
  const json = await apiProxyClient.floorsRead.getJson<unknown>({ searchParams, signal });
  return FloorsResponseSchema.parse(json);
}
