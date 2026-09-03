// apps/web/lib/api/building-units.ts
import { z } from "zod";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";

export const BuildingUnitSchema = z.object({
  id: z.string(),
  dong_name: z.string(),
  ho_name: z.string(),
  floor_label: z.string(),
  exclusive_area_m2: z.number().nullish(),
  usage_name: z.string(),
});

export type BuildingUnit = z.infer<typeof BuildingUnitSchema>;

export const BuildingUnitsResponseSchema = z.object({
  units: z.array(BuildingUnitSchema),
  // 커서는 상류가 발급한 불투명 토큰이다 (root ADR-0078 §2) — 그대로 되돌려 보낼 뿐,
  // 해석하거나 조립하지 않는다.
  next_cursor: z.string().nullish(),
});

export type BuildingUnitsResponse = z.infer<typeof BuildingUnitsResponseSchema>;

export async function fetchBuildingUnits(
  buildingId: string,
  options?: { cursor?: string; limit?: number; signal?: AbortSignal },
): Promise<BuildingUnitsResponse> {
  const searchParams = new URLSearchParams();
  if (options?.limit !== undefined) searchParams.set("limit", String(options.limit));
  if (options?.cursor !== undefined) searchParams.set("cursor", options.cursor);
  const json = await apiProxyClient.buildingUnitsRead.getJson<unknown>(
    { building_id: buildingId },
    { searchParams, signal: options?.signal },
  );
  return BuildingUnitsResponseSchema.parse(json);
}
