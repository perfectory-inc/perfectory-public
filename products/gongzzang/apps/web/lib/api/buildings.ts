// apps/web/lib/api/buildings.ts
import { z } from "zod";
import { apiProxyClient } from "@/lib/api/api-proxy-client.generated";

export const BuildingSchema = z.object({
  id: z.string(),
  name: z.string(),
  // 대장이 말하지 않은 값은 키가 생략된다 (root ADR-0078 §1) — 0 이나 빈 문자열로
  // 지어내지 않는다.
  purpose: z.string().nullish(),
  total_area_m2: z.number().nullish(),
  approved_at: z.string().nullish(),
});

export type Building = z.infer<typeof BuildingSchema>;

export const BuildingsResponseSchema = z.object({
  buildings: z.array(BuildingSchema),
});

export type BuildingsResponse = z.infer<typeof BuildingsResponseSchema>;

export async function fetchBuildings(
  parcelPnu: string,
  signal?: AbortSignal,
): Promise<BuildingsResponse> {
  const searchParams = new URLSearchParams({ parcel_pnu: parcelPnu });
  const json = await apiProxyClient.buildingsRead.getJson<unknown>({ searchParams, signal });
  return BuildingsResponseSchema.parse(json);
}
