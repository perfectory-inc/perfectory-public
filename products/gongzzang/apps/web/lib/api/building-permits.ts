import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelBuildingPermitsByPnuSchema as Generated } from "./building-permits.generated";

// A parcel's 건축인허가 기본개요 (one per permit) is its own by-PNU document on the parcel edge
// (`parcel-building-permits` lane, root ADR-0451): each permit in the provider's words (건축구분명,
// 주용도명, 대지·건축·연면적, 건축허가·착공예정·착공연기·실제착공·사용승인일). The document's shape is
// the schema registry's (ADR-0336).
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
