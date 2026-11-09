import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelNearbyByPnuSchema as Generated } from "./nearby.generated";

// What is near a parcel is its own by-PNU document on the parcel edge (`parcel-nearby` lane, root
// ADR-0465): the nearest 공항·철도역·도시철도역·초중등학교·대학 with the distance from the parcel's
// centroid, the 상가 count around it and the 고속도로 출입시설 of its 시군구, computed in Gold. The
// document's shape is the schema registry's (ADR-0336).
export const NearbyDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type Nearby = z.infer<typeof NearbyDocumentSchema>["nearby"];

/** The nearest-point entries, in the order the panel lists them. */
export const NEAREST = [
  "airport",
  "railway_station",
  "subway_station",
  "school",
  "university",
] as const;

/** A distance in the panel's words: metres below 1 km, else km to one decimal. */
export function distanceLabel(metres: number): string {
  return metres < 1000 ? `${metres}m` : `${(metres / 1000).toFixed(1)}km`;
}

/** A parcel the lane has no document for (not published yet): null, not a failure. */
export async function fetchNearby(pnu: string, signal?: AbortSignal): Promise<Nearby | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/nearby/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`nearby edge fetch failed: ${response.status}`);
  const document = NearbyDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) {
    throw new Error("nearby edge response PNU disagrees with request");
  }
  return document.nearby;
}
