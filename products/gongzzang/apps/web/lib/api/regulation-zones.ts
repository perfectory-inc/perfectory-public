import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelRegulationZonesByPnuSchema as Generated } from "./regulation-zones.generated";

// A parcel's regulation zones are their own by-PNU document on the parcel edge
// (`parcel-regulation-zones` lane, root ADR-0375): the 용도지역지구 zones whose polygon covers part of
// the parcel's, computed by the Foundation from the zone polygons, so the panel answers without the
// map or its tiles. Every word is the provider's (root ADR-0070/0078). The document's shape is the
// schema registry's (root ADR-0336), narrowed to what the panel reads; this file only extends it.
export const RegulationZonesDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type RegulationZone = z.infer<
  typeof RegulationZonesDocumentSchema
>["regulation_zones"]["zones"][number];

export type RegulationZones = { zones: RegulationZone[]; zoneTotal: number };

/** A parcel the lane has no document for (not published yet, or in no zone): null, not a failure. */
export async function fetchRegulationZones(
  pnu: string,
  signal?: AbortSignal,
): Promise<RegulationZones | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/regulation-zones/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`regulation zones edge fetch failed: ${response.status}`);
  const document = RegulationZonesDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) {
    throw new Error("regulation zones edge response PNU disagrees with request");
  }
  const { zones } = document.regulation_zones;
  return { zones, zoneTotal: document.zone_total ?? zones.length };
}
