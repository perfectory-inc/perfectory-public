import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelSubParcelsByPnuSchema as Generated } from "./sub-parcels.generated";

// The buildings a parcel shares with other parcels are their own by-PNU document on the parcel edge
// (`parcel-sub-parcels` lane, root ADR-0399), from the 건축물대장 부속지번 read from both ends: the
// other parcels a register on this 대지 also stands on, and the registers on another 대지 that also
// stand here. The document's shape is the schema registry's (root ADR-0336).
export const SubParcelsDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type SubParcels = z.infer<typeof SubParcelsDocumentSchema>["sub_parcels"];
export type SubParcelLink = SubParcels["on_other_parcels"][number];

/** A parcel the lane has no document for (not published yet, or no link): null, not a failure. */
export async function fetchSubParcels(
  pnu: string,
  signal?: AbortSignal,
): Promise<SubParcels | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/sub-parcels/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`sub-parcels edge fetch failed: ${response.status}`);
  const document = SubParcelsDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("sub-parcels edge response PNU disagrees with request");
  return document.sub_parcels;
}

/**
 * The other parcel's 지번 as the panel names it: the provider's `시군구-법정동-대지구분-번-지` read as
 * `[산 ]번[-지]` (대지구분 1 is 산, as the 건축물대장 writes it; FP-ADR-0023), or the text as it came.
 */
export function jibunLabel(jibun: string | null | undefined): string | null {
  if (!jibun) return null;
  const parts = jibun.split("-");
  if (parts.length !== 5) return jibun;
  const [, , kind, bon, bu] = parts;
  const main = String(Number(bon));
  const sub = Number(bu);
  if (Number.isNaN(Number(bon)) || Number.isNaN(sub)) return jibun;
  return `${kind === "1" ? "산 " : ""}${main}${sub > 0 ? `-${sub}` : ""}`;
}
