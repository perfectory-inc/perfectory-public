import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelRegisterMastersByPnuSchema as Generated } from "./register-masters.generated";

// A parcel's 건축물대장 총괄표제부 (one per site of several buildings) is its own by-PNU document on the
// parcel edge (`parcel-register-masters` lane, root ADR-0450): each site in the provider's words
// (대지면적, 건축면적, 연면적, 주용도, 주·부속건축물수, 총주차수, 허가·착공·사용승인일). The document's
// shape is the schema registry's (ADR-0336).
export const RegisterMastersDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type RegisterMasters = z.infer<typeof RegisterMastersDocumentSchema>["register_masters"] & {
  total: number;
};
export type RegisterMasterRecord = RegisterMasters["records"][number];

/** A parcel the lane has no document for (not published yet, or no site): null, not a failure. */
export async function fetchRegisterMasters(
  pnu: string,
  signal?: AbortSignal,
): Promise<RegisterMasters | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/register-masters/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`register masters edge fetch failed: ${response.status}`);
  const document = RegisterMastersDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) {
    throw new Error("register masters edge response PNU disagrees with request");
  }
  return {
    ...document.register_masters,
    total: document.register_master_total ?? document.register_masters.records.length,
  };
}

/** A figure the provider filled: the register writes 0 where nothing was entered, so 0 is not shown. */
export function filled(value: string | null | undefined): value is string {
  return !!value && Number(value) !== 0;
}
