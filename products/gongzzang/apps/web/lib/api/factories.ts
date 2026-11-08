import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelFactoriesByPnuSchema as Generated } from "./factories.generated";

// The registered factories on a parcel are their own by-PNU document on the parcel edge
// (`parcel-factories` lane, root ADR-0449), from 한국산업단지공단 전국등록공장현황: each factory in the
// provider's words (회사명, 단지명, 생산품). The document's shape is the schema registry's (ADR-0336).
export const FactoriesDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type Factories = z.infer<typeof FactoriesDocumentSchema>["factories"] & {
  total: number;
};
export type FactoryRecord = Factories["records"][number];

/** A parcel the lane has no document for (not published yet, or no factory): null, not a failure. */
export async function fetchFactories(pnu: string, signal?: AbortSignal): Promise<Factories | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/factories/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`factories edge fetch failed: ${response.status}`);
  const document = FactoriesDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("factories edge response PNU disagrees with request");
  return {
    ...document.factories,
    total: document.factory_total ?? document.factories.records.length,
  };
}
