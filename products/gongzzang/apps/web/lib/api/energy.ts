import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelEnergyByPnuSchema as Generated } from "./energy.generated";

// A parcel's building energy is its own by-PNU document on the parcel edge (`parcel-energy` lane,
// root ADR-0398), built from the 건축HUB 월별 전기·가스 files: the 지번's newest twelve usage months,
// each energy summed over the month's rows. 전기 is in the provider's stated unit (kWh); 가스 has no
// unit the provider states, so the panel shows its 사용량 as the source's number (root ADR-0398 §2).
// The document's shape is the schema registry's (root ADR-0336), narrowed to what the panel reads.
export const EnergyDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type Energy = z.infer<typeof EnergyDocumentSchema>["energy"];
export type EnergyMonth = Energy["months"][number];

/** A parcel the lane has no document for (not published yet, or no usage row): null, not a failure. */
export async function fetchEnergy(pnu: string, signal?: AbortSignal): Promise<Energy | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/energy/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`energy edge fetch failed: ${response.status}`);
  const document = EnergyDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("energy edge response PNU disagrees with request");
  return document.energy;
}

/** `YYYYMM` as `YYYY.MM`, the panel's month label; anything else as it came. */
export function monthLabel(useYm: string): string {
  return /^\d{6}$/.test(useYm) ? `${useYm.slice(0, 4)}.${useYm.slice(4)}` : useYm;
}

/** The months oldest first, for a chart read left to right. */
export function chronological(energy: Energy): EnergyMonth[] {
  return [...energy.months].sort((a, b) => a.use_ym.localeCompare(b.use_ym));
}

/** The largest 전기 사용량 of the window, the chart's full bar; 0 when no month has one. */
export function electricityPeak(energy: Energy): number {
  return Math.max(0, ...energy.months.map((m) => m.electricity_usage ?? 0));
}
