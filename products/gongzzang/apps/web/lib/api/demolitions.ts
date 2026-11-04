import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelDemolitionsByPnuSchema as Generated } from "./demolitions.generated";

// A parcel's demolished and lost buildings are their own by-PNU document on the parcel edge
// (`parcel-demolitions` lane, root ADR-0399), from the 건축인허가 철거멸실관리대장: each report in the
// provider's words (data.go.kr 15044683). The document's shape is the schema registry's (ADR-0336).
export const DemolitionsDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type Demolitions = z.infer<typeof DemolitionsDocumentSchema>["demolitions"] & {
  total: number;
};
export type DemolitionRecord = Demolitions["records"][number];

/** A parcel the lane has no document for (not published yet, or no report): null, not a failure. */
export async function fetchDemolitions(
  pnu: string,
  signal?: AbortSignal,
): Promise<Demolitions | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/demolitions/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`demolitions edge fetch failed: ${response.status}`);
  const document = DemolitionsDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("demolitions edge response PNU disagrees with request");
  return {
    ...document.demolitions,
    total: document.demolition_total ?? document.demolitions.records.length,
  };
}

/** `YYYYMMDD` as `YYYY.MM.DD`; anything else as it came (the provider's text). */
export function dayLabel(day: string): string {
  return /^\d{8}$/.test(day) ? `${day.slice(0, 4)}.${day.slice(4, 6)}.${day.slice(6)}` : day;
}

/** The report's date line: 철거멸실일, or else 철거시작일~철거종료일, or null when it has none. */
export function reportDates(record: DemolitionRecord): string | null {
  if (record.demolition_loss_date) return dayLabel(record.demolition_loss_date);
  const span = [record.demolition_start_date, record.demolition_end_date].filter(
    (d): d is string => !!d,
  );
  return span.length > 0 ? span.map(dayLabel).join("~") : null;
}
