import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import type { Energy } from "./energy";
import { ParcelUtilityByPnuSchema as Generated } from "./utility.generated";

// 인입 가능성: what public data says about whether a parcel is reached by city gas and a road
// (platforms/foundation-platform/docs/catalog/parcel-utility-connectivity-sources.md). Every signal
// is read from a by-PNU document: gas from the energy document, the road from the parcel's own, the
// sewer from the parcel's 인입 가능성 document (`parcel-utility` lane, root ADR-0497). A signal is
// evidence, not a ruling: no public source says a line can be connected, so a parcel without one
// shows nothing, never "없음".
export const UtilityDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type UtilityRecords = z.infer<typeof UtilityDocumentSchema>["utility"];

/** A parcel the lane has no document for (not published yet, or no record): null, not a failure. */
export async function fetchUtility(
  pnu: string,
  signal?: AbortSignal,
): Promise<UtilityRecords | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/utility/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`utility edge fetch failed: ${response.status}`);
  const document = UtilityDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu) throw new Error("utility edge response PNU disagrees with request");
  return document.utility;
}

/**
 * 도시가스: the newest month the 지번 has a 가스 사용량 row in its energy document (root ADR-0398) —
 * a 수용가 metered there, so a gas line reaches the parcel. Null when no month of the window has a
 * row: the provider leaves out houses and 산업용 meters, so "no row" says nothing about the pipe.
 */
export function gasConnection(energy: Energy | null | undefined): { latestUseYm: string } | null {
  const months = (energy?.months ?? []).filter((month) => month.gas_rows > 0);
  if (months.length === 0) return null;
  const latest = months.reduce((a, b) => (b.use_ym > a.use_ym ? b : a));
  return { latestUseYm: latest.use_ym };
}

/**
 * 오수정화방식: the methods the parcel's 건축물대장 list for its 오수정화시설, most facilities first, in
 * the register's words (하수종말처리장연결, 부패탱크방법 …), at most `limit`. Empty when none is named.
 */
export function sewageMethods(records: UtilityRecords | null | undefined, limit = 3): string[] {
  return (records?.sewage.methods ?? [])
    .map((method) => method.name?.trim())
    .filter((name): name is string => Boolean(name))
    .slice(0, limit);
}
