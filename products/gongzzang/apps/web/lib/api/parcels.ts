// apps/web/lib/api/parcels.ts
//
// Parcel detail reads from the R2 edge, not the serving database (root ADR-0096). The browser
// fetches the pre-baked by-PNU JSON object through the thin Cloudflare Worker at the parcel
// edge base URL; there is no server hop and no projection database in the path. The edge
// document is mapped into the panel's existing `ParcelInfo` shape so the summary card is
// unchanged — only the source moved from Postgres to R2.
import { z } from "zod";
import { env } from "@/lib/env";
import {
  fetchLaneDocument,
  type KindState,
  kindSection,
  type LaneDocument,
  lanesForSections,
} from "./by-pnu-lane";
import type { ByPnuLaneName } from "./by-pnu-lanes.generated";

const parcelEdgeBaseUrl = (): string => env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");

/** The panel's view model. Kept stable so `ParcelSummaryCard` needs no change. */
export const ParcelInfoSchema = z.object({
  pnu: z.string(),
  sido_code: z.string(),
  sigungu_code: z.string(),
  eupmyeondong_code: z.string(),
  sido_name: z.string(),
  sigungu_name: z.string(),
  eupmyeondong_name: z.string(),
  land_use_type: z.string(),
  zoning: z.string().nullish(),
  official_land_price_per_m2: z.number().int().nullish(),
  gosi_year_month: z.string().nullish(),
});

export type ParcelInfo = z.infer<typeof ParcelInfoSchema>;

/**
 * The slice of the baked edge document this panel reads. The bake shapes every section like a
 * `foundation-contracts` response DTO (root ADR-0096); unknown sections stay ignored here.
 */
const EdgeParcelProfileSchema = z.object({
  schema_version: z.string(),
  pnu: z.string(),
  zonings: z.array(z.object({ zone_code: z.string(), zone_name: z.string() })).nullish(),
  price: z
    .object({
      price_per_m2: z.number().int(),
      base_year: z.number().int(),
      base_month: z.number().int(),
    })
    .nullish(),
  characteristics: z.object({ land_category: z.string().nullish() }).nullish(),
});

export type EdgeParcelProfile = z.infer<typeof EdgeParcelProfileSchema>;

/**
 * Maps a baked edge document into the panel view model. Korean admin names are not part of the
 * public parcel document (personal/derived data is served elsewhere), so codes are derived
 * from the PNU — the panel already falls back to a code-based heading when names are absent.
 */
export function toParcelInfo(profile: EdgeParcelProfile): ParcelInfo {
  const pnu = profile.pnu;
  const zoning = profile.zonings?.[0]?.zone_name ?? null;
  const price = profile.price;
  return {
    pnu,
    // First ten PNU digits are the legal-dong code: 2 sido, 5 sigungu, then eupmyeondong.
    sido_code: pnu.slice(0, 2),
    sigungu_code: pnu.slice(0, 5),
    eupmyeondong_code: pnu.slice(0, 8),
    sido_name: "",
    sigungu_name: "",
    eupmyeondong_name: "",
    land_use_type: profile.characteristics?.land_category ?? "",
    zoning,
    official_land_price_per_m2: price?.price_per_m2 ?? null,
    gosi_year_month: price
      ? `${price.base_year}-${String(price.base_month).padStart(2, "0")}`
      : null,
  };
}

/** Raised when the current serving generation holds no object for this parcel. */
export class ParcelNotServedError extends Error {
  constructor(pnu: string) {
    super(`parcel ${pnu} is not in the current serving generation`);
    this.name = "ParcelNotServedError";
  }
}

/** The bundle document: every parcel kind in one, until each kind's lane replaces it (ADR-0193). */
export async function fetchParcel(pnu: string, signal?: AbortSignal): Promise<EdgeParcelProfile> {
  const url = `${parcelEdgeBaseUrl()}/parcels/by-pnu/${encodeURIComponent(pnu)}`;
  const response = await fetch(url, { signal, headers: { accept: "application/json" } });
  if (response.status === 404) throw new ParcelNotServedError(pnu);
  if (!response.ok) throw new Error(`parcel edge fetch failed: ${response.status}`);
  return EdgeParcelProfileSchema.parse(await response.json());
}

/** The bundle sections the summary draws (root ADR-0193 §5); each kind's lane serves it by name. */
export const SUMMARY_SECTIONS = ["zonings", "price", "characteristics"] as const;
type Section = (typeof SUMMARY_SECTIONS)[number];
/** Which lane serves each section, from the generated registry. A section no lane serves yet is
 * read from the bundle alone. */
export const SUMMARY_LANES = lanesForSections("parcel", SUMMARY_SECTIONS);

export type LaneQuery = { data?: LaneDocument; isError: boolean };
export type ParcelSummary = {
  info: ParcelInfo;
  states: Record<Section, KindState<unknown>["state"]>;
};

/**
 * The summary of one parcel from its kinds' lanes and, while it is still served, the bundle. A
 * lane section that does not parse is a failure of that kind alone: it falls back like one.
 */
export function parcelSummary(
  bundle: EdgeParcelProfile | undefined,
  pnu: string,
  lanes: Readonly<Partial<Record<Section, LaneQuery>>>,
): ParcelSummary {
  const profile: EdgeParcelProfile = { schema_version: bundle?.schema_version ?? "", pnu };
  const states = {} as ParcelSummary["states"];
  for (const section of SUMMARY_SECTIONS) {
    let kind = kindSection(lanes[section], section, bundle);
    if (kind.state === "ok") {
      const parsed = EdgeParcelProfileSchema.shape[section].safeParse(kind.value);
      kind = parsed.success ? kind : kindSection(undefined, section, bundle);
    }
    if (kind.state === "ok") Object.assign(profile, { [section]: kind.value });
    states[section] = kind.state;
  }
  return { info: toParcelInfo(profile), states };
}

/**
 * The bundle and every summary lane read at once. Each lane is asked once: a lane not published
 * yet (503) is not retried, the bundle answers for it. A parcel the bundle does not serve is still
 * drawn from what its lanes serve; only a parcel nothing serves is an error.
 */
export async function fetchParcelSummary(
  pnu: string,
  signal?: AbortSignal,
): Promise<ParcelSummary> {
  const served = Object.entries(SUMMARY_LANES) as [Section, ByPnuLaneName][];
  const [bundle, ...answers] = await Promise.allSettled([
    fetchParcel(pnu, signal),
    ...served.map(([, lane]) => fetchLaneDocument(lane, pnu, signal)),
  ]);
  const lanes: Partial<Record<Section, LaneQuery>> = {};
  served.forEach(([section], i) => {
    const answer = answers[i];
    lanes[section] =
      answer?.status === "fulfilled" ? { data: answer.value, isError: false } : { isError: true };
  });
  if (bundle.status === "fulfilled") return parcelSummary(bundle.value, pnu, lanes);
  const summary = parcelSummary(undefined, pnu, lanes);
  if (Object.values(summary.states).includes("ok")) return summary;
  throw bundle.reason;
}
