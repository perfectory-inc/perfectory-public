// apps/web/lib/api/parcels.ts
//
// Parcel detail reads from the R2 edge, not the serving database (root ADR-0096). The browser
// fetches the baked by-PNU JSON document through the thin Cloudflare Worker at the parcel edge
// base URL; there is no server hop and no projection database in the path. Every value the panel
// shows is the source's own (root ADR-0070/0078): names and words as the source gives them, never
// a code translated here.
import { z } from "zod";

const parcelEdgeBaseUrl = (): string => {
  const base = process.env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL;
  if (!base) throw new Error("NEXT_PUBLIC_PARCEL_EDGE_BASE_URL is required for parcel detail");
  return base.endsWith("/") ? base.slice(0, -1) : base;
};

const text = z.string().nullish();
const number = z.number().nullish();

/**
 * The baked edge document, as the bake shapes it (`foundation-contracts` `ParcelByPnuDocument`).
 * Sections the panel does not read stay ignored; a field the bake does not write yet is optional.
 */
const ParcelDocumentSchema = z.object({
  schema_version: z.string(),
  pnu: z.string(),
  // The legal dong the PNU's first ten digits name, from `reference.legal_dong_code_snapshot`.
  legal_dong: z.object({ name: z.string(), lowest_name: text }).nullish(),
  zonings: z
    .array(
      z.object({
        zone_code: z.string(),
        zone_name: z.string(),
        // 1 포함, 2 저촉 (토지이용계획); the source's own code, shown by the panel's words for it.
        inclusion_code: text,
      }),
    )
    .nullish(),
  price: z
    .object({
      price_per_m2: z.number().int(),
      base_year: z.number().int(),
      base_month: z.number().int(),
      announced_date: text,
    })
    .nullish(),
  characteristics: z
    .object({
      land_category: text,
      area_m2: number,
      land_use_situation: text,
      terrain_height: text,
      terrain_shape: text,
      road_contact: text,
    })
    .nullish(),
  forest_ledger: z.object({ ownership_kind_name: text, co_owner_count: number }).nullish(),
  transfer_history: z.array(z.object({ reason: text, moved_at: text })).nullish(),
  land_rights: z.array(z.object({ building_name: text, dong_name: text, ho_name: text })).nullish(),
  land_right_total: number,
});

export type ParcelDocument = z.infer<typeof ParcelDocumentSchema>;

/** Parses the edge document; exported for tests that hold the panel to a document shape. */
export function parseParcelDocument(input: unknown): ParcelDocument {
  return ParcelDocumentSchema.parse(input);
}

/**
 * The lot number a PNU spells: its 11th digit is the 대장 구분 (2 is 임야대장, written 산), then
 * four digits of 본번 and four of 부번, a 부번 of 0 left out. The same rule as dawneer's
 * `consoles/dawneer/web/src/lib/labels.ts`.
 */
export function jibunOf(pnu: string, mountain: string): string {
  const main = Number(pnu.slice(11, 15));
  const sub = Number(pnu.slice(15, 19));
  const lot = sub === 0 ? `${main}` : `${main}-${sub}`;
  return pnu[10] === "2" ? `${mountain} ${lot}` : lot;
}

/** Raised when the current serving generation holds no document for this parcel. */
export class ParcelNotServedError extends Error {
  constructor(pnu: string) {
    super(`parcel ${pnu} is not in the current serving generation`);
    this.name = "ParcelNotServedError";
  }
}

export async function fetchParcel(pnu: string, signal?: AbortSignal): Promise<ParcelDocument> {
  const url = `${parcelEdgeBaseUrl()}/parcels/by-pnu/${encodeURIComponent(pnu)}`;
  const response = await fetch(url, { signal, headers: { accept: "application/json" } });
  if (response.status === 404) throw new ParcelNotServedError(pnu);
  if (!response.ok) throw new Error(`parcel edge fetch failed: ${response.status}`);
  return parseParcelDocument(await response.json());
}
