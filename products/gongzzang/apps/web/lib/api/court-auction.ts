import { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";

// A parcel's 법원경매 (court auctions) are their own by-PNU document on the parcel edge
// (`parcel-court-auction` lane, root ADR-0280, ADR-0287), apart from the parcel document — mirroring
// the 공매 (`parcel-public-auction`) lane. The panel shows the source's own words (root ADR-0070/0078);
// code-only fields the screen never reads are dropped by the schema rather than declared. The court
// data is a 공공저작물 that must be attributed (저작권법 제24조의2, root ADR-0281 §6): `source_attribution`
// is "대한민국법원 법원경매정보", carried in the document and shown by the panel, not invented here.
export const CourtAuctionDocumentSchema = z.object({
  schema_version: z.literal("foundation-platform.parcel_court_auction_by_pnu.v1"),
  pnu: PnuSchema,
  court_auction: z.object({
    source_attribution: z.string(),
    rounds: z.array(
      z.object({
        building_name: z.string().nullish(),
        court_name: z.string().nullish(),
        case_no: z.string().nullish(),
        user_case_no: z.string().nullish(),
        appraisal_amount_krw: z.number().nullish(),
        minimum_bid_price_krw: z.number().nullish(),
        sale_amount_krw: z.number().nullish(),
        sale_date: z.string().nullish(),
        sale_time_raw: z.string().nullish(),
        sale_place: z.string().nullish(),
        failed_bid_count: z.number().nullish(),
      }),
    ),
  }),
});

export type CourtAuction = z.infer<typeof CourtAuctionDocumentSchema>["court_auction"];
export type CourtAuctionRound = CourtAuction["rounds"][number];

/** A parcel with no 법원경매 is not served: 404 is "none" (null), not a failure. */
export async function fetchCourtAuction(
  pnu: string,
  signal?: AbortSignal,
): Promise<CourtAuction | null> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/court-auction/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`court auction edge fetch failed: ${response.status}`);
  const document = CourtAuctionDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu)
    throw new Error("court auction edge response PNU disagrees with request");
  return document.court_auction;
}
