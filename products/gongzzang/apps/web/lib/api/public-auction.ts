import { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";

// A parcel's on-bid public auctions are their own by-PNU document on the parcel edge
// (`parcel-public-auction` lane, root ADR-0280), apart from the parcel document. The panel shows
// the source's own words (root ADR-0070/0078); code-only fields the screen never reads are dropped
// by the schema rather than declared. A null `usg_*_ctgr_name` is "no name yet" (data.go.kr
// 15157768 code table pending), not a value to invent.
export const PublicAuctionDocumentSchema = z.object({
  schema_version: z.literal("foundation-platform.parcel_public_auction_by_pnu.v1"),
  pnu: PnuSchema,
  public_auction: z.object({
    rounds: z.array(
      z.object({
        item_name: z.string().nullish(),
        appraisal_amount_krw: z.number().nullish(),
        lowest_bid_price_krw: z.number().nullish(),
        bid_begins_at: z.string().nullish(),
        bid_ends_at: z.string().nullish(),
        pbct_nsq: z.string().nullish(),
        land_area_m2: z.number().nullish(),
        building_area_m2: z.number().nullish(),
        share_item_yn: z.string().nullish(),
        usg_scls_ctgr_name: z.string().nullish(),
        usg_mcls_ctgr_name: z.string().nullish(),
        usg_lcls_ctgr_name: z.string().nullish(),
      }),
    ),
  }),
});

export type PublicAuctionRound = z.infer<
  typeof PublicAuctionDocumentSchema
>["public_auction"]["rounds"][number];

/** A parcel with no on-bid public auction is not served: 404 is "none", not a failure. */
export async function fetchPublicAuction(
  pnu: string,
  signal?: AbortSignal,
): Promise<PublicAuctionRound[]> {
  PnuSchema.parse(pnu);
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  const response = await fetch(`${base}/parcels/public-auction/by-pnu/${pnu}`, {
    signal,
    headers: { accept: "application/json" },
  });
  if (response.status === 404) return [];
  if (!response.ok) throw new Error(`public auction edge fetch failed: ${response.status}`);
  const document = PublicAuctionDocumentSchema.parse(await response.json());
  if (document.pnu !== pnu)
    throw new Error("public auction edge response PNU disagrees with request");
  return document.public_auction.rounds;
}
