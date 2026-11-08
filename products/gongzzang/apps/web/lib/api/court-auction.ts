import type { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelCourtAuctionByPnuSchema as Generated } from "./court-auction.generated";

// A parcel's 법원경매 (court auctions) are their own by-PNU document on the parcel edge
// (`parcel-court-auction` lane, root ADR-0280, ADR-0287, ADR-0353), apart from the parcel document,
// mirroring the 공매 lane, with the parcel's 매각결과 beside its items in progress (root ADR-0454).
// The panel shows the source's own words (root ADR-0070/0078). The document's shape is the foundation's schema registry's (root ADR-0336): the generated schema is
// its current revision narrowed to what this panel reads, accepting every revision of the major, so
// this file only extends it. `source_attribution` stays in the document but is not shown (root ADR-0428).
export const CourtAuctionDocumentSchema = Generated.extend({ pnu: PnuSchema });

export type CourtAuction = z.infer<typeof CourtAuctionDocumentSchema>["court_auction"];
export type CourtAuctionRound = CourtAuction["rounds"][number];
export type CourtAuctionSaleResult = NonNullable<CourtAuction["sale_results"]>[number];

/** What the 경매 page counts: the items still ahead and every 매각결과 of the parcel (root ADR-0454). */
export function courtAuctionTotal(auction: CourtAuction): number {
  return auction.rounds.length + (auction.sale_result_total ?? auction.sale_results?.length ?? 0);
}

/** Today's date in Korea (the court's sale dates are KST calendar dates), as `YYYY-MM-DD`. */
export function todayKst(now: Date = new Date()): string {
  return new Date(now.getTime() + 9 * 3_600_000).toISOString().slice(0, 10);
}

/**
 * The items still ahead: the Gold is built once a day, so an item whose sale date passed since is
 * dropped here rather than shown for up to a day after its auction.
 */
export function upcoming(auction: CourtAuction, today: string = todayKst()): CourtAuction {
  return { ...auction, rounds: auction.rounds.filter((round) => round.sale_date >= today) };
}

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
  return upcoming(document.court_auction);
}
