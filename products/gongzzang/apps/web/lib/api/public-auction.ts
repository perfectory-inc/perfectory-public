import { z } from "zod";
import { env } from "@/lib/env";
import { PnuSchema } from "./building-edge";
import { ParcelPublicAuctionByPnuSchema as Generated } from "./public-auction.generated";

// A parcel's on-bid public auctions are their own by-PNU document on the parcel edge
// (`parcel-public-auction` lane, root ADR-0280), apart from the parcel document. The panel shows
// the source's own words (root ADR-0070/0078). A null `usg_*_ctgr_name` is "no name yet"
// (data.go.kr 15157768 code table pending), not a value to invent.
// The document's shape is the foundation's schema registry's (root ADR-0336): the generated schema
// is its current revision narrowed to what this panel reads, accepting every revision of the major
// by subject and major alone, so a revision that adds a field changes nothing here. This file only
// extends it.
// Each auction round carries its onbid media (root ADR-0298): photos and the appraisal report,
// content-addressed by sha256 on the catalog edge. A round may legitimately carry none, and a
// revision-1 round has no `photos` at all: both read as [].
const GeneratedRound = Generated.shape.public_auction.shape.rounds.element;
const PublicAuctionPhotoSchema = GeneratedRound.shape.photos.unwrap().unwrap().element;

export type PublicAuctionPhoto = z.infer<typeof PublicAuctionPhotoSchema>;

export const PublicAuctionDocumentSchema = Generated.extend({
  pnu: PnuSchema,
  public_auction: Generated.shape.public_auction.extend({
    rounds: z.array(
      GeneratedRound.extend({
        photos: GeneratedRound.shape.photos.transform((photos) => photos ?? []),
      }),
    ),
  }),
});

/**
 * The catalog edge serves each onbid media object content-addressed by its sha256 (root ADR-0298),
 * off the same host the by-PNU documents come from. Returns the public image/PDF URL for a photo.
 */
export function onbidMediaSrc(photo: Pick<PublicAuctionPhoto, "sha256" | "media_format">): string {
  const base = env.NEXT_PUBLIC_PARCEL_EDGE_BASE_URL.replace(/\/$/, "");
  return `${base}/onbid/media/sha256/${photo.sha256}.${photo.media_format}`;
}

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
