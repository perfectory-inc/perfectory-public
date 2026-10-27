// apps/web/components/panels/parcel/queries.ts
// The parcel views' reads, keyed as `PanelEntryView` keys a view (`["panel", kind, view, id]`), so
// the panel's own sections and the pushed views share one cache entry per read.
import { fetchBuildings } from "@/lib/api/buildings";
import { fetchCourtAuction } from "@/lib/api/court-auction";
import { fetchPublicAuction } from "@/lib/api/public-auction";
import { fetchListings } from "@/lib/listings/api";

export const parcelBuildingsQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "buildings", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchBuildings(pnu, signal),
  staleTime: 5 * 60_000,
});

export const parcelListingsQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "listings", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchParcelListings(pnu, signal),
  staleTime: 60_000,
});

export const parcelPublicAuctionQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "public-auction", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchPublicAuction(pnu, signal),
  staleTime: 60_000,
});

export const parcelCourtAuctionQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "court-auction", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchCourtAuction(pnu, signal),
  staleTime: 60_000,
});

export function fetchParcelListings(pnu: string, signal?: AbortSignal) {
  return fetchListings(
    {
      filters: {
        types: [],
        transactions: [],
        minAreaM2: undefined,
        maxAreaM2: undefined,
        minPriceKrw: undefined,
        maxPriceKrw: undefined,
        sort: "created_at_desc",
        adminCode: undefined,
        landUseType: undefined,
      },
      pnu,
    },
    signal,
  );
}
