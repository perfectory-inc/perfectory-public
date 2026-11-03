// apps/web/components/panels/parcel/queries.ts
// The parcel views' reads, keyed as `PanelEntryView` keys a view (`["panel", kind, view, id]`), so
// the panel's own sections and the pushed views share one cache entry per read.
import { fetchBuildings } from "@/lib/api/buildings";
import { fetchDemolitions } from "@/lib/api/demolitions";
import { fetchEnergy } from "@/lib/api/energy";
import { fetchOwnership } from "@/lib/api/ownership";
import { fetchPublicAuction } from "@/lib/api/public-auction";
import { fetchRegulationZones } from "@/lib/api/regulation-zones";
import { fetchSubParcels } from "@/lib/api/sub-parcels";
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

export const parcelOwnershipQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "ownership", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchOwnership(pnu, signal),
  // The lane changes when the 토지소유 file does, twice a year (root ADR-0361).
  staleTime: 60 * 60_000,
});

export const parcelSubParcelsQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "sub-parcels", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchSubParcels(pnu, signal),
  // The 부속지번 file is published monthly (root ADR-0395, ADR-0399).
  staleTime: 60 * 60_000,
});

export const parcelEnergyQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "energy", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchEnergy(pnu, signal),
  // The lane changes when a month's 전기·가스 file lands, about monthly (root ADR-0398).
  staleTime: 60 * 60_000,
});

export const parcelDemolitionsQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "demolitions", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchDemolitions(pnu, signal),
  // The 철거멸실관리대장 is published monthly and rarely changes (root ADR-0397, ADR-0399).
  staleTime: 60 * 60_000,
});

export const parcelRegulationZonesQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "regulation-zones", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchRegulationZones(pnu, signal),
  // The lane changes when a 용도지역지구 release does, once a month (root ADR-0362, ADR-0375).
  staleTime: 60 * 60_000,
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
