// apps/web/components/panels/parcel/queries.ts
// The parcel views' reads, keyed as `PanelEntryView` keys a view (`["panel", kind, view, id]`), so
// the panel's own sections and the pushed views share one cache entry per read.
import { fetchBuildingPermits } from "@/lib/api/building-permits";
import { fetchBuildings } from "@/lib/api/buildings";
import { fetchCourtAuction } from "@/lib/api/court-auction";
import { fetchDemolitions } from "@/lib/api/demolitions";
import { fetchEnergy } from "@/lib/api/energy";
import { fetchFactories } from "@/lib/api/factories";
import { fetchNearby } from "@/lib/api/nearby";
import { fetchOwnership } from "@/lib/api/ownership";
import { fetchPublicAuction } from "@/lib/api/public-auction";
import { fetchRealTransaction } from "@/lib/api/real-transaction";
import { fetchRegisterMasters } from "@/lib/api/register-masters";
import { fetchRegulationZones } from "@/lib/api/regulation-zones";
import { fetchSubParcels } from "@/lib/api/sub-parcels";
import { fetchUtility } from "@/lib/api/utility";
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

export const parcelFactoriesQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "factories", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchFactories(pnu, signal),
  // The 등록공장 file is a yearly edition (root ADR-0280 §4, ADR-0449).
  staleTime: 60 * 60_000,
});

export const parcelBuildingPermitsQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "building-permits", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchBuildingPermits(pnu, signal),
  // The 건축인허가 기본개요 is a monthly hub.go.kr release (root ADR-0356, ADR-0451).
  staleTime: 60 * 60_000,
});

export const parcelNearbyQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "nearby", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchNearby(pnu, signal),
  // Computed in Gold from facility files that change a few times a year and monthly 상가 (ADR-0465).
  staleTime: 60 * 60_000,
});

export const parcelUtilityQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "utility", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchUtility(pnu, signal),
  // Built from the monthly 건축물대장 오수정화시설 file (root ADR-0497).
  staleTime: 60 * 60_000,
});

export const parcelRegisterMastersQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "register-masters", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchRegisterMasters(pnu, signal),
  // The 총괄표제부 is a monthly hub.go.kr release (root ADR-0357, ADR-0450).
  staleTime: 60 * 60_000,
});

export const parcelRegulationZonesQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "regulation-zones", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchRegulationZones(pnu, signal),
  // The lane changes when a 용도지역지구 release does, once a month (root ADR-0362, ADR-0375).
  staleTime: 60 * 60_000,
});

export const parcelCourtAuctionQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "court-auction", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchCourtAuction(pnu, signal),
  staleTime: 60_000,
});

export const parcelRealTransactionQuery = (pnu: string) => ({
  queryKey: ["panel", "parcel", "real-transaction", pnu] as const,
  queryFn: ({ signal }: { signal: AbortSignal }) => fetchRealTransaction(pnu, signal),
  // The lane's Gold is rebuilt once a day (root ADR-0371).
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
