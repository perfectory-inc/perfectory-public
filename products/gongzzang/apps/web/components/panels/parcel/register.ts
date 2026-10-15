// apps/web/components/panels/parcel/register.ts
// Side-effect-only module: importing this file triggers defineKind('parcel') once.
// No exports. T6 imports it from app/listings/page.tsx for registration.

import { type BuildingsResponse, fetchBuildings } from "@/lib/api/buildings";
import { type FloorsResponse, fetchFloors } from "@/lib/api/floors";
import { fetchParcel, type ParcelDocument } from "@/lib/api/parcels";
import { PNU_PATTERN } from "@/lib/identity/patterns";
import type { ListingsResponse } from "@/lib/listings/api";
import { defineKind, defineView } from "@/lib/panel/registry";
import { ParcelBuildingsCard } from "./buildings";
import { ParcelFloorsCard } from "./floors";
import { ParcelListingsCard } from "./listings";
import { fetchParcelListings } from "./queries";
import { ParcelEmptyCard, ParcelErrorCard, ParcelLoadingSkeleton } from "./skeletons";
import { ParcelSummaryCard } from "./summary";

defineKind({
  kind: "parcel",
  idPattern: PNU_PATTERN,
  views: {
    summary: defineView<"parcel", ParcelDocument>({
      component: ParcelSummaryCard,
      fetcher: (id, signal) => fetchParcel(id, signal),
      staleTime: 5 * 60_000,
      links: [],
    }),
    buildings: defineView<"parcel", BuildingsResponse>({
      component: ParcelBuildingsCard,
      fetcher: (id, signal) => fetchBuildings(id, signal),
      // The units' prices are one document per building (root ADR-0204), named by the building ids
      // this fetch returns, so the card asks them all at once when it arrives.
      staleTime: 5 * 60_000,
      links: [],
    }),
    floors: defineView<"parcel", FloorsResponse>({
      component: ParcelFloorsCard,
      fetcher: (id, signal) => fetchFloors(id, signal),
      staleTime: 5 * 60_000,
      links: [],
    }),
    listings: defineView<"parcel", ListingsResponse>({
      component: ParcelListingsCard,
      fetcher: (id, signal) => fetchParcelListings(id, signal),
      staleTime: 60_000,
      links: [],
    }),
  },
  loadingComponent: ParcelLoadingSkeleton,
  errorComponent: ParcelErrorCard,
  emptyComponent: ParcelEmptyCard,
  // parcel data flows through V-World which is auth-gated upstream + the
  // /listings map (only entry point) is auth-gated, so true is the safe pick.
  // Spec § 6 example shows false; we diverge consciously here.
  authGate: { required: true },
  i18nNamespace: "panels.parcel",
  telemetryAttrs: (entry) => ({ pnu: entry.id }),
});
