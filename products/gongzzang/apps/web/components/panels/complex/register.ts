// apps/web/components/panels/complex/register.ts
// Side-effect-only module: importing this file triggers defineKind('complex') once.
// No exports. `lib/panel/panel-renderer.tsx` imports it for registration, the same place the
// `parcel` and `listing` kinds are registered.

import { type ComplexInfo, fetchComplex } from "@/lib/api/complexes";
import { LAKEHOUSE_COMPLEX_ID_PATTERN } from "@/lib/identity/patterns";
import { defineKind, defineView } from "@/lib/panel/registry";
import { ComplexEmptyCard, ComplexErrorCard, ComplexLoadingSkeleton } from "./skeletons";
import { ComplexSummaryCard } from "./summary";

defineKind({
  kind: "complex",
  // The `complex` tile promotes `complex_id` — the lakehouse UUIDv5 — as its feature id, so that is
  // what a map click hands the stack. Pinning the version nibble is what keeps a Catalog `id`
  // (`Uuid::now_v7()`) from being accepted here and silently resolving to nothing.
  idPattern: LAKEHOUSE_COMPLEX_ID_PATTERN,
  views: {
    summary: defineView<"complex", ComplexInfo>({
      component: ComplexSummaryCard,
      fetcher: (id, signal) => fetchComplex(id, signal),
      // A designation record changes on the order of years, not minutes, and the panel is opened
      // repeatedly while panning. Same 5 minutes the parcel summary uses.
      staleTime: 5 * 60_000,
      links: [],
    }),
  },
  loadingComponent: ComplexLoadingSkeleton,
  errorComponent: ComplexErrorCard,
  emptyComponent: ComplexEmptyCard,
  // The backing route is `authenticated_user` in the traffic/auth registry, and the only entry
  // point — the /listings map — is auth-gated, so an unauthenticated open would fetch a 401.
  authGate: { required: true },
  i18nNamespace: "panels.complex",
  telemetryAttrs: (entry) => ({ lakehouse_complex_id: entry.id }),
});
