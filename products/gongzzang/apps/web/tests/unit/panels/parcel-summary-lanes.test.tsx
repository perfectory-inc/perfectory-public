import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { afterEach, expect, it, vi } from "vitest";
import "@/components/panels/parcel/register";
import { BY_PNU_LANES } from "@/lib/api/by-pnu-lanes.generated";
import { SUMMARY_LANES } from "@/lib/api/parcels";
import koMessages from "@/lib/i18n/ko.json";
import { PanelEntryView } from "@/lib/panel/panel-entry-view";

vi.mock("@/lib/panel/use-panel-stack", () => ({
  usePanelStack: () => ({ push: vi.fn(), pop: vi.fn() }),
}));
vi.mock("@/lib/panel/telemetry", () => ({ reportPanelOpened: vi.fn() }));

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";
const zoning = BY_PNU_LANES["parcel-zoning"];
const bundle = {
  schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
  pnu: PNU,
  zonings: [{ zone_code: "UQA121", zone_name: "residential" }],
  price: { price_per_m2: 1000, base_year: 2026, base_month: 1 },
  characteristics: { land_category: "x" },
};
const zoningDocument = {
  schema_version: zoning.documentSchemaVersion,
  pnu: PNU,
  source: { table: zoning.goldTable, iceberg_snapshot_id: "1" },
  zonings: [{ zone_code: "UQA410", zone_name: "industrial" }],
  attached_via: null,
};
const json = (body: unknown) => new Response(JSON.stringify(body));

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

/**
 * Opens the parcel summary panel. The bundle answers `bundleAnswer`, the zoning lane `laneAnswer`,
 * and the other summary lanes 503 (not published yet).
 */
function openSummary(bundleAnswer: () => Response, laneAnswer: () => Response) {
  const fetch = vi.fn(async (url: string) => {
    const path = new URL(url).pathname;
    if (path.startsWith(zoning.path)) return laneAnswer();
    if (path.startsWith("/parcels/by-pnu/")) return bundleAnswer();
    return new Response(null, { status: 503 });
  });
  vi.stubGlobal("fetch", fetch);
  render(
    <NextIntlClientProvider locale="ko" messages={koMessages}>
      <QueryClientProvider
        client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
      >
        <PanelEntryView entry={{ kind: "parcel", id: PNU, view: "summary" }} depth={1} />
      </QueryClientProvider>
    </NextIntlClientProvider>,
  );
  return fetch;
}

it("draws the zoning its own lane serves over the bundle's, every lane read with the bundle", async () => {
  const fetch = openSummary(
    () => json(bundle),
    () => json(zoningDocument),
  );
  expect(await screen.findByText("공업지역")).toBeInTheDocument();
  expect(screen.queryByText("주거지역")).toBeNull();
  expect(fetch.mock.calls.map(([url]) => new URL(url).pathname).sort()).toEqual(
    [
      `/parcels/by-pnu/${PNU}`,
      ...Object.values(SUMMARY_LANES).map((lane) => `${BY_PNU_LANES[lane].path}${PNU}`),
    ].sort(),
  );
  // The price and characteristics lanes are not published: the bundle answers for them.
  expect(screen.getByText("1,000 원/㎡")).toBeInTheDocument();
});

it("keeps the bundle's zoning and asks once while the lane is not published (503)", async () => {
  const fetch = openSummary(
    () => json(bundle),
    () => new Response(null, { status: 503 }),
  );
  expect(await screen.findByText("주거지역")).toBeInTheDocument();
  expect(screen.queryByText("불러오지 못했어요")).toBeNull();
  // Each lane is asked once: none of the three 503s is retried.
  expect(fetch).toHaveBeenCalledTimes(1 + Object.keys(SUMMARY_LANES).length);
});

it("draws what the lanes serve for a parcel the bundle does not serve", async () => {
  openSummary(
    () => new Response(null, { status: 404 }),
    () => json(zoningDocument),
  );
  expect(await screen.findByText("공업지역")).toBeInTheDocument();
  // The unpublished kinds have no source left: they are drawn as failed, not as none.
  expect(screen.getAllByText("불러오지 못했어요")).toHaveLength(2);
});
