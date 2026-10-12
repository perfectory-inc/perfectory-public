import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { afterEach, expect, it, vi } from "vitest";
import { ParcelSummaryCard } from "@/components/panels/parcel/summary";
import { BY_PNU_LANES } from "@/lib/api/by-pnu-lanes.generated";
import type { EdgeParcelProfile } from "@/lib/api/parcels";
import koMessages from "@/lib/i18n/ko.json";

vi.mock("@/lib/panel/use-panel-stack", () => ({ usePanelStack: () => ({ push: vi.fn() }) }));

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";
const zoning = BY_PNU_LANES["parcel-zoning"];
const bundle: EdgeParcelProfile = {
  schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
  pnu: PNU,
  zonings: [{ zone_code: "UQA121", zone_name: "residential" }],
  price: { price_per_m2: 1000, base_year: 2026, base_month: 1 },
  characteristics: { land_category: "x" },
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function serveLanes(answer: (url: string) => Response) {
  const fetch = vi.fn(async (url: string) => answer(url));
  vi.stubGlobal("fetch", fetch);
  render(
    <QueryClientProvider
      client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
    >
      <NextIntlClientProvider locale="ko" messages={koMessages}>
        <ParcelSummaryCard entry={{ kind: "parcel", id: PNU, view: "summary" }} data={bundle} />
      </NextIntlClientProvider>
    </QueryClientProvider>,
  );
  return fetch;
}

it("draws the zoning its own lane serves over the bundle's", async () => {
  const fetch = serveLanes(
    () =>
      new Response(
        JSON.stringify({
          schema_version: zoning.documentSchemaVersion,
          pnu: PNU,
          source: { table: zoning.goldTable, iceberg_snapshot_id: "1" },
          zonings: [{ zone_code: "UQA410", zone_name: "industrial" }],
          attached_via: null,
        }),
      ),
  );
  expect(await screen.findByText("공업지역")).toBeInTheDocument();
  expect(screen.queryByText("주거지역")).toBeNull();
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(fetch.mock.calls[0]?.[0]).toMatch(new RegExp(`/parcels/zoning/by-pnu/${PNU}$`));
});

it("keeps the bundle's zoning while the lane is not published", async () => {
  const fetch = serveLanes(() => new Response(null, { status: 503 }));
  await waitFor(() => expect(fetch).toHaveBeenCalled());
  expect(screen.getByText("주거지역")).toBeInTheDocument();
  expect(screen.queryByText("불러오지 못했어요")).toBeNull();
});
