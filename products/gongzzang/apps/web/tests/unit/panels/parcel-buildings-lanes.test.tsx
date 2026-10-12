import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { afterEach, expect, it, vi } from "vitest";
import "@/components/panels/parcel/register";
import { BY_PNU_LANES } from "@/lib/api/by-pnu-lanes.generated";
import koMessages from "@/lib/i18n/ko.json";
import { PanelEntryView } from "@/lib/panel/panel-entry-view";

vi.mock("@/lib/panel/use-panel-stack", () => ({
  usePanelStack: () => ({ push: vi.fn(), pop: vi.fn() }),
}));
vi.mock("@/lib/panel/telemetry", () => ({ reportPanelOpened: vi.fn() }));

// Repository-reserved synthetic PNU and ids.
const PNU = "9999900000100000000";
const parcelId = "00000000-0000-8000-8000-000000000000";
const unit = (id: string, ho: string) => ({
  id,
  parcel_id: parcelId,
  building_id: null,
  building_name: "",
  dong_name: "101동",
  ho_name: ho,
  floor_label: "1층",
  exclusive_area_m2: null,
  usage_name: "",
  structure_name: "",
});
const bundle = {
  schema_version: "foundation-platform.building_by_pnu_profile.v3",
  pnu: PNU,
  source: { table: "gold.building_panel", iceberg_snapshot_id: "1" },
  buildings: [],
  unlinked_units: [unit("00000000-0000-8000-8000-000000000003", "번들호")],
};
const units = BY_PNU_LANES["building-units"];
const unitsDocument = {
  schema_version: units.documentSchemaVersion,
  pnu: PNU,
  source: { table: units.goldTable, iceberg_snapshot_id: "2" },
  units: [],
  unlinked_units: [unit("00000000-0000-8000-8000-000000000004", "레인호")],
};
const json = (body: unknown) => new Response(JSON.stringify(body));
const BUILDING_LANES = Object.values(BY_PNU_LANES).filter((lane) => lane.host === "building");

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

/** Opens the parcel's buildings view: the bundle answers `bundleAnswer`, the units lane `unitsAnswer`,
 * every other building lane (and the unit price lane) 503, not published yet. */
function openBuildings(bundleAnswer: () => Response, unitsAnswer: () => Response) {
  const fetch = vi.fn(async (url: string) => {
    const path = new URL(url).pathname;
    if (path.startsWith(units.path)) return unitsAnswer();
    if (path.startsWith("/buildings/by-pnu/")) return bundleAnswer();
    return new Response(null, { status: 503 });
  });
  vi.stubGlobal("fetch", fetch);
  render(
    <NextIntlClientProvider locale="ko" messages={koMessages}>
      <QueryClientProvider
        client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
      >
        <PanelEntryView entry={{ kind: "parcel", id: PNU, view: "buildings" }} depth={1} />
      </QueryClientProvider>
    </NextIntlClientProvider>,
  );
  return fetch;
}

const buildingCalls = (fetch: ReturnType<typeof vi.fn>) =>
  fetch.mock.calls
    .map(([url]) => new URL(url as string).pathname)
    .filter((path) => path.startsWith("/buildings/"))
    .sort();

it("draws the units their own lane serves over the bundle's, every building lane read with the bundle", async () => {
  const fetch = openBuildings(
    () => json(bundle),
    () => json(unitsDocument),
  );
  expect(await screen.findByText("101동 레인호")).toBeInTheDocument();
  expect(screen.queryByText("101동 번들호")).toBeNull();
  expect(buildingCalls(fetch)).toEqual(
    [`/buildings/by-pnu/${PNU}`, ...BUILDING_LANES.map((lane) => `${lane.path}${PNU}`)].sort(),
  );
});

it("keeps the bundle's units and asks each lane once while the lanes are not published (503)", async () => {
  const fetch = openBuildings(
    () => json(bundle),
    () => new Response(null, { status: 503 }),
  );
  expect(await screen.findByText("101동 번들호")).toBeInTheDocument();
  expect(buildingCalls(fetch)).toHaveLength(1 + BUILDING_LANES.length);
});

it("draws the units lane for a parcel the bundle does not serve", async () => {
  openBuildings(
    () => new Response(null, { status: 404 }),
    () => json(unitsDocument),
  );
  expect(await screen.findByText("101동 레인호")).toBeInTheDocument();
});
