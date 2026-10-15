import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { expect, it, vi } from "vitest";
import { NO_BUILDING } from "@/components/panels/parcel/unit-prices";
import koMessages from "@/lib/i18n/ko.json";
import { PanelEntryView } from "@/lib/panel/panel-entry-view";
import "@/components/panels/parcel/register";

const PNU = "9999900000100000000";
const fetchBuildings = vi.hoisted(() => vi.fn());
const fetchUnitPrices = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api/buildings", () => ({ fetchBuildings }));
vi.mock("@/lib/api/unit-price-edge", () => ({ fetchUnitPrices }));
vi.mock("@/lib/panel/telemetry", () => ({ reportPanelOpened: vi.fn() }));
vi.mock("@/lib/panel/use-panel-stack", () => ({ usePanelStack: () => ({ pop: () => {} }) }));

const unit = (id: string, buildingId: string | null) => ({
  id,
  parcel_id: "00000000-0000-8000-8000-000000000009",
  building_id: buildingId,
  building_name: "",
  dong_name: "",
  ho_name: "101호",
  floor_label: "1층",
  exclusive_area_m2: null,
  usage_name: "",
  structure_name: "",
});
const building = (id: string, units: ReturnType<typeof unit>[]) => ({
  id,
  name: "",
  purpose: null,
  total_area_m2: null,
  approved_at: null,
  units,
});
const A = "00000000-0000-8000-8000-000000000001";
const B = "00000000-0000-8000-8000-000000000002";
const EMPTY = "00000000-0000-8000-8000-000000000003";

it("asks each building's prices, and the unlinked units', all at once when the buildings arrive (root ADR-0204)", async () => {
  fetchBuildings.mockResolvedValue({
    buildings: [
      building(A, [unit("00000000-0000-8000-8000-00000000000a", A)]),
      building(B, [unit("00000000-0000-8000-8000-00000000000b", B)]),
      building(EMPTY, []),
    ],
    unlinked_units: [unit("00000000-0000-8000-8000-00000000000c", null)],
  });
  fetchUnitPrices.mockReturnValue(new Promise(() => {}));
  render(
    <QueryClientProvider client={new QueryClient()}>
      <NextIntlClientProvider locale="ko" messages={koMessages}>
        <PanelEntryView entry={{ kind: "parcel", id: PNU, view: "buildings" }} depth={1} />
      </NextIntlClientProvider>
    </QueryClientProvider>,
  );
  await waitFor(() => expect(fetchUnitPrices).toHaveBeenCalledTimes(3));
  for (const id of [A, B, NO_BUILDING]) {
    expect(fetchUnitPrices).toHaveBeenCalledWith(PNU, id, expect.anything());
  }
  expect(fetchBuildings).toHaveBeenCalledWith(PNU, expect.anything());
});
