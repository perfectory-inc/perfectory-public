import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { beforeEach, expect, it, vi } from "vitest";
import { ParcelBuildingsCard } from "@/components/panels/parcel/buildings";
import type { BuildingsResponse } from "@/lib/api/buildings";
import koMessages from "@/lib/i18n/ko.json";

const PNU = "9999900000100000000";
const UNIT_ID = "00000000-0000-8000-8000-000000000001";
const fetchUnitPrices = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api/unit-price-edge", () => ({ fetchUnitPrices }));

const unit = {
  id: UNIT_ID,
  parcel_id: "00000000-0000-8000-8000-000000000002",
  building_id: null,
  building_name: "",
  dong_name: "101동",
  ho_name: "101호",
  floor_label: "1층",
  exclusive_area_m2: null,
  usage_name: "",
  structure_name: "",
};

const embedded = [{ base_date: "20090101", price_won: 30000000 }];

beforeEach(() => {
  fetchUnitPrices.mockReset();
});

// The price row is held while the prices load, so the summary is the one inside a details.
async function openPrices() {
  const summary = await waitFor(() => {
    const found = screen.getByText("기준일별 공시가격").closest("summary");
    if (!found) throw new Error("the prices have not arrived");
    return found;
  });
  fireEvent.click(summary);
}

function renderCard(data: BuildingsResponse = { buildings: [], unlinked_units: [unit] }) {
  render(
    <QueryClientProvider
      client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
    >
      <NextIntlClientProvider locale="ko" messages={koMessages}>
        <ParcelBuildingsCard entry={{ kind: "parcel", id: PNU, view: "buildings" }} data={data} />
      </NextIntlClientProvider>
    </QueryClientProvider>,
  );
}

it("displays every reference-date price the unit price lane serves for the unit", async () => {
  fetchUnitPrices.mockResolvedValue(
    new Map([
      [
        UNIT_ID,
        [
          { base_date: "20100601", price_won: 35000000 },
          { base_date: "20100101", price_won: 36000000 },
        ],
      ],
    ]),
  );
  renderCard();
  await openPrices();
  expect(fetchUnitPrices).toHaveBeenCalledWith(PNU, expect.anything());
  expect(screen.getByText("2010-06-01").getAttribute("datetime")).toBe("2010-06-01");
  expect(screen.getByText("2010-01-01").getAttribute("datetime")).toBe("2010-01-01");
  expect(screen.getByText("35,000,000 원")).toBeInTheDocument();
  expect(screen.getByText("36,000,000 원")).toBeInTheDocument();
});

it("shows no price for a unit the lane does not price", async () => {
  fetchUnitPrices.mockResolvedValue(new Map());
  renderCard();
  expect(await screen.findByText("101동 101호")).toBeInTheDocument();
  await waitFor(() => expect(screen.queryByText("기준일별 공시가격")).toBeNull());
});

it("draws a v2 document's own prices when the lane fails or has no document for the parcel", async () => {
  for (const lane of [
    () => fetchUnitPrices.mockRejectedValue(new Error("unit price edge fetch failed: 503")),
    () => fetchUnitPrices.mockResolvedValue(new Map()),
  ]) {
    lane();
    renderCard({ buildings: [], unlinked_units: [{ ...unit, official_price_history: embedded }] });
    await waitFor(() => expect(fetchUnitPrices).toHaveBeenCalled());
    await openPrices();
    expect(screen.getByText("30,000,000 원")).toBeInTheDocument();
    expect(screen.queryByText("공시가격을 불러오지 못했어요")).toBeNull();
    cleanup();
    fetchUnitPrices.mockReset();
  }
});

it("says the prices could not be loaded when the lane fails and the document has none", async () => {
  fetchUnitPrices.mockRejectedValue(new Error("unit price edge fetch failed: 503"));
  renderCard();
  expect(await screen.findByText("공시가격을 불러오지 못했어요")).toBeInTheDocument();
  expect(screen.queryByText("기준일별 공시가격")).toBeNull();
});

it("has the prices on their way before a building's units are opened", async () => {
  fetchUnitPrices.mockResolvedValue(new Map());
  renderCard({
    buildings: [
      {
        id: "00000000-0000-8000-8000-000000000003",
        name: "",
        purpose: null,
        total_area_m2: null,
        approved_at: null,
        units: [unit],
      },
    ],
    unlinked_units: [],
  });
  await waitFor(() => expect(fetchUnitPrices).toHaveBeenCalledWith(PNU, expect.anything()));
});

it("asks for no prices for a parcel without units", async () => {
  renderCard({
    buildings: [
      {
        id: "00000000-0000-8000-8000-000000000003",
        name: "",
        purpose: null,
        total_area_m2: null,
        approved_at: null,
        units: [],
      },
    ],
    unlinked_units: [],
  });
  expect(await screen.findByText("호실 보기")).toBeInTheDocument();
  expect(fetchUnitPrices).not.toHaveBeenCalled();
});

it("holds the price row while the prices are on their way", async () => {
  fetchUnitPrices.mockReturnValue(new Promise(() => {}));
  renderCard();
  await waitFor(() => expect(fetchUnitPrices).toHaveBeenCalled());
  const held = screen.getByText("기준일별 공시가격");
  expect(held.getAttribute("aria-hidden")).toBe("true");
  expect(held.closest("details")).toBeNull();
});

it("writes a unit's floor from the data's floor kind and number, not a server label", async () => {
  fetchUnitPrices.mockResolvedValue(new Map());
  const { floor_label: _label, ...laneUnit } = unit;
  renderCard({
    buildings: [],
    unlinked_units: [{ ...laneUnit, floor_kind: "basement", floor_number: 2 }],
  });
  expect(await screen.findByText(/지하 2층/)).toBeInTheDocument();
});
