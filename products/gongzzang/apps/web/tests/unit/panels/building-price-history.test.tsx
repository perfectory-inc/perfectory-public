import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { expect, it, vi } from "vitest";
import { ParcelBuildingsCard } from "@/components/panels/parcel/buildings";
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

function renderCard() {
  render(
    <QueryClientProvider
      client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
    >
      <NextIntlClientProvider locale="ko" messages={koMessages}>
        <ParcelBuildingsCard
          entry={{ kind: "parcel", id: PNU, view: "buildings" }}
          data={{ buildings: [], unlinked_units: [unit] }}
        />
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
  fireEvent.click(await screen.findByText("기준일별 공시가격"));
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
  expect(screen.queryByText("기준일별 공시가격")).toBeNull();
});
