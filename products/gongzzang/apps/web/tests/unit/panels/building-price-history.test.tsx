import { fireEvent, render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { expect, it } from "vitest";
import { ParcelBuildingsCard } from "@/components/panels/parcel/buildings";
import koMessages from "@/lib/i18n/ko.json";

it("displays every reference-date price for an unlinked unit", () => {
  render(
    <NextIntlClientProvider locale="ko" messages={koMessages}>
      <ParcelBuildingsCard
        entry={{ kind: "parcel", id: "9999900000100000000", view: "buildings" }}
        data={{
          buildings: [],
          unlinked_units: [
            {
              id: "00000000-0000-8000-8000-000000000001",
              parcel_id: "00000000-0000-8000-8000-000000000002",
              building_id: null,
              building_name: "",
              dong_name: "101동",
              ho_name: "101호",
              floor_label: "1층",
              exclusive_area_m2: null,
              usage_name: "",
              structure_name: "",
              official_price_history: [
                { base_date: "20100601", price_won: 35000000 },
                { base_date: "20100101", price_won: 36000000 },
              ],
            },
          ],
        }}
      />
    </NextIntlClientProvider>,
  );
  fireEvent.click(screen.getByText("기준일별 공시가격"));
  expect(screen.getByText("2010-06-01").getAttribute("datetime")).toBe("2010-06-01");
  expect(screen.getByText("2010-01-01").getAttribute("datetime")).toBe("2010-01-01");
  expect(screen.getByText("35,000,000 원")).toBeInTheDocument();
  expect(screen.getByText("36,000,000 원")).toBeInTheDocument();
});
