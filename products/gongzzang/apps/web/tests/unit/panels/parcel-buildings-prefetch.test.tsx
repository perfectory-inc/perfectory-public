import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { expect, it, vi } from "vitest";
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

it("asks for the units' prices together with the buildings, not after them", async () => {
  fetchBuildings.mockReturnValue(new Promise(() => {}));
  fetchUnitPrices.mockReturnValue(new Promise(() => {}));
  render(
    <QueryClientProvider client={new QueryClient()}>
      <NextIntlClientProvider locale="ko" messages={koMessages}>
        <PanelEntryView entry={{ kind: "parcel", id: PNU, view: "buildings" }} depth={1} />
      </NextIntlClientProvider>
    </QueryClientProvider>,
  );
  await waitFor(() => expect(fetchUnitPrices).toHaveBeenCalledWith(PNU, expect.anything()));
  expect(fetchBuildings).toHaveBeenCalledWith(PNU, expect.anything());
});
