import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("@/lib/panel/use-panel-stack", () => ({ usePanelStack: () => ({ push: vi.fn() }) }));

import { ParcelSummaryCard } from "@/components/panels/parcel/summary";
import { type ParcelInfo, toParcelInfo } from "@/lib/api/parcels";
import koMessages from "@/lib/i18n/ko.json";

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100000000";
const entry = { kind: "parcel", id: PNU, view: "summary" } as const;

function renderCard(data: ParcelInfo): void {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <NextIntlClientProvider locale="ko" messages={koMessages} onError={() => {}}>
      {children}
    </NextIntlClientProvider>
  );
  // The zone row as a lane that answered draws it (root ADR-0193 §5); the other rows are empty.
  const states = { zonings: "ok", price: "empty", characteristics: "empty" } as const;
  render(<ParcelSummaryCard entry={entry} data={{ info: data, states }} />, { wrapper });
}

describe("ParcelSummaryCard — zoning", () => {
  it("shows the source's zone name as given, not a message key built from it", () => {
    renderCard(
      toParcelInfo({
        schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
        pnu: PNU,
        zonings: [{ zone_code: "UQA122", zone_name: "제2종일반주거지역" }],
      }),
    );

    expect(screen.getByText("제2종일반주거지역")).toBeTruthy();
    expect(screen.queryByText(/zoningValues/)).toBeNull();
  });
});
