import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("@/lib/panel/use-panel-stack", () => ({ usePanelStack: () => ({ push: vi.fn() }) }));

const reads = vi.hoisted(() => ({
  buildings: { buildings: [] as unknown[], unlinked_units: [] as unknown[] },
  listings: { listings: [] as unknown[], total: 0 },
  publicAuction: [] as unknown[],
  ownership: null as unknown,
}));
vi.mock("@/components/panels/parcel/queries", () => ({
  parcelBuildingsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "buildings", pnu],
    queryFn: async () => reads.buildings,
  }),
  parcelListingsQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "listings", pnu],
    queryFn: async () => reads.listings,
  }),
  parcelPublicAuctionQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "public-auction", pnu],
    queryFn: async () => reads.publicAuction,
  }),
  parcelOwnershipQuery: (pnu: string) => ({
    queryKey: ["panel", "parcel", "ownership", pnu],
    queryFn: async () => reads.ownership,
  }),
}));

import { ParcelSummaryCard } from "@/components/panels/parcel/summary";
import { type ParcelDocument, parseParcelDocument } from "@/lib/api/parcels";
import koMessages from "@/lib/i18n/ko.json";

// Repository-reserved synthetic PNU (99999 range, public-fixture-safety guard).
const PNU = "9999900000100120003";
const entry = { kind: "parcel", id: PNU, view: "summary" } as const;

function parcel(extra: Record<string, unknown> = {}): ParcelDocument {
  return parseParcelDocument({
    schema_version: "foundation-platform.parcel_by_pnu_profile.v1",
    pnu: PNU,
    ...extra,
  });
}

function renderPanel(data: ParcelDocument): void {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={client}>
      <NextIntlClientProvider locale="ko" messages={koMessages} onError={() => {}}>
        {children}
      </NextIntlClientProvider>
    </QueryClientProvider>
  );
  render(<ParcelSummaryCard entry={entry} data={data} />, { wrapper });
}

const anchors = () =>
  within(screen.queryByRole("navigation", { name: "구획" }) ?? document.createElement("nav"))
    .queryAllByRole("button")
    .map((button) => button.textContent);

describe("parcel panel header", () => {
  it("names the legal dong from the document and spells the lot from the PNU", () => {
    renderPanel(parcel({ legal_dong: { name: "가가시 나나구 다다동", lowest_name: "다다동" } }));
    expect(screen.getByText("가가시 나나구 다다동")).toBeTruthy();
    expect(screen.getByRole("heading", { level: 2 }).textContent).toBe("다다동 12-3");
  });

  it("shows the legal dong code, never an invented name, until the document names it", () => {
    renderPanel(parcel());
    expect(screen.getByText("법정동코드 9999900000")).toBeTruthy();
    expect(screen.getByRole("heading", { level: 2 }).textContent).toBe("12-3");
  });
});

describe("parcel panel sections", () => {
  it("shows the source's zone names with 포함/저촉, and a 규제 anchor for them", async () => {
    renderPanel(
      parcel({
        zonings: [
          { zone_code: "UQA122", zone_name: "제2종일반주거지역", inclusion_code: "1" },
          { zone_code: "UQA220", zone_name: "일반상업지역", inclusion_code: "2" },
        ],
      }),
    );
    expect(screen.getAllByText("제2종일반주거지역").length).toBeGreaterThan(0);
    expect(screen.getByText("저촉")).toBeTruthy();
    await waitFor(() => expect(anchors()).toEqual(["토지", "규제"]));
  });

  it("never shows a code where the source's name is missing", () => {
    renderPanel(parcel({ forest_ledger: { ownership_kind: "06", co_owner_count: 2 } }));
    expect(screen.queryByText("06")).toBeNull();
    expect(screen.getByText("2인")).toBeTruthy();
  });

  it("shows the 토지소유 lane's owners in the source's words instead of the forest ledger", async () => {
    reads.ownership = {
      owners: [
        {
          co_owner_serial_no: "000001",
          ownership_kind_name: "법인",
          residence_kind_name: "관외",
          ownership_change_reason: "소유권이전",
          ownership_changed_on: "2019-11-26",
        },
        {
          co_owner_serial_no: "000002",
          ownership_kind_name: "법인",
          residence_kind_name: "관외",
          ownership_change_reason: "소유권이전",
          ownership_changed_on: "2024-01-02",
        },
      ],
      unit_owner_total: 0,
      unit_owner_kinds: [],
      base_year_month: "2024-12",
      ownerTotal: 2,
    };
    try {
      renderPanel(
        parcel({ forest_ledger: { ownership_kind_name: "임야소유", co_owner_count: 9 } }),
      );
      expect(await screen.findByText("법인 2")).toBeTruthy();
      expect(screen.getByText("2024-01-02 소유권이전")).toBeTruthy();
      expect(screen.getByText("토지소유정보 · 2024-12 기준")).toBeTruthy();
      expect(screen.queryByText("임야소유")).toBeNull();
    } finally {
      reads.ownership = null;
    }
  });

  it("adds a 건물 anchor once the parcel's buildings are read", async () => {
    reads.buildings = {
      buildings: [
        {
          id: "b1",
          name: "",
          purpose: "업무시설",
          structure: "철골철근콘크리트구조",
          total_area_m2: 1200,
          stories: 5,
          below_ground_floors: 1,
          built_year: 2001,
          approved_at: null,
          units: [],
        },
      ],
      unlinked_units: [],
    };
    try {
      renderPanel(parcel());
      await waitFor(() => expect(anchors()).toEqual(["토지", "건물"]));
      expect(screen.getByText("업무시설")).toBeTruthy();
      expect(screen.getByText(/지상 5층 \/ 지하 1층/)).toBeTruthy();
    } finally {
      reads.buildings = { buildings: [], unlinked_units: [] };
    }
  });
});

describe("parcel panel categories", () => {
  it("shows no category bar while 필지정보 is the only category", async () => {
    renderPanel(parcel());
    await waitFor(() => expect(screen.getByText("12-3")).toBeTruthy());
    expect(screen.queryByRole("navigation", { name: "필지 정보 갈래" })).toBeNull();
  });

  it("adds 매물 with its count when the parcel has listings", async () => {
    reads.listings = { listings: [], total: 3 };
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      expect(
        within(bar)
          .getAllByRole("button")
          .map((b) => b.textContent),
      ).toEqual(["필지정보", "매물 3"]);
    } finally {
      reads.listings = { listings: [], total: 0 };
    }
  });

  it("adds 공매 with its count when the parcel has public auctions", async () => {
    reads.publicAuction = [{ item_name: "테스트 물건" }, { item_name: "테스트 물건" }];
    try {
      renderPanel(parcel());
      const bar = await screen.findByRole("navigation", { name: "필지 정보 갈래" });
      expect(
        within(bar)
          .getAllByRole("button")
          .map((b) => b.textContent),
      ).toEqual(["필지정보", "공매 2"]);
    } finally {
      reads.publicAuction = [];
    }
  });
});
